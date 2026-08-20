"""OAuth runtime flow tests."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import threading
from time import time
from urllib.parse import parse_qs, urlsplit

import pytest

from datasluice.domain.catalog.auth import OAuthFlow, SecretValue
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import BudgetExhaustedError, CatalogValidationError
from datasluice.runtime.clients import SyncCatalogClient
from datasluice.runtime.events import EventEmitter, ListSink
from datasluice.runtime.oauth import (
    AuthorizationCodeFlow,
    ClientCredentialsFlow,
    OAuthCredential,
    RefreshingCredentialProvider,
)
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.unit.runtime.test_clients_sync import _envelope, _profile, _request


class _SyncTransport:
    def __init__(self, response: RuntimeResponse) -> None:
        self.response = response
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return self.response

    def close(self) -> None:
        pass


class _AsyncTransport:
    def __init__(self, response: RuntimeResponse) -> None:
        self.response = response
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return self.response

    async def aclose(self) -> None:
        pass


def _flow() -> OAuthFlow:
    return OAuthFlow.authorization_code(
        "https://auth.example.test/authorize", "https://auth.example.test/token", "client-id"
    )


def _token_response() -> RuntimeResponse:
    return RuntimeResponse(
        200,
        {"content-type": "application/json"},
        json.dumps({"access_token": "access-token", "expires_in": 120}).encode(),
    )


def test_client_credentials_posts_form_and_wraps_access_token() -> None:
    transport = _SyncTransport(_token_response())

    credential = ClientCredentialsFlow(_flow(), SecretValue("client-secret"), transport).fetch()

    assert credential.access_token.reveal() == "access-token"
    assert credential.expires_in is not None
    assert 119 <= credential.expires_in <= 120
    assert transport.requests[0].body is not None
    assert parse_qs(transport.requests[0].body.decode()) == {
        "grant_type": ["client_credentials"],
        "client_id": ["client-id"],
        "client_secret": ["client-secret"],
    }


def test_client_credentials_async_uses_async_transport() -> None:
    transport = _AsyncTransport(_token_response())

    credential = asyncio.run(ClientCredentialsFlow(_flow(), SecretValue("client-secret"), transport).fetch_async())

    assert credential.access_token.reveal() == "access-token"
    assert len(transport.requests) == 1


def test_authorization_code_uses_rfc7636_s256_only() -> None:
    transport = _SyncTransport(_token_response())
    flow = AuthorizationCodeFlow(_flow(), transport)

    authorization_url = flow.authorization_url(state="state")
    query = parse_qs(urlsplit(authorization_url).query)
    verifier = flow.verifier
    expected = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()

    assert 43 <= len(verifier) <= 128
    assert query["code_challenge"] == [expected]
    assert query["code_challenge_method"] == ["S256"]
    assert "plain" not in authorization_url


def test_authorization_code_exchanges_verifier() -> None:
    transport = _SyncTransport(_token_response())
    flow = AuthorizationCodeFlow(_flow(), transport)

    credential = flow.exchange("authorization-code")

    assert transport.requests[0].body is not None
    body = parse_qs(transport.requests[0].body.decode())
    assert credential.access_token.reveal() == "access-token"
    assert body["grant_type"] == ["authorization_code"]
    assert body["code"] == ["authorization-code"]
    assert body["code_verifier"] == [flow.verifier]


def test_domain_rejects_non_https_token_endpoint() -> None:
    with pytest.raises(ValueError, match="sanitized HTTPS"):
        OAuthFlow.authorization_code(
            "https://auth.example.test/authorize", "http://auth.example.test/token", "client-id"
        )


def test_token_errors_are_typed_and_redacted() -> None:
    transport = _SyncTransport(RuntimeResponse(400, {}, b'{"error_description":"client-secret access-token"}'))

    with pytest.raises(CatalogValidationError) as exc_info:
        ClientCredentialsFlow(_flow(), SecretValue("client-secret"), transport).fetch()

    assert "client-secret" not in str(exc_info.value)
    assert "access-token" not in str(exc_info.value)


def _expired_credential() -> OAuthCredential:
    return OAuthCredential(SecretValue("expired-access-token"), time() - 1, SecretValue("refresh-token"))


def test_refreshes_once_through_the_runtime_transport() -> None:
    transport = _SyncTransport(_token_response())
    provider = RefreshingCredentialProvider(_flow(), _expired_credential(), transport)

    credential = provider.resolve()

    assert credential.access_token.reveal() == "access-token"
    assert len(transport.requests) == 1
    assert transport.requests[0].body is not None
    assert parse_qs(transport.requests[0].body.decode())["grant_type"] == ["refresh_token"]


def test_sync_refresh_is_single_flight() -> None:
    transport = _SyncTransport(_token_response())
    provider = RefreshingCredentialProvider(_flow(), _expired_credential(), transport)
    threads = [threading.Thread(target=provider.resolve) for _ in range(8)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(transport.requests) == 1


def test_async_refresh_uses_async_transport_and_is_single_flight() -> None:
    transport = _AsyncTransport(_token_response())
    provider = RefreshingCredentialProvider(_flow(), _expired_credential(), transport)

    async def resolve_all() -> list[OAuthCredential]:
        return await asyncio.gather(*(provider.resolve_async() for _ in range(8)))

    results = asyncio.run(resolve_all())

    assert all(result.access_token.reveal() == "access-token" for result in results)
    assert len(transport.requests) == 1


def test_failed_refresh_emits_a_redacted_event() -> None:
    sink = ListSink()
    transport = _SyncTransport(RuntimeResponse(400, {}, b'{"error":"refresh-token"}'))
    provider = RefreshingCredentialProvider(
        _flow(), _expired_credential(), transport, emitter=EventEmitter(sinks=(sink,))
    )

    with pytest.raises(CatalogValidationError):
        provider.resolve()

    assert sink.events[-1].outcome == "failed"
    assert "refresh-token" not in str(sink.events[-1].to_dict())


def test_refresh_counts_against_its_runtime_budget() -> None:
    now = [0.0]

    class StallingTransport(_SyncTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            now[0] = 1.0
            return super().send(request)

    provider = RefreshingCredentialProvider(
        _flow(),
        _expired_credential(),
        StallingTransport(_token_response()),
        budget=TimeBudget(connect=0.1, read=0.1, write=0.1, total=0.1),
        monotonic_clock=lambda: now[0],
    )

    with pytest.raises(BudgetExhaustedError):
        provider.resolve()


def test_sync_client_uses_a_refreshed_oauth_credential_on_dispatch() -> None:
    class Transport(_SyncTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            self.requests.append(request)
            return _token_response() if request.url == _flow().token_url else RuntimeResponse(200, {}, _envelope())

    transport = Transport(_token_response())
    provider = RefreshingCredentialProvider(_flow(), _expired_credential(), transport)

    SyncCatalogClient(transport, _profile(), credentials=provider).get(_request())

    assert len(transport.requests) == 2
    assert transport.requests[-1].headers["Authorization"] == "Bearer access-token"
