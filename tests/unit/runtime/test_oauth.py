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
from datasluice.errors.catalog import BudgetExhaustedError, CatalogValidationError, UnauthenticatedError
from datasluice.runtime.clients import SyncCatalogClient
from datasluice.runtime.events import EventEmitter, ListSink
from datasluice.runtime.oauth import (
    AuthorizationCodeFlow,
    ClientCredentialsFlow,
    OAuthCredential,
    RefreshingCredentialProvider,
)
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.unit.runtime._fixtures import _envelope, _guard, _profile, _request


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

    def close(self) -> None:
        pass


def _flow(*, scopes: frozenset[str] | None = None, redirect_uri: str | None = None) -> OAuthFlow:
    return OAuthFlow(
        authorization_url="https://auth.example.test/authorize",
        token_url="https://auth.example.test/token",
        client_id="client-id",
        redirect_uri=redirect_uri,
        scopes=frozenset(scopes or frozenset()),
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
    assert 110 <= credential.expires_in <= 120
    assert transport.requests[0].body is not None
    assert parse_qs(transport.requests[0].body.decode()) == {
        "grant_type": ["client_credentials"],
        "client_id": ["client-id"],
        "client_secret": ["client-secret"],
    }


def test_client_credentials_send_configured_scopes_sorted() -> None:
    transport = _SyncTransport(_token_response())

    ClientCredentialsFlow(_flow(scopes=frozenset({"write", "read"})), SecretValue("client-secret"), transport).fetch()

    assert transport.requests[0].body is not None
    assert parse_qs(transport.requests[0].body.decode())["scope"] == ["read write"]


def test_client_credentials_async_uses_async_transport() -> None:
    transport = _AsyncTransport(_token_response())

    credential = asyncio.run(ClientCredentialsFlow(_flow(), SecretValue("client-secret"), transport).fetch_async())

    assert credential.access_token.reveal() == "access-token"
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (b"not-json", "invalid JSON"),
        (json.dumps({"expires_in": 120}).encode(), "access token"),
        (json.dumps({"access_token": "access-token", "expires_in": "soon"}).encode(), "invalid expiry"),
        (json.dumps({"access_token": "access-token", "expires_in": -5}).encode(), "invalid expiry"),
    ],
)
def test_malformed_token_responses_are_rejected(body: bytes, message: str) -> None:
    transport = _SyncTransport(RuntimeResponse(200, {}, body))

    with pytest.raises(CatalogValidationError, match=message):
        ClientCredentialsFlow(_flow(), SecretValue("client-secret"), transport).fetch()


def test_refresh_provider_requires_a_refresh_capable_credential() -> None:
    credential = OAuthCredential(SecretValue("access-token"), None, None)

    with pytest.raises(ValueError, match="refresh-capable"):
        RefreshingCredentialProvider(_flow(), credential, _SyncTransport(_token_response()))


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
    assert query["response_type"] == ["code"]
    assert query["state"] == ["state"]
    assert "plain" not in authorization_url


def test_redirect_uris_must_be_sanitized_https() -> None:
    with pytest.raises(ValueError, match="sanitized HTTPS"):
        _flow(redirect_uri="http://app.example.test/callback")


def test_authorization_url_and_exchange_carry_the_registered_redirect_uri() -> None:
    transport = _SyncTransport(_token_response())
    flow = AuthorizationCodeFlow(_flow(redirect_uri="https://app.example.test/callback"), transport)

    query = parse_qs(urlsplit(flow.authorization_url(state="state")).query)
    credential = flow.exchange("authorization-code")

    assert transport.requests[0].body is not None
    exchange_body = parse_qs(transport.requests[0].body.decode())

    assert query["redirect_uri"] == ["https://app.example.test/callback"]
    assert exchange_body["redirect_uri"] == ["https://app.example.test/callback"]
    assert credential.access_token.reveal() == "access-token"


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


def test_token_errors_surface_status_and_rfc6749_error_code_without_body_values() -> None:
    transport = _SyncTransport(
        RuntimeResponse(400, {}, json.dumps({"error": "invalid_grant", "error_description": "client-secret"}).encode())
    )

    with pytest.raises(CatalogValidationError) as exc_info:
        ClientCredentialsFlow(_flow(), SecretValue("client-secret"), transport).fetch()

    message = str(exc_info.value)
    assert "400" in message
    assert "invalid_grant" in message
    assert "client-secret" not in message


def test_unauthorized_token_responses_map_to_unauthenticated_error() -> None:
    transport = _SyncTransport(RuntimeResponse(401, {}, b'{"error": "invalid_client"}'))

    with pytest.raises(UnauthenticatedError) as exc_info:
        ClientCredentialsFlow(_flow(), SecretValue("client-secret"), transport).fetch()

    message = str(exc_info.value)
    assert "401" in message
    assert "invalid_client" in message


def test_unknown_error_codes_are_not_echoed_into_messages() -> None:
    transport = _SyncTransport(RuntimeResponse(400, {}, b'{"error": "attacker-controlled-code"}'))

    with pytest.raises(CatalogValidationError) as exc_info:
        ClientCredentialsFlow(_flow(), SecretValue("client-secret"), transport).fetch()

    message = str(exc_info.value)
    assert "400" in message
    assert "attacker-controlled-code" not in message


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


def test_async_refresh_rebinds_its_lock_across_event_loops() -> None:
    transport = _AsyncTransport(_token_response())
    provider = RefreshingCredentialProvider(_flow(), _expired_credential(), transport)

    first = asyncio.run(provider.resolve_async())
    second = asyncio.run(provider.resolve_async())

    assert first.access_token.reveal() == "access-token"
    assert second.access_token.reveal() == "access-token"


def test_mixed_sync_and_async_resolution_share_exclusion() -> None:
    transport = _AsyncTransport(_token_response())
    fresh = OAuthCredential(SecretValue("fresh-access-token"), time() + 3600, SecretValue("refresh-token"))
    provider = RefreshingCredentialProvider(_flow(), fresh, transport)

    synced = provider.resolve()
    async_resolved = asyncio.run(provider.resolve_async())

    assert synced.access_token.reveal() == "fresh-access-token"
    assert async_resolved.access_token.reveal() == "fresh-access-token"
    assert transport.requests == []


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

    SyncCatalogClient(transport, _profile(), credentials=provider).get(_request(), _guard())

    assert len(transport.requests) == 2
    assert transport.requests[-1].headers["Authorization"] == "Bearer access-token"
