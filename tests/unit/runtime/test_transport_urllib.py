"""Stdlib catalog transport safety tests."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from http.client import IncompleteRead
from typing import Any, cast
from urllib.request import Request

import pytest

from datasluice.domain import CredentialScope
from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.transport.base import RuntimeRequest, TransportFailure
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from tests.helpers.catalog_transport import AsyncLoopbackTransport, SyncLoopbackTransport
from tests.helpers.http_server import MockResponse, start_test_server


class _FakeResponse:
    def __init__(self, status: int, headers: Mapping[str, str], body: bytes = b"") -> None:
        self.status = status
        self.headers = headers
        self._body = body
        self.closed = False

    def read(self) -> bytes:
        return self._body

    def close(self) -> None:
        self.closed = True


class _RecordingOpener:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.requests: list[Request] = []
        self.timeouts: list[float] = []
        self._responses = responses

    def open(self, request: Request, *, timeout: float) -> _FakeResponse:
        self.requests.append(request)
        self.timeouts.append(timeout)
        return self._responses[len(self.requests) - 1]


class _LoopingRedirectOpener:
    def __init__(self) -> None:
        self.count = 0

    def open(self, request: Request, *, timeout: float) -> _FakeResponse:
        del request, timeout
        self.count += 1
        return _FakeResponse(302, {"Location": f"https://example.test/loop/{self.count}"})


def test_urllib_transport_uses_verified_tls_by_default() -> None:
    assert UrllibCatalogTransport()._tls_policy.verify


def test_urllib_uses_read_budget_and_closes_completed_responses() -> None:
    response = _FakeResponse(200, {})
    opener = _RecordingOpener([response])
    transport = UrllibCatalogTransport(budget=TimeBudget(connect=1, read=7, write=2, total=9))
    cast(Any, transport)._opener = opener

    transport.send(RuntimeRequest("GET", "https://example.test/data"))

    assert opener.timeouts == [7]
    assert response.closed


def test_urllib_wraps_incomplete_response_reads_and_closes_response() -> None:
    class _IncompleteResponse(_FakeResponse):
        def read(self) -> bytes:
            raise IncompleteRead(b"partial", 10)

    response = _IncompleteResponse(200, {})
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = _RecordingOpener([response])

    with pytest.raises(TransportFailure, match="mid-response"):
        transport.send(RuntimeRequest("GET", "https://example.test/data"))

    assert response.closed


def test_tls_policy_rejects_unscoped_disablement() -> None:
    with pytest.raises(ValueError, match="explicit narrow override"):
        TLSPolicy(verify=False)


def test_loopback_sync_transport_refuses_non_loopback_targets() -> None:
    transport = SyncLoopbackTransport()

    with pytest.raises(ValueError):
        transport.get("https://127.0.0.1:443/")
    with pytest.raises(ValueError):
        transport.get("http://example.test:80/")


def test_runtime_request_freezes_header_mapping() -> None:
    request = RuntimeRequest("GET", "http://127.0.0.1:8000/", {"Authorization": "Bearer secret"})

    assert dict(request.headers) == {"Authorization": "Bearer secret"}
    with pytest.raises(TypeError):
        request.headers["X-Extra"] = "1"  # ty: ignore[invalid-assignment]: asserts MappingProxyType raises at runtime


def test_urllib_cross_origin_redirect_strips_sensitive_headers_case_insensitively() -> None:
    opener = _RecordingOpener(
        [
            _FakeResponse(
                302,
                {"Location": "https://other.test/next?token=redirect-secret&keep=value"},
            ),
            _FakeResponse(200, {}),
        ]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener
    request_headers = {
        "aUtHoRiZaTiOn": "Bearer request-secret",
        "cOoKiE": "session-secret",
        "X-API-KEY": "api-secret",
        "x-AuTh-ToKeN": "token-secret",
        "X-App-Token": "app-secret",
        "X-Benign": "preserve-me",
    }
    request = RuntimeRequest("GET", "https://example.test/start", request_headers)

    response = transport.send(request)

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert response.status_code == 200
    assert len(opener.requests) == 2
    assert all(
        name not in forwarded for name in {"authorization", "cookie", "x-api-key", "x-auth-token", "x-app-token"}
    )
    assert forwarded["x-benign"] == "preserve-me"
    assert dict(request.headers) == request_headers


def test_urllib_forwarded_url_keeps_query_intact_while_failure_surface_redacts() -> None:
    opener = _RecordingOpener(
        [
            _FakeResponse(
                302,
                {"Location": "file:///etc/passwd?token=redirect-secret&keep=value"},
            ),
        ]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener

    with pytest.raises(TransportFailure, match="file:///etc/passwd") as excinfo:
        transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer secret"}))

    assert len(opener.requests) == 1
    assert "redirect-secret" not in str(excinfo.value)
    assert "keep=value" in str(excinfo.value)


def test_urllib_cross_origin_redirect_forwards_presigned_query_verbatim() -> None:
    location = "https://cdn.test/download?X-Amz-Signature=sig123&X-Amz-Credential=AKIA%2F20260822&keep=value"
    opener = _RecordingOpener([_FakeResponse(302, {"Location": location}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener

    response = transport.send(RuntimeRequest("GET", "https://example.test/file", {"Authorization": "Bearer s"}))

    assert response.status_code == 200
    assert "X-Amz-Signature=sig123" in opener.requests[1].full_url
    assert "X-Amz-Credential=AKIA%2F20260822" in opener.requests[1].full_url
    assert "keep=value" in opener.requests[1].full_url


def test_urllib_same_origin_redirect_preserves_caller_headers() -> None:
    opener = _RecordingOpener(
        [
            _FakeResponse(302, {"Location": "https://example.test/next?keep=value"}),
            _FakeResponse(200, {}),
        ]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener
    request_headers = {
        "Authorization": "Bearer request-secret",
        "Cookie": "session-secret",
        "X-API-Key": "api-secret",
        "X-Auth-Token": "token-secret",
        "X-Benign": "preserve-me",
    }

    transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert forwarded == {key.lower(): value for key, value in request_headers.items()}


def test_urllib_https_to_http_downgrade_strips_sensitive_headers() -> None:
    opener = _RecordingOpener(
        [
            _FakeResponse(302, {"Location": "http://other.test/insecure?keep=value"}),
            _FakeResponse(200, {}),
        ]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener
    request = RuntimeRequest(
        "GET",
        "https://example.test/start",
        {"Authorization": "Bearer request-secret", "Cookie": "session-secret", "X-Benign": "preserve-me"},
    )

    response = transport.send(request)

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert response.status_code == 200
    assert all(name not in forwarded for name in {"authorization", "cookie"})
    assert forwarded["x-benign"] == "preserve-me"


def test_urllib_exceeding_max_redirects_raises_transport_failure() -> None:
    opener = _LoopingRedirectOpener()
    transport = UrllibCatalogTransport(max_redirects=3)
    cast(Any, transport)._opener = opener

    with pytest.raises(TransportFailure, match="redirect limit"):
        transport.send(RuntimeRequest("GET", "https://example.test/start"))

    assert opener.count == 4


@pytest.mark.parametrize("status", [301, 302, 303])
def test_urllib_redirect_rewrites_post_to_bodyless_get(status: int) -> None:
    opener = _RecordingOpener(
        [_FakeResponse(status, {"Location": "https://example.test/next"}), _FakeResponse(200, {})]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener
    body = b'{"key": "value"}'

    response = transport.send(
        RuntimeRequest(
            "POST",
            "https://example.test/start",
            {"Content-Type": "application/json", "Content-Length": str(len(body))},
            body,
        )
    )

    follow_up = opener.requests[1]
    forwarded = {key.lower(): value for key, value in follow_up.header_items()}
    assert response.status_code == 200
    assert follow_up.method == "GET"
    assert follow_up.data is None
    assert all(name not in forwarded for name in {"content-type", "content-length", "transfer-encoding"})


@pytest.mark.parametrize("status", [307, 308])
def test_urllib_redirect_preserves_method_and_body(status: int) -> None:
    opener = _RecordingOpener(
        [_FakeResponse(status, {"Location": "https://example.test/next"}), _FakeResponse(200, {})]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener
    body = b'{"key": "value"}'

    response = transport.send(
        RuntimeRequest(
            "POST",
            "https://example.test/start",
            {"Content-Type": "application/json"},
            body,
        )
    )

    follow_up = opener.requests[1]
    forwarded = {key.lower(): value for key, value in follow_up.header_items()}
    assert response.status_code == 200
    assert follow_up.method == "POST"
    assert follow_up.data == body
    assert forwarded["content-type"] == "application/json"


def test_urllib_malformed_location_port_does_not_escape_send() -> None:
    opener = _RecordingOpener(
        [_FakeResponse(302, {"Location": "https://example.test:abc/next"}), _FakeResponse(200, {})]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener

    response = transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))

    assert response.status_code == 200
    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert "authorization" not in forwarded


def test_urllib_credential_scope_retains_authorization_on_allowed_cross_origin_hop() -> None:
    scope = CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True)
    opener = _RecordingOpener([_FakeResponse(302, {"Location": "https://other.test/next"}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport(credential_scope=scope)
    cast(Any, transport)._opener = opener

    transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer secret"}))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert forwarded["authorization"] == "Bearer secret"


def test_urllib_credential_scope_strips_authorization_for_disallowed_target() -> None:
    scope = CredentialScope(allowed_hosts=("example.test",), send_on_redirect=True)
    opener = _RecordingOpener([_FakeResponse(302, {"Location": "https://other.test/next"}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport(credential_scope=scope)
    cast(Any, transport)._opener = opener

    transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer secret"}))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert "authorization" not in forwarded


def test_urllib_credential_scope_requires_send_on_redirect_even_for_same_origin() -> None:
    scope = CredentialScope(allowed_hosts=("example.test",), allowed_schemes=("https",))
    opener = _RecordingOpener([_FakeResponse(302, {"Location": "/next"}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport(credential_scope=scope)
    cast(Any, transport)._opener = opener

    transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer secret"}))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert "authorization" not in forwarded


def test_urllib_credential_scope_blocks_scheme_downgrade_to_disallowed_http() -> None:
    scope = CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True)
    opener = _RecordingOpener([_FakeResponse(302, {"Location": "http://other.test/next"}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport(credential_scope=scope)
    cast(Any, transport)._opener = opener

    transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer secret"}))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert "authorization" not in forwarded


def test_async_loopback_transport_decodes_chunked_round_trip() -> None:
    payload = b"chunked-round-trip-body-" * 10

    async def fetch(base_url: str) -> bytes:
        transport = AsyncLoopbackTransport()
        try:
            return (await transport.get(f"{base_url}/chunked")).body
        finally:
            await transport.aclose()

    server, base_url = start_test_server({"/chunked": MockResponse(body=payload, chunk_size=7)})
    try:
        assert asyncio.run(fetch(base_url)) == payload
    finally:
        server.shutdown()
        server.server_close()
