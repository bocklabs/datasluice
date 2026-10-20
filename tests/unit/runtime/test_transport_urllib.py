from __future__ import annotations

import asyncio
import ssl
from http.client import IncompleteRead
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlencode

import pytest

from datasluice.domain import CredentialScope
from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import BudgetExhaustedError
from datasluice.runtime.resilience import DeadlineMonitor
from datasluice.runtime.transport.base import RedirectPolicy, RuntimeRequest, TransportError
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport, _tls_context
from tests.helpers.catalog_transport import (
    AsyncLoopbackTransport,
    SyncLoopbackTransport,
    _FakeResponse,
    _RecordingOpener,
)
from tests.helpers.http_server import MockResponse, start_test_server

if TYPE_CHECKING:
    from urllib.request import Request


class _LoopingRedirectOpener:
    def __init__(self) -> None:
        self.count = 0

    def open(self, request: Request, *, timeout: float) -> _FakeResponse:
        del request, timeout
        self.count += 1
        return _FakeResponse(302, {"Location": f"https://example.test/loop/{self.count}"})


def test_urllib_transport_uses_verified_tls_by_default() -> None:
    assert UrllibCatalogTransport()._tls_policy.verify
    context = _tls_context(TLSPolicy())
    assert context.check_hostname
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.minimum_version == ssl.TLSVersion.TLSv1_2


def test_urllib_uses_read_budget_and_closes_completed_responses() -> None:
    response = _FakeResponse(200, {})
    opener = _RecordingOpener([response])
    transport = UrllibCatalogTransport(budget=TimeBudget(connect=1, read=7, write=2, total=9))
    cast("Any", transport)._opener = opener

    transport.send(RuntimeRequest("GET", "https://example.test/data"))

    assert opener.timeouts == [7]
    assert response.closed


def test_urllib_send_enforces_max_response_bytes() -> None:
    opener = _RecordingOpener([_FakeResponse(200, {}, body=b"abcdef"), _FakeResponse(200, {}, body=b"abcdef")])
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener

    request = RuntimeRequest("GET", "https://example.test/", max_response_bytes=5)
    with pytest.raises(TransportError, match="byte limit"):
        transport.send(request)

    assert transport.send(RuntimeRequest("GET", "https://example.test/", max_response_bytes=6)).body == b"abcdef"


def test_urllib_checks_the_deadline_during_reads_and_closes_the_response() -> None:
    now = [0.0]

    class SlowResponse(_FakeResponse):
        def read(self, size: int | None = None) -> bytes:
            now[0] = 2.0
            return super().read(size)

    response = SlowResponse(200, {}, b"late")
    opener = _RecordingOpener([response])
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener
    deadline = DeadlineMonitor(TimeBudget(connect=1, read=1, write=1, total=1), clock=lambda: now[0])
    request = RuntimeRequest("GET", "https://example.test/", deadline=deadline)
    with pytest.raises(BudgetExhaustedError):
        transport.send(request)
    assert opener.timeouts == [1.0]
    assert response.closed


def test_urllib_send_stream_enforces_max_response_bytes_and_allows_the_boundary() -> None:
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = _RecordingOpener([_FakeResponse(200, {}, body=b"abcdef")])
    response = transport.send_stream(
        RuntimeRequest("GET", "https://example.test/", redirect_policy=RedirectPolicy.NO_FOLLOW, max_response_bytes=5)
    )
    with pytest.raises(TransportError, match="byte limit"):
        list(response)

    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = _RecordingOpener([_FakeResponse(200, {}, body=b"abcdef")])
    response = transport.send_stream(
        RuntimeRequest("GET", "https://example.test/", redirect_policy=RedirectPolicy.NO_FOLLOW, max_response_bytes=6)
    )
    assert b"".join(response) == b"abcdef"


def test_urllib_no_follow_returns_the_original_redirect_without_contacting_its_target() -> None:
    response = _FakeResponse(302, {"Location": "https://target.test/secret"})
    opener = _RecordingOpener([response])
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener

    result = transport.send(RuntimeRequest("GET", "https://origin.test/root", redirect_policy=RedirectPolicy.NO_FOLLOW))

    assert result.status_code == 302
    assert result.headers["Location"] == "https://target.test/secret"
    assert len(opener.requests) == 1
    assert opener.requests[0].full_url == "https://origin.test/root"
    assert response.closed


def test_urllib_real_socket_no_follow_returns_the_original_redirect() -> None:
    server, base_url = start_test_server()
    server.responses = {
        "/origin": MockResponse(status=302, headers={"Location": f"{base_url}/target"}, body=b"redirect"),
        "/target": MockResponse(status=200, body=b"target"),
    }
    transport = UrllibCatalogTransport()
    try:
        response = transport.send(RuntimeRequest("GET", f"{base_url}/origin", redirect_policy=RedirectPolicy.NO_FOLLOW))
    finally:
        transport.close()
        server.shutdown()
        server.server_close()

    assert response.status_code == 302
    assert response.headers["Location"] == f"{base_url}/target"
    assert server.captured_paths == ["/origin"]


def test_urllib_real_socket_stream_no_follow_returns_the_original_redirect() -> None:
    server, base_url = start_test_server()
    server.responses = {
        "/origin": MockResponse(status=302, headers={"Location": f"{base_url}/target"}, body=b"redirect"),
        "/target": MockResponse(status=200, body=b"target"),
    }
    transport = UrllibCatalogTransport()
    try:
        response = transport.send_stream(
            RuntimeRequest("GET", f"{base_url}/origin", redirect_policy=RedirectPolicy.NO_FOLLOW)
        )
        assert response.status_code == 302
        assert response.headers["Location"] == f"{base_url}/target"
        response.close()
    finally:
        transport.close()
        server.shutdown()
        server.server_close()

    assert server.captured_paths == ["/origin"]


def test_urllib_real_socket_stream_preserves_duplicate_response_headers() -> None:
    server, base_url = start_test_server(
        {
            "/export": MockResponse(
                headers={"Content-Type": "text/csv"},
                extra_headers=(("Content-Type", "application/json"),),
                body=b"id\n1\n",
            )
        }
    )
    transport = UrllibCatalogTransport()
    try:
        response = transport.send_stream(
            RuntimeRequest("GET", f"{base_url}/export", redirect_policy=RedirectPolicy.NO_FOLLOW)
        )
        assert response.headers["Content-Type"] == "text/csv,application/json"
        response.close()
    finally:
        transport.close()
        server.shutdown()
        server.server_close()


def test_urllib_wraps_incomplete_response_reads_and_closes_response() -> None:
    class _IncompleteResponse(_FakeResponse):
        def read(self, size: int | None = None) -> bytes:
            del size
            raise IncompleteRead(b"partial", 10)

    response = _IncompleteResponse(200, {})
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = _RecordingOpener([response])

    request = RuntimeRequest("GET", "https://example.test/data")
    with pytest.raises(TransportError, match="mid-response"):
        transport.send(request)

    assert response.closed


def test_tls_policy_rejects_unscoped_disablement() -> None:
    with pytest.raises(ValueError, match="cannot be disabled"):
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
    headers = cast("dict[str, str]", request.headers)
    with pytest.raises(TypeError):
        headers["X-Extra"] = "1"


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
    cast("Any", transport)._opener = opener
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
    assert "x-benign" not in forwarded
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
    cast("Any", transport)._opener = opener

    request = RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer secret"})
    with pytest.raises(TransportError, match="file:///etc/passwd") as excinfo:
        transport.send(request)

    assert len(opener.requests) == 1
    assert "redirect-secret" not in str(excinfo.value)
    assert "keep=value" in str(excinfo.value)


def test_urllib_refuses_userinfo_redirect_target_before_a_second_request() -> None:
    opener = _RecordingOpener([_FakeResponse(302, {"Location": "https://user:fixture-password@example.test/next"})])
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener

    request = RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer secret"})
    with pytest.raises(TransportError, match="embedded credentials") as excinfo:
        transport.send(request)

    assert len(opener.requests) == 1
    assert "fixture-password" not in str(excinfo.value)


def test_urllib_cross_origin_redirect_forwards_presigned_query_verbatim() -> None:
    location = "https://cdn.test/download?X-Amz-Signature=sig123&X-Amz-Credential=AKIA%2F20260822&keep=value"
    opener = _RecordingOpener([_FakeResponse(302, {"Location": location}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener

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
    cast("Any", transport)._opener = opener
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
    cast("Any", transport)._opener = opener
    request = RuntimeRequest(
        "GET",
        "https://example.test/start",
        {"Authorization": "Bearer request-secret", "Cookie": "session-secret", "X-Benign": "preserve-me"},
    )

    response = transport.send(request)

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert response.status_code == 200
    assert all(name not in forwarded for name in {"authorization", "cookie"})
    assert "x-benign" not in forwarded


def test_urllib_exceeding_max_redirects_raises_transport_error() -> None:
    opener = _LoopingRedirectOpener()
    transport = UrllibCatalogTransport(max_redirects=3)
    cast("Any", transport)._opener = opener

    request = RuntimeRequest("GET", "https://example.test/start")
    with pytest.raises(TransportError, match="redirect limit"):
        transport.send(request)

    assert opener.count == 4


@pytest.mark.parametrize("status", [301, 302, 303])
def test_urllib_redirect_rewrites_post_to_bodyless_get(status: int) -> None:
    opener = _RecordingOpener(
        [_FakeResponse(status, {"Location": "https://example.test/next"}), _FakeResponse(200, {})]
    )
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener
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
    cast("Any", transport)._opener = opener
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


@pytest.mark.parametrize("status", [307, 308])
def test_urllib_cross_origin_body_redirect_fails_closed(status: int) -> None:
    body = urlencode({"token": "redirect-body-secret"}).encode()
    opener = _RecordingOpener([_FakeResponse(status, {"Location": "https://other.test/next"})])
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener

    request = RuntimeRequest(
        "POST",
        "https://origin.test/oauth/revoke",
        {"Content-Type": "application/x-www-form-urlencoded"},
        body,
    )
    with pytest.raises(TransportError, match="different redirect origin"):
        transport.send(request)

    assert len(opener.requests) == 1
    assert opener.requests[0].data == body


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_urllib_cross_origin_non_post_body_redirect_fails_closed(status: int) -> None:
    """A 301/302 keeps a non-POST body, so the refusal cannot key off the status alone."""
    body = urlencode({"token": "redirect-body-secret"}).encode()
    opener = _RecordingOpener([_FakeResponse(status, {"Location": "https://other.test/next"})])
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener

    request = RuntimeRequest(
        "PUT",
        "https://origin.test/resources/1/extras/",
        {"Content-Type": "application/x-www-form-urlencoded"},
        body,
    )
    with pytest.raises(TransportError, match="different redirect origin"):
        transport.send(request)

    assert len(opener.requests) == 1
    assert opener.requests[0].data == body


def test_urllib_malformed_location_port_does_not_escape_send() -> None:
    opener = _RecordingOpener(
        [_FakeResponse(302, {"Location": "https://example.test:abc/next"}), _FakeResponse(200, {})]
    )
    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = opener

    response = transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))

    assert response.status_code == 200
    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert "authorization" not in forwarded


@pytest.mark.parametrize(
    ("scope", "location", "retains_authorization"),
    [
        pytest.param(
            CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True),
            "https://other.test/next",
            True,
            id="allowed-cross-origin",
        ),
        pytest.param(
            CredentialScope(allowed_hosts=("example.test",), send_on_redirect=True),
            "https://other.test/next",
            False,
            id="disallowed-target",
        ),
        pytest.param(
            CredentialScope(allowed_hosts=("example.test",), allowed_schemes=("https",)),
            "/next",
            False,
            id="same-origin-without-send-on-redirect",
        ),
        pytest.param(
            CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True),
            "http://other.test/next",
            False,
            id="disallowed-http-downgrade",
        ),
    ],
)
def test_urllib_credential_scope_controls_redirect_authorization(
    scope: CredentialScope, location: str, retains_authorization: bool
) -> None:
    opener = _RecordingOpener([_FakeResponse(302, {"Location": location}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport(credential_scope=scope)
    cast("Any", transport)._opener = opener

    transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer secret"}))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    if retains_authorization:
        assert forwarded["authorization"] == "Bearer secret"
    else:
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


def test_urllib_transport_failures_retain_their_cause() -> None:
    """Urllib causes are deliberately chained because their text carries no request context."""

    class _FailingOpener:
        def open(self, request: Request, *, timeout: float) -> _FakeResponse:
            raise OSError("connection reset")

    transport = UrllibCatalogTransport()
    cast("Any", transport)._opener = _FailingOpener()
    request = RuntimeRequest("GET", "https://example.test/start")
    with pytest.raises(TransportError, match="could not complete") as excinfo:
        transport.send(request)
    assert isinstance(excinfo.value.__cause__, OSError)
