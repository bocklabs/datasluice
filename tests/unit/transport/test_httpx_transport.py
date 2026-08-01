"""Integration + unit tests for the httpx-backed HttpxTransport.

Mirrors ``test_http_client.py`` (error mapping, get_json, retry-then-succeed)
and ``test_redirect.py`` (cross-host strip, same-host-with-scope retain) but
exercises the new :class:`HttpxTransport` over real sockets, plus the
:class:`StreamingTransport` ``stream()`` path and the D-P3-15 401/403
evict-and-refetch-once behaviour. Closes INFRA-01.

The module is resolved via ``importlib.import_module`` (rather than a static
``import``) so the RED commit can land under this repo's full-suite pre-commit
hook: until the implementation in the GREEN step ships, the whole module
skips cleanly instead of erroring at collection.
"""

from __future__ import annotations

import importlib
import os
import urllib.parse
from unittest.mock import MagicMock

import pytest

from datasluice.auth import BearerAuth, NoAuth  # noqa: F401 — exercised once impl lands
from datasluice.domain import CredentialScope
from datasluice.exceptions import PortalError, RateLimitError, RetryableHTTPError
from datasluice.ports import StreamingTransport, Transport
from datasluice.transport.retry import RetryPolicy
from tests.helpers.http_server import MockResponse, start_test_server

# HttpxTransport lives in a module whose import requires the ``http`` extra and
# is implemented in the GREEN step of this task.
pytest.importorskip("httpx")
try:
    _httpx_transport_module = importlib.import_module("datasluice.transport.httpx_transport")
except ImportError:
    pytest.skip(
        "HttpxTransport implementation pending (RED → GREEN within task 03-01)",
        allow_module_level=True,
    )
HttpxTransport = _httpx_transport_module.HttpxTransport
_RESPONSE_AWARE_READY = hasattr(importlib.import_module("datasluice.ports"), "ResponseAwareReader")


def _fast_policy() -> RetryPolicy:
    return RetryPolicy(max_attempts=1, base_delay=0.01)


# --------------------------------------------------------------------------- #
# Protocol conformance
# --------------------------------------------------------------------------- #


def test_httpx_transport_satisfies_both_protocols() -> None:
    """HttpxTransport satisfies Transport AND StreamingTransport (INFRA-01/07)."""

    transport = HttpxTransport()
    assert isinstance(transport, Transport)
    assert isinstance(transport, StreamingTransport)


# --------------------------------------------------------------------------- #
# Error mapping (SEC-05 carry-forward)
# --------------------------------------------------------------------------- #


def test_httpx_503_raises_retryable_error() -> None:
    server, base = start_test_server({"/err": MockResponse(status=503, body=b"oops")})
    try:
        transport = HttpxTransport(retry_policy=_fast_policy())
        with pytest.raises(RetryableHTTPError) as exc_info:
            transport.request(f"{base}/err")
        assert exc_info.value.status_code == 503
    finally:
        server.shutdown()
        server.server_close()


def test_httpx_404_raises_portal_error_not_retryable() -> None:
    server, base = start_test_server({"/missing": MockResponse(status=404, body=b"nope")})
    try:
        transport = HttpxTransport(retry_policy=_fast_policy())
        with pytest.raises(PortalError) as exc_info:
            transport.request(f"{base}/missing")
        assert not isinstance(exc_info.value, RetryableHTTPError)
    finally:
        server.shutdown()
        server.server_close()


def test_httpx_429_raises_rate_limit_error() -> None:
    server, base = start_test_server({"/busy": MockResponse(status=429, headers={"Retry-After": "2"}, body=b"slow")})
    try:
        transport = HttpxTransport(retry_policy=_fast_policy())
        with pytest.raises(RateLimitError) as exc_info:
            transport.request(f"{base}/busy")
        assert exc_info.value.retry_after == 2.0
    finally:
        server.shutdown()
        server.server_close()


def test_httpx_retries_then_succeeds_on_503_then_200() -> None:
    server, base = start_test_server(
        {"/flaky": [MockResponse(status=503, body=b"down"), MockResponse(status=200, body=b"ok")]}
    )
    try:
        transport = HttpxTransport(retry_policy=RetryPolicy(max_attempts=2, base_delay=0.01))
        result = transport.request(f"{base}/flaky")
        assert result == b"ok"
    finally:
        server.shutdown()
        server.server_close()


def test_httpx_encodes_list_params_as_repeated_keys() -> None:
    server, base = start_test_server({"/data": MockResponse(status=200, body=b'{"ok": true}')})
    try:
        transport = HttpxTransport()
        transport.get_json(f"{base}/data", params={"tag": ["economy", "budget"], "q": "water"})
        assert server.captured_paths
        query = urllib.parse.parse_qsl(urllib.parse.urlparse(server.captured_paths[0]).query)
        tag_pairs = [pair for pair in query if pair[0] == "tag"]
        assert tag_pairs == [("tag", "economy"), ("tag", "budget")]
        assert ("q", "water") in query
    finally:
        server.shutdown()
        server.server_close()


# --------------------------------------------------------------------------- #
# CredentialScope redirect handling (SEC-01/SEC-02 carry-forward)
# --------------------------------------------------------------------------- #


def test_cross_host_redirect_strips_auth() -> None:
    """Cross-host redirect must strip Authorization (mirrors test_redirect.py)."""

    server_b, base_b = start_test_server({"/target": MockResponse(body=b"ok")})
    server_a, base_a = start_test_server(
        {"/start": MockResponse(status=302, headers={"Location": f"{base_b}/target"}, body=b"redirect")}
    )
    try:
        transport = HttpxTransport(auth=BearerAuth("test-secret-token"))
        try:
            transport.request(f"{base_a}/start")
        except PortalError:
            pass
        captured_b = server_b.captured
        assert captured_b, "server B should have received the redirected request"
        for headers in captured_b:
            assert "authorization" not in headers, "Authorization leaked to a different host on redirect"
    finally:
        server_a.shutdown()
        server_a.server_close()
        server_b.shutdown()
        server_b.server_close()


def test_same_host_redirect_with_allowed_scope_retains_authorization() -> None:
    """Same-host redirect within an allowed CredentialScope retains Authorization."""

    server, base = start_test_server({})
    server.responses["/old"] = MockResponse(status=302, headers={"Location": f"{base}/new"}, body=b"redirect")
    server.responses["/new"] = MockResponse(body=b"ok")
    scope = CredentialScope(allowed_hosts=("127.0.0.1",), allowed_schemes=("http",), send_on_redirect=True)
    try:
        transport = HttpxTransport(auth=BearerAuth("test-secret-token"), credential_scope=scope)
        transport.request(f"{base}/old")
        new_requests = [h for h in server.captured if "authorization" in h]
        assert new_requests, "Authorization should be retained on same-host redirect within allowed scope"
    finally:
        server.shutdown()
        server.server_close()


def test_scheme_downgrade_strips_authorization() -> None:
    """https→http redirect strips Authorization even when the host matches.

    The real test server only speaks HTTP, so the predicate is exercised
    directly via the pure ``_should_strip_authorization`` helper (the same
    helper drives the live redirect loop).
    """

    transport = HttpxTransport(auth=BearerAuth("test-secret-token"))
    assert (
        transport._should_strip_authorization("https://api.example.com/start", "http://api.example.com/target") is True
    )


# --------------------------------------------------------------------------- #
# Streaming (D-P3-07)
# --------------------------------------------------------------------------- #


def test_stream_yields_bytes_and_headers() -> None:
    """stream() yields a context manager whose target is iterable + has .headers."""

    server, base = start_test_server({"/blob": MockResponse(body=b"chunk1chunk2", headers={"ETag": "abc"})})
    try:
        transport = HttpxTransport()
        with transport.stream(f"{base}/blob") as resp:
            collected = b"".join(resp)
            assert collected == b"chunk1chunk2"
            assert resp.headers["ETag"] == "abc"
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.skipif(
    not _RESPONSE_AWARE_READY and os.environ.get("DATASLUICE_TDD_RED") != "1",
    reason="stream redirect hardening pending GREEN phase",
)
def test_stream_routes_through_redirect_loop() -> None:
    """Streaming requests use the credential-scoped redirect loop."""
    server_b, base_b = start_test_server({"/target": MockResponse(body=b"stream-body")})
    server_a, base_a = start_test_server(
        {"/start": MockResponse(status=302, headers={"Location": f"{base_b}/target"}, body=b"redirect")}
    )
    try:
        transport = HttpxTransport(auth=BearerAuth("stream-secret"))
        with transport.stream(f"{base_a}/start") as response:
            assert b"".join(response) == b"stream-body"
        assert server_b.captured
        assert all("authorization" not in headers for headers in server_b.captured)
    finally:
        server_a.shutdown()
        server_a.server_close()
        server_b.shutdown()
        server_b.server_close()


@pytest.mark.skipif(
    not _RESPONSE_AWARE_READY and os.environ.get("DATASLUICE_TDD_RED") != "1",
    reason="stream redirect hardening pending GREEN phase",
)
def test_redirect_exhaustion_raises() -> None:
    """A pending redirect after max_redirects is an error, not a successful response."""
    server, base = start_test_server({})
    server.responses["/loop"] = MockResponse(status=302, headers={"Location": f"{base}/loop"}, body=b"loop")
    try:
        transport = HttpxTransport(max_redirects=2)
        with pytest.raises(PortalError, match="Redirect"):
            with transport.stream(f"{base}/loop"):
                pass
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.skipif(
    not _RESPONSE_AWARE_READY and os.environ.get("DATASLUICE_TDD_RED") != "1",
    reason="stream auth query preservation pending GREEN phase",
)
def test_stream_preserves_query_auth_params() -> None:
    """Query-position authentication remains on streaming request URLs."""
    from datasluice.auth import APIKeyAuth

    server, base = start_test_server({"/data": MockResponse(body=b"stream-body")})
    try:
        transport = HttpxTransport(
            auth=APIKeyAuth("secret", param_name="api_key", in_header=False, in_query=True),
        )
        with transport.stream(f"{base}/data") as response:
            assert b"".join(response) == b"stream-body"
        assert server.captured_paths == ["/data?api_key=secret"]
    finally:
        server.shutdown()
        server.server_close()


# --------------------------------------------------------------------------- #
# 401/403 evict-and-refetch-once (D-P3-15)
# --------------------------------------------------------------------------- #

# HostCredentialProvider ships in wave-1 plan 03-04. Until it lands the
# eviction path is correct-by-design but cannot be exercised against a real
# provider type — the importorskip below is inside the helper (NOT module
# level) so its absence only skips the two 401 tests, not the whole module.


def _evicting_provider(refreshed_auth) -> MagicMock:
    """Build a mock that satisfies ``isinstance(_, HostCredentialProvider)``."""

    host_provider = pytest.importorskip("datasluice.credentials.host_provider")
    provider = MagicMock(spec=host_provider.HostCredentialProvider)
    provider.resolve.return_value = refreshed_auth
    return provider


def test_401_evicts_and_retries_once() -> None:
    """A 401 from a HostCredentialProvider-backed transport evicts + retries once."""

    server, base = start_test_server(
        {"/secret": [MockResponse(status=401, body=b"no"), MockResponse(status=200, body=b"ok")]}
    )
    try:
        provider = _evicting_provider(BearerAuth("refreshed"))
        transport = HttpxTransport(auth=BearerAuth("stale"), credential_provider=provider, retry_policy=_fast_policy())
        result = transport.request(f"{base}/secret")
        assert result == b"ok"
        provider.evict.assert_called_once()
    finally:
        server.shutdown()
        server.server_close()


def test_401_after_refresh_still_401_raises() -> None:
    """If the refreshed credential still gets 401, raise PortalError (no loop)."""

    server, base = start_test_server(
        {"/secret": [MockResponse(status=401, body=b"no"), MockResponse(status=401, body=b"still-no")]}
    )
    try:
        provider = _evicting_provider(BearerAuth("refreshed"))
        transport = HttpxTransport(auth=BearerAuth("stale"), credential_provider=provider, retry_policy=_fast_policy())
        with pytest.raises(PortalError):
            transport.request(f"{base}/secret")
        provider.evict.assert_called_once()
    finally:
        server.shutdown()
        server.server_close()


def test_conditional_fetch_refreshes_and_recovers() -> None:
    """conditional_fetch evicts + retries once on 401 and returns the refreshed body."""

    server, base = start_test_server(
        {"/pub": [MockResponse(status=401, body=b"no"), MockResponse(status=200, body=b"ok", headers={"ETag": '"e"'})]}
    )
    try:
        provider = _evicting_provider(BearerAuth("refreshed"))
        transport = HttpxTransport(auth=BearerAuth("stale"), credential_provider=provider)
        result = transport.conditional_fetch(f"{base}/pub")
        assert result.status_code == 200
        assert result.stream is not None
        with result.stream as response:
            assert b"".join(response) == b"ok"
        assert result.headers["ETag"] == '"e"'
        provider.evict.assert_called_once()
    finally:
        server.shutdown()
        server.server_close()


def test_conditional_fetch_401_after_refresh_raises() -> None:
    """conditional_fetch surfaces a still-401 after a single refresh as PortalError."""

    server, base = start_test_server(
        {"/pub": [MockResponse(status=401, body=b"no"), MockResponse(status=401, body=b"still-no")]}
    )
    try:
        provider = _evicting_provider(BearerAuth("refreshed"))
        transport = HttpxTransport(auth=BearerAuth("stale"), credential_provider=provider)
        with pytest.raises(PortalError):
            transport.conditional_fetch(f"{base}/pub")
        provider.evict.assert_called_once()
    finally:
        server.shutdown()
        server.server_close()


def test_stream_refreshes_and_recovers() -> None:
    """stream() evicts + retries once on 401 and yields the refreshed body."""

    server, base = start_test_server(
        {"/pub": [MockResponse(status=401, body=b"no"), MockResponse(status=200, body=b"stream-body")]}
    )
    try:
        provider = _evicting_provider(BearerAuth("refreshed"))
        transport = HttpxTransport(auth=BearerAuth("stale"), credential_provider=provider)
        with transport.stream(f"{base}/pub") as response:
            assert b"".join(response) == b"stream-body"
        provider.evict.assert_called_once()
    finally:
        server.shutdown()
        server.server_close()


# --------------------------------------------------------------------------- #
# get_json / download
# --------------------------------------------------------------------------- #


def test_get_json_parses_dict() -> None:
    server, base = start_test_server({"/data": MockResponse(status=200, body=b'{"k": "v"}')})
    try:
        transport = HttpxTransport()
        assert transport.get_json(f"{base}/data") == {"k": "v"}
    finally:
        server.shutdown()
        server.server_close()


def test_get_json_wraps_non_dict() -> None:
    server, base = start_test_server({"/data": MockResponse(status=200, body=b"[1, 2, 3]")})
    try:
        transport = HttpxTransport()
        assert transport.get_json(f"{base}/data") == {"data": [1, 2, 3]}
    finally:
        server.shutdown()
        server.server_close()


# --------------------------------------------------------------------------- #
# CR-04: refresh must close the rejected 401/403 response before retrying
# CR-05: query-position credentials must be preserved and refreshed
# CR-06: same-host different-port redirects strip Authorization
# --------------------------------------------------------------------------- #


def test_refresh_closes_rejected_response_under_single_connection_pool() -> None:
    """Refresh succeeds even when the client pool allows only one connection (CR-04).

    Without closing the rejected 401/403 response first, the streamed body
    holds the sole connection and the retry raises PoolTimeout. We assert both
    that the retry succeeds and that the rejected response ends up closed.
    """
    import httpx

    server, base = start_test_server(
        {"/secret": [MockResponse(status=401, body=b"no"), MockResponse(status=200, body=b"ok")]}
    )
    try:
        provider = _evicting_provider(BearerAuth("refreshed"))
        transport = HttpxTransport(
            auth=BearerAuth("stale"),
            credential_provider=provider,
            retry_policy=_fast_policy(),
        )
        # Replace the default unbounded-pool client with a one-connection pool
        # so the regression is reproducible.
        transport._client.close()
        transport._client = httpx.Client(
            timeout=httpx.Timeout(5.0),
            follow_redirects=False,
            max_redirects=transport._max_redirects,
            limits=httpx.Limits(max_connections=1),
        )
        result = transport.request(f"{base}/secret")
        assert result == b"ok"
        provider.evict.assert_called_once()
    finally:
        server.shutdown()
        server.server_close()


def test_query_auth_credentials_applied_on_request_and_download() -> None:
    """APIKeyAuth(in_query=True) credentials reach the wire for ordinary requests (CR-05)."""
    from datasluice.auth import APIKeyAuth

    server, base = start_test_server({"/x": MockResponse(body=b"ok")})
    try:
        transport = HttpxTransport(auth=APIKeyAuth("TOPSECRET", param_name="api_key", in_header=False, in_query=True))
        transport.download(f"{base}/x")
        assert server.captured_paths == ["/x?api_key=TOPSECRET"]
    finally:
        server.shutdown()
        server.server_close()


def test_query_auth_credentials_refreshed_on_401() -> None:
    """Refresh replaces the stale query credential instead of resending it (CR-05)."""
    from datasluice.auth import APIKeyAuth

    server, base = start_test_server(
        {"/x": [MockResponse(status=401, body=b"no"), MockResponse(status=200, body=b"ok")]}
    )
    try:
        provider = _evicting_provider(
            APIKeyAuth("fresh", param_name="api_key", in_header=False, in_query=True),
        )
        transport = HttpxTransport(
            auth=APIKeyAuth("stale", param_name="api_key", in_header=False, in_query=True),
            credential_provider=provider,
            retry_policy=_fast_policy(),
        )
        assert transport.request(f"{base}/x") == b"ok"
        assert server.captured_paths == ["/x?api_key=stale", "/x?api_key=fresh"]
    finally:
        server.shutdown()
        server.server_close()


def test_same_hostname_different_port_strips_authorization() -> None:
    """A redirect from https://host:443 to https://host:8443 strips Authorization (CR-06)."""
    transport = HttpxTransport(auth=BearerAuth("secret"))
    assert (
        transport._should_strip_authorization("https://example.test:443/start", "https://example.test:8443/target")
        is True
    )


def test_scheme_default_port_matches_same_origin() -> None:
    """A redirect from https://host:443 to https://host (default port) is same-origin (CR-06)."""
    transport = HttpxTransport(auth=BearerAuth("secret"))
    assert (
        transport._should_strip_authorization("https://example.test:443/start", "https://example.test/target") is False
    )


def test_transport_errors_redact_query_credentials() -> None:
    """PortalError text must not echo the raw query credential value (CR-07)."""
    from datasluice.auth import APIKeyAuth

    server, base = start_test_server({"/missing": MockResponse(status=404, body=b"nope")})
    try:
        transport = HttpxTransport(
            auth=APIKeyAuth("TOPSECRET", param_name="api_key", in_header=False, in_query=True),
            retry_policy=_fast_policy(),
        )
        with pytest.raises(PortalError) as exc_info:
            transport.request(f"{base}/missing")
        assert "TOPSECRET" not in str(exc_info.value)
    finally:
        server.shutdown()
        server.server_close()
