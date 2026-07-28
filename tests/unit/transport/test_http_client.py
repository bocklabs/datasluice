"""Integration tests for HttpClient hardening: credential_scope, error mapping, get_json."""

from __future__ import annotations

import inspect
import urllib.parse

import pytest

from datasluice.domain import CredentialScope
from datasluice.exceptions import PortalError, RateLimitError, RetryableHTTPError
from datasluice.transport import HttpClient
from datasluice.transport.retry import RetryPolicy
from tests.helpers.http_server import MockResponse, start_test_server


def _fast_policy() -> RetryPolicy:
    return RetryPolicy(max_attempts=1, base_delay=0.01)


def test_http_client_accepts_credential_scope_parameter() -> None:
    client = HttpClient()
    assert client._credential_scope is None
    scope = CredentialScope(allowed_hosts=("api.example.com",), send_on_redirect=True)
    client_scoped = HttpClient(credential_scope=scope)
    assert client_scoped._credential_scope is scope


def test_http_client_init_signature_has_credential_scope() -> None:
    sig = inspect.signature(HttpClient.__init__)
    assert "credential_scope" in sig.parameters
    assert sig.parameters["credential_scope"].default is None


def test_http_client_503_raises_retryable_error() -> None:
    server, base = start_test_server({"/err": MockResponse(status=503, body=b"oops")})
    try:
        client = HttpClient(retry_policy=_fast_policy())
        with pytest.raises(RetryableHTTPError) as exc_info:
            client.request(f"{base}/err")
        assert exc_info.value.status_code == 503
    finally:
        server.shutdown()
        server.server_close()


def test_http_client_404_raises_portal_error_not_retryable() -> None:
    server, base = start_test_server({"/missing": MockResponse(status=404, body=b"nope")})
    try:
        client = HttpClient(retry_policy=_fast_policy())
        with pytest.raises(PortalError) as exc_info:
            client.request(f"{base}/missing")
        assert not isinstance(exc_info.value, RetryableHTTPError)
    finally:
        server.shutdown()
        server.server_close()


def test_http_client_429_raises_rate_limit_error() -> None:
    server, base = start_test_server({"/busy": MockResponse(status=429, headers={"Retry-After": "2"}, body=b"slow")})
    try:
        client = HttpClient(retry_policy=_fast_policy())
        with pytest.raises(RateLimitError) as exc_info:
            client.request(f"{base}/busy")
        assert exc_info.value.retry_after == 2.0
    finally:
        server.shutdown()
        server.server_close()


def test_http_client_get_json() -> None:
    server, base = start_test_server({"/data": MockResponse(status=200, body=b'{"hello": "world"}')})
    try:
        client = HttpClient()
        result = client.get_json(f"{base}/data")
        assert result == {"hello": "world"}
    finally:
        server.shutdown()
        server.server_close()


def test_http_client_retries_then_succeeds_on_503_then_200() -> None:
    server, base = start_test_server(
        {"/flaky": [MockResponse(status=503, body=b"down"), MockResponse(status=200, body=b"ok")]}
    )
    try:
        client = HttpClient(retry_policy=RetryPolicy(max_attempts=2, base_delay=0.01))
        result = client.request(f"{base}/flaky")
        assert result == b"ok"
    finally:
        server.shutdown()
        server.server_close()


def test_http_client_encodes_list_params_as_repeated_keys() -> None:
    server, base = start_test_server({"/data": MockResponse(status=200, body=b'{"ok": true}')})
    try:
        client = HttpClient()
        client.get_json(f"{base}/data", params={"tag": ["economy", "budget"], "q": "water"})
        assert server.captured_paths
        query = urllib.parse.parse_qsl(urllib.parse.urlparse(server.captured_paths[0]).query)
        tag_pairs = [pair for pair in query if pair[0] == "tag"]
        assert tag_pairs == [("tag", "economy"), ("tag", "budget")]
        assert ("q", "water") in query
    finally:
        server.shutdown()
        server.server_close()
