"""Unit tests for authentication strategies."""

from __future__ import annotations

from datasluice.auth import APIKeyAuth, BasicAuth, BearerAuth, HeadersAuth, NoAuth


def test_no_auth() -> None:
    headers, params = NoAuth().apply({"Accept": "application/json"}, {"q": "test"})
    assert headers == {"Accept": "application/json"}
    assert params == {"q": "test"}


def test_api_key_in_header() -> None:
    auth = APIKeyAuth("secret-key")
    headers, _ = auth.apply({})
    assert headers["X-Api-Key"] == "secret-key"


def test_api_key_in_query() -> None:
    auth = APIKeyAuth("secret", in_header=False, param_name="api_key", in_query=True)
    _, params = auth.apply({}, {})
    assert params["api_key"] == "secret"


def test_bearer_auth() -> None:
    auth = BearerAuth("mytoken")
    headers, _ = auth.apply({})
    assert headers["Authorization"] == "Bearer mytoken"


def test_basic_auth() -> None:
    auth = BasicAuth("user", "pass")
    headers, _ = auth.apply({})
    assert headers["Authorization"].startswith("Basic ")


def test_headers_auth() -> None:
    auth = HeadersAuth({"X-Custom": "value"})
    headers, _ = auth.apply({"Accept": "*/*"})
    assert headers["X-Custom"] == "value"
    assert headers["Accept"] == "*/*"
