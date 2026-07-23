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


def test_bearer_auth_repr_redacts_token() -> None:
    auth = BearerAuth("a-very-secret-token")
    assert "a-very-secret-token" not in repr(auth)
    assert "***" in repr(auth)
    assert "Bearer" in repr(auth)


def test_api_key_auth_repr_redacts_key() -> None:
    auth = APIKeyAuth("my-secret-key")
    assert "my-secret-key" not in repr(auth)
    assert "***" in repr(auth)
    assert "X-Api-Key" in repr(auth)


def test_basic_auth_repr_redacts_password() -> None:
    auth = BasicAuth("user", "p4ssw0rd")
    assert "p4ssw0rd" not in repr(auth)
    assert "***" in repr(auth)
    assert "user" in repr(auth)


def test_headers_auth_repr_redacts_values() -> None:
    auth = HeadersAuth({"X-Secret": "leaked-value"})
    assert "leaked-value" not in repr(auth)
    assert "X-Secret" not in repr(auth)
    assert "***" in repr(auth)
