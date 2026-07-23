"""Unit tests for the CredentialScope model and credential-aware redirect handling."""

from __future__ import annotations

import dataclasses
import urllib.request
from http.client import HTTPMessage
from io import BytesIO

import pytest
from datasluice.transport.redirect import CredentialAwareRedirectHandler

from datasluice.domain import CredentialScope
from datasluice.exceptions import PortalError, RetryableHTTPError


def test_credential_scope_defaults() -> None:
    scope = CredentialScope()
    assert scope.allowed_hosts == ()
    assert scope.allowed_schemes == ("https",)
    assert scope.send_on_redirect is False


def test_credential_scope_is_frozen() -> None:
    scope = CredentialScope()
    with pytest.raises(dataclasses.FrozenInstanceError):
        scope.send_on_redirect = True


def test_credential_scope_custom_values() -> None:
    scope = CredentialScope(allowed_hosts=("api.example.com",), send_on_redirect=True)
    assert scope.allowed_hosts == ("api.example.com",)
    assert scope.send_on_redirect is True


def test_retryable_http_error_carries_status_code() -> None:
    exc = RetryableHTTPError("msg", 503)
    assert exc.status_code == 503
    assert str(exc) == "msg"


def test_retryable_http_error_is_portal_error() -> None:
    assert issubclass(RetryableHTTPError, PortalError)


def _redirect(
    handler: CredentialAwareRedirectHandler, req_url: str, new_url: str, headers: dict[str, str]
) -> urllib.request.Request | None:
    req = urllib.request.Request(req_url, headers=headers)
    return handler.redirect_request(req, BytesIO(b""), 302, "Found", HTTPMessage(), new_url)


def test_redirect_handler_strips_authorization_cross_host() -> None:
    handler = CredentialAwareRedirectHandler()
    new_req = _redirect(
        handler, "http://a.example.com/start", "http://b.example.com/target", {"Authorization": "Bearer secret"}
    )
    assert new_req is not None
    assert "Authorization" not in new_req.headers


def test_redirect_handler_retains_authorization_same_origin() -> None:
    handler = CredentialAwareRedirectHandler()
    new_req = _redirect(
        handler, "http://a.example.com/start", "http://a.example.com/target", {"Authorization": "Bearer secret"}
    )
    assert new_req is not None
    assert new_req.headers["Authorization"] == "Bearer secret"


def test_redirect_handler_strips_on_scheme_downgrade() -> None:
    handler = CredentialAwareRedirectHandler()
    new_req = _redirect(
        handler, "https://a.example.com/start", "http://a.example.com/target", {"Authorization": "Bearer secret"}
    )
    assert new_req is not None
    assert "Authorization" not in new_req.headers


def test_redirect_handler_scope_allows_listed_host() -> None:
    scope = CredentialScope(allowed_hosts=("b.example.com",), allowed_schemes=("http",), send_on_redirect=True)
    handler = CredentialAwareRedirectHandler(scope)
    new_req = _redirect(
        handler, "http://a.example.com/start", "http://b.example.com/target", {"Authorization": "Bearer secret"}
    )
    assert new_req is not None
    assert new_req.headers["Authorization"] == "Bearer secret"


def test_redirect_handler_scope_strips_unlisted_host() -> None:
    scope = CredentialScope(allowed_hosts=("c.example.com",), allowed_schemes=("http",), send_on_redirect=True)
    handler = CredentialAwareRedirectHandler(scope)
    new_req = _redirect(
        handler, "http://a.example.com/start", "http://b.example.com/target", {"Authorization": "Bearer secret"}
    )
    assert new_req is not None
    assert "Authorization" not in new_req.headers
