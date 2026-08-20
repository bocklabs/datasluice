"""Unit tests for the CredentialScope model and credential-aware redirect handling."""

from __future__ import annotations

import dataclasses

import pytest

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
        scope.send_on_redirect = True  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime


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
