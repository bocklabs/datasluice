"""Tests for opt-in OS keychain credential discovery."""

from __future__ import annotations

import sys

import pytest

from datasluice.domain.catalog.auth import CKANCredential, CredentialSource, SecretValue
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import CredentialResolutionError
from datasluice.runtime.credentials.keychain import KeychainCredentialProvider


def test_missing_keyring_names_the_keychain_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "keyring", None)

    with pytest.raises(ImportError, match=r"datasluice\[keychain\]"):
        KeychainCredentialProvider().discover(CatalogPlatform.CKAN, {})


def test_password_returning_keyring_discovers_secret_value() -> None:
    calls: list[tuple[str, str]] = []

    def get_password(service: str, username: str) -> str | None:
        calls.append((service, username))
        return "keychain-secret"

    discovered = KeychainCredentialProvider(get_password).discover(CatalogPlatform.CKAN, {})

    credential = discovered[CredentialSource.KEYCHAIN]
    assert calls == [("datasluice", "ckan-api-token")]
    assert isinstance(credential, CKANCredential)
    assert isinstance(credential.api_token, SecretValue)
    assert credential.api_token.reveal() == "keychain-secret"


def test_missing_keychain_password_does_not_discover_credentials() -> None:
    discovered = KeychainCredentialProvider(lambda service, username: None).discover(CatalogPlatform.CKAN, {})

    assert discovered == {}


def test_keychain_backend_errors_are_redacted() -> None:
    def get_password(service: str, username: str) -> str | None:
        raise RuntimeError("keychain-secret")

    with pytest.raises(CredentialResolutionError, match=r"details redacted: \*\*\*") as exc_info:
        KeychainCredentialProvider(get_password).discover(CatalogPlatform.CKAN, {})

    assert "keychain-secret" not in str(exc_info.value)
