"""Tests for opt-in OS keychain credential discovery."""

from __future__ import annotations

import sys

import pytest

from datasluice.domain.catalog.auth import (
    CKANCredential,
    CredentialSource,
    SecretValue,
    SocrataCredential,
    UDataCredential,
)
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import CredentialResolutionError
from datasluice.runtime.credentials.keychain import KeychainCredentialProvider


def test_missing_keyring_names_the_keychain_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "keyring", None)

    with pytest.raises(ImportError, match=r"datasluice\[keychain\]"):
        KeychainCredentialProvider().discover(CatalogPlatform.CKAN, {})


@pytest.mark.parametrize(
    ("platform", "username", "credential_type", "attribute"),
    [
        (CatalogPlatform.CKAN, "ckan-api-token", CKANCredential, "api_token"),
        (CatalogPlatform.UDATA, "udata-api-key", UDataCredential, "api_key"),
        (CatalogPlatform.SOCRATA, "socrata-app-token", SocrataCredential, "app_token"),
    ],
)
def test_password_returning_keyring_discovers_exact_credential_keys(
    platform: CatalogPlatform,
    username: str,
    credential_type: type,
    attribute: str,
) -> None:
    calls: list[tuple[str, str]] = []

    def get_password(service: str, item: str) -> str | None:
        calls.append((service, item))
        return "keychain-secret"

    discovered = KeychainCredentialProvider(get_password).discover(platform, {})

    assert set(discovered) == {CredentialSource.KEYCHAIN}
    credential = discovered[CredentialSource.KEYCHAIN]
    assert calls == [("datasluice", username)]
    assert isinstance(credential, credential_type)
    assert isinstance(getattr(credential, attribute), SecretValue)
    assert getattr(credential, attribute).reveal() == "keychain-secret"


def test_unmapped_platforms_discover_no_credentials() -> None:
    discovered = KeychainCredentialProvider(lambda service, username: "unused").discover(CatalogPlatform("other"), {})

    assert discovered == {}


def test_missing_keychain_password_does_not_discover_credentials() -> None:
    discovered = KeychainCredentialProvider(lambda service, username: None).discover(CatalogPlatform.CKAN, {})

    assert discovered == {}


def test_empty_keychain_password_does_not_discover_credentials() -> None:
    discovered = KeychainCredentialProvider(lambda service, username: "").discover(CatalogPlatform.CKAN, {})

    assert discovered == {}


def test_keychain_backend_errors_are_redacted() -> None:
    def get_password(service: str, username: str) -> str | None:
        raise RuntimeError("keychain-secret")

    with pytest.raises(CredentialResolutionError, match=r"details redacted: \*\*\*") as exc_info:
        KeychainCredentialProvider(get_password).discover(CatalogPlatform.CKAN, {})

    assert "keychain-secret" not in str(exc_info.value)
