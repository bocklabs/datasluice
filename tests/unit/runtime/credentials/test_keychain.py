"""Tests for opt-in OS keychain credential discovery."""

from __future__ import annotations

import sys
import traceback

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


def test_keychain_backend_errors_redact_the_entire_cause_chain() -> None:
    """Neither the message nor any __cause__ link may carry the raw backend message."""
    planted_secret = "keychain-cause-chain-secret"

    def get_password(service: str, username: str) -> str | None:
        raise RuntimeError(planted_secret)

    with pytest.raises(CredentialResolutionError, match=r"details redacted: \*\*\*") as exc_info:
        KeychainCredentialProvider(get_password).discover(CatalogPlatform.CKAN, {})

    rendered = _redaction_surface(exc_info.value)
    assert planted_secret not in rendered


@pytest.mark.parametrize(
    ("planted_secret", "backend_message"),
    [
        ("raw-userinfo-pass", "connect failed for https://reader:raw-userinfo-pass@keychain.example/ckan"),
        ("raw-query-key", "lookup failed: https://backend.test/?api_key=raw-query-key&x=1"),
        ("raw-bearer-token-123", "token rejected: Bearer raw-bearer-token-123"),
    ],
    ids=["userinfo-url", "credential-query-param", "bearer-scheme"],
)
def test_keychain_backend_errors_redact_credential_shaped_cause_chain(
    planted_secret: str,
    backend_message: str,
) -> None:
    """Credential-shaped backend messages are scrubbed from the error and every cause link."""

    def get_password(service: str, username: str) -> str | None:
        raise RuntimeError(backend_message)

    with pytest.raises(CredentialResolutionError, match=r"details redacted: \*\*\*") as exc_info:
        KeychainCredentialProvider(get_password).discover(CatalogPlatform.CKAN, {})

    assert planted_secret not in _redaction_surface(exc_info.value)


def _redaction_surface(error: BaseException) -> str:
    """Render the message, exception-only traceback, and visible exception chain of *error*."""
    surfaces = [str(error), *traceback.format_exception_only(type(error), error)]
    linked: BaseException | None = error.__cause__
    if linked is None and not error.__suppress_context__:
        linked = error.__context__
    while linked is not None:
        surfaces.append(str(linked))
        surfaces.extend(traceback.format_exception_only(type(linked), linked))
        linked = linked.__cause__
    return "\n".join(surfaces)
