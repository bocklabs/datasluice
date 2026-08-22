"""Tests for explicit and opt-in environment credential discovery."""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from datasluice.domain.catalog.auth import (
    CatalogCredential,
    CKANCredential,
    CredentialResolutionPolicy,
    CredentialSource,
    SecretValue,
    SocrataCredential,
    UDataCredential,
)
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import discover_enabled
from datasluice.runtime.credentials.environment import EnvironmentCredentialProvider
from datasluice.runtime.credentials.explicit import explicit_resolver


def test_explicit_credentials_win_over_discovered_keychain_credentials() -> None:
    explicit = CKANCredential(api_token="explicit-secret")

    resolved = explicit_resolver(explicit).resolve(
        {CredentialSource.KEYCHAIN: CKANCredential(api_token="keychain-secret")},
        policy=CredentialResolutionPolicy(enabled_sources=frozenset({CredentialSource.KEYCHAIN})),
    )

    assert resolved is explicit


def test_default_policy_does_not_call_any_provider() -> None:
    provider = _CountingProvider()

    discovered = discover_enabled(
        {CredentialSource.ENVIRONMENT: provider},
        platform=CatalogPlatform.CKAN,
        context={},
    )

    assert discovered == {}
    assert provider.calls == 0


def test_empty_enabled_sources_policy_does_not_call_any_provider() -> None:
    provider = _CountingProvider()

    discovered = discover_enabled(
        {CredentialSource.ENVIRONMENT: provider},
        platform=CatalogPlatform.CKAN,
        context={},
        policy=CredentialResolutionPolicy(enabled_sources=frozenset()),
    )

    assert discovered == {}
    assert provider.calls == 0


def test_unset_environment_variable_discovers_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATASLUICE_CKAN_API_TOKEN", raising=False)

    discovered = _discover_ckan_environment()

    assert discovered == {}


def test_empty_environment_variable_is_treated_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATASLUICE_CKAN_API_TOKEN", "")

    discovered = _discover_ckan_environment()

    assert discovered == {}


def test_unmapped_platform_environment_discovery_returns_no_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Platforms outside the fixed allowlist discover nothing, even with mapped variables set."""
    monkeypatch.setenv("DATASLUICE_CKAN_API_TOKEN", "orphan-secret")

    discovered = discover_enabled(
        {CredentialSource.ENVIRONMENT: EnvironmentCredentialProvider()},
        platform=CatalogPlatform("other"),
        context={},
        policy=CredentialResolutionPolicy(enabled_sources=frozenset({CredentialSource.ENVIRONMENT})),
    )

    assert discovered == {}


def _discover_ckan_environment() -> Mapping[CredentialSource, CatalogCredential]:
    return discover_enabled(
        {CredentialSource.ENVIRONMENT: EnvironmentCredentialProvider()},
        platform=CatalogPlatform.CKAN,
        context={},
        policy=CredentialResolutionPolicy(enabled_sources=frozenset({CredentialSource.ENVIRONMENT})),
    )


@pytest.mark.parametrize(
    ("platform", "name", "secret", "credential_type", "attribute"),
    [
        (CatalogPlatform.CKAN, "DATASLUICE_CKAN_API_TOKEN", "ckan-secret", CKANCredential, "api_token"),
        (CatalogPlatform.UDATA, "DATASLUICE_UDATA_API_KEY", "udata-secret", UDataCredential, "api_key"),
        (CatalogPlatform.SOCRATA, "DATASLUICE_SOCRATA_APP_TOKEN", "socrata-secret", SocrataCredential, "app_token"),
    ],
)
def test_environment_provider_discovers_documented_platform_secret(
    monkeypatch: pytest.MonkeyPatch,
    platform: CatalogPlatform,
    name: str,
    secret: str,
    credential_type: type[CatalogCredential],
    attribute: str,
) -> None:
    monkeypatch.setenv(name, secret)

    discovered = discover_enabled(
        {CredentialSource.ENVIRONMENT: EnvironmentCredentialProvider()},
        platform=platform,
        context={},
        policy=CredentialResolutionPolicy(enabled_sources=frozenset({CredentialSource.ENVIRONMENT})),
    )

    credential = discovered[CredentialSource.ENVIRONMENT]
    assert isinstance(credential, credential_type)
    assert isinstance(getattr(credential, attribute), SecretValue)
    assert getattr(credential, attribute).reveal() == secret


class _CountingProvider:
    def __init__(self) -> None:
        self.calls = 0

    def discover(
        self,
        platform: CatalogPlatform,
        context: Mapping[str, object],
    ) -> Mapping[CredentialSource, CatalogCredential]:
        del platform, context
        self.calls += 1
        return {}
