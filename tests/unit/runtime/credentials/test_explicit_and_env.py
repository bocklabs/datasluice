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
