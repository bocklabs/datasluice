from __future__ import annotations

from dataclasses import asdict

import pytest

from datasluice.domain.catalog.auth import (
    CKANCredential,
    CredentialResolutionPolicy,
    CredentialResolver,
    CredentialSource,
    EffectivePermissions,
    OAuthFlow,
    SocrataCredential,
    UDataCredential,
)
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.errors.catalog import (
    CatalogRateLimitError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
    map_catalog_error,
)


def test_platform_credentials_are_typed_and_explicit_injection_wins() -> None:
    explicit = CKANCredential(api_token="explicit-secret")
    resolver = CredentialResolver(explicit=explicit)

    assert resolver.resolve({CredentialSource.ENVIRONMENT: UDataCredential(api_key="environment-secret")}) is explicit
    assert "explicit-secret" not in repr(explicit)
    assert "explicit-secret" not in str(asdict(explicit))
    assert SocrataCredential(app_token="app-token", username="user", password="password").username == "user"


def test_discovery_is_disabled_until_a_source_is_selected() -> None:
    discovered = UDataCredential(api_key="environment-secret")
    resolver = CredentialResolver()
    policy = CredentialResolutionPolicy(enabled_sources=frozenset({CredentialSource.ENVIRONMENT}))

    assert resolver.resolve({CredentialSource.ENVIRONMENT: discovered}) is None
    assert resolver.resolve({CredentialSource.ENVIRONMENT: discovered}, policy=policy) is discovered
    assert OAuthFlow.authorization_code("https://issuer.example/authorize", "https://issuer.example/token", "client")


def test_effective_permissions_distinguish_missing_and_insufficient_access() -> None:
    permissions = EffectivePermissions(
        platform=CatalogPlatform.CKAN,
        scopes=frozenset({"dataset:read"}),
        roles=frozenset({"user"}),
    )

    with pytest.raises(UnauthenticatedError):
        EffectivePermissions(platform=CatalogPlatform.CKAN).require("datasets.update", scopes={"dataset:write"})
    with pytest.raises(ForbiddenError):
        permissions.require("datasets.update", scopes={"dataset:write"}, roles={"admin"})
    permissions.require("datasets.get", scopes={"dataset:read"}, roles={"user"})


def test_native_and_normalized_errors_are_bounded_redacted_and_actionable() -> None:
    native = NativeCatalogError(
        message="vendor rejected request",
        operation="datasets.update",
        platform=CatalogPlatform.CKAN,
        status_code=429,
        vendor_code="rate_limited",
        metadata={"request_id": "abc", "api_token": "secret", "body": "very sensitive"},
    )

    assert native.metadata["api_token"] == "***"
    assert native.metadata["body"] == "***"
    assert "secret" not in str(native)
    error = map_catalog_error(native)
    assert isinstance(error, CatalogRateLimitError)
    assert error.safe_action
    assert error.__cause__ is native
