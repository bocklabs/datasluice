"""Explicit, opt-in credential discovery providers for catalog runtimes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

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
from datasluice.errors.catalog import CatalogValidationError


class DiscoveryProvider(Protocol):
    """Discover a credential for one platform using caller-owned context."""

    def discover(
        self,
        platform: CatalogPlatform,
        context: Mapping[str, object],
    ) -> Mapping[CredentialSource, CatalogCredential]:
        """Return credentials discovered for one platform without global state."""


class CredentialResolutionError(CatalogValidationError):
    """Raised when an enabled credential source cannot resolve safely."""


def discover_enabled(
    providers: Mapping[CredentialSource, DiscoveryProvider],
    *,
    platform: CatalogPlatform,
    context: Mapping[str, object],
    policy: CredentialResolutionPolicy | None = None,
) -> dict[CredentialSource, CatalogCredential]:
    """Discover only the credential sources explicitly enabled by policy."""
    if policy is None:
        return {}
    discovered: dict[CredentialSource, CatalogCredential] = {}
    for source in CredentialSource:
        if source in policy.enabled_sources and (provider := providers.get(source)) is not None:
            try:
                discovered.update(provider.discover(platform, context))
            except (CredentialResolutionError, ImportError):
                raise
            except Exception as exc:
                raise _resolution_error(source.value, platform, exc) from None
    return discovered


def credential_from_secret(platform: CatalogPlatform, secret: str) -> CatalogCredential:
    """Build the platform credential that owns one discovered secret."""
    secret_value = SecretValue(secret)
    if platform == CatalogPlatform.CKAN:
        return CKANCredential(api_token=secret_value)
    if platform == CatalogPlatform.UDATA:
        return UDataCredential(api_key=secret_value)
    if platform == CatalogPlatform.SOCRATA:
        return SocrataCredential(app_token=secret_value)
    raise ValueError(f"Credential discovery is unsupported for platform {platform!s}.")


def credential_from_fields(platform: CatalogPlatform, fields: Mapping[str, object]) -> CatalogCredential:
    """Build a platform credential from a secret-manager field mapping."""
    if platform == CatalogPlatform.CKAN:
        return credential_from_secret(platform, _required_secret(fields, "api_token"))
    if platform == CatalogPlatform.UDATA:
        return credential_from_secret(platform, _required_secret(fields, "api_key"))
    if platform == CatalogPlatform.SOCRATA:
        app_token = SecretValue(_required_secret(fields, "app_token"))
        username = fields.get("username")
        password = fields.get("password")
        if username is not None and not isinstance(username, str):
            raise ValueError("Socrata secret-manager usernames must be strings.")
        if password is not None and not isinstance(password, str):
            raise ValueError("Socrata secret-manager passwords must be strings.")
        return SocrataCredential(
            app_token=app_token, username=username, password=SecretValue(password) if password else None
        )
    raise ValueError(f"Credential discovery is unsupported for platform {platform!s}.")


def _required_secret(fields: Mapping[str, object], key: str) -> str:
    value = fields.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Secret-manager credential field {key!r} must be a non-empty string.")
    return value


def _resolution_error(source: str, platform: CatalogPlatform, exc: Exception) -> CredentialResolutionError:
    return CredentialResolutionError(
        f"Unable to resolve {platform!s} credentials from {source}; details redacted: ***.",
        operation="credentials.resolve",
        platform=platform,
        safe_action="Check the selected credential source configuration and retry.",
    )


__all__: Sequence[str] = (
    "CredentialResolutionError",
    "DiscoveryProvider",
    "credential_from_fields",
    "credential_from_secret",
    "discover_enabled",
)
