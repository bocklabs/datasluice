"""Explicit typed credential and permission contracts for catalog connectors."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from collections.abc import Mapping, Set
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from urllib.parse import urlsplit

from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.errors.catalog import ForbiddenError, UnauthenticatedError


class SecretValue:
    """A secret whose display representation cannot reveal its value."""

    __slots__ = ("_value",)
    _value: str

    def __init__(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("Credential secret values must be non-empty strings.")
        object.__setattr__(self, "_value", value)

    def reveal(self) -> str:
        """Return the secret value for an authenticated transport boundary."""
        return self._value

    def __repr__(self) -> str:
        """Return a redacted secret representation."""
        return "SecretValue(***)"

    def __str__(self) -> str:
        """Return a redacted secret display value."""
        return "***"


def _secret(value: SecretValue | str) -> SecretValue:
    return value if isinstance(value, SecretValue) else SecretValue(value)


@dataclass(frozen=True, slots=True)
class CKANCredential:
    """A CKAN API-token credential."""

    api_token: SecretValue | str

    def __post_init__(self) -> None:
        object.__setattr__(self, "api_token", _secret(self.api_token))


@dataclass(frozen=True, slots=True)
class UDataCredential:
    """A uData API-key credential."""

    api_key: SecretValue | str

    def __post_init__(self) -> None:
        object.__setattr__(self, "api_key", _secret(self.api_key))


@dataclass(frozen=True, slots=True)
class SocrataCredential:
    """A Socrata application token with optional account credentials."""

    app_token: SecretValue | str
    username: str | None = None
    password: SecretValue | str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "app_token", _secret(self.app_token))
        if self.username is not None and (not isinstance(self.username, str) or not self.username):
            raise ValueError("Socrata usernames must be non-empty strings when supplied.")
        if self.password is not None:
            object.__setattr__(self, "password", _secret(self.password))
        if (self.username is None) != (self.password is None):
            raise ValueError("Socrata username and password must be supplied together.")


type CatalogCredential = CKANCredential | UDataCredential | SocrataCredential


class CredentialSource(StrEnum):
    """Optional credential discovery source."""

    ENVIRONMENT = "environment"
    KEYCHAIN = "keychain"
    SECRET_MANAGER = "secret-manager"


@dataclass(frozen=True, slots=True)
class CredentialResolutionPolicy:
    """Opt-in policy selecting credential discovery sources."""

    enabled_sources: frozenset[CredentialSource] = frozenset()

    def __post_init__(self) -> None:
        if not isinstance(self.enabled_sources, frozenset) or not all(
            isinstance(source, CredentialSource) for source in self.enabled_sources
        ):
            raise ValueError("Credential discovery sources must be an immutable CredentialSource set.")


_NO_CREDENTIAL_DISCOVERY = CredentialResolutionPolicy()

_CREDENTIAL_SCOPE_KEY = secrets.token_bytes(32)


@dataclass(frozen=True, slots=True)
class CredentialResolver:
    """Resolve explicit credentials first and use discovery only when selected."""

    explicit: CatalogCredential | None = None

    def __post_init__(self) -> None:
        credential_types = CKANCredential | UDataCredential | SocrataCredential
        if self.explicit is not None and not isinstance(self.explicit, credential_types):
            raise ValueError("Explicit credentials must use a supported catalog credential type.")

    def resolve(
        self,
        discovered: Mapping[CredentialSource, CatalogCredential],
        *,
        policy: CredentialResolutionPolicy | None = None,
    ) -> CatalogCredential | None:
        """Return explicit credentials or the first credential enabled by policy."""
        if self.explicit is not None:
            return self.explicit
        policy = policy or _NO_CREDENTIAL_DISCOVERY
        for source in CredentialSource:
            if source in policy.enabled_sources and source in discovered:
                return discovered[source]
        return None


def credential_scope(credentials: object | None) -> str:
    """Return a stable, non-reversible scope for one credential identity."""
    credential = credentials.explicit if isinstance(credentials, CredentialResolver) else credentials
    if credential is None:
        return "anonymous"
    dataclass_fields = getattr(credential, "__dataclass_fields__", None)
    if not isinstance(dataclass_fields, dict) or not dataclass_fields:
        return "anonymous"
    digest = hmac.new(_CREDENTIAL_SCOPE_KEY, digestmod=hashlib.sha256)
    for name in dataclass_fields:
        value = getattr(credential, name)
        digest.update(name.encode())
        digest.update((value.reveal() if isinstance(value, SecretValue) else str(value)).encode())
    return f"{type(credential).__name__.lower()}-{digest.hexdigest()[:16]}"


@dataclass(frozen=True, slots=True)
class OAuthFlow:
    """A public OAuth descriptor that never stores client or refresh secrets."""

    authorization_url: str
    token_url: str
    client_id: str
    scopes: frozenset[str] = frozenset()
    supports_refresh: bool = True
    redirect_uri: str | None = None

    def __post_init__(self) -> None:
        for name, url in (("authorization", self.authorization_url), ("token", self.token_url)):
            parsed = urlsplit(url)
            if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
                raise ValueError(f"OAuth {name} URL must be a sanitized HTTPS URI.")
        if self.redirect_uri is not None:
            parsed = urlsplit(self.redirect_uri)
            if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
                raise ValueError("OAuth redirect URI must be a sanitized HTTPS URI.")
        if not isinstance(self.client_id, str) or not self.client_id:
            raise ValueError("OAuth client IDs must be non-empty strings.")
        if not isinstance(self.scopes, frozenset) or not all(isinstance(scope, str) and scope for scope in self.scopes):
            raise ValueError("OAuth scopes must be immutable non-empty strings.")
        if type(self.supports_refresh) is not bool:
            raise ValueError("OAuth refresh support must be a boolean.")

    @classmethod
    def authorization_code(
        cls, authorization_url: str, token_url: str, client_id: str, redirect_uri: str | None = None
    ) -> OAuthFlow:
        """Create a standard authorization-code OAuth flow descriptor."""
        return cls(
            authorization_url=authorization_url,
            token_url=token_url,
            client_id=client_id,
            redirect_uri=redirect_uri,
        )


@dataclass(frozen=True, slots=True)
class EffectivePermissions:
    """Known effective scopes and roles used for deterministic pre-dispatch guards."""

    platform: CatalogPlatform
    scopes: frozenset[str] = frozenset()
    roles: frozenset[str] = frozenset()
    authenticated: bool = False
    operation_scopes: Mapping[str, frozenset[str]] = field(default_factory=dict)
    credential_scope: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.platform, CatalogPlatform):
            raise ValueError("Effective permissions require a CatalogPlatform.")
        for values, name in ((self.scopes, "scope"), (self.roles, "role")):
            if not isinstance(values, frozenset) or not all(isinstance(value, str) and value for value in values):
                raise ValueError(f"Effective permission {name}s must be immutable non-empty strings.")
        if type(self.authenticated) is not bool:
            raise ValueError("Effective permission authentication state must be a boolean.")
        if self.credential_scope is not None and (
            not isinstance(self.credential_scope, str) or not self.credential_scope
        ):
            raise ValueError("Effective permission credential scope must be a non-empty string when supplied.")
        operations = {operation: frozenset(required) for operation, required in self.operation_scopes.items()}
        if not all(isinstance(operation, str) and operation for operation in operations):
            raise ValueError("Operation permission requirements must use non-empty operation names.")
        object.__setattr__(self, "operation_scopes", MappingProxyType(operations))

    @classmethod
    def for_credential(
        cls,
        credential: CatalogCredential,
        *,
        platform: CatalogPlatform,
        scopes: frozenset[str] = frozenset(),
        roles: frozenset[str] = frozenset(),
        authenticated: bool = True,
        operation_scopes: Mapping[str, frozenset[str]] | None = None,
    ) -> EffectivePermissions:
        """Bind effective permission evidence to one explicit credential identity."""
        return cls(
            platform=platform,
            scopes=scopes,
            roles=roles,
            authenticated=authenticated,
            operation_scopes=operation_scopes or {},
            credential_scope=credential_scope(credential),
        )

    def require(
        self,
        operation: str,
        *,
        scopes: Set[str] = frozenset(),
        roles: Set[str] = frozenset(),
    ) -> None:
        """Raise a distinct normalized error when known permissions reject an operation."""
        if not isinstance(operation, str) or not operation:
            raise ValueError("Permission guards require a non-empty operation name.")
        required_scopes = frozenset(scopes) | self.operation_scopes.get(operation, frozenset())
        required_roles = frozenset(roles)
        if not self.authenticated and not self.scopes and not self.roles:
            raise UnauthenticatedError(
                "Credentials are required for this catalog operation.",
                operation=operation,
                platform=self.platform,
                capability_state="unauthorized",
                safe_action="Provide valid credentials and retry the operation.",
            )
        if not required_scopes.issubset(self.scopes) or not required_roles.issubset(self.roles):
            raise ForbiddenError(
                "Known credentials do not include the required catalog permission.",
                operation=operation,
                platform=self.platform,
                capability_state="forbidden",
                safe_action="Use credentials with the required scope or role.",
            )
