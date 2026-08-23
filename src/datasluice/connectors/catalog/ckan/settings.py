"""Immutable construction settings for the CKAN live clients."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

from datasluice.domain.catalog.auth import (
    CatalogCredential,
    CKANCredential,
    CredentialResolver,
    SocrataCredential,
    UDataCredential,
)
from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.capability import AsyncProbeRunner, ProbeRunner
from datasluice.runtime.clients import AsyncCatalogTransport
from datasluice.runtime.constants import (
    DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
)
from datasluice.runtime.resilience import BreakerRegistry
from datasluice.runtime.transport.base import CatalogTransport

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost"})
PROBE_POLICIES = ("auto", "declared-baseline")

type SyncTransportOverride = CatalogTransport | Callable[[], CatalogTransport]
type AsyncTransportOverride = AsyncCatalogTransport | Callable[[], AsyncCatalogTransport]


@runtime_checkable
class PortalRatePolicy(Protocol):
    """Structural contract satisfied by connector rate-policy records."""

    source_note: str


def normalize_origin(value: str) -> str:
    """Normalize one deployment base URL to its scheme and netloc origin form.

    Args:
        value: The caller-supplied base URL.

    Returns:
        The origin-form URL carrying only scheme and netloc.

    Raises:
        ValueError: If the URL is not a sanitized HTTP(S) origin, or an http
            origin targets a non-loopback host.
    """
    if not isinstance(value, str) or not value:
        raise ValueError("CKAN client settings require a non-empty base URL string.")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("CKAN base URLs must be absolute HTTP(S) origins.")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("CKAN base URLs cannot carry userinfo credentials.")
    if parsed.query or parsed.fragment:
        raise ValueError("CKAN base URLs cannot carry query strings or fragments.")
    if parsed.scheme == "http" and parsed.hostname not in LOOPBACK_HOSTS:
        raise ValueError("Plain-text HTTP origins are restricted to loopback CKAN deployments.")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _transport_role(value: object) -> str:
    has_send = hasattr(value, "send")
    if callable(value) and has_send:
        return "ambiguous"
    if callable(value):
        return "factory"
    if has_send:
        return "instance"
    return "invalid"


@dataclass(frozen=True, slots=True)
class CKANClientSettings:
    """The immutable install-and-use contract for constructing CKAN clients.

    Transport ownership is explicit per mode: an injected transport instance
    is borrowed by constructed clients and never closed by them, while a
    transport factory (or no override at all) produces a client-owned
    transport that the client closes exactly once.
    """

    base_url: str
    credential: CatalogCredential | CredentialResolver | None = None
    sync_transport: SyncTransportOverride | None = None
    async_transport: AsyncTransportOverride | None = None
    tls_policy: TLSPolicy | None = None
    budget: TimeBudget | None = None
    breakers: BreakerRegistry | None = None
    max_attempts: int = 3
    retry_sleep: Callable[[float], None] | None = None
    async_retry_sleep: Callable[[float], Awaitable[None]] | None = None
    probe_runner: ProbeRunner | None = None
    async_probe_runner: AsyncProbeRunner | None = None
    probe_policy: Literal["auto", "declared-baseline"] = "auto"
    capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS
    rate_policy: PortalRatePolicy | None = None
    max_upload_bytes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", normalize_origin(self.base_url))
        if self.credential is not None and not isinstance(
            self.credential, CKANCredential | UDataCredential | SocrataCredential | CredentialResolver
        ):
            raise TypeError("CKAN client settings require a typed catalog credential or resolver.")
        for field_name in ("sync_transport", "async_transport"):
            override = getattr(self, field_name)
            if override is None:
                continue
            role = _transport_role(override)
            if role == "invalid":
                raise TypeError(f"CKAN {field_name} must be a transport instance or a zero-argument factory.")
            if role == "ambiguous":
                raise TypeError(f"CKAN {field_name} cannot be both a transport instance and a factory.")
        if self.tls_policy is not None and not isinstance(self.tls_policy, TLSPolicy):
            raise TypeError("CKAN client TLS policy must use TLSPolicy.")
        if self.budget is not None and not isinstance(self.budget, TimeBudget):
            raise TypeError("CKAN client budgets must use TimeBudget.")
        if self.breakers is not None and not isinstance(self.breakers, BreakerRegistry):
            raise TypeError("CKAN client breakers must use BreakerRegistry.")
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("CKAN client attempts require a positive integer.")
        if self.retry_sleep is not None and not callable(self.retry_sleep):
            raise TypeError("CKAN sync retry sleep must be callable.")
        if self.async_retry_sleep is not None and not callable(self.async_retry_sleep):
            raise TypeError("CKAN async retry sleep must be callable.")
        if self.probe_runner is not None and not isinstance(self.probe_runner, ProbeRunner):
            raise TypeError("CKAN probe runners must implement ProbeRunner.")
        if self.async_probe_runner is not None and not isinstance(self.async_probe_runner, AsyncProbeRunner):
            raise TypeError("CKAN async probe runners must implement AsyncProbeRunner.")
        if self.probe_policy not in PROBE_POLICIES:
            raise ValueError("CKAN probe policies are 'auto' or 'declared-baseline'.")
        if (
            type(self.capability_cache_ttl) not in (int, float)
            or self.capability_cache_ttl != self.capability_cache_ttl
            or self.capability_cache_ttl < 0
        ):
            raise ValueError("Capability cache TTL must be a finite non-negative number.")
        if self.rate_policy is not None and not isinstance(self.rate_policy, PortalRatePolicy):
            raise TypeError("CKAN rate policies must satisfy the PortalRatePolicy record contract.")
        if self.max_upload_bytes is not None and (type(self.max_upload_bytes) is not int or self.max_upload_bytes < 1):
            raise ValueError("Upload byte ceilings must be positive integers when supplied.")

    @property
    def owns_sync_transport(self) -> bool:
        """Return whether factory-constructed sync transports are client-owned."""
        return self.sync_transport is None or _transport_role(self.sync_transport) == "factory"

    @property
    def owns_async_transport(self) -> bool:
        """Return whether factory-constructed async transports are client-owned."""
        role = _transport_role(self.async_transport) if self.async_transport is not None else "none"
        return self.async_transport is None or role == "factory"
