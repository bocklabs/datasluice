"""Immutable construction settings for the uData live clients."""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

from datasluice.domain.catalog.auth import CredentialResolver, UDataCredential
from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.capability import AsyncProbeRunner, ProbeRunner
from datasluice.runtime.clients import AsyncCatalogTransport
from datasluice.runtime.constants import DEFAULT_CAPABILITY_CACHE_TTL_SECONDS
from datasluice.runtime.resilience import BreakerRegistry
from datasluice.runtime.transport.base import CatalogTransport

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})

type SyncTransportOverride = CatalogTransport | Callable[[], CatalogTransport]
type AsyncTransportOverride = AsyncCatalogTransport | Callable[[], AsyncCatalogTransport]


def normalize_origin(value: str) -> str:
    """Normalize one uData deployment base URL without keeping a mount path.

    Args:
        value: The caller-supplied base URL.

    Returns:
        The normalized URL carrying scheme and netloc only.

    Raises:
        ValueError: If the URL is not a sanitized HTTP(S) origin, or an http
            origin targets a non-loopback host.
    """
    if not isinstance(value, str) or not value:
        raise ValueError("uData client settings require a non-empty base URL string.")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("uData base URLs must be absolute HTTP(S) origins.")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("uData base URLs cannot carry userinfo credentials.")
    if parsed.query or parsed.fragment:
        raise ValueError("uData base URLs cannot carry query strings or fragments.")
    if parsed.scheme == "http" and parsed.hostname not in LOOPBACK_HOSTS:
        raise ValueError("Plain-text HTTP origins are restricted to loopback uData deployments.")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), "", "", ""))


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
class UDataClientSettings:
    """The immutable install-and-use contract for constructing uData clients.

    Transport ownership is explicit per mode: an injected transport instance
    is borrowed by constructed clients and never closed by them, while a
    transport factory (or no override at all) produces a client-owned
    transport that the client closes exactly once.
    """

    base_url: str
    credential: UDataCredential | CredentialResolver | None = None
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
    capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", normalize_origin(self.base_url))
        if self.credential is not None and not isinstance(self.credential, UDataCredential | CredentialResolver):
            raise TypeError("uData client settings require a uData credential or resolver.")
        for field_name in ("sync_transport", "async_transport"):
            override = getattr(self, field_name)
            if override is None:
                continue
            role = _transport_role(override)
            if role == "invalid":
                raise TypeError(f"uData {field_name} must be a transport instance or a zero-argument factory.")
            if role == "ambiguous":
                raise TypeError(f"uData {field_name} cannot be both a transport instance and a factory.")
        if self.tls_policy is not None and not isinstance(self.tls_policy, TLSPolicy):
            raise TypeError("uData client TLS policy must use TLSPolicy.")
        if self.budget is not None and not isinstance(self.budget, TimeBudget):
            raise TypeError("uData client budgets must use TimeBudget.")
        if self.breakers is not None and not isinstance(self.breakers, BreakerRegistry):
            raise TypeError("uData client breakers must use BreakerRegistry.")
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("uData client attempts require a positive integer.")
        if self.retry_sleep is not None and not callable(self.retry_sleep):
            raise TypeError("uData sync retry sleep must be callable.")
        if self.async_retry_sleep is not None and not callable(self.async_retry_sleep):
            raise TypeError("uData async retry sleep must be callable.")
        if self.probe_runner is not None and not isinstance(self.probe_runner, ProbeRunner):
            raise TypeError("uData probe runners must implement ProbeRunner.")
        if self.async_probe_runner is not None and not isinstance(self.async_probe_runner, AsyncProbeRunner):
            raise TypeError("uData async probe runners must implement AsyncProbeRunner.")
        if (
            type(self.capability_cache_ttl) not in (int, float)
            or not math.isfinite(self.capability_cache_ttl)
            or self.capability_cache_ttl < 0
        ):
            raise ValueError("Capability cache TTL must be a finite non-negative number.")

    @property
    def owns_sync_transport(self) -> bool:
        """Return whether factory-constructed sync transports are client-owned."""
        return self.sync_transport is None or _transport_role(self.sync_transport) == "factory"

    @property
    def owns_async_transport(self) -> bool:
        """Return whether factory-constructed async transports are client-owned."""
        role = _transport_role(self.async_transport) if self.async_transport is not None else "none"
        return self.async_transport is None or role == "factory"
