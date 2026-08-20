"""Lazy default builders for the catalog runtime."""

from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING

from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport

if TYPE_CHECKING:
    from datasluice.domain.catalog.observability import TLSPolicy
    from datasluice.domain.catalog.resilience import TimeBudget
    from datasluice.runtime.transport.base import CatalogTransport
    from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport


def create_default_sync_transport(
    *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None
) -> CatalogTransport:
    """Create the richest installed synchronous runtime transport."""
    if importlib.util.find_spec("httpx") is None:
        return UrllibCatalogTransport(tls_policy=tls_policy, budget=budget)
    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport

    return HttpxCatalogTransport(tls_policy=tls_policy, budget=budget)


def create_default_async_transport(
    *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None
) -> AsyncHttpxCatalogTransport:
    """Create the optional async runtime transport with an actionable error."""
    if importlib.util.find_spec("httpx") is None:
        raise ImportError("Async catalog clients require the HTTP extra: install datasluice[http].")
    from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport

    return AsyncHttpxCatalogTransport(tls_policy=tls_policy, budget=budget)
