"""Lazy default builders for the catalog runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.runtime.extras import require_extra
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
    try:
        require_extra("http")
    except ImportError:
        return UrllibCatalogTransport(tls_policy=tls_policy, budget=budget)
    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport

    return HttpxCatalogTransport(tls_policy=tls_policy, budget=budget)


def create_default_async_transport(
    *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None
) -> AsyncHttpxCatalogTransport:
    """Create the optional async runtime transport with an actionable error."""
    require_extra("http")
    from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport

    return AsyncHttpxCatalogTransport(tls_policy=tls_policy, budget=budget)
