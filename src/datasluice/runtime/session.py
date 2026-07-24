"""DataSluiceSession — the public facade and composition root (ARCH-03).

The session wires :class:`PluginManager` (Plan 02-03), the transport factory
(:func:`create_default_transport`), and explicit auth into a zero-config
facade. It replaces the legacy ``DataSluice`` class (D-01) and removes the
``Settings`` env-var system (D-14, CORR-04).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from datasluice.auth import NoAuth
from datasluice.config.defaults import DEFAULT_LOG_LEVEL, DEFAULT_PAGE_SIZE
from datasluice.domain import Query, SearchResult
from datasluice.logging import configure_logging, get_logger
from datasluice.runtime.context import ConnectorContext
from datasluice.runtime.defaults import create_default_transport
from datasluice.runtime.plugin_manager import PluginManager

if TYPE_CHECKING:
    from collections.abc import Callable

    from datasluice.auth import BaseAuth
    from datasluice.connectors.base import BaseAdapter
    from datasluice.ports import Transport

logger = get_logger("session")


class DataSluiceSession:
    """Public facade and composition root for DataSluice.

    Wires the :class:`PluginManager`, transport, and auth into a zero-config
    session. Every override is explicit — no env-var-driven settings (D-14).

    Args:
        auth: Authentication strategy; defaults to :class:`NoAuth` (D-11).
        transport: Optional pre-configured transport satisfying the
            :class:`Transport` port (D-02). A default :class:`HttpClient` is
            constructed when omitted.
        page_size: Default page size hint for paginated catalog calls (D-12).
        plugins: Optional :class:`PluginManager` for dependency injection (D-06).
            A fresh instance is constructed when omitted.

    Example:
        >>> from datasluice import DataSluiceSession
        >>> session = DataSluiceSession()
        >>> connector = session.portal("https://catalog.data.gov")
        >>> results = connector.search()
    """

    def __init__(
        self,
        *,
        auth: BaseAuth | None = None,
        transport: Transport | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        plugins: PluginManager | None = None,
    ) -> None:
        self.auth = auth or NoAuth()
        self.page_size = page_size
        self._transport = transport or create_default_transport(self.auth)
        self.plugins = plugins or PluginManager()
        configure_logging(DEFAULT_LOG_LEVEL)
        logger.debug("DataSluiceSession initialised with %d connector(s)", len(self.plugins.list_connectors()))

    def portal(self, url: str) -> BaseAdapter:
        """Resolve and construct a connector for *url*.

        Auto-detects the portal type via :func:`detect_portal_type` and resolves
        the factory through :class:`PluginManager`. The returned connector
        structurally conforms to :class:`CatalogPort`.

        Args:
            url: Base URL of the open-data portal.

        Returns:
            A connector instance conforming to :class:`CatalogPort`.

        Raises:
            PortalDetectionError: If the portal type cannot be determined.
            AdapterNotFoundError: If no connector is registered for the detected type.
        """
        from datasluice.discovery import detect_portal_type

        portal_type = detect_portal_type(url)
        factory = cast("Callable[[ConnectorContext], BaseAdapter]", self.plugins.get(portal_type))
        ctx = ConnectorContext(base_url=url, transport=self._transport, auth=self.auth, page_size=self.page_size)
        connector = factory(ctx)
        logger.debug("Resolved connector %s for %s", portal_type, url)
        return connector

    def search(self, url: str, query: str | Query | None = None, **kwargs: Any) -> SearchResult:
        """Convenience method: resolve a portal and search in one call.

        Args:
            url: Base URL of the open-data portal.
            query: Search text or a :class:`Query` object.
            **kwargs: Additional :class:`Query` fields (limit, tags, etc.).

        Returns:
            A :class:`SearchResult` page.
        """
        if isinstance(query, Query):
            q = query
        else:
            q = Query(text=query, **kwargs)
        return self.portal(url).search(q)

    def __repr__(self) -> str:
        return f"<DataSluiceSession(connectors={len(self.plugins.list_connectors())})>"
