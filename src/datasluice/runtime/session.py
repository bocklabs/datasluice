"""DataSluiceSession — the public facade and composition root (ARCH-03).

The session wires :class:`PluginManager` (Plan 02-03), the transport factory
(:func:`create_default_transport`), and explicit auth into a zero-config
facade. It replaces the legacy ``DataSluice`` class (D-01) and removes the
``Settings`` env-var system (D-14, CORR-04).

Phase 3 (D-P3-02): the session gains explicit kwargs (``timeout``/``retries``/
``rate_limit``/``cache_dir``/``cache_ttl``) and injectables
(``transport=``/``storage=``/``cache=``/``credential_provider=``). Scalar knobs
configure only the default-constructed transport; when ``transport=`` is
injected the scalars are ignored (D-P3-05). The session surface stays
``portal()``/``search()``-only (D-P3-22) and ``ConnectorContext`` is unchanged
(D-P3-21).
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any, cast

from datasluice.auth import NoAuth
from datasluice.config.defaults import (
    DEFAULT_CACHE_TTL,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RATE_LIMIT,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
)
from datasluice.domain import Query, SearchResult
from datasluice.logging import configure_logging, get_logger
from datasluice.runtime.context import ConnectorContext
from datasluice.runtime.defaults import create_default_transport
from datasluice.runtime.plugin_manager import PluginManager

if TYPE_CHECKING:
    from collections.abc import Callable

    from datasluice.auth import BaseAuth
    from datasluice.connectors.base import BaseAdapter
    from datasluice.ports import CachePort, CredentialProvider, StoragePort, Transport

logger = get_logger("session")


class _StaticCredentialProvider:
    """Wraps a fixed ``BaseAuth`` into the :class:`CredentialProvider` port (D-P3-14).

    Returns the same auth for every host and never expires — used when the
    session is given an explicit ``auth=`` but no dynamic provider. Keeps the
    transport's 401/403 eviction path uniform (a provider is always present
    when auth is configured) without inventing expiry semantics for static
    credentials.
    """

    def __init__(self, auth: BaseAuth) -> None:
        self._auth = auth

    def resolve(self, host: str | None = None) -> BaseAuth:
        return self._auth


class DataSluiceSession:
    """Public facade and composition root for DataSluice.

    Wires the :class:`PluginManager`, transport, auth, and (optional) storage
    / cache / credential provider into a zero-config session. Every override
    is explicit — no env-var-driven settings (D-14).

    Args:
        auth: Authentication strategy; defaults to :class:`NoAuth` (D-11). When
            set, it is wrapped in a :class:`_StaticCredentialProvider` unless
            ``credential_provider=`` is also given (D-P3-14).
        transport: Optional pre-configured transport satisfying the
            :class:`Transport` port (D-02). A default transport is constructed
            when omitted; scalar knobs below are IGNORED when this is injected
            (D-P3-05).
        page_size: Default page size hint for paginated catalog calls (D-12).
        plugins: Optional :class:`PluginManager` for dependency injection (D-06).
            A fresh instance is constructed when omitted.
        timeout: Request timeout in seconds (default transport only).
        retries: Max retry attempts (default transport only).
        rate_limit: Optional requests-per-second cap (default transport only).
        cache_dir: Directory for the default content cache; ``None`` (default)
            means no cache is wired (D-P3-02).
        cache_ttl: Cache entry TTL in seconds.
        storage: Optional :class:`StoragePort` instance (Phase 4 download path).
        cache: Optional :class:`CachePort` instance; wins over ``cache_dir``.
        credential_provider: Optional :class:`CredentialProvider`; wins over
            ``auth=`` wrapping (D-P3-14).

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
        timeout: float = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        rate_limit: float | None = DEFAULT_RATE_LIMIT,
        cache_dir: str | None = None,
        cache_ttl: int = DEFAULT_CACHE_TTL,
        storage: StoragePort | None = None,
        cache: CachePort | None = None,
        credential_provider: CredentialProvider | None = None,
    ) -> None:
        self.auth = auth or NoAuth()
        self.page_size = page_size
        self.storage = storage

        if credential_provider is not None:
            self._credential_provider = credential_provider
        elif auth is not None:
            self._credential_provider = _StaticCredentialProvider(self.auth)
        else:
            self._credential_provider = None

        if transport is not None:
            logger.debug("transport= injected; timeout/retries/rate_limit scalars ignored (D-P3-05)")
            self._transport = transport
        else:
            self._transport = create_default_transport(
                self.auth,
                credential_provider=self._credential_provider,
                timeout=timeout,
                retries=retries,
                rate_limit=rate_limit,
            )

        if cache is not None:
            self._cache = cache
        elif cache_dir is not None:
            self._cache = self._build_default_cache(cache_dir, cache_ttl)
        else:
            self._cache = None

        self.plugins = plugins or PluginManager()
        configure_logging(DEFAULT_LOG_LEVEL)
        logger.debug("DataSluiceSession initialised with %d connector(s)", len(self.plugins.list_connectors()))

    @staticmethod
    def _build_default_cache(cache_dir: str, cache_ttl: int) -> CachePort | None:
        """Lazily construct the default ContentCache (plan 03-03) if importable.

        Resolved via :mod:`importlib` so this plan does not hard-depend on
        03-03 having landed; if the module is absent the cache is left as
        ``None`` and the session still operates search-only.
        """

        try:
            cache_module = importlib.import_module("datasluice.io.content_cache")
        except ImportError:
            logger.debug("ContentCache (plan 03-03) not importable; cache_dir=%s unused", cache_dir)
            return None
        return cache_module.ContentCache(cache_dir, ttl=cache_ttl)

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
