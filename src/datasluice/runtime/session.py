"""DataSluiceSession composition substrate with explicit catalog handoff."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from datasluice.auth import NoAuth
from datasluice.config.defaults import (
    DEFAULT_CACHE_TTL,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RATE_LIMIT,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
)
from datasluice.contracts.catalog.protocols import CatalogConnectorContext
from datasluice.logging import configure_logging, get_logger
from datasluice.runtime.defaults import create_default_transport
from datasluice.runtime.plugin_manager import PluginManager

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from datasluice.auth import BaseAuth
    from datasluice.ports import CachePort, CredentialProvider, StateStore, StoragePort, Transport
    from datasluice.sync.sync import SyncOutcome

logger = get_logger("session")

_SESSION_SYNC_READY = True


class _StaticCredentialProvider:
    """Wraps a fixed ``BaseAuth`` into the :class:`CredentialProvider` port.

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
    """Own transport, storage, cache, and state dependencies for data-plane work.

        Args:
            auth: Authentication strategy; defaults to :class:`NoAuth`. When
                set, it is wrapped in a :class:`_StaticCredentialProvider` unless
                ``credential_provider=`` is also given.
            transport: Optional pre-configured transport satisfying the
                :class:`Transport` port. A default transport is constructed
                when omitted; scalar knobs below are IGNORED when this is injected
    .
            page_size: Default page size hint retained for caller-owned configuration.
            plugins: Optional :class:`PluginManager` retained for explicit extension management.
                A fresh instance is constructed when omitted.
            timeout: Request timeout in seconds (default transport only).
            retries: Max retry attempts (default transport only).
            rate_limit: Optional requests-per-second cap (default transport only).
            cache_dir: Directory for the default content cache; ``None`` (default)
                means no cache is wired.
            cache_ttl: Cache entry TTL in seconds.
            storage: Optional :class:`StoragePort` instance (download path).
            cache: Optional :class:`CachePort` instance; wins over ``cache_dir``.
            credential_provider: Optional :class:`CredentialProvider`; wins over
                ``auth=`` wrapping.
            state_store: Optional :class:`StateStore`; defaults to a fresh
                :class:`InMemoryStateStore` owned by this session.

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
        state_store: StateStore | None = None,
    ) -> None:
        self.auth = auth if auth is not None else NoAuth()
        self.page_size = page_size
        self.storage = storage
        if state_store is None:
            from datasluice.sync.state_store import InMemoryStateStore

            self.state_store = InMemoryStateStore()
        else:
            self.state_store = state_store

        if credential_provider is not None:
            self._credential_provider = credential_provider
        elif auth is not None:
            self._credential_provider = _StaticCredentialProvider(self.auth)
        else:
            self._credential_provider = None

        if transport is not None:
            logger.debug("transport= injected; timeout/retries/rate_limit scalars ignored")
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
        logger.debug("DataSluiceSession initialized")

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
        try:
            return cache_module.ContentCache(cache_dir, ttl=cache_ttl)
        except Exception:
            logger.warning(
                "ContentCache construction failed for cache_dir=%s; disabling cache", cache_dir, exc_info=True
            )
            return None

    def open_catalog[T](self, factory: Callable[[CatalogConnectorContext], T], context: CatalogConnectorContext) -> T:
        """Construct one canonical catalog connector from caller-owned inputs."""
        if not callable(factory):
            raise TypeError("Catalog construction requires a callable factory.")
        if not isinstance(context, CatalogConnectorContext):
            raise TypeError("Catalog construction requires a CatalogConnectorContext.")
        return factory(context)

    def sync_resources(
        self,
        resources: Iterable[Any],
        *,
        destination_uri: str,
        reader: Any | None = None,
        resume: bool = False,
    ) -> Iterator[SyncOutcome]:
        """Synchronize resources through this session's runtime dependencies.

        Args:
            resources: Resource values to synchronize.
            destination_uri: fsspec destination URI for materialized output.
            reader: Optional resource reader; defaults to a data-plane reader
                backed by the session transport.
            resume: Continue from durable checkpoints when true.

        Returns:
            The underlying lazy iterator of per-resource sync outcomes.
        """
        from datasluice.data.access import DataPlaneResourceReader
        from datasluice.sync.sync import sync_resources

        selected_reader = reader if reader is not None else DataPlaneResourceReader(transport=self._transport)
        return sync_resources(
            resources,
            state_store=self.state_store,
            reader=selected_reader,
            destination_uri=destination_uri,
            transport=self._transport,
            resume=resume,
        )

    def __repr__(self) -> str:
        return "<DataSluiceSession>"
