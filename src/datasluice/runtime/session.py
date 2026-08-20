"""DataSluiceSession composition substrate for the catalog runtime."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from datasluice.contracts.catalog.protocols import CatalogConnectorContext
from datasluice.domain.catalog.auth import CredentialResolver
from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.logging import configure_logging, get_logger
from datasluice.runtime.clients import AsyncCatalogClient, SyncCatalogClient
from datasluice.runtime.constants import (
    DEFAULT_BREAKER_COOLDOWN_SECONDS,
    DEFAULT_BREAKER_FAILURE_THRESHOLD,
    DEFAULT_CONNECT_BUDGET_SECONDS,
    DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    DEFAULT_READ_BUDGET_SECONDS,
    DEFAULT_WRITE_BUDGET_SECONDS,
)
from datasluice.runtime.defaults import create_default_async_transport, create_default_sync_transport
from datasluice.runtime.events import EventEmitter, EventSink
from datasluice.runtime.plugin_manager import PluginManager
from datasluice.runtime.resilience import BreakerRegistry
from datasluice.runtime.transport.base import CatalogTransport

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile, EffectiveCapabilityProfile
    from datasluice.runtime.clients import AsyncCatalogTransport
    from datasluice.sync.sync import SyncOutcome

logger = get_logger("session")

_SESSION_SYNC_READY = True


def _default_budget() -> TimeBudget:
    return TimeBudget(
        connect=DEFAULT_CONNECT_BUDGET_SECONDS,
        read=DEFAULT_READ_BUDGET_SECONDS,
        write=DEFAULT_WRITE_BUDGET_SECONDS,
        total=DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    )


class DataSluiceSession:
    """Compose explicitly injected catalog-runtime dependencies and safe defaults."""

    def __init__(
        self,
        *,
        transport: CatalogTransport | Any | None = None,
        async_transport: AsyncCatalogTransport | None = None,
        credentials: CredentialResolver | None = None,
        emitter: EventEmitter | None = None,
        sinks: tuple[EventSink, ...] = (),
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        tls_policy: TLSPolicy | None = None,
        plugins: PluginManager | None = None,
        storage: Any | None = None,
        cache: Any | None = None,
        cache_dir: str | None = None,
        cache_ttl: int = 3600,
        state_store: Any | None = None,
    ) -> None:
        self.credentials = credentials or CredentialResolver()
        self.budget = budget or _default_budget()
        self.breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold,
            cooldown=breaker_cooldown,
        )
        self.emitter = emitter or EventEmitter(sinks=sinks)
        self.tls_policy = tls_policy or TLSPolicy()
        self._transport = transport or create_default_sync_transport(tls_policy=self.tls_policy, budget=self.budget)
        self._async_transport = async_transport
        self.storage = storage
        if cache is not None:
            self._cache = cache
        elif cache_dir is not None:
            self._cache = self._build_default_cache(cache_dir, cache_ttl)
        else:
            self._cache = None
        if state_store is None:
            from datasluice.sync.state_store import InMemoryStateStore

            self.state_store = InMemoryStateStore()
        else:
            self.state_store = state_store
        self.plugins = plugins or PluginManager()
        configure_logging("WARNING")
        logger.debug("DataSluiceSession initialized with injected runtime composition")

    @staticmethod
    def _build_default_cache(cache_dir: str, cache_ttl: int) -> Any | None:
        """Lazily construct the optional content cache."""
        try:
            cache_module = importlib.import_module("datasluice.io.content_cache")
        except ImportError:
            logger.debug("ContentCache is unavailable; cache_dir is unused")
            return None
        return cache_module.ContentCache(cache_dir, ttl=cache_ttl)

    def sync_client(self, profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile) -> SyncCatalogClient:
        """Create one synchronous catalog client over this session's pipeline."""
        return SyncCatalogClient(
            self._transport,
            profile,
            credentials=self.credentials,
            budget=self.budget,
            breakers=self.breakers,
            emitter=self.emitter,
        )

    def async_client(self, profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile) -> AsyncCatalogClient:
        """Create one asynchronous catalog client over this session's pipeline."""
        transport = self._async_transport or create_default_async_transport(
            tls_policy=self.tls_policy, budget=self.budget
        )
        return AsyncCatalogClient(
            transport,
            profile,
            credentials=self.credentials,
            budget=self.budget,
            breakers=self.breakers,
            emitter=self.emitter,
        )

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
        """Synchronize resources through this session's runtime dependencies."""
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
