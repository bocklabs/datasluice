"""Transport-free CKAN catalog contract façade."""

from __future__ import annotations

from types import TracebackType

from datasluice.contracts.catalog.native.ckan import AsyncCKANServices, SyncCKANServices
from datasluice.contracts.catalog.protocols import (
    AsyncCatalogClient,
    AsyncManagedExecutor,
    CatalogConnectorContext,
    SyncCatalogClient,
    SyncManagedExecutor,
)
from datasluice.domain.catalog.profiles import EffectiveCapabilityProfile


class CKANConnector:
    """Expose injected CKAN service projections without a transport implementation."""

    def __init__(
        self,
        *,
        context: CatalogConnectorContext,
        normalized_sync: SyncCatalogClient,
        normalized_async: AsyncCatalogClient,
        native_sync: SyncCKANServices,
        native_async: AsyncCKANServices,
        effective_profile: EffectiveCapabilityProfile,
    ) -> None:
        self._sync_executor = SyncManagedExecutor(context)
        self._async_executor = AsyncManagedExecutor(context)
        self.normalized_sync = normalized_sync
        self.normalized_async = normalized_async
        self.native_sync = native_sync
        self.native_async = native_async
        self.effective_profile = effective_profile

    def close(self) -> None:
        """Release the synchronous executor only when the context owns it."""
        self._sync_executor.close()

    def __enter__(self) -> CKANConnector:
        """Enter the synchronous façade context."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release synchronous resources on context exit."""
        self.close()

    async def aclose(self) -> None:
        """Release the asynchronous executor only when the context owns it."""
        await self._async_executor.aclose()

    async def __aenter__(self) -> CKANConnector:
        """Enter the asynchronous façade context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release asynchronous resources on context exit."""
        await self.aclose()
