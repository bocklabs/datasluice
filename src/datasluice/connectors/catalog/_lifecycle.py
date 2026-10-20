from __future__ import annotations

from typing import TYPE_CHECKING, Self

from datasluice.contracts.catalog.protocols import AsyncManagedExecutor, SyncManagedExecutor

if TYPE_CHECKING:
    from types import TracebackType

    from datasluice.contracts.catalog.protocols import CatalogConnectorContext


class _ConnectorLifecycle:
    def __init__(self, context: CatalogConnectorContext) -> None:
        self._sync_executor = SyncManagedExecutor(context)
        self._async_executor = AsyncManagedExecutor(context)

    def close(self) -> None:
        """Release the synchronous executor only when the context owns it."""
        self._sync_executor.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    async def aclose(self) -> None:
        """Release the asynchronous executor only when the context owns it."""
        await self._async_executor.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()
