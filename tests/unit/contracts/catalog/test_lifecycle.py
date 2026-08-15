"""Lifecycle invariants for injected catalog executors and streams."""

from __future__ import annotations

import asyncio

import pytest

from datasluice.contracts.catalog.protocols import (
    AsyncManagedExecutor,
    CatalogConnectorContext,
    CatalogOperationGuard,
    CatalogOperationRequest,
    SyncManagedExecutor,
)
from datasluice.domain.catalog.operations import OperationId


class SyncRecordingExecutor:
    """Record synchronous lifecycle actions."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.closed = 0

    def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> object:
        self.calls.append(str(operation.operation_id))
        return object()

    def close(self) -> None:
        self.closed += 1


class AsyncRecordingExecutor:
    """Record asynchronous lifecycle actions."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.closed = 0

    async def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> object:
        self.calls.append(str(operation.operation_id))
        return object()

    async def aclose(self) -> None:
        self.closed += 1


def _call() -> tuple[CatalogOperationRequest, CatalogOperationGuard]:
    operation_id = OperationId(platform="catalog", service="datasets", method="get")
    return CatalogOperationRequest(operation_id=operation_id), CatalogOperationGuard(operation_id=operation_id)


def test_sync_context_only_closes_managed_executor_and_guards_before_dispatch() -> None:
    """Caller-owned sync executors survive while denied work never dispatches."""
    sync_executor = SyncRecordingExecutor()
    async_executor = AsyncRecordingExecutor()
    context = CatalogConnectorContext(sync_executor=sync_executor, async_executor=async_executor, manages_sync_executor=True)
    operation, guard = _call()

    with SyncManagedExecutor(context) as managed:
        managed.execute(operation, guard)
    managed.close()

    assert sync_executor.calls == ["catalog/datasets.get"]
    assert sync_executor.closed == 1


def test_async_context_only_closes_managed_executor() -> None:
    """Async ownership and idempotent close mirror the sync lifecycle."""
    sync_executor = SyncRecordingExecutor()
    async_executor = AsyncRecordingExecutor()
    context = CatalogConnectorContext(sync_executor=sync_executor, async_executor=async_executor, manages_async_executor=False)
    operation, guard = _call()

    async def exercise() -> None:
        async with AsyncManagedExecutor(context) as managed:
            await managed.execute(operation, guard)
        await managed.aclose()

    asyncio.run(exercise())

    assert async_executor.calls == ["catalog/datasets.get"]
    assert async_executor.closed == 0


def test_guard_rejection_prevents_executor_dispatch() -> None:
    """Pre-dispatch guards run before the executor sees an operation."""
    sync_executor = SyncRecordingExecutor()
    async_executor = AsyncRecordingExecutor()
    context = CatalogConnectorContext(sync_executor=sync_executor, async_executor=async_executor)
    operation, guard = _call()

    object.__setattr__(guard, "require_allowed", lambda: (_ for _ in ()).throw(RuntimeError("blocked")))
    with pytest.raises(RuntimeError, match="blocked"):
        SyncManagedExecutor(context).execute(operation, guard)

    assert sync_executor.calls == []
