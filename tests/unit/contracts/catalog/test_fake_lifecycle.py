"""Lifecycle tests for independent catalog loopback test transports."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from datasluice.contracts.catalog.protocols import (
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    SyncCatalogOperationExecutor,
    SyncManagedExecutor,
)
from tests.helpers.catalog_transport import AsyncLoopbackTransport, SyncLoopbackTransport
from tests.helpers.http_server import _CapturingServer


def test_sync_transport_uses_real_loopback_path_and_headers(
    catalog_fixture_server: tuple[_CapturingServer, str],
) -> None:
    """The synchronous test transport independently captures socket metadata."""
    server, base_url = catalog_fixture_server
    transport = SyncLoopbackTransport()

    response = transport.get(f"{base_url}/sync?case=one", headers={"X-Case": "sync"})

    assert response.body == b"sync"
    assert server.captured_paths == ["/sync?case=one"]
    assert server.captured[0]["x-case"] == "sync"
    transport.close()
    transport.close()
    assert transport.close_count == 1


def test_async_transport_uses_an_independent_loopback_socket(
    catalog_fixture_server: tuple[_CapturingServer, str],
) -> None:
    """The async test transport does not wrap urllib or a synchronous helper."""
    server, base_url = catalog_fixture_server

    async def exercise() -> AsyncLoopbackTransport:
        transport = AsyncLoopbackTransport()
        response = await transport.get(f"{base_url}/async", headers={"X-Case": "async"})
        assert response.body == b"async"
        await transport.aclose()
        await transport.aclose()
        return transport

    transport = asyncio.run(exercise())
    assert server.captured_paths == ["/async"]
    assert server.captured[0]["x-case"] == "async"
    assert transport.close_count == 1


def test_async_cancellation_releases_the_test_transport(catalog_fixture_server: tuple[_CapturingServer, str]) -> None:
    """Cancellation closes the caller's test transport deterministically."""
    _, base_url = catalog_fixture_server

    async def exercise() -> AsyncLoopbackTransport:
        transport = AsyncLoopbackTransport()
        task = asyncio.create_task(transport.get(f"{base_url}/cancel"))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await transport.aclose()
        assert transport.closed
        assert transport._writer is None
        return transport

    assert asyncio.run(exercise()).close_count == 1


def test_caller_owned_transport_is_not_closed_by_connector_lifecycle() -> None:
    """Caller-owned dependencies survive connector shutdown while ownership remains explicit."""
    transport = SyncLoopbackTransport()
    context = CatalogConnectorContext(
        sync_executor=cast(SyncCatalogOperationExecutor, transport),
        async_executor=cast(AsyncCatalogOperationExecutor, object()),
        manages_sync_executor=False,
    )

    SyncManagedExecutor(context).close()

    assert transport.close_count == 0
