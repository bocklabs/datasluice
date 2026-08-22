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


def test_async_cancellation_releases_the_test_transport(
    catalog_fixture_server: tuple[_CapturingServer, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation parked mid-read runs the transport's own writer cleanup deterministically."""
    _, base_url = catalog_fixture_server
    close_calls: list[str] = []
    parked: asyncio.Event = asyncio.Event()

    async def exercise() -> AsyncLoopbackTransport:
        original_open_connection = asyncio.open_connection

        async def gated_open_connection(host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            reader, writer = await original_open_connection(host, port)
            original_close = writer.close
            original_wait_closed = writer.wait_closed
            original_drain = writer.drain

            def spy_close() -> None:
                close_calls.append("close")
                original_close()

            async def spy_wait_closed() -> None:
                close_calls.append("wait_closed")
                await original_wait_closed()

            async def gated_drain() -> None:
                await original_drain()
                parked.set()
                await asyncio.Future()

            writer.close = spy_close  # ty: ignore[invalid-assignment]
            writer.wait_closed = spy_wait_closed  # ty: ignore[invalid-assignment]
            writer.drain = lambda: gated_drain()  # ty: ignore[invalid-assignment]
            return reader, writer

        monkeypatch.setattr(asyncio, "open_connection", gated_open_connection)
        transport = AsyncLoopbackTransport()
        task = asyncio.create_task(transport.get(f"{base_url}/cancel"))
        await asyncio.wait_for(parked.wait(), timeout=5.0)
        assert transport._writer is not None
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert close_calls == ["close", "wait_closed"]
        assert transport._writer is None
        await transport.aclose()
        assert transport.closed
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
