from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from datasluice.connectors.catalog.ckan import CKANConnector
from datasluice.connectors.catalog.socrata import SocrataConnector
from datasluice.connectors.catalog.udata import UDataConnector
from datasluice.contracts.catalog.protocols import CatalogConnectorContext


@pytest.mark.parametrize("connector_type", [CKANConnector, UDataConnector, SocrataConnector])
@pytest.mark.parametrize("owns_sync", [False, True])
@pytest.mark.parametrize("owns_async", [False, True])
def test_connector_contexts_close_only_owned_executors_once(
    connector_type: type[CKANConnector] | type[UDataConnector] | type[SocrataConnector],
    owns_sync: bool,
    owns_async: bool,
) -> None:
    sync_executor = Mock()
    async_executor = AsyncMock()
    context = CatalogConnectorContext(
        sync_executor=sync_executor,
        async_executor=async_executor,
        manages_sync_executor=owns_sync,
        manages_async_executor=owns_async,
    )
    projection = Mock()
    connector = connector_type(
        context=context,
        normalized_sync=projection,
        normalized_async=projection,
        native_sync=projection,
        native_async=projection,
        effective_profile=projection,
    )

    for _ in range(2):
        with connector as entered:
            assert entered is connector
        connector.close()
    assert sync_executor.close.call_count == int(owns_sync)

    async def run() -> None:
        for _ in range(2):
            async with connector as entered:
                assert entered is connector
            await connector.aclose()
        assert async_executor.aclose.await_count == int(owns_async)

    asyncio.run(run())
