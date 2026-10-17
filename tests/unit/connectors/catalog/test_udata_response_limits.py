from __future__ import annotations

import asyncio
from functools import partial

import pytest

from datasluice.connectors.catalog.udata.mapping import _DATASETS_OPERATION_ID
from datasluice.errors.catalog import NativeCatalogError
from tests.helpers.udata_test_support import (
    async_client,
    async_route_table,
    declared_routes,
    sync_client,
    sync_route_table,
)


@pytest.mark.parametrize("root", [False, True])
@pytest.mark.parametrize("explicit_none", [False, True])
def test_sync_native_calls_always_bound_response_bytes(root: bool, explicit_none: bool) -> None:
    router = sync_route_table(declared_routes(("GET", "/api/1/datasets/", 200, b" " * (1_048_576 + 1))))
    with sync_client(router) as client:
        call = client._root_call if root else client._dataset_call
        if explicit_none:
            call = partial(call, max_response_bytes=None)
        with pytest.raises(NativeCatalogError, match="byte limit"):
            call(
                method="GET",
                path="/api/1/datasets/",
                owning_operation=_DATASETS_OPERATION_ID,
            )
    assert router.requests[-1].max_response_bytes == 1_048_576


@pytest.mark.parametrize("root", [False, True])
@pytest.mark.parametrize("explicit_none", [False, True])
def test_async_native_calls_always_bound_response_bytes(root: bool, explicit_none: bool) -> None:
    router = async_route_table(declared_routes(("GET", "/api/1/datasets/", 200, b" " * (1_048_576 + 1))))

    async def run() -> None:
        async with async_client(router) as client:
            call = client._root_call_async if root else client._dataset_call_async
            if explicit_none:
                call = partial(call, max_response_bytes=None)
            with pytest.raises(NativeCatalogError, match="byte limit"):
                await call(
                    method="GET",
                    path="/api/1/datasets/",
                    owning_operation=_DATASETS_OPERATION_ID,
                )

    asyncio.run(run())
    assert router.requests[-1].max_response_bytes == 1_048_576
