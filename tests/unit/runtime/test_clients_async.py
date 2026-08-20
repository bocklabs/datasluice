"""Asynchronous catalog runtime client tests."""

from __future__ import annotations

import asyncio

import pytest

from datasluice.contracts.catalog.protocols import AsyncCatalogClient as AsyncCatalogClientProtocol
from datasluice.domain.catalog.operations import OperationId
from datasluice.errors.catalog import CatalogNotFoundError, UnsupportedCapabilityError
from datasluice.runtime.clients import AsyncCatalogClient
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.unit.runtime.test_clients_sync import _envelope, _profile, _request


class _AsyncTransport:
    def __init__(self) -> None:
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(200, {}, _envelope())

    async def aclose(self) -> None:
        self.close_count += 1


def test_async_client_dispatches_dataset_get_and_closes_once() -> None:
    async def exercise() -> _AsyncTransport:
        transport = _AsyncTransport()
        client = AsyncCatalogClient(transport, _profile())

        assert isinstance(client, AsyncCatalogClientProtocol)
        assert (await client.datasets.get(_request())).items[0].name == "Fixture dataset"
        await client.aclose()
        await client.aclose()
        return transport

    transport = asyncio.run(exercise())
    assert transport.close_count == 1


def test_async_client_matches_sync_guard_and_error_semantics() -> None:
    async def exercise() -> None:
        transport = _AsyncTransport()
        client = AsyncCatalogClient(transport, _profile())

        with pytest.raises(UnsupportedCapabilityError) as unsupported:
            await client.datasets.get(_request(OperationId("reference", "resources", "get")))
        assert unsupported.value.safe_action
        assert transport.requests == []

        class _NotFoundTransport(_AsyncTransport):
            async def send(self, request: RuntimeRequest) -> RuntimeResponse:
                return RuntimeResponse(404, {}, b"")

        with pytest.raises(CatalogNotFoundError):
            await AsyncCatalogClient(_NotFoundTransport(), _profile()).datasets.get(_request())

    asyncio.run(exercise())
