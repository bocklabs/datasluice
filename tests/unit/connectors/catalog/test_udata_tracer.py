from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import httpx
import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.probes import UDataVersionError
from datasluice.connectors.catalog.udata.services.extensions import AsyncExtensionsService, SyncExtensionsService
from datasluice.contracts.catalog.native.udata import AsyncUDataServices, SyncUDataServices
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import UDataCredential
from datasluice.domain.catalog.models import NativeRecord
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import BudgetExhaustedError, CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport, HttpxCatalogTransport

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

_DATASET_OPERATION_ID = next(op_id for op_id in declared_udata_profile().operations if op_id.method == "list-datasets")


def _datasets_operation() -> tuple[CatalogOperationRequest, CatalogOperationGuard]:
    operation = CatalogOperationRequest(operation_id=_DATASET_OPERATION_ID, payload={"page": 1, "page_size": 20})
    guard = CatalogOperationGuard(operation_id=_DATASET_OPERATION_ID)
    return operation, guard


def _site_body(version: str = "17.6.0") -> bytes:
    return json.dumps(
        {"feed_size": 0, "id": "site", "keywords": [], "metrics": {}, "title": "uData", "version": version}
    ).encode()


def _page_body() -> bytes:
    return json.dumps(
        {
            "data": [{"id": "abc", "title": "Title", "slug": "title", "description": "d"}],
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "previous_page": None,
            "total": 1,
        }
    ).encode()


class RecordingTransport:
    def __init__(self, *, site_version: str = "17.6.0", responses: list[RuntimeResponse] | None = None) -> None:
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0
        self._site_version = site_version
        self._responses = list(responses or [])

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if request.url.endswith("/api/1/swagger.json"):
            return RuntimeResponse(
                200,
                {},
                json.dumps({"swagger": "2.0", "basePath": "/api/1", "paths": {"/datasets/": {"get": {}}}}).encode(),
            )
        if request.url.endswith("/api/1/site/"):
            return RuntimeResponse(status_code=200, headers={}, body=_site_body(self._site_version))
        if self._responses:
            return self._responses.pop(0)
        return RuntimeResponse(status_code=200, headers={}, body=_page_body())

    def close(self) -> None:
        self.close_count += 1


class RecordingAsyncTransport:
    def __init__(self, *, site_version: str = "17.6.0") -> None:
        self.requests: list[RuntimeRequest] = []
        self.aclose_count = 0
        self._site_version = site_version

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if request.url.endswith("/api/1/swagger.json"):
            return RuntimeResponse(
                200,
                {},
                json.dumps({"swagger": "2.0", "basePath": "/api/1", "paths": {"/datasets/": {"get": {}}}}).encode(),
            )
        if request.url.endswith("/api/1/site/"):
            return RuntimeResponse(status_code=200, headers={}, body=_site_body(self._site_version))
        return RuntimeResponse(status_code=200, headers={}, body=_page_body())

    async def aclose(self) -> None:
        self.aclose_count += 1


def test_sync_tracer_performs_one_anonymous_probe_then_one_dataset_read() -> None:
    transport = RecordingTransport()
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="https://example.test",
        owns_transport=False,
    )
    operation, guard = _datasets_operation()

    envelope = client.datasets_list(operation, guard)

    assert [r.url for r in transport.requests] == [
        "https://example.test/api/1/site/",
        "https://example.test/api/1/swagger.json",
        "https://example.test/api/1/datasets/?page=1&page_size=20",
    ]
    assert all(not r.headers.get("X-API-KEY") for r in transport.requests)
    assert len(envelope.items) == 1
    record = envelope.items[0]
    assert isinstance(record, NativeRecord)
    assert record.id.value == "abc"
    assert envelope.page is not None
    assert envelope.page.total_items == 1


def test_version_mismatch_blocks_dataset_dispatch_with_typed_remedy() -> None:
    transport = RecordingTransport(site_version="18.0.0")
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    )
    operation, guard = _datasets_operation()

    with pytest.raises(UDataVersionError) as excinfo:
        client.datasets_list(operation, guard)

    assert excinfo.value.metadata["version_state"] == "unsupported"
    assert len(transport.requests) == 1
    assert transport.requests[0].url.endswith("/api/1/site/")


def test_injected_credential_attaches_only_to_the_dataset_read() -> None:
    transport = RecordingTransport()
    credential = UDataCredential(api_key="secret-key")
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )
    operation, guard = _datasets_operation()

    client.datasets_list(operation, guard)

    assert transport.requests[0].headers == {}
    assert transport.requests[1].headers == {}
    assert transport.requests[2].headers.get("X-API-KEY") == "secret-key"


def test_unknown_payload_parameters_are_rejected_before_dispatch() -> None:
    transport = RecordingTransport()
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    )
    dataset_op = _DATASET_OPERATION_ID
    operation = CatalogOperationRequest(operation_id=dataset_op, payload={"q": "x"})
    guard = CatalogOperationGuard(operation_id=dataset_op)

    with pytest.raises(CatalogValidationError):
        client.datasets_list(operation, guard)

    assert len(transport.requests) == 1


def test_owned_transport_closes_once_and_borrowed_never() -> None:
    owned = RecordingTransport()
    client = SyncUDataClient(
        owned,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=True,
    )
    client.close()
    client.close()
    with client:
        pass

    assert owned.close_count == 1

    borrowed = RecordingTransport()
    with SyncUDataClient(
        borrowed,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    ):
        pass

    assert borrowed.close_count == 0


def test_async_tracer_probes_anonymously_reads_once_and_owns_closure() -> None:
    owned = RecordingAsyncTransport()
    credential = UDataCredential(api_key="secret-key")
    client = AsyncUDataClient(
        owned,
        declared_udata_profile(),
        origin="https://example.test",
        credentials=credential,
        owns_transport=True,
    )
    dataset_op = _DATASET_OPERATION_ID
    operation = CatalogOperationRequest(operation_id=dataset_op, payload={})
    guard = CatalogOperationGuard(operation_id=_DATASET_OPERATION_ID)

    async def run() -> None:
        async with client as active:
            envelope = await active.datasets_list(operation, guard)
            assert len(envelope.items) == 1
        with pytest.raises(RuntimeError):
            await active.site_version()

    asyncio.run(run())

    assert owned.requests[0].headers == {}
    assert owned.requests[1].headers == {}
    assert owned.requests[2].headers.get("X-API-KEY") == "secret-key"
    assert owned.aclose_count == 1


def test_live_clients_expose_every_declared_native_protocol_member() -> None:
    """Both live client modes satisfy the declared uData service projections, including extensions."""
    sync_client = SyncUDataClient(
        RecordingTransport(),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    )
    async_client = AsyncUDataClient(
        RecordingAsyncTransport(),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    )

    assert isinstance(sync_client, SyncUDataServices)
    assert isinstance(async_client, AsyncUDataServices)
    assert isinstance(sync_client.extensions, SyncExtensionsService)
    assert isinstance(async_client.extensions, AsyncExtensionsService)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("authenticated", [False, True])
def test_dataset_body_read_enforces_total_deadline(asynchronous: bool, authenticated: bool) -> None:
    now = [0.0]
    closed = []
    credential = UDataCredential(api_key="secret-key") if authenticated else None

    class Stream(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            now[0] = 2.0
            yield _page_body()

        def close(self) -> None:
            closed.append(True)

    class AsyncStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            now[0] = 2.0
            yield _page_body()

        async def aclose(self) -> None:
            closed.append(True)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("site/"):
            return httpx.Response(200, content=_site_body())
        if request.url.path.endswith("swagger.json"):
            return httpx.Response(
                200, json={"swagger": "2.0", "basePath": "/api/1", "paths": {"/datasets/": {"get": {}}}}
            )
        assert bool(request.headers.get("X-API-KEY")) == authenticated
        return httpx.Response(200, stream=AsyncStream() if asynchronous else Stream())

    if asynchronous:

        async def run() -> None:
            async with AsyncUDataClient(
                AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler)),
                declared_udata_profile(),
                origin="http://127.0.0.1:5640",
                credentials=credential,
                budget=TimeBudget(1, 1, 1, 1),
                clock=lambda: now[0],
            ) as client:
                with pytest.raises(BudgetExhaustedError):
                    await client.datasets_list(*_datasets_operation())

        asyncio.run(run())
    else:
        with (
            SyncUDataClient(
                HttpxCatalogTransport(transport=httpx.MockTransport(handler)),
                declared_udata_profile(),
                origin="http://127.0.0.1:5640",
                credentials=credential,
                budget=TimeBudget(1, 1, 1, 1),
                clock=lambda: now[0],
            ) as client,
            pytest.raises(BudgetExhaustedError),
        ):
            client.datasets_list(*_datasets_operation())
    assert now[0] == 2.0
    assert closed == [True]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_dataset_read_rejects_late_custom_transport_success(asynchronous: bool) -> None:
    now = [0.0]

    class LateTransport(RecordingTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            response = super().send(request)
            if "/datasets/?" in request.url:
                now[0] = 2.0
            return response

    transport = LateTransport()

    class AsyncTransport(RecordingAsyncTransport):
        async def send(self, request: RuntimeRequest) -> RuntimeResponse:
            return transport.send(request)

    with pytest.raises(BudgetExhaustedError):
        if asynchronous:
            client = AsyncUDataClient(
                AsyncTransport(),
                declared_udata_profile(),
                origin="https://example.test",
                owns_transport=False,
                budget=TimeBudget(1, 1, 1, 1),
                clock=lambda: now[0],
            )
            asyncio.run(client.datasets_list(*_datasets_operation()))
        else:
            sync_client = SyncUDataClient(
                transport,
                declared_udata_profile(),
                origin="https://example.test",
                owns_transport=False,
                budget=TimeBudget(1, 1, 1, 1),
                clock=lambda: now[0],
            )
            sync_client.datasets_list(*_datasets_operation())


@pytest.mark.parametrize("owns_transport", [False, True])
@pytest.mark.parametrize("interrupt_probe", [False, True])
def test_sync_tracer_interruption_respects_transport_ownership(owns_transport: bool, interrupt_probe: bool) -> None:
    class InterruptingTransport(RecordingTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            if request.url.endswith("/api/1/site/") == interrupt_probe:
                raise KeyboardInterrupt
            return super().send(request)

    transport = InterruptingTransport()
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin="http://127.0.0.1:5640", owns_transport=owns_transport
    )
    operation = _datasets_operation()
    with client, pytest.raises(KeyboardInterrupt):
        client.datasets_list(*operation)
    client.close()
    assert transport.close_count == int(owns_transport)


@pytest.mark.parametrize("owns_transport", [False, True])
@pytest.mark.parametrize("cancel_probe", [False, True])
def test_async_tracer_cancellation_drains_transport_work(owns_transport: bool, cancel_probe: bool) -> None:
    async def run() -> None:
        started = asyncio.Event()
        finished = asyncio.Event()

        class CancellingTransport(RecordingAsyncTransport):
            async def send(self, request: RuntimeRequest) -> RuntimeResponse:
                if request.url.endswith("/api/1/site/") == cancel_probe:
                    started.set()
                    try:
                        await asyncio.Future()
                    finally:
                        finished.set()
                return await super().send(request)

        transport = CancellingTransport()
        client = AsyncUDataClient(
            transport, declared_udata_profile(), origin="http://127.0.0.1:5640", owns_transport=owns_transport
        )
        async with client:
            task = asyncio.create_task(client.datasets_list(*_datasets_operation()))
            await asyncio.wait_for(started.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await client.aclose()
        assert finished.is_set()
        assert transport.aclose_count == int(owns_transport)

    asyncio.run(run())
