"""Tracer unit tests: strict gate, one dataset read, and close ownership."""

from __future__ import annotations

import asyncio
import json

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.probes import UDataVersionError
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import UDataCredential
from datasluice.domain.catalog.models import NativeRecord
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

_DATASET_OPERATION_ID = next(op_id for op_id in declared_udata_profile().operations if "dataset" in op_id.method)


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
        self.aclose_count = 0
        self._site_version = site_version
        self._responses = list(responses or [])

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
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
        self.close_count = 0
        self.aclose_count = 0
        self._site_version = site_version

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
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
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    )
    operation, guard = _datasets_operation()

    envelope = client.datasets_list(operation, guard)

    assert [r.url for r in transport.requests] == [
        "http://127.0.0.1:5640/api/1/site/",
        "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20",
    ]
    assert all(not r.headers.get("X-API-KEY") for r in transport.requests)
    assert len(envelope.items) == 1
    record = envelope.items[0]
    assert isinstance(record, NativeRecord)
    assert record.id.value == "abc"
    assert envelope.page is not None and envelope.page.total_items == 1


def test_version_mismatch_blocks_dataset_dispatch_with_typed_remedy() -> None:
    transport = RecordingTransport(site_version="17.5.9")
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    )
    operation, guard = _datasets_operation()

    with pytest.raises(UDataVersionError) as excinfo:
        client.datasets_list(operation, guard)

    assert excinfo.value.metadata["version_state"] == "malformed"
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
    assert transport.requests[1].headers.get("X-API-KEY") == "secret-key"


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
        origin="http://127.0.0.1:5640",
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
    assert owned.requests[1].headers.get("X-API-KEY") == "secret-key"
    assert owned.aclose_count == 1
