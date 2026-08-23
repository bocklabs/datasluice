"""Deterministic loopback coverage for the CKAN resource surface and bounded uploads."""

from __future__ import annotations

import asyncio
import io
import json
from collections.abc import Mapping
from pathlib import Path

import pytest

import datasluice.connectors.catalog.ckan.services.filestore as filestore_module
from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.connectors.catalog.ckan.results import CKANMutationResult
from datasluice.connectors.catalog.ckan.services.resources import AsyncResourcesService, SyncResourcesService
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.models import NativeRecord, ValueRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"

RESOURCE_SHOW_RESULT: dict[str, object] = {
    "id": "res-9",
    "package_id": "pkg-1",
    "name": "sample",
    "url": "https://example.test/sample.csv",
}

STREAMING_MARKERS = ("chunked", "stream=True", "iter_content", "iter_bytes", "iter_raw")


def _success_body(result: object) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


def _failure_body(error: Mapping[str, object]) -> bytes:
    return json.dumps({"success": False, "error": dict(error)}).encode("utf-8")


class SyncCaptureTransport:
    """A deterministic loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    def close(self) -> None:
        self.close_count += 1


class AsyncCaptureTransport:
    """A deterministic async loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    async def aclose(self) -> None:
        self.close_count += 1


def _client(
    transport: SyncCaptureTransport,
    *,
    max_upload_bytes: int | None = None,
) -> SyncCKANClient:
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
        max_upload_bytes=max_upload_bytes,
    )


def _async_client(transport: AsyncCaptureTransport) -> AsyncCKANClient:
    return AsyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
    )


def _upload_parts(request: RuntimeRequest) -> dict[str, tuple[bytes, str | None]]:
    return {part.field_name: (part.data, part.file_name) for part in request.files}


def test_upload_buffers_path_and_handle_sources_to_identical_wire_bytes(tmp_path: Path) -> None:
    """D-03: str paths and binary handles produce byte-identical multipart bodies."""
    payload = b"col_a,col_b\n1,2\n3,4\n"
    source = tmp_path / "updated_file.csv"
    source.write_bytes(payload)

    path_transport = SyncCaptureTransport(body=_success_body({"id": "res-1", "package_id": "pkg-1"}))
    handle_transport = SyncCaptureTransport(body=_success_body({"id": "res-2", "package_id": "pkg-1"}))
    path_client = _client(path_transport)
    handle_client = _client(handle_transport)

    result_path = path_client.resources.resource_create(package_id="pkg-1", name="data", upload=str(source))
    result_handle = handle_client.resources.resource_create(package_id="pkg-1", name="data", upload=io.BytesIO(payload))

    path_request = path_transport.requests[0]
    handle_request = handle_transport.requests[0]
    assert len(path_transport.requests) == 1
    assert len(handle_transport.requests) == 1
    assert path_request.method == "POST"
    assert path_request.url.endswith("/api/3/action/resource_create")
    assert path_request.body is None
    assert path_request.headers["Content-Type"] == "multipart/form-data"
    assert _upload_parts(path_request)["upload"] == (payload, "updated_file.csv")
    path_bytes = {key: value[0] for key, value in _upload_parts(path_request).items()}
    handle_bytes = {key: value[0] for key, value in _upload_parts(handle_request).items()}
    assert path_bytes == handle_bytes
    assert _upload_parts(handle_request)["upload"] == (payload, None)
    assert _upload_parts(path_request)["name"] == (b"data", None)
    assert _upload_parts(path_request)["package_id"] == (b"pkg-1", None)
    assert isinstance(result_path, CKANMutationResult)
    assert isinstance(result_handle, CKANMutationResult)


def test_multipart_fields_precede_the_upload_part_in_sorted_order() -> None:
    """Field parts render deterministically ahead of the fixed upload part."""
    transport = SyncCaptureTransport(body=_success_body({"id": "res-1", "package_id": "pkg-1"}))
    client = _client(transport)

    client.resources.resource_create(package_id="pkg-1", name="data", upload=io.BytesIO(b"x"))

    field_order = [part.field_name for part in transport.requests[0].files]
    assert field_order == ["name", "package_id", "upload"]


def test_oversized_source_refuses_before_any_transport_io() -> None:
    """T-03-06-02 mitigation: the ceiling refusal names the remedy at zero wire hits."""
    transport = SyncCaptureTransport(body=_success_body({}))
    client = _client(transport, max_upload_bytes=8)

    with pytest.raises(CatalogValidationError) as excinfo:
        client.resources.resource_create(package_id="pkg-1", upload=io.BytesIO(b"x" * 100))

    assert transport.requests == []
    assert "8" in str(excinfo.value)
    assert "max_upload_bytes" in excinfo.value.safe_action


def test_server_size_limit_envelope_maps_to_a_size_mentioning_safe_action() -> None:
    """Server-side media/size limits surface as typed validation errors."""
    transport = SyncCaptureTransport(
        body=_failure_body({"__type": "Validation Error", "message": "File size too large: maximum size exceeded"})
    )
    client = _client(transport)

    with pytest.raises(CatalogValidationError) as excinfo:
        client.resources.resource_create(package_id="pkg-1", upload=io.BytesIO(b"data"))

    assert len(transport.requests) == 1
    assert "Reduce the uploaded file size" in excinfo.value.safe_action


def test_resource_search_decodes_resource_records_with_native_paging() -> None:
    """Reads follow the standard paths and decode their own record kinds (D-19)."""
    transport = SyncCaptureTransport(body=_success_body([{"id": "res-9", "package_id": "pkg-1"}]))
    client = _client(transport)

    search = client.resources.resource_search(q="res-9", limit=5, offset=10)

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/resource_search")
    assert json.loads(request.body or b"{}") == {"q": "res-9", "limit": 5, "offset": 10}
    record = search.items[0]
    assert isinstance(record, NativeRecord)
    assert record.resource_kind.value == "resource"
    assert record.id.value == "res-9"


def test_resource_show_returns_its_own_record_kind() -> None:
    transport = SyncCaptureTransport(body=_success_body(RESOURCE_SHOW_RESULT))
    client = _client(transport)

    envelope = client.resources.resource_show(id="res-9")

    record = envelope.items[0]
    assert isinstance(record, NativeRecord)
    assert record.resource_kind.value == "resource"
    assert record.id.value == "res-9"


def test_resource_delete_returns_mutation_result_with_receipt() -> None:
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    result = client.resources.resource_delete(id="res-9")

    assert transport.requests[0].url.endswith("/api/3/action/resource_delete")
    value_item = result.result.items[0]
    assert isinstance(value_item, ValueRecord)
    receipt = result.receipt
    assert isinstance(receipt, MutationReceipt)
    assert receipt.outcome == "succeeded"
    assert receipt.target.value == "res-9"


def test_filestore_projection_routes_through_the_resource_paths() -> None:
    """The façade carries zero dedicated endpoints and reuses resource actions."""
    transport = SyncCaptureTransport(body=_success_body({"id": "res-9", "package_id": "pkg-1"}))
    client = _client(transport)
    operation = CatalogOperationRequest(
        operation_id=OperationId(
            platform="ckan", service="action-api-v3", method="resource-list-show-create-update-patch-delete-upload"
        ),
        payload={"action": "resource_patch", "id": "res-9", "name": "renamed"},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    envelope = client.filestore.upload_and_resource_file_replacement(operation, guard)

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/resource_patch")
    record = envelope.items[0]
    assert isinstance(record, NativeRecord)
    assert record.id.value == "res-9"
    assert not [entry for entry in CKAN_ACTIONS.entries if entry.group == "filestore"]
    assert "zero dedicated Action API endpoints" in (filestore_module.__doc__ or "")


def test_every_manifest_resource_action_exposes_a_typed_method_on_both_mode_services() -> None:
    entries = [entry for entry in CKAN_ACTIONS.entries if entry.group == "resources"]
    assert len(entries) == 6
    sync_surface = {name for name in dir(SyncResourcesService) if not name.startswith("_")}
    async_surface = {name for name in dir(AsyncResourcesService) if not name.startswith("_")}
    for entry in entries:
        assert entry.name in sync_surface, f"sync surface misses {entry.name}"
        assert entry.name in async_surface, f"async surface misses {entry.name}"


def test_services_package_carries_no_streaming_constructs() -> None:
    services_dir = (
        Path(__file__).resolve().parents[4] / "src" / "datasluice" / "connectors" / "catalog" / "ckan" / "services"
    )
    sources = sorted(services_dir.glob("*.py"))
    assert sources
    for source in sources:
        text = source.read_text(encoding="utf-8")
        for marker in STREAMING_MARKERS:
            assert marker not in text, f"{source.name} carries streaming construct {marker!r}"


def test_async_resources_mirror_sync_semantics(tmp_path: Path) -> None:
    source = tmp_path / "async.csv"
    source.write_bytes(b"a,b\n")
    transport = AsyncCaptureTransport(body=_success_body({"id": "res-2", "package_id": "pkg-1"}))
    client = _async_client(transport)

    result = asyncio.run(client.resources.resource_create(package_id="pkg-1", upload=str(source)))

    request = transport.requests[0]
    assert request.headers["Content-Type"] == "multipart/form-data"
    assert _upload_parts(request)["upload"] == (b"a,b\n", "async.csv")
    assert isinstance(result, CKANMutationResult)
    assert result.receipt.outcome == "succeeded"
