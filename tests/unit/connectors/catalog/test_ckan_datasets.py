"""Deterministic loopback coverage for the exhaustive CKAN dataset action surface."""

from __future__ import annotations

import asyncio
import inspect
import json
from importlib import resources
from typing import cast

import pytest

from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.connectors.catalog.ckan.results import CKANMutationResult
from datasluice.connectors.catalog.ckan.services.datasets import AsyncDatasetsService, SyncDatasetsService
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import CKANCredential
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ValueRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, UnsupportedCapabilityError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"

PACKAGE_CREATE_RESULT: dict[str, object] = {"id": "pkg-1", "name": "created-dataset", "title": "Created"}
COLLABORATOR_RESULT: dict[str, object] = {
    "pkg-1": [{"user_id": "u-1", "capacity": "editor"}],
}


def _success_body(result: object) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


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


class StubProbeRunner:
    """A bounded probe runner returning sanitized demo-origin evidence."""

    def __init__(self, response_class: ProbeResponseClass = ProbeResponseClass.SUCCESS) -> None:
        self.response_class = response_class
        self.probed: list[OperationId] = []

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.probed.append(operation_id)
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url="https://demo.ckan.org/api/3/action/status_show",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=self.response_class,
        )


def _client(
    transport: SyncCaptureTransport,
    *,
    credential: CKANCredential | None = None,
    probe_runner: StubProbeRunner | None = None,
) -> SyncCKANClient:
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        credentials=credential,
        owns_transport=False,
        probe_runner=probe_runner,
    )


def _async_client(transport: AsyncCaptureTransport) -> AsyncCKANClient:
    return AsyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
    )


def _confirmed_destructive_policy() -> MutationPolicy:
    return MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def test_package_search_sends_solr_rows_and_start_verbatim() -> None:
    """D-04 fidelity: Solr paging parameters cross the wire untranslated."""
    transport = SyncCaptureTransport(body=_success_body({"count": 0, "results": []}))
    client = _client(transport)

    client.datasets.package_search(q="health", rows=5, start=10)

    request = transport.requests[0]
    assert request.method == "POST"
    assert request.url.endswith("/api/3/action/package_search")
    assert json.loads(request.body or b"{}") == {"q": "health", "rows": 5, "start": 10}


def test_package_search_preserves_every_server_sent_record_key_losslessly() -> None:
    """CON-04/D-01: record payloads keep all keys including deprecated-style extras."""
    first: dict[str, object] = {"id": "one", "name": "first", "title": "First"}
    second: dict[str, object] = {
        "id": "two",
        "name": "second",
        "title": "Second",
        "facets": {"organization": "health"},
    }
    transport = SyncCaptureTransport(body=_success_body({"count": 2, "results": [first, second]}))
    client = _client(transport)

    envelope = client.datasets.package_search(q="health")

    assert envelope.page is not None and envelope.page.total_items == 2
    assert len(envelope.items) == 2
    assert all(isinstance(item, NativeRecord) for item in envelope.items)
    records = [item for item in envelope.items if isinstance(item, NativeRecord)]
    assert records[0].resource_kind.value == "dataset"
    assert dict(records[0].payload) == first
    assert dict(records[1].payload) == second


def test_current_package_list_uses_native_limit_offset_only() -> None:
    """The documented canonical pagination parameters ride verbatim."""
    transport = SyncCaptureTransport(body=_success_body([{"id": "one", "name": "first"}]))
    client = _client(transport)

    envelope = client.datasets.current_package_list_with_resources(limit=7, offset=14)

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/current_package_list_with_resources")
    assert json.loads(request.body or b"{}") == {"limit": 7, "offset": 14}
    record = next(item for item in envelope.items if isinstance(item, NativeRecord))
    assert record.id.value == "one"


def test_typed_signature_rejects_the_deprecated_page_parameter_with_type_error() -> None:
    """D-01: the deprecated pagination name is unrepresentable on the typed surface."""
    transport = SyncCaptureTransport(body=_success_body([]))
    client = _client(transport)

    with pytest.raises(TypeError):
        client.datasets.current_package_list_with_resources(page=2)  # ty: ignore[unknown-argument]

    assert transport.requests == []


def test_umbrella_payload_with_deprecated_page_raises_validation_error_before_dispatch() -> None:
    """Umbrella payload-dict calls get an explicit typed refusal at zero I/O."""
    transport = SyncCaptureTransport(body=_success_body([]))
    client = _client(transport)
    operation = CatalogOperationRequest(
        operation_id=OperationId(platform="ckan", service="action-api-v3", method="dataset-list-show-search"),
        payload={"action": "current_package_list_with_resources", "page": 2},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    with pytest.raises(CatalogValidationError) as excinfo:
        client.datasets.list_show_search(operation, guard)

    assert transport.requests == []
    assert "page" in excinfo.value.safe_action or "page" in str(excinfo.value)


def test_normalized_list_rejects_the_deprecated_page_key_before_dispatch() -> None:
    """The normalized list projection enforces the same deprecation discipline."""
    transport = SyncCaptureTransport(body=_success_body([]))
    client = _client(transport)
    operation = CatalogOperationRequest(
        operation_id=OperationId(platform="ckan", service="datasets", method="list"),
        payload={"page": 3},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    with pytest.raises(CatalogValidationError):
        client.datasets.list(operation, guard)

    assert transport.requests == []


def test_package_create_returns_mutation_result_with_redacted_receipt() -> None:
    """Standard-tier mutations return decoded results plus a redacted receipt."""
    transport = SyncCaptureTransport(body=_success_body(PACKAGE_CREATE_RESULT))
    credential = CKANCredential(api_token="secret-token-123")
    client = _client(transport, credential=credential)

    result = client.datasets.package_create(name="created-dataset", title="Created", owner_org="org-1")

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/package_create")
    assert json.loads(request.body or b"{}") == {
        "name": "created-dataset",
        "title": "Created",
        "owner_org": "org-1",
    }
    assert isinstance(result, CKANMutationResult)
    record = next(item for item in result.result.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "dataset"
    assert record.id.value == "pkg-1"
    receipt = result.receipt
    assert isinstance(receipt, MutationReceipt)
    assert receipt.outcome == "succeeded"
    assert receipt.operation == "ckan/action-api-v3.dataset-create-update-patch-delete-purge"
    assert receipt.target.value == "created-dataset"
    serialized = json.dumps(result.to_dict())
    assert "secret-token-123" not in serialized
    assert "Authorization" not in serialized


def test_dataset_purge_refuses_an_unconfirmed_policy_at_zero_transport_io() -> None:
    """T-03-06-01 mitigation: destructive purge gates pre-dispatch with zero wire hits."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    with pytest.raises(CatalogValidationError) as excinfo:
        client.datasets.dataset_purge(id="pkg-1", policy=MutationPolicy(destructive=True))

    assert transport.requests == []
    assert excinfo.value.safe_action
    assert "destructive" in str(excinfo.value)


def test_dataset_purge_requires_a_policy_at_the_call_boundary() -> None:
    """The destructive tier cannot be engaged without an explicit policy keyword."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    with pytest.raises(TypeError):
        client.datasets.dataset_purge(id="pkg-1")  # ty: ignore[missing-argument]

    assert transport.requests == []


def test_confirmed_dataset_purge_dispatches_once_with_receipt() -> None:
    """A confirmed destructive policy dispatches exactly once and yields a receipt."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    result = client.datasets.dataset_purge(id="pkg-1", policy=_confirmed_destructive_policy())

    assert len(transport.requests) == 1
    assert transport.requests[0].url.endswith("/api/3/action/dataset_purge")
    value_item = result.result.items[0]
    assert isinstance(value_item, ValueRecord)
    assert result.receipt.outcome == "succeeded"


def test_collaborator_reads_dispatch_under_attached_probe_runner() -> None:
    """Config-gated collaborator actions stay honest behind optional-tier evidence."""
    transport = SyncCaptureTransport(body=_success_body(COLLABORATOR_RESULT))
    runner = StubProbeRunner()
    client = _client(transport, probe_runner=runner)

    envelope = client.datasets.package_collaborator_list(id="pkg-1")

    assert runner.probed
    assert len(transport.requests) == 1
    assert transport.requests[0].url.endswith("/api/3/action/package_collaborator_list")
    mapping = envelope.items[0]
    assert isinstance(mapping, MappingRecord)
    assert dict(mapping.payload)["pkg-1"] == ({"user_id": "u-1", "capacity": "editor"},)


def test_disabled_collaborator_probe_state_surfaces_a_typed_refusal_not_a_silent_noop() -> None:
    """A deployment without collaborators resolves to the typed unsupported path."""
    transport = SyncCaptureTransport(body=_success_body(COLLABORATOR_RESULT))
    runner = StubProbeRunner(response_class=ProbeResponseClass.DEPLOYMENT_DISABLED)
    client = _client(transport, probe_runner=runner)

    with pytest.raises(UnsupportedCapabilityError):
        client.datasets.package_collaborator_list_for_user(user_id="u-1")

    assert transport.requests == []


def test_every_manifest_dataset_action_exposes_a_typed_method_on_both_mode_services() -> None:
    """Manifest-driven completeness: each registered dataset action names a typed method."""
    entries = [entry for entry in CKAN_ACTIONS.entries if entry.group == "datasets"]
    assert len(entries) == 20
    sync_surface = {name for name in dir(SyncDatasetsService) if not name.startswith("_")}
    async_surface = {name for name in dir(AsyncDatasetsService) if not name.startswith("_")}
    for entry in entries:
        assert entry.name in sync_surface, f"sync surface misses {entry.name}"
        assert entry.name in async_surface, f"async surface misses {entry.name}"


def test_async_package_search_and_create_mirror_the_sync_semantics() -> None:
    """The async twin keeps faithful paging and receipt-bearing mutations."""
    transport = AsyncCaptureTransport(body=_success_body(PACKAGE_CREATE_RESULT))
    client = _async_client(transport)

    search_transport = AsyncCaptureTransport(body=_success_body({"count": 0, "results": []}))
    search_client = _async_client(search_transport)
    asyncio.run(search_client.datasets.package_search(q="health", rows=5, start=10))

    result = asyncio.run(client.datasets.package_create(name="created-dataset"))

    assert json.loads(search_transport.requests[0].body or b"{}") == {"q": "health", "rows": 5, "start": 10}
    assert isinstance(result, CKANMutationResult)
    assert result.receipt.outcome == "succeeded"


def test_dataset_service_surfaces_stay_in_structural_lockstep_across_modes() -> None:
    """Sync/async dataset projections expose identical members with mode-correct dispatch."""
    sync_members = {name for name in dir(SyncDatasetsService) if not name.startswith("__")}
    async_members = {name for name in dir(AsyncDatasetsService) if not name.startswith("__")}
    assert sync_members == async_members
    public = {name for name in vars(SyncDatasetsService) if not name.startswith("_")}
    for name in public:
        assert inspect.iscoroutinefunction(getattr(AsyncDatasetsService, name)), name
        assert not inspect.iscoroutinefunction(getattr(SyncDatasetsService, name)), name


def _cases_document() -> dict[str, object]:
    raw = (
        resources.files("datasluice.contracts.catalog.fixtures")
        .joinpath("ckan")
        .joinpath("cases.json")
        .read_text(encoding="utf-8")
    )
    return cast("dict[str, object]", json.loads(raw))


_DATASET_PREFIX = "action-api-v3.dataset-"
_RESOURCE_UMBRELLA = "ckan/action-api-v3.resource-list-show-create-update-patch-delete-upload"


def _dataset_or_resource_rows() -> list[dict[str, object]]:
    document = _cases_document()
    cases = cast("list[object]", document.get("cases", []))
    rows = [row for row in cases if isinstance(row, dict)]
    return [
        row
        for row in rows
        if str(row.get("operation", "")).split("/", 1)[-1].startswith(_DATASET_PREFIX)
        or row.get("operation") == _RESOURCE_UMBRELLA
    ]


def test_corpus_dataset_and_resource_rows_resolve_to_manifest_actions() -> None:
    """Every corpus row of these families names a manifest-owned v2 operation id."""
    from datasluice.contracts.catalog.fixtures import load_reference_fixture_set

    fixture_set = load_reference_fixture_set("ckan")
    owners = {entry.owning_operation_id for entry in CKAN_ACTIONS.entries}
    rows = _dataset_or_resource_rows()
    assert rows
    for row in rows:
        assert str(row["operation"]) in owners, f"corpus row outside the manifest: {row}"

    pairs = {(str(case.operation_id), case.outcome) for case in fixture_set.cases}
    assert ("ckan/action-api-v3.dataset-list-show-search", "core") in pairs
    assert ("ckan/action-api-v3.dataset-create-update-patch-delete-purge", "authenticated-success") in pairs
    assert ("ckan/action-api-v3.dataset-create-update-patch-delete-purge", "forbidden") in pairs
    assert ("ckan/action-api-v3.dataset-collaborators", "deployment-disabled") in pairs
    assert ("ckan/action-api-v3.resource-list-show-create-update-patch-delete-upload", "core") in pairs


def test_corpus_receipt_metadata_is_bounded_and_token_free() -> None:
    """Purge/receipt evidence rides allowlisted metadata carrying no token shapes."""
    rows = [row for row in _dataset_or_resource_rows() if isinstance(row.get("receipt_metadata"), dict)]
    assert rows
    blob = json.dumps([row["receipt_metadata"] for row in rows]).lower()
    for forbidden in ("token", "authorization", "bearer", "secret", "password", "api_key"):
        assert forbidden not in blob, f"receipt metadata leaks credential shape: {forbidden}"
    for row in rows:
        metadata = cast("dict[str, object]", row["receipt_metadata"])
        assert set(metadata) <= {"receipt_id_shape", "actor_role_class"}
