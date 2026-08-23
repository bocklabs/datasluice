"""Deterministic loopback coverage for the CKAN live clients."""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
from collections.abc import Mapping

import pytest

from datasluice.connectors.catalog.ckan.clients import (
    AsyncCKANClient,
    SyncCKANClient,
    create_async_client,
    create_sync_client,
    declared_ckan_profile,
)
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS, ActionEntry, ActionInventory
from datasluice.connectors.catalog.ckan.settings import CKANClientSettings
from datasluice.contracts.catalog.native.ckan import SyncCKANServices
from datasluice.contracts.catalog.protocols import (
    CatalogOperationGuard,
    CatalogOperationRequest,
    SyncCatalogClient,
)
from datasluice.domain.catalog.auth import CKANCredential
from datasluice.domain.catalog.models import DatasetRecord, MappingRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.errors.catalog import CatalogValidationError, UnauthenticatedError, UnsupportedCapabilityError
from datasluice.runtime.capability import ProbeRunner
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
DATASTORE_OPERATION_ID = "ckan/datastore-extension.query-and-record-crud"

STATUS_RESULT: dict[str, object] = {
    "ckan_version": "2.11.5",
    "site_title": "Demo CKAN",
    "extensions": ["datastore"],
}
STATUS_PAYLOAD_FROZEN: dict[str, object] = {
    "ckan_version": "2.11.5",
    "site_title": "Demo CKAN",
    "extensions": ("datastore",),
}
PACKAGE_RESULT: dict[str, object] = {
    "id": "abc-123",
    "name": "my-dataset",
    "title": "My Dataset",
    "notes": "A seeded dataset",
}


def _success_body(result: Mapping[str, object]) -> bytes:
    return json.dumps({"success": True, "result": dict(result)}).encode("utf-8")


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


class StubProbeRunner:
    """A bounded probe runner returning sanitized demo-origin evidence."""

    def __init__(self) -> None:
        self.probed: list[OperationId] = []

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.probed.append(operation_id)
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url="https://demo.ckan.org/api/3/action/datastore_search",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=ProbeResponseClass.SUCCESS,
        )


def _settings(
    transport: SyncCaptureTransport,
    *,
    credential: CKANCredential | None = None,
) -> CKANClientSettings:
    return CKANClientSettings(base_url=LOOPBACK_ORIGIN, sync_transport=transport, credential=credential)


def _direct_client(
    transport: SyncCaptureTransport,
    *,
    inventory: ActionInventory = CKAN_ACTIONS,
    probe_policy: str = "auto",
    probe_runner: StubProbeRunner | None = None,
    owns_transport: bool = True,
) -> SyncCKANClient:
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        inventory=inventory,
        probe_policy=probe_policy,
        probe_runner=probe_runner,
        owns_transport=owns_transport,
    )


def _datastore_inventory() -> ActionInventory:
    return ActionInventory(
        (
            ActionEntry(
                name="datastore_search",
                group="datastore",
                owning_operation_id=DATASTORE_OPERATION_ID,
                mutation_class="read",
                result_kind="mapping",
            ),
        )
    )


def _datastore_call() -> tuple[CatalogOperationRequest, CatalogOperationGuard]:
    operation = CatalogOperationRequest(
        operation_id=OperationId(platform="ckan", service="datastore-extension", method="query-and-record-crud"),
        payload={"action": "datastore_search", "resource_id": "seed-resource"},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)
    return operation, guard


@pytest.mark.skipif(importlib.util.find_spec("httpx") is None, reason="datasluice[ckan] requires httpx")
def test_status_show_flows_end_to_end_through_factory_settings_spine_and_mapping() -> None:
    """One documented read decodes an authentic CKAN envelope into a typed mapping item."""
    transport = SyncCaptureTransport(body=_success_body(STATUS_RESULT))
    client = create_sync_client(_settings(transport))

    assert isinstance(client, SyncCatalogClient)
    assert isinstance(client, SyncCKANServices)

    envelope = client.action_discovery.status_show()

    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.method == "POST"
    assert request.url == f"{LOOPBACK_ORIGIN}/api/3/action/status_show"
    assert json.loads(request.body or b"{}") == {}
    assert len(envelope.items) == 1
    item = envelope.items[0]
    assert isinstance(item, MappingRecord)
    assert dict(item.payload) == STATUS_PAYLOAD_FROZEN


@pytest.mark.skipif(importlib.util.find_spec("httpx") is None, reason="datasluice[ckan] requires httpx")
def test_authorization_header_rides_the_real_ckan_credential() -> None:
    """The existing Authorization seam carries a genuine CKANCredential api_token."""
    transport = SyncCaptureTransport(body=_success_body(STATUS_RESULT))
    settings = _settings(transport, credential=CKANCredential(api_token="secret-token-123"))
    client = create_sync_client(settings)

    client.action_discovery.help_show()

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/help_show")
    assert request.headers["Authorization"] == "secret-token-123"


def test_anonymous_dispatch_sends_no_credential_headers() -> None:
    """Anonymous reads attach only content headers."""
    transport = SyncCaptureTransport(body=_success_body(STATUS_RESULT))
    client = _direct_client(transport)

    client.action_discovery.status_show()

    request = transport.requests[0]
    assert "Authorization" not in request.headers


def test_error_envelope_maps_to_a_typed_error_with_bounded_metadata() -> None:
    """A success:false envelope raises a typed error naming a safe next action."""
    transport = SyncCaptureTransport(body=_failure_body({"__type": "Authorization Error", "message": "bad token"}))
    client = _direct_client(transport)

    with pytest.raises(UnauthenticatedError) as excinfo:
        client.action_discovery.status_show()

    assert excinfo.value.safe_action
    assert "__type" not in excinfo.value.metadata


def test_umbrella_route_validates_the_payload_action_against_the_registry() -> None:
    """Umbrella methods accept only manifest-registered actions owned by their group."""
    transport = SyncCaptureTransport(body=_success_body(STATUS_RESULT))
    client = _direct_client(transport)
    operation = CatalogOperationRequest(
        operation_id=OperationId(platform="ckan", service="action-api-v3", method="discovery-help-and-status"),
        payload={"action": "status_show"},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    envelope = client.action_discovery.discovery_help_and_status(operation, guard)

    item = envelope.items[0]
    assert isinstance(item, MappingRecord)
    assert dict(item.payload) == STATUS_PAYLOAD_FROZEN

    rogue = CatalogOperationRequest(operation_id=operation.operation_id, payload={"action": "package_purge"})
    with pytest.raises(CatalogValidationError):
        client.action_discovery.discovery_help_and_status(rogue, guard)
    assert len(transport.requests) == 1


def test_optional_capability_refuses_without_probe_runner_at_zero_io() -> None:
    """OPTIONAL-tier dispatch refuses before any wire I/O naming both remedies."""
    transport = SyncCaptureTransport(body=_success_body({"records": []}))
    client = _direct_client(transport, inventory=_datastore_inventory(), probe_policy="auto")
    operation, guard = _datastore_call()

    with pytest.raises(UnsupportedCapabilityError) as excinfo:
        client.datastore.query_and_record_crud(operation, guard)

    assert transport.requests == []
    assert "probe runner" in excinfo.value.safe_action
    assert "declared-baseline" in excinfo.value.safe_action


def test_optional_capability_dispatches_with_attached_probe_runner() -> None:
    """An attached probe runner supplies the missing evidence over the demo origin."""
    transport = SyncCaptureTransport(body=_success_body({"records": [], "count": 0}))
    runner = StubProbeRunner()
    client = _direct_client(transport, inventory=_datastore_inventory(), probe_policy="auto", probe_runner=runner)
    operation, guard = _datastore_call()

    envelope = client.datastore.query_and_record_crud(operation, guard)

    assert runner.probed
    assert len(transport.requests) == 1
    record_item = envelope.items[0]
    assert isinstance(record_item, MappingRecord)
    assert dict(record_item.payload) == {"records": (), "count": 0}


def test_optional_capability_dispatches_under_declared_baseline_policy() -> None:
    """Explicit declared-baseline selection consumes the declared profile trustingly."""
    transport = SyncCaptureTransport(body=_success_body({"records": [], "count": 0}))
    client = _direct_client(transport, inventory=_datastore_inventory(), probe_policy="declared-baseline")
    operation, guard = _datastore_call()

    client.datastore.query_and_record_crud(operation, guard)

    assert len(transport.requests) == 1


def test_borrowed_transport_is_never_closed_by_the_client() -> None:
    """An injected transport instance stays caller-owned across close()."""
    transport = SyncCaptureTransport(body=_success_body(STATUS_RESULT))
    client = _direct_client(transport, owns_transport=False)

    client.close()
    client.close()

    assert transport.close_count == 0


@pytest.mark.skipif(importlib.util.find_spec("httpx") is None, reason="datasluice[ckan] requires httpx")
def test_owned_factory_transport_closes_exactly_once() -> None:
    """A factory-produced transport is client-owned and closes exactly once."""
    holder: dict[str, SyncCaptureTransport] = {}

    def factory() -> SyncCaptureTransport:
        holder["transport"] = SyncCaptureTransport(body=_success_body(STATUS_RESULT))
        return holder["transport"]

    client = create_sync_client(CKANClientSettings(base_url=LOOPBACK_ORIGIN, sync_transport=factory))
    assert client.transport is holder["transport"]

    client.close()
    client.close()

    assert holder["transport"].close_count == 1


def test_normalized_datasets_get_round_trips_to_its_own_record_kind() -> None:
    """The normalized dataset projection decodes its own record kind over internal envelopes."""
    transport = SyncCaptureTransport(body=_success_body(PACKAGE_RESULT))
    client = _direct_client(transport)
    operation = CatalogOperationRequest(
        operation_id=OperationId(platform="ckan", service="datasets", method="get"),
        payload={"id": "my-dataset"},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    envelope = client.datasets.get(operation, guard)

    assert transport.requests[0].url.endswith("/api/3/action/package_show")
    assert json.loads(transport.requests[0].body or b"{}") == {"id": "my-dataset"}
    assert len(envelope.items) == 1
    record = envelope.items[0]
    assert isinstance(record, DatasetRecord)
    assert record.name == "my-dataset"
    assert record.description == "A seeded dataset"
    assert record.id.value == "abc-123"


def test_runner_conformance_holds_for_the_stub() -> None:
    """The stub probe runner satisfies the published ProbeRunner protocol."""
    runner: ProbeRunner = StubProbeRunner()
    assert isinstance(runner, ProbeRunner)


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


def _async_client(transport: AsyncCaptureTransport, *, owns_transport: bool = True) -> AsyncCKANClient:
    return AsyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=owns_transport,
    )


def _async_settings(transport: AsyncCaptureTransport) -> CKANClientSettings:
    return CKANClientSettings(base_url=LOOPBACK_ORIGIN, async_transport=transport)


@pytest.mark.skipif(importlib.util.find_spec("httpx") is None, reason="datasluice[ckan] requires httpx")
def test_async_status_show_flows_end_to_end_over_the_loopback_twin() -> None:
    """The async twin decodes the same authentic CKAN envelope through its own pipeline."""
    transport = AsyncCaptureTransport(body=_success_body(STATUS_RESULT))
    client = create_async_client(_async_settings(transport))

    envelope = asyncio.run(client.action_discovery.status_show())

    assert len(transport.requests) == 1
    assert transport.requests[0].url == f"{LOOPBACK_ORIGIN}/api/3/action/status_show"
    item = envelope.items[0]
    assert isinstance(item, MappingRecord)
    assert dict(item.payload) == STATUS_PAYLOAD_FROZEN


def test_sync_async_clients_maintain_strict_structural_parity() -> None:
    """Client members mirror one-to-one modulo only the documented constructor deltas."""
    lifecycle = {"close": "aclose", "__enter__": "__aenter__", "__exit__": "__aexit__"}
    sync_members = {
        lifecycle.get(name, name)
        for name, value in vars(SyncCKANClient).items()
        if callable(value) or isinstance(value, property)
    }
    async_members = {
        name for name, value in vars(AsyncCKANClient).items() if callable(value) or isinstance(value, property)
    }
    assert sync_members == async_members

    assert set(inspect.signature(SyncCKANClient.__init__).parameters) == set(
        inspect.signature(AsyncCKANClient.__init__).parameters
    )
    sync_annotation = inspect.signature(SyncCKANClient.__init__).parameters["retry_sleep"].annotation
    async_annotation = inspect.signature(AsyncCKANClient.__init__).parameters["retry_sleep"].annotation
    assert "Awaitable" in str(async_annotation)
    assert "Awaitable" not in str(sync_annotation)

    families = (
        "datasets",
        "resources",
        "organizations",
        "action_discovery",
        "groups",
        "users",
        "vocabularies_licenses",
        "relationships_activity",
        "views",
        "datastore",
        "filestore",
        "extensions",
    )
    transport = SyncCaptureTransport(body=_success_body(STATUS_RESULT))
    async_transport = AsyncCaptureTransport(body=_success_body(STATUS_RESULT))
    sync_client = _direct_client(transport)
    async_client = _async_client(async_transport)
    for family in families:
        sync_service = getattr(sync_client, family)
        async_service = getattr(async_client, family)
        sync_surface = {name for name in dir(sync_service) if not name.startswith("__")}
        async_surface = {name for name in dir(async_service) if not name.startswith("__")}
        assert sync_surface == async_surface, family


def test_async_borrowed_transport_is_never_drained_by_the_client() -> None:
    """An injected async transport stays caller-owned across aclose()."""
    transport = AsyncCaptureTransport(body=_success_body(STATUS_RESULT))
    client = _async_client(transport, owns_transport=False)

    asyncio.run(client.aclose())
    asyncio.run(client.aclose())

    assert transport.close_count == 0


@pytest.mark.skipif(importlib.util.find_spec("httpx") is None, reason="datasluice[ckan] requires httpx")
def test_async_owned_transport_acloses_exactly_once() -> None:
    """A factory-produced async transport is client-owned and drains exactly once."""
    holder: dict[str, AsyncCaptureTransport] = {}

    def factory() -> AsyncCaptureTransport:
        holder["transport"] = AsyncCaptureTransport(body=_success_body(STATUS_RESULT))
        return holder["transport"]

    client = create_async_client(CKANClientSettings(base_url=LOOPBACK_ORIGIN, async_transport=factory))
    assert client.transport is holder["transport"]

    asyncio.run(client.aclose())
    asyncio.run(client.aclose())

    assert holder["transport"].close_count == 1
