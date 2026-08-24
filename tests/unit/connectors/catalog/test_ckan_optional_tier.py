"""Seed-based dual-state coverage for the optional activity and resource-view tiers.

Stub probe runners drive ``EffectiveCapabilityCache`` states so each optional
family proves both the blocked (pre-dispatch, zero transport I/O) and allowed
paths independently — the RUN-01 observable distinction the v2 ids make
executable per family.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.connectors.catalog.ckan.results import CKANMutationResult
from datasluice.connectors.catalog.ckan.services.relationships_activity import (
    AsyncRelationshipsActivityService,
    SyncRelationshipsActivityService,
)
from datasluice.connectors.catalog.ckan.services.views import AsyncViewsService, SyncViewsService
from datasluice.domain.catalog.models import MappingRecord, NativeRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.errors.catalog import ForbiddenError, UnsupportedCapabilityError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
ACTIVITY_ID = "ckan/action-api-v3.activity"
VIEWS_ID = "ckan/action-api-v3.resource-views"

ACTIVITY_RESULT: dict[str, object] = {
    "id": "act-1",
    "user_id": "user-1",
    "object_id": "dataset-a",
    "activity_type": "changed package",
}
VIEW_RESULT: dict[str, object] = {"id": "view-1", "resource_id": "res-1", "view_type": "image_view", "title": "Chart"}


def _success_body(result: object) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


def _failure_body(error: dict[str, object]) -> bytes:
    return json.dumps({"success": False, "error": dict(error)}).encode("utf-8")


class SyncCaptureTransport:
    """A deterministic loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    def close(self) -> None:
        return None


class AsyncCaptureTransport:
    """A deterministic async loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    async def aclose(self) -> None:
        return None


class SeededProbeRunner:
    """A bounded stub runner answering seeded response classes per OperationId."""

    def __init__(
        self,
        *,
        unsupported: frozenset[str] = frozenset(),
        deployment_disabled: frozenset[str] = frozenset(),
    ) -> None:
        self.unsupported = unsupported
        self.deployment_disabled = deployment_disabled
        self.probed: list[OperationId] = []

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.probed.append(operation_id)
        value = str(operation_id)
        if value in self.deployment_disabled:
            response_class = ProbeResponseClass.DEPLOYMENT_DISABLED
        elif value in self.unsupported:
            response_class = ProbeResponseClass.UNSUPPORTED
        else:
            response_class = ProbeResponseClass.SUCCESS
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url="https://demo.ckan.org/api/3/action/status_show",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=response_class,
        )


class AsyncSeededProbeRunner:
    """The async twin of the seeded stub runner."""

    def __init__(self, *, unsupported: frozenset[str] = frozenset()) -> None:
        self.unsupported = unsupported
        self.probed: list[OperationId] = []

    async def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.probed.append(operation_id)
        response_class = (
            ProbeResponseClass.UNSUPPORTED if str(operation_id) in self.unsupported else (ProbeResponseClass.SUCCESS)
        )
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url="https://demo.ckan.org/api/3/action/status_show",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=response_class,
        )


def _client(transport: SyncCaptureTransport, runner: SeededProbeRunner | None = None) -> SyncCKANClient:
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
        probe_policy="auto",
        probe_runner=runner if runner is not None else SeededProbeRunner(),
    )


def _async_client(transport: AsyncCaptureTransport, runner: AsyncSeededProbeRunner | None = None) -> AsyncCKANClient:
    return AsyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
        probe_policy="auto",
        probe_runner=runner if runner is not None else AsyncSeededProbeRunner(),
    )


def _names_for(owning_id: str) -> set[str]:
    return {entry.name for entry in CKAN_ACTIONS.entries if entry.owning_operation_id == owning_id}


def test_every_activity_and_view_action_exposes_a_typed_method_on_both_mode_services() -> None:
    """Each optional-tier manifest action names a callable member on both projections."""
    sync_relationships = {name for name in dir(SyncRelationshipsActivityService) if not name.startswith("_")}
    async_relationships = {name for name in dir(AsyncRelationshipsActivityService) if not name.startswith("_")}
    activity_names = _names_for(ACTIVITY_ID)
    assert len(activity_names) == 13
    for action in activity_names:
        assert action in sync_relationships, f"sync surface misses {action}"
        assert action in async_relationships, f"async surface misses {action}"

    sync_views = {name for name in dir(SyncViewsService) if not name.startswith("_")}
    async_views = {name for name in dir(AsyncViewsService) if not name.startswith("_")}
    view_names = _names_for(VIEWS_ID)
    assert len(view_names) == 9
    for action in view_names:
        assert action in sync_views, f"sync surface misses {action}"
        assert action in async_views, f"async surface misses {action}"


def test_activity_unsupported_blocks_package_activity_list_before_any_transport_io() -> None:
    """UNPROBED-unsupported activity evidence refuses pre-dispatch at zero I/O."""
    transport = SyncCaptureTransport(body=_success_body([]))
    client = _client(transport, SeededProbeRunner(unsupported=frozenset({ACTIVITY_ID})))

    with pytest.raises(UnsupportedCapabilityError) as excinfo:
        client.relationships_activity.package_activity_list(id="dataset-a")

    assert transport.requests == []
    assert excinfo.value.capability_state == "unsupported"
    assert excinfo.value.safe_action


def test_activity_success_state_dispatches_and_decodes_activity_kind_records() -> None:
    """SUCCESS activity evidence dispatches and decodes activity-kind records."""
    transport = SyncCaptureTransport(body=_success_body([ACTIVITY_RESULT]))
    client = _client(transport)

    envelope = client.relationships_activity.package_activity_list(id="dataset-a")

    assert len(transport.requests) == 1
    assert transport.requests[0].url.endswith("/api/3/action/package_activity_list")
    record = envelope.items[0]
    assert isinstance(record, NativeRecord)
    assert record.resource_kind.value == "activity"
    assert record.id.value == "act-1"


def test_activity_resolves_independently_of_the_core_relationships_id() -> None:
    """Blocked activity evidence never blocks the core relationships-follows family."""
    transport = SyncCaptureTransport(body=_success_body([{"subject": "a", "object": "b"}]))
    client = _client(transport, SeededProbeRunner(unsupported=frozenset({ACTIVITY_ID})))

    envelope = client.relationships_activity.package_relationships_list(
        id="dataset-a", id2="dataset-b", rel="depends_on"
    )

    assert len(transport.requests) == 1
    assert isinstance(envelope.items[0], MappingRecord)
    with pytest.raises(UnsupportedCapabilityError):
        client.relationships_activity.dashboard_activity_list()
    assert len(transport.requests) == 1


def test_views_unsupported_blocks_resource_view_list_before_any_transport_io() -> None:
    """UNSUPPORTED views evidence refuses pre-dispatch at zero I/O."""
    transport = SyncCaptureTransport(body=_success_body([VIEW_RESULT]))
    client = _client(transport, SeededProbeRunner(unsupported=frozenset({VIEWS_ID})))

    with pytest.raises(UnsupportedCapabilityError) as excinfo:
        client.views.resource_view_list(id="res-1")

    assert transport.requests == []
    assert excinfo.value.capability_state == "unsupported"


def test_views_success_state_dispatches_and_decodes_view_records() -> None:
    """SUCCESS views evidence dispatches and decodes lossless view mappings."""
    transport = SyncCaptureTransport(body=_success_body([VIEW_RESULT]))
    client = _client(transport)

    envelope = client.views.resource_view_list(id="res-1")

    assert len(transport.requests) == 1
    assert transport.requests[0].url.endswith("/api/3/action/resource_view_list")
    assert json.loads(transport.requests[0].body or b"{}") == {"id": "res-1"}
    mapping = envelope.items[0]
    assert isinstance(mapping, MappingRecord)
    assert dict(mapping.payload)["view_type"] == "image_view"


def test_dashboard_and_activity_create_dispatch_receipt_bearing_when_available() -> None:
    """Available activity evidence lets the standard mutations return receipts."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    marked = client.relationships_activity.dashboard_mark_activities_old()

    assert isinstance(marked, CKANMutationResult)
    assert marked.receipt.outcome == "succeeded"

    create_transport = SyncCaptureTransport(body=_success_body(ACTIVITY_RESULT))
    create_client = _client(create_transport)
    created = create_client.relationships_activity.activity_create(
        user_id="user-1",
        object_id="dataset-a",
        activity_type="changed package",
        data={"package": {"title": "New"}},
    )
    assert isinstance(created, CKANMutationResult)
    assert json.loads(create_transport.requests[0].body or b"{}") == {
        "user_id": "user-1",
        "object_id": "dataset-a",
        "activity_type": "changed package",
        "data": {"package": {"title": "New"}},
    }


def test_send_email_notifications_forbidden_envelope_maps_to_forbidden_error() -> None:
    """The privileged notification action maps forbidden envelopes with a safe action."""
    transport = SyncCaptureTransport(
        body=_failure_body({"__type": "Authorization Error", "message": "not authorized to send notifications"})
    )
    client = _client(transport)

    with pytest.raises(ForbiddenError) as excinfo:
        client.relationships_activity.send_email_notifications()

    assert excinfo.value.safe_action
    assert len(transport.requests) == 1


def test_default_resource_views_pass_their_objects_verbatim() -> None:
    """Default-view creation actions cross with their documented objects untouched."""
    package_transport = SyncCaptureTransport(body=_success_body([VIEW_RESULT]))
    package_client = _client(package_transport)

    package = {"id": "dataset-a", "resources": [{"id": "res-1"}]}
    package_result = package_client.views.package_create_default_resource_views(
        package=package, create_datastore_views=True
    )

    assert json.loads(package_transport.requests[0].body or b"{}") == {
        "package": package,
        "create_datastore_views": True,
    }
    assert package_result.receipt.target.resource_kind.value == "dataset"

    resource_transport = SyncCaptureTransport(body=_success_body([VIEW_RESULT]))
    resource_client = _client(resource_transport)

    resource = {"id": "res-1", "format": "CSV"}
    resource_result = resource_client.views.resource_create_default_resource_views(resource=resource)

    assert json.loads(resource_transport.requests[0].body or b"{}") == {"resource": resource}
    assert resource_result.receipt.target.resource_kind.value == "resource"


def test_async_views_mirror_the_dual_state_semantics_per_family() -> None:
    """The async twin blocks and dispatches the views family from its own evidence."""
    blocked_transport = AsyncCaptureTransport(body=_success_body([VIEW_RESULT]))
    blocked_client = _async_client(blocked_transport, AsyncSeededProbeRunner(unsupported=frozenset({VIEWS_ID})))

    with pytest.raises(UnsupportedCapabilityError):
        asyncio.run(blocked_client.views.resource_view_list(id="res-1"))
    assert blocked_transport.requests == []

    allowed_transport = AsyncCaptureTransport(body=_success_body([VIEW_RESULT]))
    allowed_client = _async_client(allowed_transport)
    envelope = asyncio.run(allowed_client.views.resource_view_list(id="res-1"))
    assert isinstance(envelope.items[0], MappingRecord)
    assert len(allowed_transport.requests) == 1
