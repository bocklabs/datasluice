"""Deterministic loopback coverage for the exhaustive CKAN organization surface."""

from __future__ import annotations

import asyncio
import inspect
import json

import pytest

from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.connectors.catalog.ckan.mapping import RECORD_KINDS, RESULT_KINDS
from datasluice.connectors.catalog.ckan.results import CKANMutationResult
from datasluice.connectors.catalog.ckan.services.organizations import (
    AsyncOrganizationsService,
    SyncOrganizationsService,
)
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import CKANCredential
from datasluice.domain.catalog.models import NativeRecord, ValueRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError, UnauthenticatedError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
ORG_READ_ID = "ckan/action-api-v3.organization-list-show-search"
ORG_WRITE_ID = "ckan/action-api-v3.organization-create-update-delete-members"

EXPECTED_ORGANIZATION_ACTIONS = frozenset(
    {
        "organization_list",
        "organization_list_for_user",
        "organization_show",
        "organization_autocomplete",
        "organization_create",
        "organization_update",
        "organization_patch",
        "organization_delete",
        "organization_purge",
        "organization_member_create",
        "organization_member_delete",
        "member_create",
        "member_delete",
        "member_list",
        "member_roles_list",
    }
)

ORGANIZATION_RESULT: dict[str, object] = {"id": "org-1", "name": "health-org", "title": "Health"}
MEMBER_RESULT: dict[str, object] = {
    "id": "m-1",
    "object": "user-1",
    "object_type": "user",
    "capacity": "editor",
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


def _client(transport: SyncCaptureTransport) -> SyncCKANClient:
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
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


def test_organization_manifest_holds_exactly_the_documented_fifteen_actions() -> None:
    """Manifest-driven completeness: exact name set with honest tier and id splits."""
    entries = [entry for entry in CKAN_ACTIONS.entries if entry.group == "organizations"]
    assert {entry.name for entry in entries} == EXPECTED_ORGANIZATION_ACTIONS
    assert len(entries) == 15
    assert {entry.owning_operation_id for entry in entries} == {ORG_READ_ID, ORG_WRITE_ID}
    reads = [entry for entry in entries if entry.mutation_class == "read"]
    standards = [entry for entry in entries if entry.mutation_class == "standard"]
    destructive = [entry for entry in entries if entry.mutation_class == "destructive"]
    assert len(reads) == 6
    assert len(standards) == 8
    assert len(destructive) == 1
    purge = next(entry for entry in entries if entry.name == "organization_purge")
    assert purge.mutation_class == "destructive"
    for entry in entries:
        spec = RESULT_KINDS.get(entry.name)
        assert spec is not None, f"{entry.name} is absent from the mapping truth table"
        outcome, family = spec
        assert outcome == entry.result_kind
        if family is not None:
            assert family in RECORD_KINDS


def test_every_manifest_organization_action_exposes_a_typed_method_on_both_mode_services() -> None:
    """Each registered organization action names a callable member on both projections."""
    sync_surface = {name for name in dir(SyncOrganizationsService) if not name.startswith("_")}
    async_surface = {name for name in dir(AsyncOrganizationsService) if not name.startswith("_")}
    for action in EXPECTED_ORGANIZATION_ACTIONS:
        assert action in sync_surface, f"sync surface misses {action}"
        assert action in async_surface, f"async surface misses {action}"


def test_organization_surfaces_stay_in_structural_lockstep_across_modes() -> None:
    """Sync/async organization projections expose identical members, mode-correct dispatch."""
    sync_members = {name for name in dir(SyncOrganizationsService) if not name.startswith("__")}
    async_members = {name for name in dir(AsyncOrganizationsService) if not name.startswith("__")}
    assert sync_members == async_members
    public = {name for name in vars(SyncOrganizationsService) if not name.startswith("_")}
    for name in public:
        assert inspect.iscoroutinefunction(getattr(AsyncOrganizationsService, name)), name
        assert not inspect.iscoroutinefunction(getattr(SyncOrganizationsService, name)), name


def test_organization_list_sends_native_sort_and_offset_verbatim() -> None:
    """D-04 fidelity: documented list parameters cross the wire untranslated."""
    transport = SyncCaptureTransport(body=_success_body(["health-org", "transit-org"]))
    client = _client(transport)

    envelope = client.organizations.organization_list(sort="name desc", limit=5, offset=10)

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/organization_list")
    assert json.loads(request.body or b"{}") == {"sort": "name desc", "limit": 5, "offset": 10}
    values = [item for item in envelope.items if isinstance(item, ValueRecord)]
    assert len(values) == 2


def test_organization_show_decodes_an_organization_kind_record() -> None:
    """Reads under the core read id decode their own organization kind."""
    transport = SyncCaptureTransport(body=_success_body(ORGANIZATION_RESULT))
    client = _client(transport)

    envelope = client.organizations.organization_show(id="org-1", include_users=True)

    assert transport.requests[0].url.endswith("/api/3/action/organization_show")
    assert json.loads(transport.requests[0].body or b"{}") == {"id": "org-1", "include_users": True}
    record = next(item for item in envelope.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "organization"
    assert record.id.value == "org-1"
    assert dict(record.payload) == ORGANIZATION_RESULT


def test_organization_autocomplete_and_member_reads_decode_their_own_kinds() -> None:
    """Autocomplete yields organization records; member_list yields member records."""
    transport = SyncCaptureTransport(body=_success_body([ORGANIZATION_RESULT]))
    client = _client(transport)
    envelope = client.organizations.organization_autocomplete(q="hea")
    record = next(item for item in envelope.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "organization"

    member_transport = SyncCaptureTransport(body=_success_body([MEMBER_RESULT]))
    member_client = _client(member_transport)
    member_envelope = member_client.organizations.member_list(id="org-1", object_type="user")
    member_record = next(item for item in member_envelope.items if isinstance(item, NativeRecord))
    assert member_record.resource_kind.value == "member"
    assert dict(member_record.payload)["capacity"] == "editor"


def test_member_roles_list_returns_value_items_as_a_read() -> None:
    """The role enumeration rides the read id and decodes scalar values."""
    transport = SyncCaptureTransport(body=_success_body(["admin", "editor", "member"]))
    client = _client(transport)

    envelope = client.organizations.member_roles_list()

    assert transport.requests[0].url.endswith("/api/3/action/member_roles_list")
    assert all(isinstance(item, ValueRecord) for item in envelope.items)


def test_member_create_passes_documented_parameters_verbatim() -> None:
    """The generic member trio crosses the wire with id/object/object_type/capacity."""
    transport = SyncCaptureTransport(body=_success_body(MEMBER_RESULT))
    client = _client(transport)

    result = client.organizations.member_create(id="org-1", object="user-1", object_type="user", capacity="editor")

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/member_create")
    assert json.loads(request.body or b"{}") == {
        "id": "org-1",
        "object": "user-1",
        "object_type": "user",
        "capacity": "editor",
    }
    assert isinstance(result, CKANMutationResult)
    record = next(item for item in result.result.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "member"
    assert result.receipt.target.value == "user-1"


def test_member_delete_passes_object_identity_verbatim_with_receipt() -> None:
    """Member removal keeps the documented parameter names and returns a receipt."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    result = client.organizations.member_delete(id="org-1", object="user-1", object_type="user")

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/member_delete")
    assert json.loads(request.body or b"{}") == {"id": "org-1", "object": "user-1", "object_type": "user"}
    assert isinstance(result.receipt, MutationReceipt)
    assert result.receipt.outcome == "succeeded"


def test_unconfirmed_organization_purge_refuses_at_zero_transport_io() -> None:
    """T-03-08-01 mitigation: the destructive tier gates pre-dispatch with zero wire hits."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    with pytest.raises(CatalogValidationError) as excinfo:
        client.organizations.organization_purge(id="org-1", policy=MutationPolicy(destructive=True))

    assert transport.requests == []
    assert "destructive" in str(excinfo.value)


def test_organization_purge_requires_a_policy_at_the_call_boundary() -> None:
    """The destructive tier cannot be engaged without an explicit policy keyword."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    with pytest.raises(TypeError):
        client.organizations.organization_purge(id="org-1")  # ty: ignore[missing-argument]

    assert transport.requests == []


def test_confirmed_organization_purge_dispatches_once_with_redacted_receipt() -> None:
    """A confirmed destructive policy dispatches exactly once and redacts its receipt."""
    transport = SyncCaptureTransport(body=_success_body(None))
    credential = CKANCredential(api_token="secret-token-123")
    client = SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        credentials=credential,
        owns_transport=False,
    )

    result = client.organizations.organization_purge(id="org-1", policy=_confirmed_destructive_policy())

    assert len(transport.requests) == 1
    assert transport.requests[0].url.endswith("/api/3/action/organization_purge")
    value_item = result.result.items[0]
    assert isinstance(value_item, ValueRecord)
    assert result.receipt.outcome == "succeeded"
    assert result.receipt.target.value == "org-1"
    serialized = json.dumps(result.to_dict())
    assert "secret-token-123" not in serialized
    assert "Authorization" not in serialized


def test_forbidden_authorization_envelope_raises_forbidden_error_with_capability_state() -> None:
    """Server 403 responses map to ForbiddenError carrying capability_state=forbidden."""
    transport = SyncCaptureTransport(status_code=403, body=_success_body(None))
    client = _client(transport)

    with pytest.raises(ForbiddenError) as excinfo:
        client.organizations.organization_delete(id="org-1")

    assert excinfo.value.capability_state == "forbidden"


def test_unauthenticated_authorization_envelope_raises_the_distinct_unauthenticated_error() -> None:
    """Server rejection of bad credentials stays a separate class from forbidden."""
    transport = SyncCaptureTransport(status_code=401, body=_success_body(None))
    client = _client(transport)

    with pytest.raises(UnauthenticatedError) as unauth_excinfo:
        client.organizations.organization_member_create(id="org-1", username="u-1", role="editor")

    assert unauth_excinfo.value.capability_state == "unauthorized"
    assert not isinstance(unauth_excinfo.value, ForbiddenError)

    mapping_transport = SyncCaptureTransport(
        status_code=200,
        body=json.dumps({"success": False, "error": {"__type": "Authorization Error", "message": "bad token"}}).encode(
            "utf-8"
        ),
    )
    envelope_client = _client(mapping_transport)
    with pytest.raises(UnauthenticatedError):
        envelope_client.organizations.organization_member_delete(id="org-1", username="u-1")


def test_async_organization_mutations_mirror_the_sync_semantics() -> None:
    """The async twin keeps receipt-bearing mutations and own-kind decoding."""
    transport = AsyncCaptureTransport(body=_success_body(ORGANIZATION_RESULT))
    client = _async_client(transport)

    result = asyncio.run(client.organizations.organization_create(name="health-org", title="Health"))

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/organization_create")
    assert json.loads(request.body or b"{}") == {"name": "health-org", "title": "Health"}
    assert isinstance(result, CKANMutationResult)
    record = next(item for item in result.result.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "organization"
    assert result.receipt.outcome == "succeeded"

    listing_transport = AsyncCaptureTransport(body=_success_body(ORGANIZATION_RESULT))
    listing_client = _async_client(listing_transport)
    envelope = asyncio.run(listing_client.organizations.organization_show(id="org-1"))
    shown = next(item for item in envelope.items if isinstance(item, NativeRecord))
    assert shown.resource_kind.value == "organization"


def test_organization_projection_rejects_foreign_group_actions_before_dispatch() -> None:
    """Guard-first dispatch: org methods refuse actions owned by other groups at zero I/O."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    operation = CatalogOperationRequest(
        operation_id=OperationId(platform="ckan", service="action-api-v3", method="group-create-update-delete-members"),
        payload={"action": "group_create"},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    with pytest.raises(CatalogValidationError):
        client.organizations.list_show_create_update_delete_members(operation, guard)

    assert transport.requests == []


def test_normalized_organizations_get_round_trips_through_the_new_projection() -> None:
    """The normalized organization projection keeps decoding its own record kind."""
    from datasluice.domain.catalog.models import OrganizationRecord

    transport = SyncCaptureTransport(body=_success_body({"id": "org-1", "name": "health-org"}))
    client = _client(transport)

    operation = CatalogOperationRequest(
        operation_id=OperationId(platform="ckan", service="organizations", method="get"),
        payload={"id": "health-org"},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    envelope = client.organizations.get(operation, guard)

    assert transport.requests[0].url.endswith("/api/3/action/organization_show")
    record = envelope.items[0]
    assert isinstance(record, OrganizationRecord)
    assert record.name == "health-org"
