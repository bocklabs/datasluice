"""Deterministic loopback coverage for the CKAN group and user surfaces."""

from __future__ import annotations

import asyncio
import inspect
import json

import pytest

from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.connectors.catalog.ckan.mapping import RECORD_KINDS, RESULT_KINDS
from datasluice.connectors.catalog.ckan.results import CKANMutationResult, CKANTokenResult
from datasluice.connectors.catalog.ckan.services.groups import AsyncGroupsService, SyncGroupsService
from datasluice.connectors.catalog.ckan.services.users import AsyncUsersService, SyncUsersService
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import CKANCredential
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ValueRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.redaction import REDACTED
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, UnauthenticatedError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
GROUP_READ_ID = "ckan/action-api-v3.group-list-show-search"
GROUP_WRITE_ID = "ckan/action-api-v3.group-create-update-delete-members"
USER_READ_ID = "ckan/action-api-v3.user-list-show"
USER_WRITE_ID = "ckan/action-api-v3.user-create-update-delete-token-management"

EXPECTED_GROUP_ACTIONS = frozenset(
    {
        "group_list",
        "group_list_authz",
        "group_show",
        "group_package_show",
        "group_autocomplete",
        "group_create",
        "group_update",
        "group_patch",
        "group_delete",
        "group_purge",
        "group_member_create",
        "group_member_delete",
    }
)

EXPECTED_USER_ACTIONS = frozenset(
    {
        "user_list",
        "user_show",
        "user_autocomplete",
        "user_create",
        "user_invite",
        "user_update",
        "user_patch",
        "user_delete",
        "get_site_user",
        "api_token_create",
        "api_token_list",
        "api_token_revoke",
    }
)

GROUP_RESULT: dict[str, object] = {"id": "grp-1", "name": "transit-group", "title": "Transit"}
PACKAGE_ROW: dict[str, object] = {"id": "pkg-9", "name": "route-dataset", "title": "Routes"}


def _success_body(result: object) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


def _failure_body(error: dict[str, object]) -> bytes:
    return json.dumps({"success": False, "error": error}).encode("utf-8")


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


def test_group_manifest_holds_exactly_the_documented_twelve_actions() -> None:
    """Manifest-driven completeness: exact name set with honest tier and id splits."""
    entries = [entry for entry in CKAN_ACTIONS.entries if entry.group == "groups"]
    assert {entry.name for entry in entries} == EXPECTED_GROUP_ACTIONS
    assert len(entries) == 12
    assert {entry.owning_operation_id for entry in entries} == {GROUP_READ_ID, GROUP_WRITE_ID}
    reads = [entry for entry in entries if entry.mutation_class == "read"]
    standards = [entry for entry in entries if entry.mutation_class == "standard"]
    destructive = [entry for entry in entries if entry.mutation_class == "destructive"]
    assert len(reads) == 5
    assert len(standards) == 6
    assert len(destructive) == 1
    purge = next(entry for entry in entries if entry.name == "group_purge")
    assert purge.mutation_class == "destructive"
    for entry in entries:
        spec = RESULT_KINDS.get(entry.name)
        assert spec is not None, f"{entry.name} is absent from the mapping truth table"
        outcome, family = spec
        assert outcome == entry.result_kind
        if family is not None:
            assert family in RECORD_KINDS


def test_every_manifest_group_action_exposes_a_typed_method_on_both_mode_services() -> None:
    """Each registered group action names a callable member on both projections."""
    sync_surface = {name for name in dir(SyncGroupsService) if not name.startswith("_")}
    async_surface = {name for name in dir(AsyncGroupsService) if not name.startswith("_")}
    for action in EXPECTED_GROUP_ACTIONS:
        assert action in sync_surface, f"sync surface misses {action}"
        assert action in async_surface, f"async surface misses {action}"


def test_group_surfaces_stay_in_structural_lockstep_across_modes() -> None:
    """Sync/async group projections expose identical members, mode-correct dispatch."""
    sync_members = {name for name in dir(SyncGroupsService) if not name.startswith("__")}
    async_members = {name for name in dir(AsyncGroupsService) if not name.startswith("__")}
    assert sync_members == async_members
    public = {name for name in vars(SyncGroupsService) if not name.startswith("_")}
    for name in public:
        assert inspect.iscoroutinefunction(getattr(AsyncGroupsService, name)), name
        assert not inspect.iscoroutinefunction(getattr(SyncGroupsService, name)), name


def test_group_show_decodes_a_group_kind_record() -> None:
    """Reads under the core group read id decode their own group kind."""
    transport = SyncCaptureTransport(body=_success_body(GROUP_RESULT))
    client = _client(transport)

    envelope = client.groups.group_show(id="grp-1", include_dataset_count=True)

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/group_show")
    assert json.loads(request.body or b"{}") == {"id": "grp-1", "include_dataset_count": True}
    record = next(item for item in envelope.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "group"
    assert record.id.value == "grp-1"


def test_group_reads_dispatch_under_the_core_read_id_with_native_kinds() -> None:
    """List/authz/package-show/autocomplete keep their documented outcome shapes."""
    list_transport = SyncCaptureTransport(body=_success_body(["transit-group"]))
    client = _client(list_transport)
    listing = client.groups.group_list(limit=3, offset=6)
    assert all(isinstance(item, ValueRecord) for item in listing.items)
    assert list_transport.requests[0].url.endswith("/api/3/action/group_list")

    authz_transport = SyncCaptureTransport(body=_success_body(["transit-group"]))
    authz_client = _client(authz_transport)
    authz = authz_client.groups.group_list_authz()
    assert all(isinstance(item, ValueRecord) for item in authz.items)
    assert authz_transport.requests[0].url.endswith("/api/3/action/group_list_authz")

    packages_transport = SyncCaptureTransport(body=_success_body([PACKAGE_ROW]))
    packages_client = _client(packages_transport)
    packages = packages_client.groups.group_package_show(id="grp-1")
    package_record = next(item for item in packages.items if isinstance(item, NativeRecord))
    assert package_record.resource_kind.value == "dataset"
    assert packages_transport.requests[0].url.endswith("/api/3/action/group_package_show")

    autocomplete_transport = SyncCaptureTransport(body=_success_body([GROUP_RESULT]))
    autocomplete_client = _client(autocomplete_transport)
    autocomplete = autocomplete_client.groups.group_autocomplete(q="tra")
    group_record = next(item for item in autocomplete.items if isinstance(item, NativeRecord))
    assert group_record.resource_kind.value == "group"
    assert autocomplete_transport.requests[0].url.endswith("/api/3/action/group_autocomplete")


def test_group_member_variants_pass_documented_parameters_verbatim() -> None:
    """Group member create/delete cross the wire with id/username/role shapes."""
    transport = SyncCaptureTransport(body=_success_body({"id": "gm-1", "object": "user-2", "capacity": "editor"}))
    client = _client(transport)

    result = client.groups.group_member_create(id="grp-1", username="user-2", role="editor")

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/group_member_create")
    assert json.loads(request.body or b"{}") == {"id": "grp-1", "username": "user-2", "role": "editor"}
    assert isinstance(result, CKANMutationResult)
    record = next(item for item in result.result.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "member"

    removal_transport = SyncCaptureTransport(body=_success_body(None))
    removal_client = _client(removal_transport)
    removal = removal_client.groups.group_member_delete(id="grp-1", username="user-2")
    assert json.loads(removal_transport.requests[0].body or b"{}") == {"id": "grp-1", "username": "user-2"}
    assert removal.receipt.outcome == "succeeded"


def test_unconfirmed_group_purge_refuses_at_zero_transport_io() -> None:
    """The destructive tier gates pre-dispatch with zero wire hits (T-03-08-01)."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    with pytest.raises(CatalogValidationError) as excinfo:
        client.groups.group_purge(id="grp-1", policy=MutationPolicy(destructive=True))

    assert transport.requests == []
    assert "destructive" in str(excinfo.value)


def test_confirmed_group_purge_dispatches_once_with_receipt() -> None:
    """A confirmed destructive policy dispatches exactly once and yields a receipt."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    result = client.groups.group_purge(id="grp-1", policy=_confirmed_destructive_policy())

    assert len(transport.requests) == 1
    assert transport.requests[0].url.endswith("/api/3/action/group_purge")
    value_item = result.result.items[0]
    assert isinstance(value_item, ValueRecord)
    assert result.receipt.outcome == "succeeded"
    assert result.receipt.target.value == "grp-1"


def test_async_group_mutations_mirror_the_sync_semantics() -> None:
    """The async twin keeps receipt-bearing mutations and own-kind decoding."""
    transport = AsyncCaptureTransport(body=_success_body(GROUP_RESULT))
    client = _async_client(transport)

    result = asyncio.run(client.groups.group_create(name="transit-group", title="Transit"))

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/group_create")
    assert json.loads(request.body or b"{}") == {"name": "transit-group", "title": "Transit"}
    assert isinstance(result, CKANMutationResult)
    record = next(item for item in result.result.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "group"
    assert result.receipt.outcome == "succeeded"


def test_group_projection_rejects_foreign_group_actions_before_dispatch() -> None:
    """Guard-first dispatch: group methods refuse actions owned by other groups."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    operation = CatalogOperationRequest(
        operation_id=OperationId(platform="ckan", service="action-api-v3", method="organization-list-show-search"),
        payload={"action": "organization_list"},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    with pytest.raises(CatalogValidationError):
        client.groups.list_show_search(operation, guard)

    assert transport.requests == []


def test_user_manifest_holds_exactly_the_documented_twelve_actions() -> None:
    """Manifest-driven completeness: exact name set across the split user ids."""
    entries = [entry for entry in CKAN_ACTIONS.entries if entry.group == "users"]
    assert {entry.name for entry in entries} == EXPECTED_USER_ACTIONS
    assert len(entries) == 12
    assert {entry.owning_operation_id for entry in entries} == {USER_READ_ID, USER_WRITE_ID}
    reads = [entry for entry in entries if entry.mutation_class == "read"]
    standards = [entry for entry in entries if entry.mutation_class == "standard"]
    destructive = [entry for entry in entries if entry.mutation_class == "destructive"]
    assert len(reads) == 4
    assert len(standards) == 8
    assert len(destructive) == 0
    token_create = next(entry for entry in entries if entry.name == "api_token_create")
    assert token_create.result_kind == "token-secret"
    for entry in entries:
        spec = RESULT_KINDS.get(entry.name)
        assert spec is not None, f"{entry.name} is absent from the mapping truth table"
        outcome, family = spec
        assert outcome == entry.result_kind
        if family is not None:
            assert family in RECORD_KINDS


def test_every_manifest_user_action_exposes_a_typed_method_on_both_mode_services() -> None:
    """Each registered user action names a callable member on both projections."""
    sync_surface = {name for name in dir(SyncUsersService) if not name.startswith("_")}
    async_surface = {name for name in dir(AsyncUsersService) if not name.startswith("_")}
    for action in EXPECTED_USER_ACTIONS:
        assert action in sync_surface, f"sync surface misses {action}"
        assert action in async_surface, f"async surface misses {action}"


def test_user_surfaces_stay_in_structural_lockstep_across_modes() -> None:
    """Sync/async user projections expose identical members, mode-correct dispatch."""
    sync_members = {name for name in dir(SyncUsersService) if not name.startswith("__")}
    async_members = {name for name in dir(AsyncUsersService) if not name.startswith("__")}
    assert sync_members == async_members
    public = {name for name in vars(SyncUsersService) if not name.startswith("_")}
    for name in public:
        assert inspect.iscoroutinefunction(getattr(AsyncUsersService, name)), name
        assert not inspect.iscoroutinefunction(getattr(SyncUsersService, name)), name


def test_api_token_trio_captures_documented_routes_and_stays_secret_safe() -> None:
    """D-05/T-03-08-02: created tokens ride reveal-only wrappers, never payloads."""
    create_transport = SyncCaptureTransport(
        body=_success_body({"token": "issued-secret-123", "jti": "jti-9", "user": "u-1"})
    )
    credential = CKANCredential(api_token="secret-token-123")
    client = SyncCKANClient(
        create_transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        credentials=credential,
        owns_transport=False,
    )

    wrapper = client.users.api_token_create(user="u-1", name="ci-token")

    request = create_transport.requests[0]
    assert request.url.endswith("/api/3/action/api_token_create")
    assert json.loads(request.body or b"{}") == {"user": "u-1", "name": "ci-token"}
    assert isinstance(wrapper, CKANTokenResult)
    assert wrapper.reveal() == "issued-secret-123"
    assert wrapper.to_dict()["token"] == REDACTED
    assert set(dict(wrapper.result_metadata)) == {"jti", "user"}

    list_transport = SyncCaptureTransport(body=_success_body([{"id": "t-1", "last_access": None}, {"id": "t-2"}]))
    list_client = _client(list_transport)
    listing = list_client.users.api_token_list(user="u-1")
    assert list_transport.requests[0].url.endswith("/api/3/action/api_token_list")
    assert json.loads(list_transport.requests[0].body or b"{}") == {"user": "u-1"}
    items = [item for item in listing.items if isinstance(item, MappingRecord)]
    assert [dict(item.payload)["id"] for item in items] == ["t-1", "t-2"]

    revoke_transport = SyncCaptureTransport(body=_success_body(None))
    revoke_client = _client(revoke_transport)
    revoked = revoke_client.users.api_token_revoke(token_id="t-1")
    assert revoke_transport.requests[0].url.endswith("/api/3/action/api_token_revoke")
    assert json.loads(revoke_transport.requests[0].body or b"{}") == {"jti": "t-1"}
    assert revoked.receipt.outcome == "succeeded"
    assert revoked.receipt.target.resource_kind.value == "token"

    serialized = json.dumps(wrapper.to_dict())
    assert "issued-secret-123" not in serialized
    assert "issued-secret-123" not in repr(wrapper)


def test_api_token_revoke_requires_the_token_id_at_the_call_boundary() -> None:
    """Revocation cannot be engaged without the token identifier keyword."""
    transport = SyncCaptureTransport(body=_success_body(None))
    client = _client(transport)

    with pytest.raises(TypeError):
        client.users.api_token_revoke()  # ty: ignore[missing-argument]

    assert transport.requests == []


def test_get_site_user_passes_no_parameters_and_maps_authorization_envelopes() -> None:
    """The site-user fetch sends an empty verbatim body; rejections map to typed errors."""
    transport = SyncCaptureTransport(body=_success_body({"id": "site-uid", "name": "default"}))
    client = _client(transport)

    result = client.users.get_site_user()

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/get_site_user")
    assert json.loads(request.body or b"{}") == {}
    record = next(item for item in result.result.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "user"

    rejected = SyncCaptureTransport(
        status_code=401,
        body=_failure_body({"__type": "Authorization Error", "message": "not authenticated"}),
    )
    rejected_client = _client(rejected)
    with pytest.raises(UnauthenticatedError) as excinfo:
        rejected_client.users.get_site_user()
    assert excinfo.value.capability_state == "unauthorized"


def test_async_token_creation_mirrors_the_sync_semantics() -> None:
    """The async twin extracts the same reveal-only wrapper from its own pipeline."""
    transport = AsyncCaptureTransport(body=_success_body({"token": "async-secret-456", "user": "u-1", "jti": "jti-10"}))
    client = _async_client(transport)

    wrapper = asyncio.run(client.users.api_token_create(user="u-1", name="nightly"))

    assert transport.requests[0].url.endswith("/api/3/action/api_token_create")
    assert wrapper.reveal() == "async-secret-456"
    assert wrapper.to_dict()["token"] == REDACTED
