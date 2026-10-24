from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from io import BytesIO
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.organizations import (
    MembershipRequestInput,
    OrganizationCreateInput,
    OrganizationInvitationInput,
    OrganizationListQuery,
    OrganizationMemberInput,
    OrganizationMutationResult,
    OrganizationRefusalInput,
    OrganizationSuggestQuery,
    OrganizationUpdateInput,
)
from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput
from datasluice.connectors.catalog.udata.services import _receipts
from datasluice.connectors.catalog.udata.services import auth_oauth as auth_oauth_service
from datasluice.connectors.catalog.udata.services import organizations_memberships as organization_service
from datasluice.connectors.catalog.udata.services import users_tokens as users_tokens_service
from datasluice.connectors.catalog.udata.services.organizations_memberships import (
    AsyncOrganizationsMembershipsService,
    SyncOrganizationsMembershipsService,
)
from datasluice.connectors.catalog.udata.wire import organizations as wire
from datasluice.contracts.catalog.native.udata import (
    AsyncUDataOrganizationsMembershipsService,
    SyncUDataOrganizationsMembershipsService,
)
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.redaction import REDACTED
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError, NativeCatalogError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.helpers.udata_test_support import swagger_response

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datasluice.connectors.catalog.udata.mapping import NativePage, UDataPageEnvelope
    from datasluice.domain.catalog.models import MappingRecord, NativeRecord

SHAPED_PAGE_SENTINEL = cast("UDataPageEnvelope", object())


class _Router:
    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if response := swagger_response(request, self.routes):
            return response
        self.requests.append(request)
        value = self.routes[(request.method, request.url)]
        status, payload = value if isinstance(value, tuple) else (200, value)
        body = payload if isinstance(payload, bytes) else b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(status_code=status, headers={"Content-Type": "application/json"}, body=body)

    def close(self) -> None:
        return None


class _AsyncRouter:
    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if response := swagger_response(request, self.routes):
            return response
        self.requests.append(request)
        value = self.routes[(request.method, request.url)]
        status, payload = value if isinstance(value, tuple) else (200, value)
        body = payload if isinstance(payload, bytes) else b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(status_code=status, headers={"Content-Type": "application/json"}, body=body)

    async def aclose(self) -> None:
        return None


ORIGIN = "http://127.0.0.1:5640"
SITE = {"feed_size": 0, "id": "site", "keywords": [], "metrics": {}, "title": "uData", "version": "17.6.0"}
CREDENTIAL = UDataCredential(api_key="secret-key")
PERMISSIONS = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA)


def _routes(routes: dict[tuple[str, str], object]) -> dict[tuple[str, str], object]:
    return {("GET", f"{ORIGIN}/api/1/site/"): SITE, **routes}


def _policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def test_clients_expose_the_pinned_organization_protocols() -> None:
    with SyncUDataClient(_Router(_routes({})), declared_udata_profile(), origin=ORIGIN) as sync:
        assert isinstance(sync.organizations_memberships, SyncUDataOrganizationsMembershipsService)

    async def run() -> None:
        async with AsyncUDataClient(_AsyncRouter(_routes({})), declared_udata_profile(), origin=ORIGIN) as client:
            assert isinstance(client.organizations_memberships, AsyncUDataOrganizationsMembershipsService)

    asyncio.run(run())


def test_organization_services_are_typed_and_mode_parity_is_preserved() -> None:
    expected = {
        name
        for name in dir(SyncOrganizationsMembershipsService)
        if not name.startswith("_") and callable(getattr(SyncOrganizationsMembershipsService, name))
    }
    assert expected == {
        name
        for name in dir(AsyncOrganizationsMembershipsService)
        if not name.startswith("_") and callable(getattr(AsyncOrganizationsMembershipsService, name))
    }


def test_organization_wire_builders_preserve_exact_paths_and_omission() -> None:
    assert wire.list_organizations_request(OrganizationListQuery(q="health", page=2, page_size=5)) == (
        "GET",
        "/api/1/organizations/?page=2&page_size=5&q=health",
        {},
        None,
    )
    assert wire.create_organization_request(OrganizationCreateInput(name="Evidence", description="Org")) == (
        "POST",
        "/api/1/organizations/",
        {},
        {"name": "Evidence", "description": "Org"},
    )
    assert wire.update_organization_request("org-1", OrganizationUpdateInput(acronym="E")) == (
        "PUT",
        "/api/1/organizations/org-1/",
        {},
        {"acronym": "E"},
    )
    assert wire.membership_request_request("org-1", MembershipRequestInput(comment="please")) == (
        "POST",
        "/api/1/organizations/org-1/membership/",
        {},
        {"comment": "please"},
    )
    with pytest.raises(CatalogValidationError):
        wire.get_organization_request("../secret")
    with pytest.raises(CatalogValidationError, match="unsupported"):
        wire.rdf_organization_format_request("org-1", "yaml")


def test_every_organization_route_has_an_exact_wire_shape() -> None:
    actual = [
        wire.list_organizations_request()[0:2],
        wire.create_organization_request(OrganizationCreateInput(name="Evidence", description="Org"))[0:2],
        wire.get_organization_request("org-1")[0:2],
        wire.update_organization_request("org-1", OrganizationUpdateInput(acronym="E"))[0:2],
        wire.delete_organization_request("org-1")[0:2],
        wire.organization_export_request("org-1", "datasets")[0:2],
        wire.organization_export_request("org-1", "dataservices")[0:2],
        wire.organization_export_request("org-1", "discussions")[0:2],
        wire.organization_export_request("org-1", "datasets-resources")[0:2],
        wire.rdf_organization_request("org-1")[0:2],
        wire.rdf_organization_format_request("org-1", "ttl")[0:2],
        wire.available_organization_badges_request()[0:2],
        wire.organization_badge_request("org-1", "certified")[0:2],
        wire.organization_badge_request("org-1", "certified", delete=True)[0:2],
        wire.organization_contacts_request("org-1")[0:2],
        wire.organization_contacts_suggest_request("org-1", OrganizationSuggestQuery("ev"))[0:2],
        wire.membership_requests_request("org-1")[0:2],
        wire.membership_request_request("org-1", MembershipRequestInput("please"))[0:2],
        wire.membership_action_request("org-1", "request-1", "accept")[0:2],
        wire.membership_action_request("org-1", "request-1", "refuse")[0:2],
        wire.membership_action_request("org-1", "request-1", "cancel")[0:2],
        wire.invite_member_request("org-1", OrganizationInvitationInput(email="member@example.test"))[0:2],
        wire.member_request("org-1", "user-1", method="PUT", body={"role": "editor"})[0:2],
        wire.member_request("org-1", "user-1", method="DELETE")[0:2],
        wire.assignments_request("org-1")[0:2],
        wire.member_assignments_request("org-1", "user-1", [])[0:2],
        wire.suggest_organizations_request(OrganizationSuggestQuery("ev"))[0:2],
        wire.organization_logo_request("org-1")[0:2],
        wire.organization_logo_request("org-1", resize=True)[0:2],
        wire.organization_owned_request("org-1", "datasets")[0:2],
        wire.organization_owned_request("org-1", "reuses")[0:2],
        wire.organization_owned_request("org-1", "discussions")[0:2],
        wire.organization_roles_request()[0:2],
        wire.search_organizations_request()[0:2],
        wire.organization_extras_request("org-1", method="GET")[0:2],
        wire.organization_extras_request("org-1", method="PUT", body={"key": "value"})[0:2],
        wire.organization_extras_request("org-1", method="DELETE", body=["key"])[0:2],
        wire.followers_request("org-1", method="GET")[0:2],
        wire.followers_request("org-1", method="POST")[0:2],
        wire.followers_request("org-1", method="DELETE")[0:2],
    ]
    assert actual == [
        ("GET", "/api/1/organizations/?page=1&page_size=20"),
        ("POST", "/api/1/organizations/"),
        ("GET", "/api/1/organizations/org-1/"),
        ("PUT", "/api/1/organizations/org-1/"),
        ("DELETE", "/api/1/organizations/org-1/"),
        ("GET", "/api/1/organizations/org-1/datasets.csv"),
        ("GET", "/api/1/organizations/org-1/dataservices.csv"),
        ("GET", "/api/1/organizations/org-1/discussions.csv"),
        ("GET", "/api/1/organizations/org-1/datasets-resources.csv"),
        ("GET", "/api/1/organizations/org-1/catalog"),
        ("GET", "/api/1/organizations/org-1/catalog.ttl"),
        ("GET", "/api/1/organizations/badges/"),
        ("POST", "/api/1/organizations/org-1/badges/"),
        ("DELETE", "/api/1/organizations/org-1/badges/certified/"),
        ("GET", "/api/1/organizations/org-1/contacts/?page=1&page_size=20"),
        ("GET", "/api/1/organizations/org-1/contacts/suggest/?q=ev&size=10"),
        ("GET", "/api/1/organizations/org-1/membership/"),
        ("POST", "/api/1/organizations/org-1/membership/"),
        ("POST", "/api/1/organizations/org-1/membership/request-1/accept/"),
        ("POST", "/api/1/organizations/org-1/membership/request-1/refuse/"),
        ("POST", "/api/1/organizations/org-1/membership/request-1/cancel/"),
        ("POST", "/api/1/organizations/org-1/member/"),
        ("PUT", "/api/1/organizations/org-1/member/user-1/"),
        ("DELETE", "/api/1/organizations/org-1/member/user-1/"),
        ("GET", "/api/1/organizations/org-1/assignments/"),
        ("PUT", "/api/1/organizations/org-1/member/user-1/assignments/"),
        ("GET", "/api/1/organizations/suggest/?q=ev&size=10"),
        ("POST", "/api/1/organizations/org-1/logo/"),
        ("PUT", "/api/1/organizations/org-1/logo/"),
        ("GET", "/api/1/organizations/org-1/datasets/?page=1&page_size=20"),
        ("GET", "/api/1/organizations/org-1/reuses/"),
        ("GET", "/api/1/organizations/org-1/discussions/"),
        ("GET", "/api/1/organizations/roles/"),
        ("GET", "/api/2/organizations/search/?page=1&page_size=20"),
        ("GET", "/api/2/organizations/org-1/extras/"),
        ("PUT", "/api/2/organizations/org-1/extras/"),
        ("DELETE", "/api/2/organizations/org-1/extras/"),
        ("GET", "/api/1/organizations/org-1/followers/"),
        ("POST", "/api/1/organizations/org-1/followers/"),
        ("DELETE", "/api/1/organizations/org-1/followers/"),
    ]


def test_sync_organization_read_and_mutation_decode_with_redacted_receipt() -> None:
    routes = _routes(
        {
            ("GET", f"{ORIGIN}/api/1/organizations/?page=1&page_size=20"): {
                "data": [{"id": "org-1", "name": "Evidence", "description": "Org"}],
                "page": 1,
                "page_size": 20,
                "total": 1,
            },
            ("POST", f"{ORIGIN}/api/1/organizations/"): (201, {"id": "org-1", "name": "Evidence"}),
        }
    )
    transport = _Router(routes)
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL, owns_transport=False
    )
    with client:
        page = client.organizations_memberships.list_organizations()
        result = client.organizations_memberships.create_organization(
            OrganizationCreateInput(name="Evidence", description="Org"),
            PERMISSIONS,
            _policy(wire.CREATE_ORGANIZATION_OPERATION, "Evidence"),
        )
    assert page.items[0].id.value == "org-1"
    assert result.record is not None
    assert result.record.id.value == "org-1"
    assert result.receipt.operation == wire.CREATE_ORGANIZATION_OPERATION
    assert "secret-key" not in json.dumps(result.to_dict())


def test_destructive_delete_requires_exact_confirmation_without_dispatch() -> None:
    transport = _Router(_routes({}))
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL, owns_transport=False
    )
    with client:
        policy = MutationPolicy(destructive=True)
        with pytest.raises(ForbiddenError):
            client.organizations_memberships.delete_organization("org-1", PERMISSIONS, policy)


def test_organization_inputs_reject_invalid_roles_and_ambiguous_invitations() -> None:
    with pytest.raises(ValueError):
        OrganizationMemberInput("owner")
    with pytest.raises(ValueError):
        MembershipRequestInput("please", role="owner")
    with pytest.raises(ValueError):
        OrganizationInvitationInput(user="user-1", email="member@example.test")
    with pytest.raises(ValueError):
        OrganizationInvitationInput(email="invalid-email")
    with pytest.raises(ValueError):
        OrganizationInvitationInput(email="member@example..test")
    with pytest.raises(ValueError):
        OrganizationInvitationInput(email="member@example.test", assignments=({"dataset": "one"},))
    with pytest.raises(ValueError):
        OrganizationMemberInput("editor", fields={"role": "admin"})


@pytest.mark.parametrize("value", [None, 1, 1.5, {"nested": True}, ["a"]])
def test_organization_filters_reject_values_outside_the_declared_union(value: object) -> None:
    filters = cast("Mapping[str, str | bool | tuple[str, ...]]", {"status": value})
    with pytest.raises(ValueError, match="filter"):
        OrganizationListQuery(filters=filters)


def test_organization_filters_reject_a_non_mapping_container() -> None:
    filters = cast("Mapping[str, str | bool | tuple[str, ...]]", [("status", "validated")])
    with pytest.raises(ValueError, match="mapping"):
        OrganizationListQuery(filters=filters)


def test_organization_filters_accept_the_declared_union() -> None:
    query = OrganizationListQuery(filters={"status": "validated", "featured": True, "tag": ("a", "b")})

    assert query.query_params() == [
        ("page", "1"),
        ("page_size", "20"),
        ("featured", "true"),
        ("status", "validated"),
        ("tag", "a"),
        ("tag", "b"),
    ]


def test_organization_fields_cannot_override_typed_values() -> None:
    with pytest.raises(ValueError, match="cannot override"):
        OrganizationCreateInput(name="Evidence", description="Org", fields={"name": "Other"})
    with pytest.raises(ValueError, match="cannot override"):
        OrganizationUpdateInput(acronym="E", fields={"description": "Other"})


def test_organization_inputs_reject_non_json_field_values_with_value_error() -> None:
    with pytest.raises(ValueError, match="JSON-safe"):
        OrganizationCreateInput(name="Evidence", description="Org", fields={"broken": object()})
    with pytest.raises(ValueError, match="JSON-safe"):
        OrganizationUpdateInput(acronym="E", fields={"broken": object()})
    bad_fields = cast("Mapping[str, object]", 5)
    with pytest.raises(ValueError, match="JSON mapping"):
        OrganizationCreateInput(name="Evidence", description="Org", fields=bad_fields)
    with pytest.raises(ValueError, match="JSON mapping"):
        OrganizationUpdateInput(acronym="E", fields=bad_fields)


@pytest.mark.parametrize(
    "assignment",
    [
        pytest.param({"dataset": datetime(2026, 1, 1, tzinfo=UTC)}, id="datetime"),
        pytest.param({"dataset": object()}, id="bare-object"),
        pytest.param({1: "dataset"}, id="non-string-key"),
    ],
)
def test_organization_assignments_reject_non_json_values_at_construction(assignment: object) -> None:
    accepted = cast("tuple[Mapping[str, str], ...]", (assignment,))

    with pytest.raises(ValueError, match="assignments"):
        MembershipRequestInput("please", role="partial_editor", assignments=accepted)
    with pytest.raises(ValueError, match="assignments"):
        OrganizationInvitationInput(email="member@example.test", role="partial_editor", assignments=accepted)


def test_organization_assignments_survive_caller_mutation_after_construction() -> None:
    caller_assignment: dict[str, str] = {"dataset": "dataset-1"}
    client_input = MembershipRequestInput("please", role="partial_editor", assignments=(caller_assignment,))
    frozen = client_input.payload()
    frozen_json = json.dumps(frozen)

    caller_assignment["dataset"] = "dataset-tampered"
    caller_assignment["injected"] = "dataset-injected"

    assert client_input.payload() == frozen
    assert json.dumps(client_input.payload()) == frozen_json
    assert client_input.payload()["assignments"] == [{"dataset": "dataset-1"}]


def test_contact_points_are_typed_as_contact_points_in_both_modes() -> None:
    contacts_url = f"{ORIGIN}/api/1/organizations/org-1/contacts/?page=1&page_size=20"
    payload = {"data": [{"id": "contact-1", "name": "Contact"}], "page": 1, "page_size": 20, "total": 1}

    sync = SyncUDataClient(_Router(_routes({("GET", contacts_url): payload})), declared_udata_profile(), origin=ORIGIN)
    with sync:
        sync_records = sync.organizations_memberships.get_organization_contact_point("org-1")

    async def run() -> tuple[MappingRecord, ...]:
        async with AsyncUDataClient(
            _AsyncRouter(_routes({("GET", contacts_url): payload})), declared_udata_profile(), origin=ORIGIN
        ) as client:
            return await client.organizations_memberships.get_organization_contact_point("org-1")

    async_records = asyncio.run(run())
    assert [record.payload["resource_kind"] for record in (*sync_records, *async_records)] == [
        "contact-point",
        "contact-point",
    ]


def test_badges_require_admin_evidence_before_dispatch_in_both_modes() -> None:
    permissions = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset())
    sync_transport = _Router(_routes({}))
    sync = SyncUDataClient(sync_transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with sync:
        policy = _policy(wire.ADD_ORGANIZATION_BADGE_OPERATION, "org-1/certified")
        with pytest.raises(ForbiddenError):
            sync.organizations_memberships.add_organization_badge("org-1", "certified", permissions, policy)
    assert not sync_transport.requests

    async_transport = _AsyncRouter(_routes({}))
    async_client = AsyncUDataClient(async_transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)

    async def run() -> None:
        async with async_client:
            policy = _policy(wire.ADD_ORGANIZATION_BADGE_OPERATION, "org-1/certified")
            with pytest.raises(ForbiddenError):
                await async_client.organizations_memberships.add_organization_badge(
                    "org-1", "certified", permissions, policy
                )

    asyncio.run(run())
    assert not async_transport.requests


def test_refusal_route_rejection_keeps_a_composite_receipt_target() -> None:
    transport = _Router(_routes({}))
    client = SyncUDataClient(transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client:
        body = OrganizationRefusalInput("No")
        policy = _policy(wire.REFUSE_MEMBERSHIP_OPERATION, "org/invalid/request-1")
        with pytest.raises(CatalogValidationError) as raised:
            client.organizations_memberships.refuse_membership("org/invalid", "request-1", body, PERMISSIONS, policy)
    receipt = raised.value.__dict__["mutation_receipt"]
    assert receipt.outcome == "rejected"
    assert receipt.target.value == "org/invalid/request-1"
    assert not transport.requests


def test_logo_close_failure_preserves_the_success_receipt() -> None:
    class CloseFailingSource(BytesIO):
        failed = False

        def close(self) -> None:
            if not self.failed:
                self.failed = True
                raise OSError("close failed")
            super().close()

    path = f"{ORIGIN}/api/1/organizations/org-1/logo/"
    transport = _Router(_routes({("POST", path): {"id": "logo"}}))
    client = SyncUDataClient(transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client:
        upload = ResourceUploadInput(CloseFailingSource(b"logo"), "logo.png", 4)
        policy = _policy(wire.ORGANIZATION_LOGO_OPERATION, "org-1")
        with pytest.raises(OSError) as raised:
            client.organizations_memberships.organization_logo("org-1", upload, PERMISSIONS, policy)
    assert raised.value.__dict__["mutation_receipt"].outcome == "succeeded"


def test_async_organization_get_matches_sync_wire() -> None:
    route = ("GET", f"{ORIGIN}/api/1/organizations/org-1/")
    body = {"id": "org-1", "name": "Evidence", "description": "Org"}
    sync_transport = _Router(_routes({route: body}))
    async_transport = _AsyncRouter(_routes({route: body}))
    sync = SyncUDataClient(sync_transport, declared_udata_profile(), origin=ORIGIN, owns_transport=False)
    async_client = AsyncUDataClient(async_transport, declared_udata_profile(), origin=ORIGIN, owns_transport=False)
    with sync:
        sync_value = sync.organizations_memberships.get_organization("org-1")

    async def run() -> object:
        async with async_client:
            return await async_client.organizations_memberships.get_organization("org-1")

    assert asyncio.run(run()) == sync_value


def test_organization_family_reuses_the_shared_receipt_attachment_helper() -> None:
    assert organization_service._close_logo is _receipts._close_input
    assert auth_oauth_service._attach is _receipts._attach_receipt
    assert users_tokens_service._attach is _receipts._attach_receipt


def test_organization_page_decoders_delegate_to_the_shared_page_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[NativePage, tuple[NativeRecord, ...]]] = []

    def spy(page: NativePage, records: tuple[NativeRecord, ...]) -> UDataPageEnvelope:
        calls.append((page, records))
        return SHAPED_PAGE_SENTINEL

    monkeypatch.setattr(wire, "_shape_native_page", spy)
    payload: dict[str, object] = {
        "data": [{"id": "org-1", "name": "Evidence"}],
        "page": 1,
        "page_size": 20,
        "total": 1,
    }

    assert wire.parse_organization_page(payload) is SHAPED_PAGE_SENTINEL
    assert (
        wire.parse_page(payload, operation=wire.LIST_ORGANIZATIONS_OPERATION, kind=ResourceKind("badge"))
        is SHAPED_PAGE_SENTINEL
    )
    assert [(page.page, tuple(record.id.value for record in records)) for page, records in calls] == [
        (1, ("org-1",)),
        (1, ("org-1",)),
    ]


def _both_organization_page_decoders(payload: Mapping[str, object]) -> tuple[UDataPageEnvelope, UDataPageEnvelope]:
    return (
        wire.parse_organization_page(payload),
        wire.parse_page(payload, operation=wire.LIST_ORGANIZATIONS_OPERATION, kind=ResourceKind("badge")),
    )


def test_organization_page_decoders_share_one_cursor_shape() -> None:
    organization, generic = _both_organization_page_decoders(
        {
            "data": [{"id": "org-1", "name": "Evidence"}],
            "page": 2,
            "page_size": 20,
            "previous_page": f"{ORIGIN}/api/1/organizations/?page=1",
            "next_page": f"{ORIGIN}/api/1/organizations/?page=3",
            "total": 41,
        }
    )
    assert organization.page == generic.page
    assert organization.page is not None
    assert (organization.page.cursor, organization.page.next_cursor, organization.page.total_items) == ("2", "3", 41)
    assert organization.native_page.to_dict() == generic.native_page.to_dict()
    assert organization.native_page.to_dict() == {
        "present_fields": ["data", "next_page", "page", "page_size", "previous_page", "total"],
        "page": 2,
        "page_size": 20,
        "previous_page": f"{ORIGIN}/api/1/organizations/?page=1",
        "next_page": f"{ORIGIN}/api/1/organizations/?page=3",
        "total": 41,
    }
    assert organization.platform == generic.platform
    assert organization.platform is not None
    assert (organization.platform.platform, organization.platform.api_version, organization.platform.deployment) == (
        CatalogPlatform.UDATA,
        None,
        None,
    )
    extensions = organization.platform.to_dict()["extensions"]
    assert json.loads(json.dumps(extensions)) == {"udata.page": organization.native_page.to_dict()}


def test_organization_page_decoders_drop_the_next_cursor_without_a_next_page() -> None:
    organization, generic = _both_organization_page_decoders(
        {
            "data": [{"id": "org-1", "name": "Evidence"}],
            "page": 2,
            "page_size": 20,
            "total": 41,
        }
    )
    assert organization.page == generic.page
    assert organization.page is not None
    assert (organization.page.cursor, organization.page.next_cursor, organization.page.total_items) == ("2", None, 41)
    assert organization.native_page.to_dict() == generic.native_page.to_dict()
    assert "next_page" not in organization.native_page.present_fields
    assert organization.native_page.to_dict() == {
        "present_fields": ["data", "page", "page_size", "total"],
        "page": 2,
        "page_size": 20,
        "previous_page": None,
        "next_page": None,
        "total": 41,
    }
    assert organization.platform == generic.platform
    assert organization.platform is not None
    assert (organization.platform.platform, organization.platform.api_version, organization.platform.deployment) == (
        CatalogPlatform.UDATA,
        None,
        None,
    )
    extensions = organization.platform.to_dict()["extensions"]
    assert json.loads(json.dumps(extensions)) == {"udata.page": organization.native_page.to_dict()}


def test_organization_records_and_extras_redact_credentials_at_construction() -> None:
    """Retained organization payloads must be redacted before birth, not only on serialize.

    NativeRecord/MappingRecord.to_dict() redacts, but .payload, repr() and the OrganizationMutationResult.value path
    expose the retained mapping directly, so a credential-shaped server field would otherwise leave the typed surface
    unredacted.
    """
    organization = wire.parse_organization(
        {
            "id": "org-1",
            "name": "Evidence",
            "api_key": "AKIA-EXAMPLE",
            "nested": {"password": "hunter2", "kept": 1},
            "contact": "ops@example.org",
        }
    )
    assert organization.payload["api_key"] == REDACTED
    assert organization.payload["nested"] == {"password": REDACTED, "kept": 1}
    assert organization.payload["contact"] == "ops@example.org"
    assert "AKIA-EXAMPLE" not in repr(organization)

    (mapping,) = wire.parse_records(
        [{"id": "membership-1", "token": "t-secret"}], operation=wire.LIST_MEMBERSHIP_REQUESTS_OPERATION
    )
    assert mapping.payload["token"] == REDACTED
    assert "t-secret" not in repr(mapping)

    page = wire.parse_page(
        {"data": [{"id": "dataset-1", "secret": "s-value"}]},
        operation=wire.LIST_ORGANIZATION_DATASETS_OPERATION,
        kind=ResourceKind.DATASET,
    )
    assert page.items[0].payload["secret"] == REDACTED

    extras = wire.parse_extras({"api_key": "AKIA-EXAMPLE", "nested": {"password": "hunter2", "kept": 1}, "label": "ok"})
    assert extras["api_key"] == REDACTED
    assert extras["nested"] == {"password": REDACTED, "kept": 1}
    assert extras["label"] == "ok"


def test_organization_mutation_value_carries_redacted_payload() -> None:
    """OrganizationMutationResult.value is serialized through _thaw_json, not the record gate."""
    receipt = MutationReceipt(
        operation="udata/api-v1.update-organization",
        outcome="succeeded",
        target=CatalogId(platform=CatalogPlatform.UDATA, resource_kind=ResourceKind.ORGANIZATION, value="org-1"),
    )
    result = OrganizationMutationResult(
        receipt=receipt,
        value={"api_key": "AKIA-EXAMPLE", "name": "Evidence"},
    )
    assert result.to_dict()["value"] == {"api_key": REDACTED, "name": "Evidence"}


def test_organization_identifiers_reject_quote_and_control_characters() -> None:
    """Organization ids must satisfy the same segment policy as the sibling uData families."""
    for identifier in ('a"b', "a'b", "a\nb", "a/b", ".", "..", "", 4):
        typed_identifier = cast("str", identifier)
        with pytest.raises(CatalogValidationError):
            wire.get_organization_request(typed_identifier)


def _redirect_call(location: str):
    def call(**kwargs: object) -> tuple[int, object, object]:
        return 302, b"", RuntimeResponse(status_code=302, headers={"location": location}, body=b"")

    return call


def test_interrupted_organization_mutation_is_not_misreported_as_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """KeyboardInterrupt keeps the interruption outcome instead of the generic ``failed``."""

    def interrupt(**kwargs: object) -> tuple[int, object, object]:
        raise KeyboardInterrupt

    transport = _Router(_routes({}))
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL, owns_transport=False
    )
    with client:
        monkeypatch.setattr(client, "_dataset_call", interrupt)
        policy = _policy(wire.DELETE_ORGANIZATION_OPERATION, "org-1", destructive=True)
        with pytest.raises(KeyboardInterrupt) as stopped:
            client.organizations_memberships.delete_organization("org-1", PERMISSIONS, policy)
    assert stopped.value.__dict__["mutation_receipt"].outcome == "cancelled"


def test_interrupted_organization_mutation_is_not_misreported_as_failed_async(monkeypatch: pytest.MonkeyPatch) -> None:
    """The async settlement seam maps KeyboardInterrupt to the same interruption outcome."""

    def interrupt(**kwargs: object) -> tuple[int, object, object]:
        raise KeyboardInterrupt

    async def run() -> None:
        transport = _AsyncRouter(_routes({}))
        client = AsyncUDataClient(
            transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL, owns_transport=False
        )
        async with client:
            monkeypatch.setattr(client, "_dataset_call_async", interrupt)
            await client.organizations_memberships.delete_organization(
                "org-1", PERMISSIONS, _policy(wire.DELETE_ORGANIZATION_OPERATION, "org-1", destructive=True)
            )

    coroutine = run()
    with pytest.raises(KeyboardInterrupt) as stopped:
        asyncio.run(coroutine)
    assert stopped.value.__dict__["mutation_receipt"].outcome == "cancelled"


def test_organization_export_redirect_stores_only_the_sanitized_target(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = _Router(_routes({}))
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL, owns_transport=False
    )
    with client:
        monkeypatch.setattr(client, "_dataset_call", _redirect_call(f"{ORIGIN}/export/org-1/?page=2&utm_source=x"))
        document = client.organizations_memberships.organization_datasets_csv("org-1")
    assert document.location == "/export/org-1/?page=2&utm_source=x"


def test_organization_export_redirect_rejects_cross_origin_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = _Router(_routes({}))
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL, owns_transport=False
    )
    with client:
        monkeypatch.setattr(
            client,
            "_dataset_call",
            _redirect_call("https://evil.example.com/api/1/organizations/org-1/datasets.csv"),
        )
        with pytest.raises(NativeCatalogError, match="outside the configured deployment origin"):
            client.organizations_memberships.organization_datasets_csv("org-1")


def test_organization_export_redirect_rejects_userinfo_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = _Router(_routes({}))
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL, owns_transport=False
    )
    with client:
        monkeypatch.setattr(
            client,
            "_dataset_call",
            _redirect_call("http://user:pass@127.0.0.1:5640/api/1/organizations/org-1/datasets.csv"),
        )
        with pytest.raises(NativeCatalogError, match="unsafe URL components"):
            client.organizations_memberships.organization_datasets_csv("org-1")


def test_organization_badge_delete_requotes_the_validated_kind() -> None:
    method, path, _headers, body = wire.organization_badge_request("org-1", "certified%2Fbad", delete=True)
    assert method == "DELETE"
    assert path == "/api/1/organizations/org-1/badges/certified%252Fbad/"
    assert body is None
    _post_method, _post_path, _post_headers, post_body = wire.organization_badge_request("org-1", "certified%2Fbad")
    assert post_body == {"kind": "certified%2Fbad"}


def test_organization_owned_reuses_route_refuses_a_query() -> None:
    query = OrganizationListQuery()
    with pytest.raises(CatalogValidationError, match="does not accept query parameters"):
        wire.organization_owned_request("org-1", "reuses", query)
