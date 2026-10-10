"""Exact wire and safety coverage for the uData contact-point and visualization family."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import operator
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.contact_visualization import (
    ContactPointCreateInput,
    ContactPointUpdateInput,
    ContactVisualizationMutationResult,
    VisualizationCreateInput,
    VisualizationImageInput,
    VisualizationListQuery,
    VisualizationPage,
    VisualizationUpdateInput,
    segment,
)
from datasluice.connectors.catalog.udata.services.contact_visualization import (
    AsyncContactVisualizationService,
    SyncContactVisualizationService,
)
from datasluice.connectors.catalog.udata.wire import contact_visualization as wire
from datasluice.contracts.catalog.native.udata import (
    AsyncUDataContactVisualizationService,
    SyncUDataContactVisualizationService,
)
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.redaction import REDACTED
from datasluice.errors.catalog import (
    CatalogNotFoundError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
)
from datasluice.runtime.events import EventEmitter, ListSink
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.helpers.udata_test_support import (
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    UDATA_SITE,
    RouteTable,
    async_client,
    async_route_table,
    mutation_policy,
    sync_client,
    sync_route_table,
    thawed,
    with_site_route,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping

    from datasluice.domain.catalog.models import MappingRecord

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_PERMISSIONS
VISUALIZATIONS = "/api/1/visualizations/"
CONTACTS = "/api/1/contacts/"
LIST_URL = f"{ORIGIN}{VISUALIZATIONS}?page=1&page_size=20"
_MUTATION_REQUIREMENTS = {
    wire.CREATE_CONTACT_POINT_OPERATION: frozenset({"contact-create"}),
    wire.UPDATE_CONTACT_POINT_OPERATION: frozenset({"contact-edit"}),
    wire.DELETE_CONTACT_POINT_OPERATION: frozenset({"contact-delete"}),
    wire.CREATE_VISUALIZATION_OPERATION: frozenset({"visualization-create"}),
    wire.UPDATE_VISUALIZATION_OPERATION: frozenset({"visualization-edit"}),
    wire.DELETE_VISUALIZATION_OPERATION: frozenset({"visualization-delete"}),
    wire.VISUALIZATION_IMAGE_OPERATION: frozenset({"visualization-image"}),
}


def _scoped(scope: str) -> EffectivePermissions:
    """Bind every mutation requirement that needs *scope* so the permission gate can discriminate."""
    return EffectivePermissions.for_credential(
        UDATA_CREDENTIAL,
        platform=CatalogPlatform.UDATA,
        scopes=frozenset({scope}),
        operation_scopes={
            operation: frozenset({scope}) for operation, required in _MUTATION_REQUIREMENTS.items() if scope in required
        },
    )


def _denied(*operations: str) -> EffectivePermissions:
    """Declare the requirements of *operations* while granting none of their scopes."""
    return EffectivePermissions.for_credential(
        UDATA_CREDENTIAL,
        platform=CatalogPlatform.UDATA,
        scopes=frozenset({"unrelated-scope"}),
        operation_scopes={operation: _MUTATION_REQUIREMENTS[operation] for operation in operations},
    )


_CONTACT_CREATE_ONLY = _scoped("contact-create")
_CONTACT_EDIT_ONLY = _scoped("contact-edit")
_CONTACT_DELETE_ONLY = _scoped("contact-delete")
_VISUALIZATION_CREATE_ONLY = _scoped("visualization-create")
_VISUALIZATION_EDIT_ONLY = _scoped("visualization-edit")
_VISUALIZATION_DELETE_ONLY = _scoped("visualization-delete")
_VISUALIZATION_IMAGE_ONLY = _scoped("visualization-image")
_IMAGE_DENIED = _denied(wire.VISUALIZATION_IMAGE_OPERATION)


def _contact_point(contact_id: str = "cp-1") -> dict[str, object]:
    return {
        "id": contact_id,
        "name": "Support",
        "email": "support@example.test",
        "contact_form": None,
        "role": "contact",
        "owner": "5f5f5f5f5f5f5f5f5f5f5f5f",
        "organization": None,
    }


def _visualization(visualization_id: str = "viz-1") -> dict[str, object]:
    return {
        "id": visualization_id,
        "title": "Population",
        "slug": "population",
        "description": "Population by year",
        "private": False,
        "extras": {"note": "bounded"},
        "deleted_at": None,
        "x_axis": {"column_x": "year", "type": "discrete"},
        "y_axis": {"label": "count", "unit": "people", "min": 0, "max": 10, "unit_position": "suffix"},
        "series": [{"resource_id": "5f5f5f5f5f5f5f5f5f5f5f", "type": "line", "column_y": "count"}],
        "image": "http://127.0.0.1:5640/uploads/image.png",
        "metrics": {},
        "permissions": {},
        "owner": "5f5f5f5f5f5f5f5f5f5f5f5f",
        "organization": None,
        "created_at": "2026-01-01T00:00:00",
        "last_modified": "2026-01-02T00:00:00",
    }


def _page(item: object, *, total: int = 1) -> dict[str, object]:
    return {
        "data": [item],
        "page": 1,
        "page_size": 20,
        "total": total,
        "next_page": None,
        "previous_page": None,
    }


def _routes() -> RouteTable:
    """Return every stock route of the family keyed by exact method and URL."""
    return with_site_route(
        {
            ("GET", f"{ORIGIN}{VISUALIZATIONS}?page=1&page_size=20"): (200, _page(_visualization())),
            ("GET", f"{ORIGIN}{VISUALIZATIONS}?page=1&page_size=20&sort=-last_modified"): (
                200,
                _page(_visualization()),
            ),
            ("POST", f"{ORIGIN}{VISUALIZATIONS}"): (201, _visualization()),
            ("GET", f"{ORIGIN}{VISUALIZATIONS}viz-1/"): (200, _visualization()),
            ("PATCH", f"{ORIGIN}{VISUALIZATIONS}viz-1/"): (200, _visualization()),
            ("DELETE", f"{ORIGIN}{VISUALIZATIONS}viz-1/"): (204, None),
            ("POST", f"{ORIGIN}{VISUALIZATIONS}viz-1/image/"): (200, _visualization()),
            ("POST", f"{ORIGIN}{CONTACTS}"): (201, _contact_point()),
            ("GET", f"{ORIGIN}{CONTACTS}cp-1/"): (200, _contact_point()),
            ("PUT", f"{ORIGIN}{CONTACTS}cp-1/"): (200, _contact_point()),
            ("DELETE", f"{ORIGIN}{CONTACTS}cp-1/"): (204, None),
            ("GET", f"{ORIGIN}{CONTACTS}roles/"): (200, [{"id": "contact", "label": "Contact"}]),
        }
    )


def _visualization_input(
    *, owner: str | None = "5f5f5f5f5f5f5f5f5f5f5f5f", organization: str | None = None
) -> VisualizationCreateInput:
    return VisualizationCreateInput(
        title="Population",
        description="Population by year",
        x_axis={"column_x": "year", "type": "discrete"},
        series=({"resource_id": "5f5f5f5f5f5f5f5f5f5f5f5f", "type": "line", "column_y": "count"},),
        y_axis={"label": "count", "unit": "people", "min": 0, "max": 10, "unit_position": "suffix"},
        owner=owner,
        organization=organization,
    )


def _contact_input(*, owner: str | None = None, organization: str | None = None) -> ContactPointCreateInput:
    return ContactPointCreateInput(
        name="Support", role="contact", email="support@example.test", owner=owner, organization=organization
    )


def _receipt_from(error: BaseException) -> MutationReceipt:
    receipt = error.__dict__.get("mutation_receipt")
    assert isinstance(receipt, MutationReceipt), error.__dict__
    return receipt


_ASSIGNED_METHODS = {
    "contact_point_roles",
    "create_contact_point",
    "create_visualization",
    "delete_contact_point",
    "delete_visualization",
    "get_contact_point",
    "get_visualization",
    "list_visualizations",
    "update_contact_point",
    "update_visualization",
    "visualization_image",
}


def test_contact_visualization_contract_exposes_every_assigned_method_in_both_modes() -> None:
    """The family is registered on both clients and projects one typed Protocol per mode."""
    sync_names = {
        name
        for name in dir(SyncContactVisualizationService)
        if not name.startswith("_") and callable(getattr(SyncContactVisualizationService, name))
    }
    async_names = {
        name
        for name in dir(AsyncContactVisualizationService)
        if not name.startswith("_") and callable(getattr(AsyncContactVisualizationService, name))
    }
    assert sync_names == async_names == _ASSIGNED_METHODS

    with sync_client(sync_route_table(with_site_route({})), None) as client:
        assert isinstance(client.contact_visualization, SyncUDataContactVisualizationService)
        assert client.contact_visualization.error_type is NativeCatalogError

    async def run() -> None:
        async with async_client(async_route_table(with_site_route({})), None) as client:
            assert isinstance(client.contact_visualization, AsyncUDataContactVisualizationService)
            assert client.contact_visualization.error_type is NativeCatalogError

    asyncio.run(run())


def test_every_contact_visualization_route_has_an_exact_wire_shape() -> None:
    """Every verb and path matches the pinned uData 17.6 swagger route independently."""
    actual = [
        wire.list_visualizations_request(VisualizationListQuery())[0:2],
        wire.create_visualization_request(_visualization_input())[0:2],
        wire.get_visualization_request("viz-1")[0:2],
        wire.update_visualization_request("viz-1", VisualizationUpdateInput(title="New"))[0:2],
        wire.delete_visualization_request("viz-1")[0:2],
        wire.visualization_image_request("viz-1")[0:2],
        wire.create_contact_point_request(_contact_input())[0:2],
        wire.get_contact_point_request("cp-1")[0:2],
        wire.update_contact_point_request("cp-1", ContactPointUpdateInput(role="creator"))[0:2],
        wire.delete_contact_point_request("cp-1")[0:2],
        wire.contact_point_roles_request()[0:2],
    ]
    assert actual == [
        ("GET", "/api/1/visualizations/?page=1&page_size=20"),
        ("POST", "/api/1/visualizations/"),
        ("GET", "/api/1/visualizations/viz-1/"),
        ("PATCH", "/api/1/visualizations/viz-1/"),
        ("DELETE", "/api/1/visualizations/viz-1/"),
        ("POST", "/api/1/visualizations/viz-1/image/"),
        ("POST", "/api/1/contacts/"),
        ("GET", "/api/1/contacts/cp-1/"),
        ("PUT", "/api/1/contacts/cp-1/"),
        ("DELETE", "/api/1/contacts/cp-1/"),
        ("GET", "/api/1/contacts/roles/"),
    ]


def test_list_query_encodes_every_documented_filter_and_omits_the_rest() -> None:
    """Optional list filters are omitted unless supplied and encoded exactly when they are."""
    assert VisualizationListQuery().query_params() == [("page", "1"), ("page_size", "20")]
    assert VisualizationListQuery(sort="-last_modified").query_params() == [
        ("page", "1"),
        ("page_size", "20"),
        ("sort", "-last_modified"),
    ]
    assert VisualizationListQuery(private=False, owner="me", organization="org").query_params() == [
        ("page", "1"),
        ("page_size", "20"),
        ("private", "false"),
        ("owner", "me"),
        ("organization", "org"),
    ]
    assert wire.list_visualizations_request(VisualizationListQuery(sort="-last_modified"))[1] == (
        "/api/1/visualizations/?page=1&page_size=20&sort=-last_modified"
    )
    with pytest.raises(ValueError, match="sort"):
        VisualizationListQuery(sort="nonsense")
    with pytest.raises(ValueError, match="private"):
        VisualizationListQuery(private=cast("bool", "true"))


def test_create_and_update_bodies_omit_every_absent_key() -> None:
    """Presence-aware payloads never send a key the caller left out."""
    assert _contact_input().payload() == {
        "name": "Support",
        "role": "contact",
        "email": "support@example.test",
    }
    assert ContactPointUpdateInput(role="creator").payload() == {"role": "creator"}
    assert ContactPointUpdateInput(email="new@example.test").payload() == {"email": "new@example.test"}
    assert VisualizationUpdateInput(title="New").payload() == {"title": "New"}
    assert _visualization_input().payload() == {
        "title": "Population",
        "description": "Population by year",
        "private": False,
        "x_axis": {"column_x": "year", "type": "discrete"},
        "series": [{"resource_id": "5f5f5f5f5f5f5f5f5f5f5f5f", "type": "line", "column_y": "count"}],
        "y_axis": {"label": "count", "unit": "people", "min": 0, "max": 10, "unit_position": "suffix"},
        "owner": "5f5f5f5f5f5f5f5f5f5f5f5f",
    }
    assert "extras" not in _visualization_input().payload()
    assert "organization" not in _visualization_input().payload()
    with pytest.raises(ValueError, match="at least one field"):
        ContactPointUpdateInput()
    with pytest.raises(ValueError, match="at least one field"):
        VisualizationUpdateInput()


def test_image_input_encodes_the_stock_multipart_file_part() -> None:
    """The upload carries one bounded file part whose bytes never enter a result record."""
    part = VisualizationImageInput(data=b"png-bytes", content_type="image/png").part()

    assert (part.field_name, part.file_name, part.content_type, part.data) == (
        "file",
        "visualization-image.png",
        "image/png",
        b"png-bytes",
    )
    assert VisualizationImageInput(data=b"j", content_type="image/jpeg").part().file_name == "visualization-image.jpg"
    assert VisualizationImageInput(data=b"w", content_type="image/webp").part().file_name == "visualization-image.webp"
    with pytest.raises(ValueError, match="content type"):
        VisualizationImageInput(data=b"x", content_type="application/pdf")
    with pytest.raises(ValueError, match="bytes"):
        VisualizationImageInput(data=cast("bytes", "not-bytes"), content_type="image/png")


_MALFORMED_SEGMENTS = (
    "",
    ".",
    "..",
    "a/b",
    "a?b",
    "a#b",
    'a"b',
    "a'b",
    "a\x00b",
    "a\nb",
    4,
    None,
    True,
    ["viz-1"],
)


@pytest.mark.parametrize("identifier", _MALFORMED_SEGMENTS)
def test_malformed_contact_visualization_segments_never_reach_a_route(identifier: object) -> None:
    """Dot segments, separators, and control characters are refused, never quoted."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(cast("str", identifier), wire.GET_VISUALIZATION_OPERATION)


_IDENTIFIER_ROUTES: tuple[tuple[str, tuple[object, ...]], ...] = (
    ("get_visualization", ()),
    ("update_visualization", (VisualizationUpdateInput(title="New"), None)),
    ("delete_visualization", (None,)),
    ("visualization_image", (VisualizationImageInput(data=b"png-bytes", content_type="image/png"), None)),
    ("get_contact_point", ()),
    ("update_contact_point", (ContactPointUpdateInput(role="creator"), None)),
    ("delete_contact_point", (None,)),
)


@pytest.mark.parametrize("identifier", ["..", "a/b", "a\nb", "", "a#b"])
@pytest.mark.parametrize(("method", "tail"), _IDENTIFIER_ROUTES)
def test_identifier_routes_are_screened_before_any_dispatch(
    method: str, tail: tuple[object, ...], identifier: str
) -> None:
    """T-04-GEO-01: a retargeting identifier is refused before the site probe or any request."""
    router = sync_route_table(with_site_route({}))
    with (
        sync_client(router, UDATA_CREDENTIAL) as client,
        pytest.raises(CatalogValidationError, match="one URL-safe path segment"),
    ):
        getattr(client.contact_visualization, method)(identifier, *tail)
    assert router.requests == [], method


def test_linked_owner_and_organization_identifiers_are_validated_before_dispatch() -> None:
    """T-04-GEO-01: relationship identifiers travel in a body and carry the same policy."""
    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.contact_visualization.create_contact_point(
                _contact_input(owner="../admin"),
                _CONTACT_CREATE_ONLY,
                mutation_policy(wire.CREATE_CONTACT_POINT_OPERATION, "contacts"),
            )
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.contact_visualization.create_visualization(
                _visualization_input(owner=None, organization="a/b"),
                _VISUALIZATION_CREATE_ONLY,
                mutation_policy(wire.CREATE_VISUALIZATION_OPERATION, "Population"),
            )
    assert router.requests == []


def test_mutually_exclusive_relationship_identifiers_are_refused() -> None:
    """A contact point may name an owner or an organization, never both."""
    with pytest.raises(ValueError, match="either an owner or an organization"):
        _contact_input(owner="5f5f5f5f5f5f5f5f5f5f5f5f", organization="5f5f5f5f5f5f5f5f5f5f5f6")
    with pytest.raises(ValueError, match="email or a contact form"):
        ContactPointCreateInput(name="Support", role="contact")
    with pytest.raises(ValueError, match="role"):
        ContactPointCreateInput(name="Support", role="invented-role", email="support@example.test")


def test_mutation_permissions_discriminate_each_route_in_both_modes() -> None:
    """T-04-GEO-01: a denial is proven by zero network calls, and the grant still succeeds."""
    routes = _routes()

    def run_sync() -> None:
        router = sync_route_table(routes)
        with sync_client(router, UDATA_CREDENTIAL) as client:
            service = client.contact_visualization
            with pytest.raises(ForbiddenError) as denied_create:
                service.create_visualization(_visualization_input(), _VISUALIZATION_EDIT_ONLY)
            with pytest.raises(ForbiddenError) as denied_delete:
                service.delete_contact_point("cp-1", _CONTACT_CREATE_ONLY)
            assert denied_create.value.operation == wire.CREATE_VISUALIZATION_OPERATION
            assert denied_delete.value.operation == wire.DELETE_CONTACT_POINT_OPERATION
            assert [request for request in router.requests if "/api/" in request.url] == []

            created = service.create_visualization(
                _visualization_input(),
                _VISUALIZATION_CREATE_ONLY,
                mutation_policy(wire.CREATE_VISUALIZATION_OPERATION, "Population"),
            )
            deleted = service.delete_contact_point(
                "cp-1",
                _CONTACT_DELETE_ONLY,
                mutation_policy(wire.DELETE_CONTACT_POINT_OPERATION, "cp-1", destructive=True),
            )
        assert created.receipt.outcome == "succeeded"
        assert deleted.receipt.outcome == "succeeded"

    async def run_async() -> None:
        router = async_route_table(routes)

        async with async_client(router, UDATA_CREDENTIAL) as client:
            service = client.contact_visualization
            with pytest.raises(ForbiddenError) as denied_update:
                await service.update_contact_point(
                    "cp-1", ContactPointUpdateInput(role="creator"), _CONTACT_DELETE_ONLY
                )
            assert denied_update.value.operation == wire.UPDATE_CONTACT_POINT_OPERATION
            assert [request for request in router.requests if "/api/" in request.url] == []

            updated = await service.update_contact_point(
                "cp-1",
                ContactPointUpdateInput(role="creator"),
                _CONTACT_EDIT_ONLY,
                mutation_policy(wire.UPDATE_CONTACT_POINT_OPERATION, "cp-1"),
            )
            deleted = await service.delete_contact_point(
                "cp-1",
                _CONTACT_DELETE_ONLY,
                mutation_policy(wire.DELETE_CONTACT_POINT_OPERATION, "cp-1", destructive=True),
            )
        assert updated.receipt.outcome == "succeeded"
        assert deleted.receipt.outcome == "succeeded"

    run_sync()
    asyncio.run(run_async())


_MALFORMED_PAGE_SIZES = (0, -1, 101, 10_000, True, False, 1.0, 10.5, "20", None, [20])
_MALFORMED_PAGES = (0, -1, True, 1.0, "1", None)


@pytest.mark.parametrize("page_size", _MALFORMED_PAGE_SIZES)
def test_malformed_page_sizes_are_rejected_before_dispatch(page_size: object) -> None:
    """T-04-GEO-02: the pager ceiling is a real integer bound, so bool, float, and str fail."""
    with pytest.raises(ValueError, match="page_size"):
        VisualizationListQuery(page_size=cast("int", page_size))


@pytest.mark.parametrize("page", _MALFORMED_PAGES)
def test_malformed_pages_are_rejected_before_dispatch(page: object) -> None:
    """T-04-GEO-02: page 0 and negative or non-integer pages never reach the pager."""
    with pytest.raises(ValueError, match="page"):
        VisualizationListQuery(page=cast("int", page))


def test_visualization_paging_accepts_the_exact_ceiling_boundary() -> None:
    """T-04-GEO-02: the documented ceiling itself stays legal, so the bound is not off by one."""
    assert VisualizationListQuery(page=1, page_size=100).query_params() == [("page", "1"), ("page_size", "100")]
    assert VisualizationListQuery(page=1, page_size=1).query_params() == [("page", "1"), ("page_size", "1")]
    with pytest.raises(ValueError, match="page_size"):
        VisualizationListQuery(page_size=101)


def test_visualization_reads_decode_the_native_page_in_both_modes() -> None:
    """The v1 list route keeps its pager metadata and allowlisted item records."""
    with sync_client(sync_route_table(_routes()), None) as client:
        page = client.contact_visualization.list_visualizations()
        assert isinstance(page, VisualizationPage)
        assert (page.page, page.page_size, page.total, page.next_page, page.previous_page) == (1, 20, 1, None, None)
        assert page.present_fields == frozenset({"data", "page", "page_size", "total", "next_page", "previous_page"})
        assert [dict(record.payload)["id"] for record in page.items] == ["viz-1"]
        assert dict(client.contact_visualization.get_visualization("viz-1").payload)["title"] == "Population"
        assert [dict(role.payload)["id"] for role in client.contact_visualization.contact_point_roles()] == ["contact"]
        assert client.contact_visualization.get_contact_point("cp-1").payload["email"] == "support@example.test"

    async def run() -> None:
        async with async_client(async_route_table(_routes()), None) as client:
            async_page = await client.contact_visualization.list_visualizations()
            contact = await client.contact_visualization.get_contact_point("cp-1")
            roles = await client.contact_visualization.contact_point_roles()
        assert async_page.to_dict() == page.to_dict()
        assert dict(contact.payload)["role"] == "contact"
        assert [dict(role.payload)["label"] for role in roles] == ["Contact"]

    asyncio.run(run())


_CONTACT_ALLOWED_FIELDS = frozenset(
    {
        "id",
        "name",
        "email",
        "contact_form",
        "role",
        "owner",
        "organization",
        "resource_kind",
        "operation",
    }
)
_VISUALIZATION_ALLOWED_FIELDS = frozenset(
    {
        "id",
        "title",
        "slug",
        "description",
        "private",
        "extras",
        "deleted_at",
        "x_axis",
        "y_axis",
        "series",
        "image",
        "metrics",
        "permissions",
        "owner",
        "organization",
        "created_at",
        "last_modified",
        "resource_kind",
        "operation",
    }
)
_UNBOUND_FIELDS = {
    "body": {"raw": "anything"},
    "headers": {"X-API-KEY": "secret-key"},
    "api_key": "secret-key",
    "password": "hunter2",
    "internal_notes": "unreleased",
    "checksum": "deadbeef",
}


def test_contact_point_serialization_exposes_no_field_beyond_the_allowlist() -> None:
    """A contact point carries personal data, so nothing off the allowlist is ever serialized."""
    payload = {**_contact_point(), **_UNBOUND_FIELDS, "email": "support@example.test?api_key=secret-key"}
    record = wire.parse_contact_point(payload, operation=wire.GET_CONTACT_POINT_OPERATION)
    retained = dict(record.payload)
    serialized = cast("Mapping[str, object]", record.to_dict()["payload"])

    assert set(retained) <= _CONTACT_ALLOWED_FIELDS
    assert set(serialized) <= _CONTACT_ALLOWED_FIELDS
    assert "body" not in retained
    assert "headers" not in retained
    assert "api_key" not in retained
    assert "internal_notes" not in retained
    assert retained["email"] == "support@example.test?api_key=***"
    assert serialized["email"] == "support@example.test?api_key=***"
    rendered = json.dumps(record.to_dict())
    assert "secret-key" not in rendered
    assert "hunter2" not in rendered
    assert "unreleased" not in rendered


def test_contact_point_read_never_screens_out_the_allowlisted_personal_fields() -> None:
    """The allowlist is a ceiling, not a floor: documented contact fields survive redaction."""
    record = wire.parse_contact_point({**_contact_point(), **_UNBOUND_FIELDS})
    serialized = cast("Mapping[str, object]", record.to_dict()["payload"])

    assert set(serialized) == _CONTACT_ALLOWED_FIELDS
    assert serialized["name"] == "Support"
    assert serialized["email"] == "support@example.test"
    assert serialized["contact_form"] is None
    assert serialized["organization"] is None
    assert serialized["resource_kind"] == "contact-point"
    assert serialized["operation"] == wire.GET_CONTACT_POINT_OPERATION


def test_visualization_serialization_exposes_no_field_beyond_the_allowlist() -> None:
    """The same ceiling applies to every visualization record on every read path."""
    payload = {**_visualization(), **_UNBOUND_FIELDS, "extras": {"note": "bounded", "token": "secret-key"}}
    record = wire.parse_visualization(payload, operation=wire.GET_VISUALIZATION_OPERATION)
    serialized = cast("Mapping[str, object]", record.to_dict()["payload"])

    assert set(serialized) == _VISUALIZATION_ALLOWED_FIELDS
    assert "body" not in serialized
    assert cast("Mapping[str, object]", dict(record.payload)["extras"])["token"] == REDACTED
    assert cast("Mapping[str, object]", serialized["extras"])["token"] == REDACTED
    assert "secret-key" not in json.dumps(record.to_dict())


def test_visualization_page_serialization_stays_allowlisted_for_every_item() -> None:
    """A list read retains no more than a single-item read does."""
    page = wire.parse_visualization_page(_page({**_visualization(), **_UNBOUND_FIELDS}))
    serialized = page.to_dict()

    assert serialized["kind"] == "udata_visualization_page"
    for item in page.items:
        assert "api_key" not in item.payload
    for serialized_item in cast("list[Mapping[str, object]]", serialized["items"]):
        assert set(cast("Mapping[str, object]", serialized_item["payload"])) <= (_VISUALIZATION_ALLOWED_FIELDS)
    assert "secret-key" not in json.dumps(serialized)


def test_contact_role_decoding_rejects_undocumented_roles_and_missing_labels() -> None:
    """A deployment-supplied role outside the pinned vocabulary fails closed."""
    decoded = wire.parse_contact_point_roles([{"id": "creator", "label": "Creator"}])

    assert [dict(record.payload) for record in decoded] == [
        {
            "id": "creator",
            "label": "Creator",
            "resource_kind": "contact-role",
            "operation": wire.CONTACT_POINT_ROLES_OPERATION,
        }
    ]
    for payload in (
        [{"id": "invented-role", "label": "Invented"}],
        [{"id": "contact"}],
        [{"label": "Contact"}],
        {"id": "contact", "label": "Contact"},
        "contact",
        None,
    ):
        with pytest.raises(CatalogValidationError):
            wire.parse_contact_point_roles(payload)


@pytest.mark.parametrize("payload", [{"name": "Support"}, ["not-an-object"], None, 4, "support"])
def test_contact_point_decoding_fails_closed_without_an_identifier(payload: object) -> None:
    """A record without a usable identifier never reaches a typed result."""
    with pytest.raises(CatalogValidationError):
        wire.parse_contact_point(payload)


def test_contact_point_evidence_never_retains_the_caller_supplied_name() -> None:
    """T-04-GEO-03: the receipt target is the collection, so a personal name is not retained."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        result = client.contact_visualization.create_contact_point(
            _contact_input(),
            _CONTACT_CREATE_ONLY,
            mutation_policy(wire.CREATE_CONTACT_POINT_OPERATION, "contacts"),
        )

    assert isinstance(result, ContactVisualizationMutationResult)
    assert result.receipt.target.value == "cp-1"
    assert result.receipt.operation == wire.CREATE_CONTACT_POINT_OPERATION
    rendered = json.dumps(result.to_dict())
    assert "secret-key" not in rendered
    assert '"body"' not in rendered


def test_contact_point_create_without_a_server_identifier_receipts_the_collection_target() -> None:
    """A deployment that answers without an identifier never rewrites the confirmed target."""
    router = sync_route_table(
        with_site_route({("POST", f"{ORIGIN}{CONTACTS}"): (201, {"name": "Support", "role": "contact"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        result = client.contact_visualization.create_contact_point(
            _contact_input(),
            _CONTACT_CREATE_ONLY,
            mutation_policy(wire.CREATE_CONTACT_POINT_OPERATION, "contacts"),
        )

    assert result.receipt.target.value == "contacts"


def test_receipts_are_redacted_and_carry_no_raw_body_for_every_mutation() -> None:
    """Every mutation outcome yields an immutable receipt without credentials or bodies."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.contact_visualization
        outcomes = [
            service.create_visualization(
                _visualization_input(),
                _VISUALIZATION_CREATE_ONLY,
                mutation_policy(wire.CREATE_VISUALIZATION_OPERATION, "Population"),
            ),
            service.update_visualization(
                "viz-1",
                VisualizationUpdateInput(title="New"),
                _VISUALIZATION_EDIT_ONLY,
                mutation_policy(wire.UPDATE_VISUALIZATION_OPERATION, "viz-1"),
            ),
            service.delete_visualization(
                "viz-1",
                _VISUALIZATION_DELETE_ONLY,
                mutation_policy(wire.DELETE_VISUALIZATION_OPERATION, "viz-1", destructive=True),
            ),
            service.visualization_image(
                "viz-1",
                VisualizationImageInput(data=b"png-bytes", content_type="image/png"),
                _VISUALIZATION_IMAGE_ONLY,
                mutation_policy(wire.VISUALIZATION_IMAGE_OPERATION, "viz-1"),
            ),
            service.update_contact_point(
                "cp-1",
                ContactPointUpdateInput(role="creator"),
                _CONTACT_EDIT_ONLY,
                mutation_policy(wire.UPDATE_CONTACT_POINT_OPERATION, "cp-1"),
            ),
        ]

    assert [outcome.receipt.outcome for outcome in outcomes] == ["succeeded"] * 5
    assert [outcome.receipt.audit_metadata["mutation"] for outcome in outcomes] == [
        "created",
        "updated",
        "deleted",
        "updated",
        "updated",
    ]
    for outcome in outcomes:
        rendered = json.dumps(outcome.receipt.to_dict())
        assert "secret-key" not in rendered
        assert "png-bytes" not in rendered
        assert '"body"' not in rendered


def test_image_upload_sends_the_stock_multipart_file_part() -> None:
    """The multipart seam carries exactly one file part and no JSON body."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.contact_visualization.visualization_image(
            "viz-1",
            VisualizationImageInput(data=b"png-bytes", content_type="image/png"),
            _VISUALIZATION_IMAGE_ONLY,
            mutation_policy(wire.VISUALIZATION_IMAGE_OPERATION, "viz-1"),
        )

    request = router.requests[-1]
    assert (request.method, request.url) == ("POST", f"{ORIGIN}{VISUALIZATIONS}viz-1/image/")
    assert len(request.files) == 1
    part = request.files[0]
    assert (part.field_name, part.file_name, part.content_type, part.data) == (
        "file",
        "visualization-image.png",
        "image/png",
        b"png-bytes",
    )
    assert dict(request.headers)["X-API-KEY"] == "secret-key"
    assert request.body is None


def test_image_upload_requires_permission_before_any_byte_is_sent() -> None:
    """T-04-GEO-01: an unauthorized upload never reaches the transport."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as denied:
        client.contact_visualization.visualization_image(
            "viz-1",
            VisualizationImageInput(data=b"png-bytes", content_type="image/png"),
            _IMAGE_DENIED,
            mutation_policy(wire.VISUALIZATION_IMAGE_OPERATION, "viz-1"),
        )
    assert denied.value.operation == wire.VISUALIZATION_IMAGE_OPERATION
    assert [request for request in router.requests if "/api/" in request.url] == []


def test_a_missing_record_maps_to_a_typed_error_without_retry_or_body_echo() -> None:
    """A client error is terminal: one dispatch, one typed error, and no echoed body."""
    router = sync_route_table(
        with_site_route({("GET", f"{ORIGIN}{VISUALIZATIONS}viz-404/"): (404, {"message": "Not found"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(CatalogNotFoundError) as raised:
        client.contact_visualization.get_visualization("viz-404")

    assert "Not found" not in str(raised.value)
    assert len([request for request in router.requests if "viz-404" in request.url]) == 1


def test_delete_failures_keep_the_exact_target_and_a_redacted_receipt() -> None:
    """A refused delete stays visible against its exact target."""
    router = sync_route_table(with_site_route({("DELETE", f"{ORIGIN}{CONTACTS}cp-1/"): (403, {"message": "Denied"})}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as raised:
        client.contact_visualization.delete_contact_point(
            "cp-1",
            _CONTACT_DELETE_ONLY,
            mutation_policy(wire.DELETE_CONTACT_POINT_OPERATION, "cp-1", destructive=True),
        )

    receipt = _receipt_from(raised.value)
    assert receipt.outcome == "failed"
    assert receipt.target.value == "cp-1"
    assert receipt.operation == wire.DELETE_CONTACT_POINT_OPERATION
    assert b"secret-key" not in json.dumps(receipt.to_dict()).encode()


def test_a_destructive_delete_requires_a_confirmed_destructive_policy() -> None:
    """T-04-GEO-01: a delete without the destructive flag is refused before dispatch."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as raised:
        client.contact_visualization.delete_visualization(
            "viz-1",
            _VISUALIZATION_DELETE_ONLY,
            mutation_policy(wire.DELETE_VISUALIZATION_OPERATION, "viz-1"),
        )

    assert _receipt_from(raised.value).outcome == "rejected"
    assert [request for request in router.requests if request.method == "DELETE"] == []


def test_a_confirmation_bound_to_another_target_is_refused_before_dispatch() -> None:
    """T-04-GEO-01: the confirmation must name this operation and this exact target."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as raised:
        client.contact_visualization.delete_contact_point(
            "cp-1",
            _CONTACT_DELETE_ONLY,
            mutation_policy(wire.DELETE_CONTACT_POINT_OPERATION, "cp-2", destructive=True),
        )

    assert _receipt_from(raised.value).outcome == "rejected"
    assert [request for request in router.requests if request.method == "DELETE"] == []


def test_unauthenticated_mutations_fail_closed_before_dispatch() -> None:
    """A mutation without resolved credential evidence never leaves the process."""
    router = sync_route_table(_routes())
    with sync_client(router, None) as client, pytest.raises(UnauthenticatedError):
        client.contact_visualization.create_contact_point(
            _contact_input(),
            _CONTACT_CREATE_ONLY,
            mutation_policy(wire.CREATE_CONTACT_POINT_OPERATION, "contacts"),
        )
    assert [request for request in router.requests if "/api/" in request.url] == []


_LIST_ROUTE = ("GET", LIST_URL)
_DELETE_ROUTE = ("DELETE", f"{ORIGIN}{VISUALIZATIONS}viz-1/")


class _InterruptingRoutes:
    """Answer the site probe, one read route, and one mutation route, raising while armed."""

    def __init__(self, interruption: BaseException) -> None:
        self._interruption = interruption
        self.armed = True
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0
        self.aclose_count = 0

    def _respond(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        key = (request.method, request.url)
        if self.armed and key in {_LIST_ROUTE, _DELETE_ROUTE}:
            raise self._interruption
        if request.url.endswith("/api/1/site/"):
            payload: object = UDATA_SITE
        elif key == _LIST_ROUTE:
            payload = _page(_visualization())
        elif request.method == "GET":
            payload = _visualization()
        else:
            payload = None
        body = b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(200, {"Content-Type": "application/json"}, body)

    def close(self) -> None:
        self.close_count += 1

    async def aclose(self) -> None:
        self.aclose_count += 1


class _CancellingSyncTransport(_InterruptingRoutes):
    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._respond(request)


class _CancellingAsyncTransport(_InterruptingRoutes):
    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._respond(request)


def _sync_interrupted_client(
    interruption: BaseException, *, owns_transport: bool = True, emitter: EventEmitter | None = None
) -> tuple[SyncUDataClient, _CancellingSyncTransport]:
    transport = _CancellingSyncTransport(interruption)
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        emitter=emitter,
        breaker_failure_threshold=2,
        retry_sleep=lambda _: None,
        owns_transport=owns_transport,
    )
    return client, transport


def _async_interrupted_client(
    interruption: BaseException, *, owns_transport: bool = True, emitter: EventEmitter | None = None
) -> tuple[AsyncUDataClient, _CancellingAsyncTransport]:
    transport = _CancellingAsyncTransport(interruption)
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        emitter=emitter,
        breaker_failure_threshold=2,
        owns_transport=owns_transport,
    )
    return client, transport


def test_cancelled_sync_mutation_records_a_cancelled_receipt_with_the_exact_target() -> None:
    """T-04-GEO-02: an interrupted delete stays `cancelled` against its exact target."""
    client, _ = _sync_interrupted_client(KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt) as stopped:
        client.contact_visualization.delete_visualization(
            "viz-1",
            _VISUALIZATION_DELETE_ONLY,
            mutation_policy(wire.DELETE_VISUALIZATION_OPERATION, "viz-1", destructive=True),
        )

    receipt = _receipt_from(stopped.value)
    assert receipt.outcome == "cancelled"
    assert receipt.operation == wire.DELETE_VISUALIZATION_OPERATION
    assert receipt.target.value == "viz-1"
    assert b"secret-key" not in json.dumps(receipt.to_dict()).encode()
    client.close()


def test_cancelled_async_mutation_mirrors_the_sync_cancelled_receipt() -> None:
    """T-04-GEO-02: the async receipt matches sync exactly, including the target."""
    client, _ = _async_interrupted_client(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError) as cancelled:
            await client.contact_visualization.delete_visualization(
                "viz-1",
                _VISUALIZATION_DELETE_ONLY,
                mutation_policy(wire.DELETE_VISUALIZATION_OPERATION, "viz-1", destructive=True),
            )
        receipt = _receipt_from(cancelled.value)
        assert receipt.outcome == "cancelled"
        assert receipt.target.value == "viz-1"
        await client.aclose()

    asyncio.run(run())


def test_cancelled_read_leaves_post_state_ready_and_closes_the_owned_transport_once() -> None:
    """T-04-GEO-02: an interrupted read propagates and close-once ownership still holds."""
    client, transport = _sync_interrupted_client(KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        client.contact_visualization.list_visualizations()
    assert transport.close_count == 0

    transport.armed = False
    page = client.contact_visualization.list_visualizations()
    assert cast("Mapping[str, object]", thawed(page.to_dict()))["total"] == 1

    client.close()
    client.close()
    assert transport.close_count == 1


def test_cancelled_read_never_closes_a_borrowed_transport() -> None:
    """T-04-GEO-02: the caller keeps ownership of a transport it lent to the client."""
    client, transport = _async_interrupted_client(asyncio.CancelledError(), owns_transport=False)

    async def run() -> None:
        async with client:
            with pytest.raises(asyncio.CancelledError):
                await client.contact_visualization.list_visualizations()
        assert transport.aclose_count == 0

    asyncio.run(run())


def test_async_cancellation_is_never_recorded_as_a_circuit_breaker_failure() -> None:
    """T-04-GEO-02: cancellations do not trip the breaker, so the next read is admitted."""
    events = ListSink()
    client, transport = _async_interrupted_client(asyncio.CancelledError(), emitter=EventEmitter(sinks=(events,)))

    async def run() -> None:
        for _ in range(4):
            with pytest.raises(asyncio.CancelledError):
                await client.contact_visualization.list_visualizations()
        transport.armed = False
        page = await client.contact_visualization.list_visualizations()
        assert cast("Mapping[str, object]", thawed(page.to_dict()))["total"] == 1
        await client.aclose()
        await client.aclose()
        assert transport.aclose_count == 1

    asyncio.run(run())
    assert [event.outcome for event in events.events if "breaker" in event.outcome] == []


def test_sync_and_async_modes_produce_identical_wire_and_records() -> None:
    """WR-09 parity: both modes drive the same sequence and decode the same records."""
    sync_router = sync_route_table(_routes())
    async_router = async_route_table(_routes())

    with sync_client(sync_router, UDATA_CREDENTIAL) as client:
        sync_page = client.contact_visualization.list_visualizations()
        sync_contact = client.contact_visualization.get_contact_point("cp-1")

    async def run() -> None:
        async with async_client(async_router, UDATA_CREDENTIAL) as client:
            async_page = await client.contact_visualization.list_visualizations()
            async_contact = await client.contact_visualization.get_contact_point("cp-1")
        assert async_page.to_dict() == sync_page.to_dict()
        assert async_contact.to_dict() == sync_contact.to_dict()

    asyncio.run(run())

    assert [(request.method, request.url) for request in sync_router.requests] == [
        (request.method, request.url) for request in async_router.requests
    ]


_NON_FINITE_ROUTES = (
    ("get_visualization", ("viz-1",), b'{"id": "viz-1", "title": @@}', "/api/1/visualizations/viz-1/"),
    ("get_contact_point", ("cp-1",), b'{"id": "cp-1", "name": @@}', "/api/1/contacts/cp-1/"),
    (
        "list_visualizations",
        (),
        b'{"data": [{"id": "viz-1", "title": @@}]}',
        "/api/1/visualizations/?page=1&page_size=20",
    ),
    ("contact_point_roles", (), b'[{"id": "contact", "label": @@}]', "/api/1/contacts/roles/"),
)


@pytest.mark.parametrize("literal", (b"NaN", b"Infinity", b"-Infinity"))
@pytest.mark.parametrize(("read", "arguments", "shape", "path"), _NON_FINITE_ROUTES)
def test_non_finite_responses_fail_typed_without_echoing_the_raw_body(
    literal: bytes, read: str, arguments: tuple[str, ...], shape: bytes, path: str
) -> None:
    """A non-finite constant is not JSON and must never reach a typed record or an error."""
    router = sync_route_table(with_site_route({("GET", f"{ORIGIN}{path}"): (200, shape.replace(b"@@", literal))}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(NativeCatalogError) as raised:
        getattr(client.contact_visualization, read)(*arguments)

    rendered = repr(raised.value) + str(raised.value.__dict__)
    assert literal.decode() not in rendered
    assert "secret-key" not in rendered


@pytest.mark.parametrize(
    "payload",
    [None, [], 4, "page", {}, {"page": 1}, {"data": [{"title": "no id"}]}, {"data": [4]}, {"data": {}}],
)
def test_malformed_page_envelopes_fail_closed_with_the_route_identity(payload: object) -> None:
    """A page envelope without a usable data list never becomes a typed page."""
    with pytest.raises(CatalogValidationError):
        wire.parse_visualization_page(payload)


def test_typed_inputs_and_records_are_immutable() -> None:
    """Frozen inputs and records never hand a caller a mutable container."""
    client_input = _visualization_input()
    with pytest.raises(TypeError):
        cast("MutableMapping[str, object]", client_input.series[0])["column_y"] = "changed"

    record = wire.parse_visualization(_visualization())
    with pytest.raises(TypeError):
        cast("MutableMapping[str, object]", record.payload)["title"] = "changed"

    page = wire.parse_visualization_page(_page(_visualization()))
    with pytest.raises(dataclasses.FrozenInstanceError):
        operator.methodcaller("__setattr__", "total", 99)(page)


def test_mutable_pager_metadata_is_refused() -> None:
    """The typed page refuses a non-frozenset presence record rather than coercing it."""
    with pytest.raises(ValueError, match="frozenset"):
        VisualizationPage(items=(), present_fields=cast("frozenset[str]", {"data"}))
    with pytest.raises(ValueError, match="mapping records"):
        VisualizationPage(items=cast("tuple[MappingRecord, ...]", ({"id": "viz-1"},)))


def test_mutation_results_only_carry_a_receipt_and_a_redacted_record() -> None:
    """The mutation result has no raw-body passthrough and no image bytes."""
    router = sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}{VISUALIZATIONS}viz-1/image/"): (
                    200,
                    {**_visualization(), **_UNBOUND_FIELDS},
                )
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        result = client.contact_visualization.visualization_image(
            "viz-1",
            VisualizationImageInput(data=b"png-bytes", content_type="image/png"),
            _VISUALIZATION_IMAGE_ONLY,
            mutation_policy(wire.VISUALIZATION_IMAGE_OPERATION, "viz-1"),
        )

    serialized = result.to_dict()
    assert set(serialized) == {"receipt", "record"}
    rendered = json.dumps(serialized)
    assert "png-bytes" not in rendered
    assert "secret-key" not in rendered
    assert "hunter2" not in rendered
    assert '"raw"' not in rendered
    record_payload = cast("Mapping[str, object]", cast("Mapping[str, object]", serialized["record"])["payload"])
    assert record_payload["body"] == REDACTED
    assert record_payload["api_key"] == REDACTED
    assert record_payload["headers"] == REDACTED


def test_every_assigned_service_call_drives_only_declared_routes() -> None:
    """Every method the Protocol advertises reaches a declared route with the exact verb."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.contact_visualization
        service.list_visualizations(VisualizationListQuery(sort="-last_modified"))
        service.get_visualization("viz-1")
        service.create_visualization(
            _visualization_input(),
            _VISUALIZATION_CREATE_ONLY,
            mutation_policy(wire.CREATE_VISUALIZATION_OPERATION, "Population"),
        )
        service.update_visualization(
            "viz-1",
            VisualizationUpdateInput(title="New"),
            _VISUALIZATION_EDIT_ONLY,
            mutation_policy(wire.UPDATE_VISUALIZATION_OPERATION, "viz-1"),
        )
        service.visualization_image(
            "viz-1",
            VisualizationImageInput(data=b"png-bytes", content_type="image/png"),
            _VISUALIZATION_IMAGE_ONLY,
            mutation_policy(wire.VISUALIZATION_IMAGE_OPERATION, "viz-1"),
        )
        service.delete_visualization(
            "viz-1",
            _VISUALIZATION_DELETE_ONLY,
            mutation_policy(wire.DELETE_VISUALIZATION_OPERATION, "viz-1", destructive=True),
        )
        service.create_contact_point(
            _contact_input(),
            _CONTACT_CREATE_ONLY,
            mutation_policy(wire.CREATE_CONTACT_POINT_OPERATION, "contacts"),
        )
        service.get_contact_point("cp-1")
        service.update_contact_point(
            "cp-1",
            ContactPointUpdateInput(role="creator"),
            _CONTACT_EDIT_ONLY,
            mutation_policy(wire.UPDATE_CONTACT_POINT_OPERATION, "cp-1"),
        )
        service.delete_contact_point(
            "cp-1",
            _CONTACT_DELETE_ONLY,
            mutation_policy(wire.DELETE_CONTACT_POINT_OPERATION, "cp-1", destructive=True),
        )
        service.contact_point_roles()

    assert [(request.method, request.url) for request in router.requests] == [
        ("GET", f"{ORIGIN}/api/1/site/"),
        ("GET", f"{ORIGIN}{VISUALIZATIONS}?page=1&page_size=20&sort=-last_modified"),
        ("GET", f"{ORIGIN}{VISUALIZATIONS}viz-1/"),
        ("POST", f"{ORIGIN}{VISUALIZATIONS}"),
        ("PATCH", f"{ORIGIN}{VISUALIZATIONS}viz-1/"),
        ("POST", f"{ORIGIN}{VISUALIZATIONS}viz-1/image/"),
        ("DELETE", f"{ORIGIN}{VISUALIZATIONS}viz-1/"),
        ("POST", f"{ORIGIN}{CONTACTS}"),
        ("GET", f"{ORIGIN}{CONTACTS}cp-1/"),
        ("PUT", f"{ORIGIN}{CONTACTS}cp-1/"),
        ("DELETE", f"{ORIGIN}{CONTACTS}cp-1/"),
        ("GET", f"{ORIGIN}{CONTACTS}roles/"),
    ]
