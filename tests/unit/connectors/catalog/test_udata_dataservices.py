"""Exact wire and safety coverage for the uData dataservice and dataservice-follower family.

Expectations are transcribed from the pinned uData 17.6.0 source
(`udata.core.dataservices.api`, `udata.core.dataservices.apiv2`, and
`udata.core.followers.api` at commit 0546582), not from the production
request builders.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

import pytest

from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.dataservices import (
    DataserviceCreateInput,
    DataserviceDatasetLinkInput,
    DataserviceDeleteOptions,
    DataserviceFollowersQuery,
    DataserviceListQuery,
    DataserviceSearchQuery,
    DataserviceUpdateInput,
    segment,
)
from datasluice.connectors.catalog.udata.services.dataservices import (
    AsyncDataservicesService,
    SyncDataservicesService,
)
from datasluice.connectors.catalog.udata.wire import dataservices as wire
from datasluice.errors.catalog import (
    CatalogNotFoundError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
)
from datasluice.runtime.events import EventEmitter, ListSink
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportError
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    UDATA_SITE,
    SyncRouteRouter,
    anonymous_sync_client,
    async_client,
    async_route_table,
    atom_sync_route_table,
    sync_client,
    sync_route_table,
    thawed,
    udata_page,
    with_site_route,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datasluice.domain.catalog.receipts import MutationReceipt

ORIGIN = UDATA_ORIGIN
_BASE: dict[str, object] = {"title": "An API", "base_api_url": "https://example.com"}
DATASERVICES = "/api/1/dataservices"
DATASERVICES_V2 = "/api/2/dataservices"
EDIT_ONLY_PERMISSIONS = UDATA_PERMISSIONS
ADMIN_PERMISSIONS = UDATA_ADMIN_PERMISSIONS

_ASSIGNED_METHODS = {
    "list_dataservices",
    "create_dataservice",
    "recent_dataservices_atom_feed",
    "get_dataservice",
    "update_dataservice",
    "delete_dataservice",
    "feature_dataservice",
    "unfeature_dataservice",
    "dataservice_datasets_add",
    "dataservice_dataset_remove",
    "rdf_dataservice",
    "rdf_dataservice_format",
    "search_dataservices",
    "list_dataservice_followers",
    "follow_dataservice",
    "unfollow_dataservice",
}


def _policy(operation: str, target: str, *, destructive: bool = False):
    from tests.helpers.udata_test_support import mutation_policy

    return mutation_policy(operation, target, destructive=destructive)


def _dataservice(id_: str = "dataservice-1", **overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "id": id_,
        "title": "An API",
        "acronym": "AA",
        "slug": "an-api",
        "description": "A description",
        "base_api_url": "https://example.com/api",
        "machine_documentation_url": "https://example.com/swagger",
        "technical_documentation_url": None,
        "business_documentation_url": None,
        "rate_limiting": "100/day",
        "rate_limiting_url": None,
        "availability": 99.99,
        "availability_url": None,
        "format": "REST",
        "license": None,
        "tags": ["api"],
        "private": False,
        "extras": {},
        "featured": False,
        "contact_points": [],
        "created_at": "2026-01-01T00:00:00+00:00",
        "metadata_modified_at": "2026-01-01T00:00:00+00:00",
        "datasets": [],
    }
    record.update(overrides)
    return record


def test_dataservice_contract_exposes_every_assigned_method_in_both_modes() -> None:
    sync_names = {
        name
        for name in dir(SyncDataservicesService)
        if not name.startswith("_") and callable(getattr(SyncDataservicesService, name))
    }
    async_names = {
        name
        for name in dir(AsyncDataservicesService)
        if not name.startswith("_") and callable(getattr(AsyncDataservicesService, name))
    }
    assert sync_names == async_names == _ASSIGNED_METHODS


def test_row87_list_dataservices_matches_exact_path_and_query() -> None:
    assert wire.list_dataservices_request(DataserviceListQuery()) == (
        "GET",
        f"{DATASERVICES}/?page=1&page_size=20",
        {},
        None,
    )
    assert wire.list_dataservices_request(
        DataserviceListQuery(q="api", page=2, page_size=5, sort="-created", filters={"tag": ("a", "b")})
    ) == (
        "GET",
        f"{DATASERVICES}/?page=2&page_size=5&q=api&sort=-created&tag=a&tag=b",
        {},
        None,
    )


def test_row89_recent_atom_feed_matches_exact_path() -> None:
    assert wire.recent_dataservices_atom_feed_request(DataserviceListQuery()) == (
        "GET",
        f"{DATASERVICES}/recent.atom?page=1&page_size=20",
        {},
        None,
    )


def test_row90_get_dataservice_matches_exact_path() -> None:
    assert wire.get_dataservice_request("ds 1") == ("GET", f"{DATASERVICES}/ds%201/", {}, None)


def test_row99_search_dataservices_uses_the_v2_endpoint() -> None:
    assert wire.search_dataservices_request(DataserviceSearchQuery()) == (
        "GET",
        f"{DATASERVICES_V2}/search/?page=1&page_size=50",
        {},
        None,
    )
    assert wire.search_dataservices_request(
        DataserviceSearchQuery(q="api", page=3, page_size=10, filters={"featured": True})
    ) == (
        "GET",
        f"{DATASERVICES_V2}/search/?page=3&page_size=10&q=api&featured=true",
        {},
        None,
    )


def test_row92_delete_dataservice_omits_the_legal_notice_query_by_default() -> None:
    assert wire.delete_dataservice_request("ds-1", DataserviceDeleteOptions()) == (
        "DELETE",
        f"{DATASERVICES}/ds-1/",
        {},
        None,
    )
    assert wire.delete_dataservice_request("ds-1", DataserviceDeleteOptions(send_legal_notice=True)) == (
        "DELETE",
        f"{DATASERVICES}/ds-1/?send_legal_notice=true",
        {},
        None,
    )


def test_row95_dataservice_datasets_posts_the_documented_list_of_ids() -> None:
    assert wire.dataservice_datasets_add_request("ds-1", DataserviceDatasetLinkInput(("dataset-1", "dataset-2"))) == (
        "POST",
        f"{DATASERVICES}/ds-1/datasets/",
        {},
        [{"id": "dataset-1"}, {"id": "dataset-2"}],
    )


def test_row97_rdf_dataservice_and_row98_rdf_format_share_one_builder() -> None:
    assert wire.rdf_dataservice_request("ds-1") == ("GET", f"{DATASERVICES}/ds-1/rdf", {}, None)
    assert wire.rdf_dataservice_request("ds-1", "ttl") == ("GET", f"{DATASERVICES}/ds-1/rdf.ttl", {}, None)
    assert wire.rdf_dataservice_request("ds-1", "json") == ("GET", f"{DATASERVICES}/ds-1/rdf.json", {}, None)
    with pytest.raises(CatalogValidationError, match="unsupported"):
        wire.rdf_dataservice_request("ds-1", "yaml")


def test_rows253_254_255_follower_routes_share_one_path() -> None:
    assert wire.list_dataservice_followers_request("ds-1", DataserviceFollowersQuery()) == (
        "GET",
        f"{DATASERVICES}/ds-1/followers/?page=1&page_size=20",
        {},
        None,
    )
    assert wire.list_dataservice_followers_request("ds-1", DataserviceFollowersQuery(user="u1")) == (
        "GET",
        f"{DATASERVICES}/ds-1/followers/?page=1&page_size=20&user=u1",
        {},
        None,
    )
    assert wire.follow_dataservice_request("ds-1") == ("POST", f"{DATASERVICES}/ds-1/followers/", {}, None)
    assert wire.unfollow_dataservice_request("ds-1") == ("DELETE", f"{DATASERVICES}/ds-1/followers/", {}, None)


def test_row88_create_dataservice_posts_only_the_supplied_documented_keys() -> None:
    client_input = DataserviceCreateInput(title="An API", base_api_url="https://example.com/api")
    assert wire.create_dataservice_request(client_input) == (
        "POST",
        f"{DATASERVICES}/",
        {},
        {"title": "An API", "base_api_url": "https://example.com/api"},
    )
    assert wire.create_dataservice_request(
        client_input,
    )[3] == {"title": "An API", "base_api_url": "https://example.com/api"}


def test_row91_update_dataservice_is_a_patch_that_omits_absent_fields() -> None:
    assert wire.update_dataservice_request("ds-1", DataserviceUpdateInput(title="New")) == (
        "PATCH",
        f"{DATASERVICES}/ds-1/",
        {},
        {"title": "New"},
    )


@pytest.mark.parametrize("identifier", [".", "..", "a/b", "a?b", ""])
def test_dataservice_identifiers_reject_unsafe_path_segments(identifier: str) -> None:
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(identifier, wire.GET_DATASERVICE_OPERATION)


def test_dataservice_identifier_encoding_happens_once() -> None:
    assert wire.get_dataservice_request("a b") == ("GET", f"{DATASERVICES}/a%20b/", {}, None)


def test_dataservice_create_requires_a_title_and_a_base_api_url() -> None:
    with pytest.raises(ValueError):
        DataserviceCreateInput(title=" ", base_api_url="https://example.com/api")
    with pytest.raises(ValueError):
        DataserviceCreateInput(title="An API", base_api_url=" ")


def test_dataservice_update_requires_at_least_one_field() -> None:
    with pytest.raises(ValueError):
        DataserviceUpdateInput()


def test_dataservice_queries_reject_invalid_input_before_dispatch() -> None:
    with pytest.raises(ValueError):
        DataserviceListQuery(page=0)
    with pytest.raises(ValueError):
        DataserviceListQuery(page_size=0)
    with pytest.raises(ValueError):
        DataserviceListQuery(sort="not-a-sort")
    with pytest.raises(ValueError):
        DataserviceListQuery(filters={"not-a-filter": "x"})
    with pytest.raises(ValueError):
        DataserviceListQuery(filters={"featured": "yes"})
    with pytest.raises(ValueError):
        DataserviceSearchQuery(page=0)
    with pytest.raises(ValueError):
        DataserviceSearchQuery(filters={"featured": "yes"})
    with pytest.raises(ValueError):
        DataserviceFollowersQuery(page=0)
    with pytest.raises(ValueError):
        DataserviceFollowersQuery(user="")
    with pytest.raises(ValueError):
        DataserviceDatasetLinkInput(())
    with pytest.raises(ValueError):
        DataserviceDatasetLinkInput(("dataset-1", ""))
    with pytest.raises(ValueError):
        DataserviceDeleteOptions(send_legal_notice=cast("bool", "yes"))


@pytest.mark.parametrize("oversized", [101, 1_000, 10**9])
def test_dataservice_paging_is_rejected_above_the_ceiling_before_dispatch(oversized: int) -> None:
    """An arbitrarily large page_size is refused at construction, so no request can ever carry it."""
    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(ValueError, match="1 through 100"):
            DataserviceListQuery(page_size=oversized)
        with pytest.raises(ValueError, match="1 through 100"):
            DataserviceSearchQuery(page_size=oversized)
        with pytest.raises(ValueError, match="1 through 100"):
            DataserviceFollowersQuery(page_size=oversized)
        with pytest.raises(ValueError, match="1 through 100"):
            client.dataservices.list_dataservices(DataserviceListQuery(page_size=oversized))
        with pytest.raises(ValueError, match="1 through 100"):
            client.dataservices.search_dataservices(DataserviceSearchQuery(page_size=oversized))
        with pytest.raises(ValueError, match="1 through 100"):
            client.dataservices.list_dataservice_followers("ds-1", DataserviceFollowersQuery(page_size=oversized))
    assert [r.url for r in router.requests if "/dataservices" in r.url] == []


def test_dataservice_paging_accepts_the_exact_ceiling_boundary() -> None:
    """The bound is inclusive, so a caller asking for exactly the ceiling still dispatches."""
    router = sync_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}{DATASERVICES}/?page=1&page_size=100"): (
                    200,
                    udata_page(_dataservice(), page_size=100),
                )
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        page = client.dataservices.list_dataservices(DataserviceListQuery(page_size=100))
        assert cast("Mapping[str, object]", thawed(page.payload))["page_size"] == 100
    assert [r.url for r in router.requests if "/dataservices" in r.url] == [
        f"{ORIGIN}{DATASERVICES}/?page=1&page_size=100"
    ]


@pytest.mark.parametrize(
    "url_field",
    [
        "base_api_url",
        "machine_documentation_url",
        "technical_documentation_url",
        "business_documentation_url",
        "rate_limiting_url",
        "availability_url",
    ],
)
def test_dataservice_url_fields_reject_a_non_http_url(url_field: str) -> None:
    """Upstream types every one of these as a URLField, so a non-http value is refused before dispatch."""
    with pytest.raises(ValueError, match="absolute http"):
        DataserviceCreateInput(**cast("Any", {**_BASE, url_field: "ftp://x/y"}))
    with pytest.raises(ValueError, match="absolute http"):
        DataserviceUpdateInput(**cast("Any", {url_field: "ftp://x/y"}))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("availability", 101),
        ("availability", -1),
        ("availability", "99"),
        ("format", "SOAP"),
        ("tags", ("ok", "")),
        ("private", "yes"),
        ("license", " "),
    ],
)
def test_dataservice_field_validation_rejects_documented_type_violations(field: str, value: object) -> None:
    """Each documented constraint is enforced on both the create and the patch input."""
    with pytest.raises(ValueError):
        DataserviceCreateInput(**cast("Any", {**_BASE, field: value}))
    with pytest.raises(ValueError):
        DataserviceUpdateInput(**cast("Any", {field: value}))


def test_dataservice_nested_inputs_are_deep_frozen_and_json_safe() -> None:
    """Nested organization, access-type, contact-point, and extras values freeze and reject non-JSON data."""
    client_input = DataserviceCreateInput(
        title="An API",
        base_api_url="https://example.com",
        organization={"id": "org-1"},
        contact_points=({"email": "a@example.com"},),
        extras={"nested": {"k": "v"}},
    )
    with pytest.raises(TypeError):
        cast("dict[str, object]", client_input.extras)["nested"] = "changed"  # type: ignore[assignment]
    assert client_input.payload()["contact_points"] == [{"email": "a@example.com"}]
    with pytest.raises(ValueError, match="JSON-safe"):
        DataserviceCreateInput(**cast("Any", {**_BASE, "extras": {"bad": {1, 2}}}))


def test_every_dataservice_read_dispatches_under_its_own_operation_identity() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}{DATASERVICES}/?page=1&page_size=20"): (200, udata_page(_dataservice())),
                ("GET", f"{ORIGIN}{DATASERVICES}/ds-1/"): (200, _dataservice()),
                ("GET", f"{ORIGIN}{DATASERVICES}/recent.atom?page=1&page_size=20"): (200, b"<feed/>"),
                ("GET", f"{ORIGIN}{DATASERVICES}/ds-1/followers/?page=1&page_size=20"): (
                    200,
                    udata_page({"id": "f1", "follower": {"id": "u1"}}),
                ),
                ("GET", f"{ORIGIN}{DATASERVICES_V2}/search/?page=1&page_size=50"): (200, udata_page(_dataservice())),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        assert cast("Mapping[str, object]", thawed(client.dataservices.list_dataservices().payload))["total"] == 1
        assert thawed(client.dataservices.get_dataservice("ds-1").payload) == _dataservice()
        assert client.dataservices.recent_dataservices_atom_feed().payload["media_type"] == "application/atom+xml"
        assert (
            cast("Mapping[str, object]", thawed(client.dataservices.list_dataservice_followers("ds-1").payload))[
                "total"
            ]
            == 1
        )
        assert cast("Mapping[str, object]", thawed(client.dataservices.search_dataservices().payload))["total"] == 1
    read_requests = [r for r in router.requests if "dataservices" in r.url]
    assert [(r.method, r.url) for r in read_requests] == [
        ("GET", f"{ORIGIN}{DATASERVICES}/?page=1&page_size=20"),
        ("GET", f"{ORIGIN}{DATASERVICES}/ds-1/"),
        ("GET", f"{ORIGIN}{DATASERVICES}/recent.atom?page=1&page_size=20"),
        ("GET", f"{ORIGIN}{DATASERVICES}/ds-1/followers/?page=1&page_size=20"),
        ("GET", f"{ORIGIN}{DATASERVICES_V2}/search/?page=1&page_size=50"),
    ]


def test_dataservice_public_reads_succeed_without_any_credential() -> None:
    router = sync_route_table(
        with_site_route({("GET", f"{ORIGIN}{DATASERVICES}/?page=1&page_size=20"): (200, udata_page(_dataservice()))})
    )
    with anonymous_sync_client(router) as client:
        assert cast("Mapping[str, object]", thawed(client.dataservices.list_dataservices().payload))["total"] == 1
    read_request = next(r for r in router.requests if "/dataservices/" in r.url)
    assert "X-API-KEY" not in dict(read_request.headers)


def test_dataservice_create_update_and_delete_match_exact_wire_and_receipts() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}{DATASERVICES}/"): (201, _dataservice()),
                ("PATCH", f"{ORIGIN}{DATASERVICES}/ds-1/"): (200, _dataservice(title="New")),
                ("DELETE", f"{ORIGIN}{DATASERVICES}/ds-1/"): (204, None),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        created = client.dataservices.create_dataservice(
            DataserviceCreateInput(title="An API", base_api_url="https://example.com/api"),
            EDIT_ONLY_PERMISSIONS,
            _policy(wire.CREATE_DATASERVICE_OPERATION, "An API"),
        )
        assert created.receipt.target.value == "dataservice-1"
        assert created.receipt.outcome == "succeeded"
        updated = client.dataservices.update_dataservice(
            "ds-1",
            DataserviceUpdateInput(title="New"),
            EDIT_ONLY_PERMISSIONS,
            _policy(wire.UPDATE_DATASERVICE_OPERATION, "ds-1"),
        )
        assert updated.receipt.target.value == "ds-1"
        deleted = client.dataservices.delete_dataservice(
            "ds-1",
            EDIT_ONLY_PERMISSIONS,
            _policy(wire.DELETE_DATASERVICE_OPERATION, "ds-1", destructive=True),
        )
        assert deleted.receipt.target.value == "ds-1"
    writes = [r for r in router.requests if "/dataservices" in r.url]
    assert writes[0].method == "POST"
    assert writes[0].url == f"{ORIGIN}{DATASERVICES}/"
    assert json.loads(writes[0].body or b"{}") == {
        "title": "An API",
        "base_api_url": "https://example.com/api",
    }
    assert writes[1].method == "PATCH"
    assert writes[1].url == f"{ORIGIN}{DATASERVICES}/ds-1/"
    assert json.loads(writes[1].body or b"{}") == {"title": "New"}
    assert writes[2].method == "DELETE"
    assert writes[2].url == f"{ORIGIN}{DATASERVICES}/ds-1/"


def test_rows93_94_feature_transitions_use_exact_methods_and_require_admin() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}{DATASERVICES}/ds-1/featured/"): (200, _dataservice(featured=True)),
                ("DELETE", f"{ORIGIN}{DATASERVICES}/ds-1/featured/"): (200, _dataservice()),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.dataservices.feature_dataservice(
            "ds-1", ADMIN_PERMISSIONS, _policy(wire.FEATURE_DATASERVICE_OPERATION, "ds-1")
        )
        client.dataservices.unfeature_dataservice(
            "ds-1", ADMIN_PERMISSIONS, _policy(wire.UNFEATURE_DATASERVICE_OPERATION, "ds-1")
        )
    writes = [r for r in router.requests if "/featured/" in r.url]
    assert [(r.method, r.url) for r in writes] == [
        ("POST", f"{ORIGIN}{DATASERVICES}/ds-1/featured/"),
        ("DELETE", f"{ORIGIN}{DATASERVICES}/ds-1/featured/"),
    ]


def test_rows95_96_dataset_relationship_mutations_match_exact_wire() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}{DATASERVICES}/ds-1/datasets/"): (201, _dataservice()),
                ("DELETE", f"{ORIGIN}{DATASERVICES}/ds-1/datasets/dataset-1/"): (204, None),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.dataservices.dataservice_datasets_add(
            "ds-1",
            DataserviceDatasetLinkInput(("dataset-1",)),
            EDIT_ONLY_PERMISSIONS,
            _policy(wire.DATASERVICE_DATASETS_ADD_OPERATION, "ds-1"),
        )
        client.dataservices.dataservice_dataset_remove(
            "ds-1",
            "dataset-1",
            EDIT_ONLY_PERMISSIONS,
            _policy(wire.DATASERVICE_DATASET_REMOVE_OPERATION, "ds-1:dataset-1", destructive=True),
        )
    writes = [r for r in router.requests if "/datasets/" in r.url]
    assert writes[0].method == "POST"
    assert writes[0].url == f"{ORIGIN}{DATASERVICES}/ds-1/datasets/"
    assert json.loads(writes[0].body or b"[]") == [{"id": "dataset-1"}]
    assert writes[1].method == "DELETE"
    assert writes[1].url == f"{ORIGIN}{DATASERVICES}/ds-1/datasets/dataset-1/"


def test_rows254_255_follower_mutations_match_exact_wire() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}{DATASERVICES}/ds-1/followers/"): (201, {"followers": 2}),
                ("DELETE", f"{ORIGIN}{DATASERVICES}/ds-1/followers/"): (200, {"followers": 1}),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.dataservices.follow_dataservice(
            "ds-1", EDIT_ONLY_PERMISSIONS, _policy(wire.FOLLOW_DATASERVICE_OPERATION, "ds-1")
        )
        client.dataservices.unfollow_dataservice(
            "ds-1",
            EDIT_ONLY_PERMISSIONS,
            _policy(wire.UNFOLLOW_DATASERVICE_OPERATION, "ds-1", destructive=True),
        )
    writes = [r for r in router.requests if "/followers/" in r.url]
    assert [(r.method, r.url) for r in writes] == [
        ("POST", f"{ORIGIN}{DATASERVICES}/ds-1/followers/"),
        ("DELETE", f"{ORIGIN}{DATASERVICES}/ds-1/followers/"),
    ]


class _RdfRouter(SyncRouteRouter):
    """Answer the site probe and one RDF route with an explicit status, body, and headers."""

    def __init__(self, path: str, status: int, *, body: bytes, headers: dict[str, str]) -> None:
        super().__init__(with_site_route({}))
        self.requests = []
        self._route = ("GET", path)
        self._status = status
        self._body = body
        self._headers = headers

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if (request.method, urlsplit(request.url).path) == self._route:
            return RuntimeResponse(self._status, self._headers, self._body)
        return RuntimeResponse(200, {"Content-Type": "application/json"}, b'{"id": "site", "version": "17.6.0"}')


def test_rows97_98_rdf_routes_match_exact_paths_and_bounded_documents() -> None:
    """Row 97 answers a same-origin redirect; row 98 returns a bounded document."""
    redirect_router = _RdfRouter(
        f"{DATASERVICES}/ds-1/rdf", 302, body=b"", headers={"Location": f"{ORIGIN}{DATASERVICES}/ds-1/rdf.ttl"}
    )
    with sync_client(redirect_router, UDATA_CREDENTIAL) as client:
        receipt = cast("MutationReceipt", client.dataservices.rdf_dataservice("ds-1"))
        assert str(receipt.operation) == wire.RDF_DATASERVICE_OPERATION
        assert receipt.target.value == "ds-1"
        assert receipt.outcome == "skipped"
    assert [r.url for r in redirect_router.requests if "/rdf" in r.url] == [f"{ORIGIN}{DATASERVICES}/ds-1/rdf"]

    document_router = _RdfRouter(
        f"{DATASERVICES}/ds-1/rdf.ttl", 200, body=b"<rdf/>", headers={"Content-Type": "text/turtle"}
    )
    with sync_client(document_router, UDATA_CREDENTIAL) as client:
        document = client.dataservices.rdf_dataservice_format("ds-1", "ttl")
        assert document.payload["media_type"] == "text/turtle"
        assert document.payload["size_bytes"] == len(b"<rdf/>")
        assert "body" not in document.payload
    assert [r.url for r in document_router.requests if "/rdf" in r.url] == [f"{ORIGIN}{DATASERVICES}/ds-1/rdf.ttl"]

    assert [r.url for r in document_router.requests if "/rdf" in r.url] == [f"{ORIGIN}{DATASERVICES}/ds-1/rdf.ttl"]


def test_dataservice_mutation_failure_yields_redacted_receipt_with_exact_target() -> None:
    router = atom_sync_route_table(
        with_site_route({("DELETE", f"{ORIGIN}{DATASERVICES}/ds-1/"): (404, {"message": "Not found"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(CatalogNotFoundError) as error:
            client.dataservices.delete_dataservice(
                "ds-1",
                EDIT_ONLY_PERMISSIONS,
                _policy(wire.DELETE_DATASERVICE_OPERATION, "ds-1", destructive=True),
            )
        receipt = error.value.__dict__["mutation_receipt"]
        assert receipt.target.value == "ds-1"
        assert receipt.outcome == "failed"


def test_dataservice_destructive_delete_fails_closed_without_a_confirmed_policy() -> None:
    router = atom_sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError):
        client.dataservices.delete_dataservice("ds-1", EDIT_ONLY_PERMISSIONS, None)
    assert [r for r in router.requests if "/dataservices" in r.url] == []


def test_dataservice_responses_fail_typed_when_the_envelope_is_not_the_documented_shape() -> None:
    with pytest.raises(CatalogValidationError, match="must be an object"):
        wire.parse_mapping(["not-an-object"], wire.GET_DATASERVICE_OPERATION)
    with pytest.raises(CatalogValidationError, match="must be a list of objects"):
        wire.parse_mapping_sequence({"not": "a list"}, wire.LIST_DATASERVICES_OPERATION)


def test_dataservice_missing_target_maps_to_a_typed_not_found_error() -> None:
    router = sync_route_table(
        with_site_route({("GET", f"{ORIGIN}{DATASERVICES}/missing/"): (404, {"message": "Not found"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(CatalogNotFoundError):
        client.dataservices.get_dataservice("missing")


def test_dataservice_async_mode_matches_sync_exact_wire() -> None:
    router = async_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}{DATASERVICES}/?page=1&page_size=20"): (200, udata_page(_dataservice())),
                ("POST", f"{ORIGIN}{DATASERVICES}/ds-1/featured/"): (200, _dataservice(featured=True)),
            }
        )
    )

    async def run() -> None:
        async with async_client(router, UDATA_CREDENTIAL) as client:
            page = await client.dataservices.list_dataservices()
            assert cast("Mapping[str, object]", thawed(page.payload))["total"] == 1
            featured = await client.dataservices.feature_dataservice(
                "ds-1", ADMIN_PERMISSIONS, _policy(wire.FEATURE_DATASERVICE_OPERATION, "ds-1")
            )
            assert featured.receipt.target.value == "ds-1"

    asyncio.run(run())
    requests = [r for r in router.requests if "dataservices" in r.url]
    assert [(r.method, r.url) for r in requests] == [
        ("GET", f"{ORIGIN}{DATASERVICES}/?page=1&page_size=20"),
        ("POST", f"{ORIGIN}{DATASERVICES}/ds-1/featured/"),
    ]


_LIST_URL = f"{ORIGIN}{DATASERVICES}/?page=1&page_size=20"
_LIST_BODY = udata_page(_dataservice())
_DELETE_URL = f"{ORIGIN}{DATASERVICES}/ds-1/"


class _InterruptingRoutes:
    """Answer the site probe, one read route, and one mutation route, raising the interruption while armed."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, *, mutation: tuple[str, str]) -> None:
        self._route = route
        self._mutation = mutation
        self._interruption = interruption
        self.armed = True
        self.requests: list[RuntimeRequest] = []

    def _respond(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if self.armed and (request.method, request.url) in {self._route, self._mutation}:
            raise self._interruption
        if request.url.endswith("/api/1/site/"):
            payload: object = UDATA_SITE
        elif (request.method, request.url) == self._route:
            payload = _LIST_BODY
        elif request.method == "GET":
            payload = _dataservice()
        else:
            payload = None
        body = b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(200, {"Content-Type": "application/json"}, body)


class _CancellingSyncTransport(_InterruptingRoutes):
    """Sync transport that counts `close` so close-once ownership is observable."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, *, mutation: tuple[str, str]) -> None:
        super().__init__(route, interruption, mutation=mutation)
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._respond(request)

    def close(self) -> None:
        self.close_count += 1


class _CancellingAsyncTransport(_InterruptingRoutes):
    """Async twin of the interrupting transport that counts `aclose` instead of `close`."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, *, mutation: tuple[str, str]) -> None:
        super().__init__(route, interruption, mutation=mutation)
        self.aclose_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._respond(request)

    async def aclose(self) -> None:
        self.aclose_count += 1


def _dataservice_sync_client(
    interruption: BaseException, *, breaker_failure_threshold: int = 2, emitter: EventEmitter | None = None
) -> tuple[SyncUDataClient, _CancellingSyncTransport]:
    transport = _CancellingSyncTransport(("GET", _LIST_URL), interruption, mutation=("DELETE", _DELETE_URL))
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        emitter=emitter,
        breaker_failure_threshold=breaker_failure_threshold,
        retry_sleep=lambda _: None,
        owns_transport=True,
    )
    return client, transport


def _dataservice_async_client(
    interruption: BaseException, *, breaker_failure_threshold: int = 2, emitter: EventEmitter | None = None
) -> tuple[AsyncUDataClient, _CancellingAsyncTransport]:
    transport = _CancellingAsyncTransport(("GET", _LIST_URL), interruption, mutation=("DELETE", _DELETE_URL))
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        emitter=emitter,
        breaker_failure_threshold=breaker_failure_threshold,
        owns_transport=True,
    )
    return client, transport


def test_dataservice_sync_cancellation_leaves_post_state_ready_and_closes_the_owned_transport_once() -> None:
    """An interrupted read propagates, leaves the client usable, and still honours close-once ownership."""
    client, transport = _dataservice_sync_client(KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        client.dataservices.list_dataservices()
    assert transport.close_count == 0

    transport.armed = False
    assert cast("Mapping[str, object]", thawed(client.dataservices.list_dataservices().payload))["total"] == 1

    client.close()
    client.close()
    assert transport.close_count == 1


def test_dataservice_sync_cancellation_never_closes_a_borrowed_transport() -> None:
    """A borrowed transport survives the same interrupted read and the context exit untouched."""
    transport = _CancellingSyncTransport(("GET", _LIST_URL), KeyboardInterrupt(), mutation=("DELETE", _DELETE_URL))
    with (
        SyncUDataClient(
            transport,
            declared_udata_profile(),
            origin=ORIGIN,
            credentials=UDATA_CREDENTIAL,
            owns_transport=False,
        ) as client,
        pytest.raises(KeyboardInterrupt),
    ):
        client.dataservices.list_dataservices()
    assert transport.close_count == 0


def test_dataservice_sync_cancellation_is_never_recorded_as_a_circuit_breaker_failure() -> None:
    """Cancellations beyond the failure threshold still admit the next read, so a cancel is not an origin failure."""
    events = ListSink()
    client, transport = _dataservice_sync_client(KeyboardInterrupt(), emitter=EventEmitter(sinks=(events,)))

    for _ in range(4):
        with pytest.raises(KeyboardInterrupt):
            client.dataservices.list_dataservices()

    transport.armed = False
    assert cast("Mapping[str, object]", thawed(client.dataservices.list_dataservices().payload))["total"] == 1
    assert [event.outcome for event in events.events if "breaker" in event.outcome] == []
    client.close()


def test_dataservice_transport_failures_still_open_the_circuit_before_the_next_read() -> None:
    """The breaker is live for this family: genuine transport failures still fail the following read closed."""
    client, transport = _dataservice_sync_client(TransportError("no route"))

    with pytest.raises(TransportError):
        client.dataservices.list_dataservices()

    transport.armed = False
    before = len(transport.requests)
    with pytest.raises(CatalogUnavailableError, match="circuit is open"):
        client.dataservices.list_dataservices()
    assert len(transport.requests) == before
    client.close()


def test_dataservice_sync_cancelled_mutation_records_a_cancelled_receipt_with_the_exact_target() -> None:
    """A cancelled delete stays visible as `cancelled` against its exact target rather than collapsing to `failed`."""
    client, _ = _dataservice_sync_client(KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt) as stopped:
        client.dataservices.delete_dataservice(
            "ds-1",
            EDIT_ONLY_PERMISSIONS,
            _policy(wire.DELETE_DATASERVICE_OPERATION, "ds-1", destructive=True),
        )

    receipt = stopped.value.__dict__["mutation_receipt"]
    assert receipt.outcome == "cancelled"
    assert receipt.operation == wire.DELETE_DATASERVICE_OPERATION
    assert receipt.target.value == "ds-1"
    assert b"secret-key" not in json.dumps(receipt.to_dict()).encode()
    client.close()


def test_dataservice_async_cancellation_leaves_post_state_ready_and_closes_the_owned_transport_once() -> None:
    """The async mode matches sync: the cancellation propagates and ownership is still close-once."""
    client, transport = _dataservice_async_client(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError):
            await client.dataservices.list_dataservices()
        assert transport.aclose_count == 0

        transport.armed = False
        page = await client.dataservices.list_dataservices()
        assert cast("Mapping[str, object]", thawed(page.payload))["total"] == 1

        await client.aclose()
        await client.aclose()
        assert transport.aclose_count == 1

    asyncio.run(run())


def test_dataservice_async_cancellation_never_closes_a_borrowed_transport() -> None:
    """An interrupted async read against a borrowed transport leaves the caller's transport open."""
    transport = _CancellingAsyncTransport(
        ("GET", _LIST_URL), asyncio.CancelledError(), mutation=("DELETE", _DELETE_URL)
    )
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        owns_transport=False,
    )

    async def run() -> None:
        async with client:
            with pytest.raises(asyncio.CancelledError):
                await client.dataservices.list_dataservices()
        assert transport.aclose_count == 0

    asyncio.run(run())


def test_dataservice_async_cancellation_is_never_recorded_as_a_circuit_breaker_failure() -> None:
    """Async cancellations also leave the circuit closed, so the next read is admitted rather than refused."""
    events = ListSink()
    client, transport = _dataservice_async_client(asyncio.CancelledError(), emitter=EventEmitter(sinks=(events,)))

    async def run() -> None:
        for _ in range(4):
            with pytest.raises(asyncio.CancelledError):
                await client.dataservices.list_dataservices()
        transport.armed = False
        page = await client.dataservices.list_dataservices()
        assert cast("Mapping[str, object]", thawed(page.payload))["total"] == 1
        await client.aclose()

    asyncio.run(run())
    assert [event.outcome for event in events.events if "breaker" in event.outcome] == []


def test_dataservice_async_cancelled_mutation_records_a_cancelled_receipt_with_the_exact_target() -> None:
    """The async mutation receipt mirrors sync: `cancelled` against the exact target."""
    client, _ = _dataservice_async_client(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError) as cancelled:
            await client.dataservices.delete_dataservice(
                "ds-1",
                EDIT_ONLY_PERMISSIONS,
                _policy(wire.DELETE_DATASERVICE_OPERATION, "ds-1", destructive=True),
            )
        receipt = cancelled.value.__dict__["mutation_receipt"]
        assert receipt.outcome == "cancelled"
        assert receipt.target.value == "ds-1"
        await client.aclose()

    asyncio.run(run())
