"""Exact wire and safety coverage for the uData topic and topic-element family."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from functools import partial
from typing import TYPE_CHECKING, Any, cast

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.topics import (
    TopicCreateInput,
    TopicElementInput,
    TopicElementLink,
    TopicElementsCreateInput,
    TopicElementsQuery,
    TopicListQuery,
    TopicMutationResult,
    TopicSearchQuery,
    TopicUpdateInput,
    segment,
)
from datasluice.connectors.catalog.udata.services.topics import AsyncTopicsService, SyncTopicsService
from datasluice.connectors.catalog.udata.wire import topics as wire
from datasluice.contracts.catalog.native.udata import AsyncUDataTopicsService, SyncUDataTopicsService
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogError,
    CatalogNotFoundError,
    CatalogRateLimitError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
)
from datasluice.runtime.events import EventEmitter, ListSink
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportError
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    UDATA_SITE,
    AsyncRouteRouter,
    RouteTable,
    SyncRouteRouter,
    async_client,
    async_route_table,
    mutation_policy,
    sync_client,
    sync_route_table,
    with_site_route,
)

if TYPE_CHECKING:
    from datasluice.domain.catalog.receipts import MutationReceipt

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_PERMISSIONS
ADMIN_PERMISSIONS = UDATA_ADMIN_PERMISSIONS

_TOPIC_REQUIREMENTS = {
    wire.CREATE_TOPIC_OPERATION: frozenset({"topic-create"}),
    wire.UPDATE_TOPIC_OPERATION: frozenset({"topic-update"}),
    wire.DELETE_TOPIC_OPERATION: frozenset({"topic-delete"}),
    wire.TOPIC_ELEMENTS_CREATE_OPERATION: frozenset({"topic-elements-create"}),
    wire.TOPIC_ELEMENTS_DELETE_OPERATION: frozenset({"topic-elements-delete"}),
    wire.TOPIC_ELEMENT_UPDATE_OPERATION: frozenset({"topic-element-update"}),
    wire.TOPIC_ELEMENT_DELETE_OPERATION: frozenset({"topic-element-delete"}),
    wire.FEATURE_TOPIC_OPERATION: frozenset({"topic-feature"}),
    wire.UNFEATURE_TOPIC_OPERATION: frozenset({"topic-unfeature"}),
}


def _topic_permissions(*granted: str) -> EffectivePermissions:
    """Bind every topic requirement to one granted scope set, so the gate can discriminate."""
    return EffectivePermissions.for_credential(
        UDATA_CREDENTIAL,
        platform=CatalogPlatform.UDATA,
        scopes=frozenset(granted),
        operation_scopes=_TOPIC_REQUIREMENTS,
    )


CREATE_ONLY_PERMISSIONS = _topic_permissions("topic-create")
DELETE_ONLY_PERMISSIONS = _topic_permissions("topic-delete")
FEATURE_ONLY_PERMISSIONS = _topic_permissions("topic-feature")

_TOPIC_PAGE = {
    "data": [{"id": "topic-1", "name": "Health", "featured": False}],
    "page": 1,
    "page_size": 20,
    "total": 1,
    "next_page": None,
    "previous_page": None,
}
_SEARCH_PAGE = {**_TOPIC_PAGE, "query": "health", "filters": {"tag": ["health"]}}
_TOPIC_OBJECT = {"id": "topic-1", "name": "Health", "featured": False, "private": False}
_ELEMENT_PAGE = {
    "data": [{"id": "element-1", "title": "A dataset", "tags": [], "element": {"class": "Dataset", "id": "dataset-1"}}],
    "page": 1,
    "page_size": 20,
    "total": 1,
}


def _plain(value: object) -> object:
    """Return a decoded record as plain JSON data, so frozen tuples and proxies compare equal."""
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    return value


def _topic_requests(router: SyncRouteRouter | AsyncRouteRouter) -> list[tuple[str, str]]:
    """Return every dispatched topic route, excluding the shared site probe."""
    return [(request.method, request.url) for request in router.requests if not request.url.endswith("/api/1/site/")]


@pytest.mark.parametrize("identifier", [".", ".."])
def test_topics_identifiers_reject_dot_segments(identifier: str) -> None:
    """A bare dot segment is removed by RFC 3986 resolution, retargeting the route."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(identifier, "list_topics")


def test_topics_contract_exposes_every_assigned_method_in_both_modes() -> None:
    sync_names = {
        name
        for name in dir(SyncTopicsService)
        if not name.startswith("_") and callable(getattr(SyncTopicsService, name))
    }
    async_names = {
        name
        for name in dir(AsyncTopicsService)
        if not name.startswith("_") and callable(getattr(AsyncTopicsService, name))
    }
    assert sync_names == async_names
    assert sync_names == {
        "search_topics",
        "list_topics",
        "create_topic",
        "get_topic",
        "update_topic",
        "delete_topic",
        "topic_elements",
        "topic_elements_create",
        "topic_elements_delete",
        "topic_element_update",
        "topic_element_delete",
        "feature_topic",
        "unfeature_topic",
    }
    with sync_client(sync_route_table(with_site_route({})), None) as client:
        assert isinstance(client.topics, SyncUDataTopicsService)

    async def run() -> None:
        async with async_client(async_route_table(with_site_route({})), None) as client:
            assert isinstance(client.topics, AsyncUDataTopicsService)

    asyncio.run(run())


def test_every_topic_route_has_an_exact_wire_shape() -> None:
    """Each builder emits the verb and path the pinned oracle records for that signature."""
    assert [
        wire.list_topics_request(TopicListQuery())[0:2],
        wire.search_topics_request(TopicSearchQuery())[0:2],
        wire.create_topic_request(TopicCreateInput(name="Health"))[0:2],
        wire.get_topic_request("topic-1")[0:2],
        wire.update_topic_request("topic-1", TopicUpdateInput(name="Health"))[0:2],
        wire.delete_topic_request("topic-1")[0:2],
        wire.topic_elements_request("topic-1", TopicElementsQuery())[0:2],
        wire.topic_elements_create_request("topic-1", TopicElementsCreateInput((TopicElementLink("Dataset", "d-1"),)))[
            0:2
        ],
        wire.topic_elements_delete_request("topic-1")[0:2],
        wire.topic_element_update_request("topic-1", "element-1", TopicElementInput(title="A dataset"))[0:2],
        wire.topic_element_delete_request("topic-1", "element-1")[0:2],
        wire.feature_topic_request("topic-1", featured=True)[0:2],
        wire.feature_topic_request("topic-1", featured=False)[0:2],
    ] == [
        ("GET", "/api/2/topics/?page=1&page_size=20"),
        ("GET", "/api/2/topics/search/?page=1&page_size=20"),
        ("POST", "/api/2/topics/"),
        ("GET", "/api/2/topics/topic-1/"),
        ("PUT", "/api/2/topics/topic-1/"),
        ("DELETE", "/api/2/topics/topic-1/"),
        ("GET", "/api/2/topics/topic-1/elements/?page=1&page_size=20"),
        ("POST", "/api/2/topics/topic-1/elements/"),
        ("DELETE", "/api/2/topics/topic-1/elements/"),
        ("PUT", "/api/2/topics/topic-1/elements/element-1/"),
        ("DELETE", "/api/2/topics/topic-1/elements/element-1/"),
        ("POST", "/api/2/topics/topic-1/featured/"),
        ("DELETE", "/api/2/topics/topic-1/featured/"),
    ]


def test_topic_query_encodings_match_the_pinned_parsers() -> None:
    """Every documented parser argument keeps its stock name, order, and omission rule."""
    assert wire.list_topics_request(TopicListQuery())[1] == "/api/2/topics/?page=1&page_size=20"
    assert wire.list_topics_request(
        TopicListQuery(
            q="health",
            page=3,
            page_size=50,
            sort="-created",
            filters={
                "tag": ("health", "hygiene"),
                "private": True,
                "featured": False,
                "geozone": "country:fr",
                "granularity": "poi",
                "organization": "org-1",
                "owner": "user-1",
                "dataset": "dataset-1",
                "dataservice": "dataservice-1",
                "reuse": "reuse-1",
            },
        )
    )[1] == (
        "/api/2/topics/?page=3&page_size=50&q=health&sort=-created"
        "&dataservice=dataservice-1&dataset=dataset-1&featured=false"
        "&geozone=country%3Afr&granularity=poi&organization=org-1"
        "&owner=user-1&private=true&reuse=reuse-1&tag=health&tag=hygiene"
    )
    assert wire.search_topics_request(
        TopicSearchQuery(
            q="climat",
            page=2,
            page_size=10,
            sort="name",
            filters={
                "tag": ("energy",),
                "featured": True,
                "last_update_range": "last_30_days",
                "organization": "org-1",
                "producer_type": "public-service",
            },
        )
    )[1] == (
        "/api/2/topics/search/?page=2&page_size=10&q=climat&sort=name"
        "&featured=true&last_update_range=last_30_days&organization=org-1"
        "&producer_type=public-service&tag=energy"
    )
    assert (
        wire.topic_elements_request(
            "topic-1",
            TopicElementsQuery(page=2, page_size=5, element_class="Dataset", q="traffic", tag=("mobility",)),
        )[1]
        == "/api/2/topics/topic-1/elements/?page=2&page_size=5&class=Dataset&q=traffic&tag=mobility"
    )


def test_topic_mutation_bodies_carry_only_the_documented_fields() -> None:
    """Absent fields stay absent so a replace never silently clears a stored value."""
    assert wire.create_topic_request(TopicCreateInput(name="Health"))[3] == {"name": "Health"}
    assert wire.create_topic_request(
        TopicCreateInput(
            name="Health",
            description="Public health",
            tags=("health",),
            private=False,
            color=3,
            extras={"k": "v"},
            spatial={"granularity": "poi"},
            elements=(TopicElementInput(title="A dataset"),),
        )
    )[3] == {
        "name": "Health",
        "description": "Public health",
        "tags": ["health"],
        "private": False,
        "color": 3,
        "extras": {"k": "v"},
        "spatial": {"granularity": "poi"},
        "elements": [{"title": "A dataset"}],
    }
    assert wire.update_topic_request("topic-1", TopicUpdateInput(description="Updated"))[3] == {
        "description": "Updated"
    }
    assert wire.update_topic_request("topic-1", TopicUpdateInput(name="Renamed", tags=("a",)))[3] == {
        "name": "Renamed",
        "tags": ["a"],
    }
    assert wire.topic_element_update_request(
        "topic-1",
        "element-1",
        TopicElementInput(title="A dataset", tags=("traffic",), element={"class": "Reuse", "id": "reuse-1"}),
    )[3] == {
        "title": "A dataset",
        "tags": ["traffic"],
        "element": {"class": "Reuse", "id": "reuse-1"},
    }
    assert wire.topic_elements_create_request(
        "topic-1",
        TopicElementsCreateInput(
            (TopicElementLink("Dataset", "dataset-1"), TopicElementLink("Reuse", "reuse-1")),
        ),
    )[3] == [
        {"class": "Dataset", "id": "dataset-1"},
        {"class": "Reuse", "id": "reuse-1"},
    ]


def test_topic_read_wire_contract_is_exact_through_transport() -> None:
    router = sync_route_table(
        with_site_route({("GET", f"{ORIGIN}/api/2/topics/topic-1/elements/?page=1&page_size=20"): (200, _ELEMENT_PAGE)})
    )
    with sync_client(router, None) as client:
        assert _plain(client.topics.topic_elements("topic-1").payload) == _ELEMENT_PAGE

    request = router.requests[-1]
    assert request.method == "GET"
    assert request.url == f"{ORIGIN}/api/2/topics/topic-1/elements/?page=1&page_size=20"
    assert "X-API-KEY" not in dict(request.headers)


def test_topic_reads_decode_losslessly_and_fail_typed_in_both_modes() -> None:
    routes: RouteTable = {
        ("GET", f"{ORIGIN}/api/2/topics/?page=1&page_size=20"): (200, _TOPIC_PAGE),
        ("GET", f"{ORIGIN}/api/2/topics/search/?page=1&page_size=20"): (200, _SEARCH_PAGE),
        ("GET", f"{ORIGIN}/api/2/topics/topic-1/"): (200, _TOPIC_OBJECT),
        ("GET", f"{ORIGIN}/api/2/topics/topic-1/elements/?page=1&page_size=20"): (200, _ELEMENT_PAGE),
        ("GET", f"{ORIGIN}/api/2/topics/?page=1&page_size=20&q=health"): (200, _TOPIC_PAGE),
    }
    with sync_client(sync_route_table(with_site_route(routes)), None) as client:
        service = client.topics
        assert _plain(service.list_topics().payload) == _TOPIC_PAGE
        assert _plain(service.search_topics().payload) == _SEARCH_PAGE
        assert _plain(service.get_topic("topic-1").payload) == _TOPIC_OBJECT
        assert _plain(service.topic_elements("topic-1").payload) == _ELEMENT_PAGE
        assert service.list_topics(TopicListQuery(q="health")).payload["total"] == 1

    async def run() -> None:
        async with async_client(async_route_table(with_site_route(routes)), None) as client:
            assert _plain((await client.topics.list_topics()).payload) == _TOPIC_PAGE
            assert _plain((await client.topics.search_topics()).payload) == _SEARCH_PAGE
            assert _plain((await client.topics.get_topic("topic-1")).payload) == _TOPIC_OBJECT
            assert _plain((await client.topics.topic_elements("topic-1")).payload) == _ELEMENT_PAGE

    asyncio.run(run())


_MALFORMED_PAGE_SIZES = (0, -1, 101, 1000, True, False, 1.0, 20.0, "20", None, [20], b"20")
_MALFORMED_PAGES = (0, -1, True, False, 1.0, "1", None, [1])


def _topic_query_builders(**overrides: object) -> tuple[Callable[[], object], ...]:
    """Return one lazy constructor per stock topic query, varying only the bounded pair."""
    builders: tuple[Callable[..., object], ...] = (TopicListQuery, TopicSearchQuery, TopicElementsQuery)
    return tuple(partial(cast("Callable[..., object]", builder), **overrides) for builder in builders)


@pytest.mark.parametrize("page_size", _MALFORMED_PAGE_SIZES)
def test_malformed_topic_page_sizes_are_rejected_before_dispatch(page_size: object) -> None:
    """An unbounded page size is a denial-of-service lever, so the ceiling is a model invariant."""
    for build in _topic_query_builders(page_size=page_size):
        with pytest.raises(ValueError, match="page_size"):
            build()


@pytest.mark.parametrize("page", _MALFORMED_PAGES)
def test_malformed_topic_pages_are_rejected_before_dispatch(page: object) -> None:
    """`page` and `page_size` are both strictly positive stock integers."""
    for build in _topic_query_builders(page=page):
        with pytest.raises(ValueError, match="page"):
            build()


@pytest.mark.parametrize("page_size", [101, 0, -1, 10_000])
def test_an_oversized_topic_page_never_reaches_a_route(page_size: int) -> None:
    """The ceiling is enforced inside the input, so no request is built or dispatched."""
    router = sync_route_table(with_site_route({}))
    with sync_client(router, None) as client:
        with pytest.raises(ValueError, match="page_size"):
            client.topics.list_topics(TopicListQuery(page_size=page_size))
        with pytest.raises(ValueError, match="page_size"):
            client.topics.search_topics(TopicSearchQuery(page_size=page_size))
        with pytest.raises(ValueError, match="page_size"):
            client.topics.topic_elements("topic-1", TopicElementsQuery(page_size=page_size))
    assert router.requests == []


def test_the_topic_page_size_ceiling_admits_its_documented_bound() -> None:
    """Exactly 100 is the largest admissible stock page, and it is emitted verbatim."""
    assert TopicListQuery(page_size=100).query_params() == [("page", "1"), ("page_size", "100")]
    assert TopicSearchQuery(page_size=100).query_params() == [("page", "1"), ("page_size", "100")]
    assert TopicElementsQuery(page_size=100).query_params() == [("page", "1"), ("page_size", "100")]


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
    ["topic-1"],
)


@pytest.mark.parametrize("identifier", _MALFORMED_SEGMENTS)
def test_malformed_topic_segments_never_reach_a_route(identifier: object) -> None:
    """Dot segments, separators, and control characters must be refused, not quoted."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(cast("str", identifier), wire.GET_TOPIC_OPERATION)


@pytest.mark.parametrize("identifier", _MALFORMED_SEGMENTS)
def test_malformed_topic_identifiers_are_stopped_before_any_request(identifier: object) -> None:
    """Every identifier that becomes a path segment is guarded at the client boundary."""
    value = cast("str", identifier)
    router = sync_route_table(with_site_route({}))
    with sync_client(router, PERMISSIONS) as client:
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.topics.get_topic(value)
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.topics.topic_elements(value)
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.topics.delete_topic(
                value,
                PERMISSIONS,
                mutation_policy(wire.DELETE_TOPIC_OPERATION, "topic-1", destructive=True),
            )
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.topics.feature_topic(
                value, ADMIN_PERMISSIONS, mutation_policy(wire.FEATURE_TOPIC_OPERATION, "topic-1")
            )
    assert router.requests == []


@pytest.mark.parametrize("element_id", _MALFORMED_SEGMENTS)
def test_malformed_topic_element_identifiers_are_stopped_before_any_request(element_id: object) -> None:
    """The element identifier is a second path segment, guarded the same way as the topic."""
    value = cast("str", element_id)
    router = sync_route_table(with_site_route({}))
    with sync_client(router, PERMISSIONS) as client:
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.topics.topic_element_update(
                "topic-1",
                value,
                TopicElementInput(title="A dataset"),
                PERMISSIONS,
                mutation_policy(wire.TOPIC_ELEMENT_UPDATE_OPERATION, "topic-1:element-1"),
            )
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.topics.topic_element_delete(
                "topic-1",
                value,
                PERMISSIONS,
                mutation_policy(wire.TOPIC_ELEMENT_DELETE_OPERATION, "topic-1:element-1", destructive=True),
            )
    assert router.requests == []


_MALFORMED_ELEMENT_REFERENCES: tuple[object, ...] = (
    {},
    {"class": "Dataset"},
    {"id": "dataset-1"},
    {"class": "Topic", "id": "dataset-1"},
    {"class": "", "id": "dataset-1"},
    {"class": 4, "id": "dataset-1"},
    {"class": "Dataset", "id": "../secret"},
    {"class": "Dataset", "id": ""},
    {"class": "Dataset", "id": 4},
    "Dataset",
    None,
    ["Dataset", "dataset-1"],
)


@pytest.mark.parametrize("reference", _MALFORMED_ELEMENT_REFERENCES)
def test_malformed_topic_element_references_never_reach_a_route(reference: object) -> None:
    """A body-borne identifier gets the shared segment policy before it is serialized."""
    with pytest.raises((ValueError, CatalogValidationError)):
        TopicElementInput(element=cast("dict[str, str]", reference))


def test_topic_element_requires_a_title_or_a_linked_element() -> None:
    """The pinned check is enforced as an input invariant rather than by a 400."""
    with pytest.raises(ValueError, match="title or a linked element"):
        TopicElementInput(description="orphan")
    assert TopicElementInput(title="A dataset").payload() == {"title": "A dataset"}
    assert TopicElementInput(element={"class": "Dataservice", "id": "dataservice-1"}).payload() == {
        "element": {"class": "Dataservice", "id": "dataservice-1"}
    }


_MALFORMED_SORTS = ("", "created_at", "nope", "-", "--created", "-nope", 4, True, ["name"])
_MALFORMED_LIST_FILTERS = ("nope", "page", "page_size", "sort", "q", 4, None)
_MALFORMED_SEARCH_FILTERS = ("nope", "geozone", "granularity", "dataset", "owner", "private", 4, None)


@pytest.mark.parametrize("sort", _MALFORMED_SORTS)
def test_malformed_topic_sorts_are_rejected_before_dispatch(sort: object) -> None:
    """The pinned parser offers exactly `sort` and its negated forms for three keys."""
    with pytest.raises(ValueError, match="sort"):
        TopicListQuery(sort=cast("str", sort))
    with pytest.raises(ValueError, match="sort"):
        TopicSearchQuery(sort=cast("str", sort))


@pytest.mark.parametrize("key", _MALFORMED_LIST_FILTERS)
def test_undocumented_topic_list_filters_are_rejected_before_dispatch(key: str) -> None:
    """The list parser documents ten filters; anything else is refused, not forwarded."""
    with pytest.raises(ValueError, match="not documented choices"):
        TopicListQuery(filters={key: "value"})


@pytest.mark.parametrize("key", _MALFORMED_SEARCH_FILTERS)
def test_undocumented_topic_search_filters_are_rejected_before_dispatch(key: str) -> None:
    """Search exposes a narrower filter set than the list parser, so both are enforced."""
    with pytest.raises(ValueError, match="not documented choices"):
        TopicSearchQuery(filters={key: "value"})


@pytest.mark.parametrize(
    ("key", "search"),
    [("private", False), ("featured", True), ("featured", False)],
)
def test_boolean_topic_filters_must_carry_real_booleans(key: str, search: bool) -> None:
    with pytest.raises(ValueError, match="must be a boolean"):
        TopicListQuery(filters={key: "true"})
    if search:
        with pytest.raises(ValueError, match="must be a boolean"):
            TopicSearchQuery(filters={key: "yes"})


def test_topic_enum_filters_reject_undocumented_choices() -> None:
    with pytest.raises(ValueError, match="last_update_range"):
        TopicSearchQuery(filters={"last_update_range": "last_7_days"})
    with pytest.raises(ValueError, match="producer_type"):
        TopicSearchQuery(filters={"producer_type": "certified"})
    with pytest.raises(ValueError, match="element class"):
        TopicElementsQuery(element_class="Topic")


def test_topic_filters_freeze_to_immutable_json_safe_mappings() -> None:
    filters: dict[str, object] = {"tag": ("a", "b")}
    query = TopicListQuery(filters=cast("Mapping[str, str | bool | tuple[str, ...]]", filters))
    filters["tag"] = ("mutated",)
    assert query.query_params() == [
        ("page", "1"),
        ("page_size", "20"),
        ("tag", "a"),
        ("tag", "b"),
    ]
    with pytest.raises(TypeError):
        cast("dict[str, object]", query.filters)["tag"] = ("x",)
    with pytest.raises(ValueError, match="must be a string, boolean, or tuple of non-empty strings"):
        TopicListQuery(filters={"tag": cast("tuple[str, ...]", {1, 2})})


def test_topic_inputs_reject_malformed_scalar_fields() -> None:
    with pytest.raises(ValueError, match="name"):
        TopicCreateInput(name="")
    with pytest.raises(ValueError, match="private"):
        TopicCreateInput(name="Health", private=cast("bool", "yes"))
    with pytest.raises(ValueError, match="color"):
        TopicCreateInput(name="Health", color=cast("int", "3"))
    with pytest.raises(ValueError, match="tags"):
        TopicCreateInput(name="Health", tags=cast("tuple[str, ...]", ["health"]))
    with pytest.raises(ValueError, match="at least one field"):
        TopicUpdateInput()
    with pytest.raises(ValueError, match="elements"):
        TopicElementsCreateInput(())


def test_topic_mutations_return_exact_redacted_receipt_targets() -> None:
    routes: RouteTable = {
        ("POST", f"{ORIGIN}/api/2/topics/"): (201, _TOPIC_OBJECT),
        ("PUT", f"{ORIGIN}/api/2/topics/topic-1/"): (200, _TOPIC_OBJECT),
        ("DELETE", f"{ORIGIN}/api/2/topics/topic-1/"): (204, None),
        ("POST", f"{ORIGIN}/api/2/topics/topic-1/elements/"): (201, [{"id": "element-1", "title": "A dataset"}]),
        ("DELETE", f"{ORIGIN}/api/2/topics/topic-1/elements/"): (204, None),
        ("PUT", f"{ORIGIN}/api/2/topics/topic-1/elements/element-1/"): (200, {"id": "element-1"}),
        ("DELETE", f"{ORIGIN}/api/2/topics/topic-1/elements/element-1/"): (204, None),
        ("POST", f"{ORIGIN}/api/2/topics/topic-1/featured/"): (200, {**_TOPIC_OBJECT, "featured": True}),
        ("DELETE", f"{ORIGIN}/api/2/topics/topic-1/featured/"): (200, _TOPIC_OBJECT),
    }
    router = sync_route_table(with_site_route(routes))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.topics
        created = service.create_topic(
            TopicCreateInput(name="Health"), PERMISSIONS, mutation_policy(wire.CREATE_TOPIC_OPERATION, "Health")
        )
        updated = service.update_topic(
            "topic-1",
            TopicUpdateInput(description="d"),
            PERMISSIONS,
            mutation_policy(wire.UPDATE_TOPIC_OPERATION, "topic-1"),
        )
        deleted = service.delete_topic(
            "topic-1",
            PERMISSIONS,
            mutation_policy(wire.DELETE_TOPIC_OPERATION, "topic-1", destructive=True),
        )
        elements_created = service.topic_elements_create(
            "topic-1",
            TopicElementsCreateInput((TopicElementLink("Dataset", "dataset-1"),)),
            PERMISSIONS,
            mutation_policy(wire.TOPIC_ELEMENTS_CREATE_OPERATION, "topic-1"),
        )
        elements_deleted = service.topic_elements_delete(
            "topic-1",
            PERMISSIONS,
            mutation_policy(wire.TOPIC_ELEMENTS_DELETE_OPERATION, "topic-1", destructive=True),
        )
        element_updated = service.topic_element_update(
            "topic-1",
            "element-1",
            TopicElementInput(title="A dataset"),
            PERMISSIONS,
            mutation_policy(wire.TOPIC_ELEMENT_UPDATE_OPERATION, "topic-1:element-1"),
        )
        element_deleted = service.topic_element_delete(
            "topic-1",
            "element-1",
            PERMISSIONS,
            mutation_policy(wire.TOPIC_ELEMENT_DELETE_OPERATION, "topic-1:element-1", destructive=True),
        )
        featured = service.feature_topic(
            "topic-1", ADMIN_PERMISSIONS, mutation_policy(wire.FEATURE_TOPIC_OPERATION, "topic-1")
        )
        unfeatured = service.unfeature_topic(
            "topic-1",
            ADMIN_PERMISSIONS,
            mutation_policy(wire.UNFEATURE_TOPIC_OPERATION, "topic-1", destructive=True),
        )

    assert isinstance(created, TopicMutationResult)
    assert created.receipt.target.value == "topic-1"
    assert created.receipt.audit_metadata == {"mutation": "created", "status_code": 201, "target_valid": True}
    assert created.record is not None
    assert _plain(created.record.payload) == _TOPIC_OBJECT
    assert updated.receipt.target.value == "topic-1"
    assert updated.receipt.audit_metadata["mutation"] == "updated"
    assert deleted.record is None
    assert deleted.receipt.target.value == "topic-1"
    assert deleted.receipt.audit_metadata["mutation"] == "deleted"
    assert elements_created.receipt.target.value == "topic-1"
    assert elements_created.record is None
    assert elements_deleted.receipt.target.value == "topic-1"
    assert element_updated.receipt.target.value == "topic-1:element-1"
    assert element_updated.record is not None
    assert element_deleted.receipt.target.value == "topic-1:element-1"
    assert featured.receipt.target.value == "topic-1"
    assert unfeatured.receipt.target.value == "topic-1"
    receipts = json.dumps([result.receipt.to_dict() for result in (created, updated, featured, element_updated)])
    assert "secret-key" not in receipts
    assert "health" not in receipts.lower()
    assert "dataset-1" not in receipts


def test_topic_element_collection_create_returns_a_bounded_null_record() -> None:
    """The pinned route answers 201 with a list, so only the receipt survives the typed result."""
    router = sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}/api/2/topics/topic-1/elements/"): (
                    201,
                    [{"id": "element-1", "title": "A dataset"}],
                )
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        result = client.topics.topic_elements_create(
            "topic-1",
            TopicElementsCreateInput((TopicElementLink("Dataset", "dataset-1"),)),
            PERMISSIONS,
            mutation_policy(wire.TOPIC_ELEMENTS_CREATE_OPERATION, "topic-1"),
        )
    assert result.record is None
    assert result.receipt.audit_metadata["status_code"] == 201
    assert "element-1" not in json.dumps(result.to_dict())


def test_topic_permissions_discriminate_each_route_in_both_modes() -> None:
    """A denial is a pre-dispatch refusal: it performs zero network calls in either mode."""
    routes = with_site_route(
        {
            ("POST", f"{ORIGIN}/api/2/topics/"): (201, _TOPIC_OBJECT),
            ("DELETE", f"{ORIGIN}/api/2/topics/topic-1/"): (204, None),
            ("POST", f"{ORIGIN}/api/2/topics/topic-1/featured/"): (200, _TOPIC_OBJECT),
        }
    )
    create_policy = mutation_policy(wire.CREATE_TOPIC_OPERATION, "Health")
    delete_policy = mutation_policy(wire.DELETE_TOPIC_OPERATION, "topic-1", destructive=True)
    feature_policy = mutation_policy(wire.FEATURE_TOPIC_OPERATION, "topic-1")

    def run_sync() -> None:
        router = sync_route_table(routes)
        with sync_client(router, UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied_delete:
                client.topics.delete_topic("topic-1", CREATE_ONLY_PERMISSIONS, delete_policy)
            with pytest.raises(ForbiddenError) as denied_create:
                client.topics.create_topic(TopicCreateInput(name="Health"), DELETE_ONLY_PERMISSIONS, create_policy)
            with pytest.raises(ForbiddenError) as denied_feature:
                client.topics.feature_topic("topic-1", PERMISSIONS, feature_policy)
            assert denied_delete.value.operation == wire.DELETE_TOPIC_OPERATION
            assert denied_create.value.operation == wire.CREATE_TOPIC_OPERATION
            assert denied_feature.value.operation == wire.FEATURE_TOPIC_OPERATION
            assert denied_delete.value.capability_state == "forbidden"
            assert router.requests == []
            allowed = client.topics.create_topic(
                TopicCreateInput(name="Health"), CREATE_ONLY_PERMISSIONS, create_policy
            )
            removed = client.topics.delete_topic("topic-1", DELETE_ONLY_PERMISSIONS, delete_policy)
            marked = client.topics.feature_topic("topic-1", ADMIN_PERMISSIONS, feature_policy)
        assert allowed.receipt.outcome == "succeeded"
        assert removed.receipt.outcome == "succeeded"
        assert marked.receipt.outcome == "succeeded"
        assert _topic_requests(router) == [
            ("POST", f"{ORIGIN}/api/2/topics/"),
            ("DELETE", f"{ORIGIN}/api/2/topics/topic-1/"),
            ("POST", f"{ORIGIN}/api/2/topics/topic-1/featured/"),
        ]

    async def run_async() -> None:
        router = async_route_table(routes)
        async with async_client(router, UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied_delete:
                await client.topics.delete_topic("topic-1", CREATE_ONLY_PERMISSIONS, delete_policy)
            with pytest.raises(ForbiddenError) as denied_create:
                await client.topics.create_topic(
                    TopicCreateInput(name="Health"), DELETE_ONLY_PERMISSIONS, create_policy
                )
            with pytest.raises(ForbiddenError) as denied_feature:
                await client.topics.feature_topic("topic-1", PERMISSIONS, feature_policy)
            assert denied_delete.value.operation == wire.DELETE_TOPIC_OPERATION
            assert denied_create.value.operation == wire.CREATE_TOPIC_OPERATION
            assert denied_feature.value.operation == wire.FEATURE_TOPIC_OPERATION
            assert router.requests == []
            allowed = await client.topics.create_topic(
                TopicCreateInput(name="Health"), CREATE_ONLY_PERMISSIONS, create_policy
            )
            removed = await client.topics.delete_topic("topic-1", DELETE_ONLY_PERMISSIONS, delete_policy)
            marked = await client.topics.feature_topic("topic-1", ADMIN_PERMISSIONS, feature_policy)
        assert allowed.receipt.outcome == "succeeded"
        assert removed.receipt.outcome == "succeeded"
        assert marked.receipt.outcome == "succeeded"
        assert _topic_requests(router) == [
            ("POST", f"{ORIGIN}/api/2/topics/"),
            ("DELETE", f"{ORIGIN}/api/2/topics/topic-1/"),
            ("POST", f"{ORIGIN}/api/2/topics/topic-1/featured/"),
        ]

    run_sync()
    asyncio.run(run_async())


_DESTRUCTIVE_CALLS = (
    ("delete_topic", (wire.DELETE_TOPIC_OPERATION, "topic-1", True)),
    ("topic_elements_delete", (wire.TOPIC_ELEMENTS_DELETE_OPERATION, "topic-1", True)),
    ("topic_element_delete", (wire.TOPIC_ELEMENT_DELETE_OPERATION, "topic-1:element-1", True)),
    ("unfeature_topic", (wire.UNFEATURE_TOPIC_OPERATION, "topic-1", True)),
)


@pytest.mark.parametrize(("method", "policy"), _DESTRUCTIVE_CALLS)
def test_destructive_topic_mutations_require_a_confirmed_destructive_policy(
    method: str, policy: tuple[str, str, bool]
) -> None:
    """Every destructive transition needs an explicit confirmed, destructive, bound policy."""
    operation, target, destructive = policy
    router = sync_route_table(with_site_route({}))
    arguments: list[object] = ["topic-1", PERMISSIONS if method != "unfeature_topic" else ADMIN_PERMISSIONS]
    if method == "topic_element_delete":
        arguments.insert(1, "element-1")
    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(ForbiddenError) as missing:
            cast("Any", getattr(client.topics, method))(*arguments, None)
        with pytest.raises(ForbiddenError) as unbound:
            cast("Any", getattr(client.topics, method))(
                *arguments, mutation_policy(operation, "some-other-target", destructive=destructive)
            )
        with pytest.raises(ForbiddenError) as understated:
            cast("Any", getattr(client.topics, method))(
                *arguments, mutation_policy(operation, target, destructive=False)
            )
    assert missing.value.operation == operation
    assert unbound.value.operation == operation
    assert understated.value.operation == operation
    assert router.requests == []


def test_topic_mutation_dispatch_uses_the_exact_operation_in_both_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def capture(**kwargs: object) -> tuple[int, object, object]:
        calls.append(kwargs)
        return 200, _TOPIC_OBJECT, object()

    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        monkeypatch.setattr(client, "_dataset_call", capture)
        client.topics.feature_topic(
            "topic-1", ADMIN_PERMISSIONS, mutation_policy(wire.FEATURE_TOPIC_OPERATION, "topic-1")
        )
    assert calls[0]["owning_operation"] == wire.FEATURE_TOPIC_OPERATION
    assert calls[0]["path"] == "/api/2/topics/topic-1/featured/"
    assert calls[0]["method"] == "POST"

    async def run_async() -> None:
        async def async_capture(**kwargs: object) -> tuple[int, object, object]:
            assert kwargs["owning_operation"] == wire.UNFEATURE_TOPIC_OPERATION
            return 200, _TOPIC_OBJECT, object()

        async with async_client(async_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            monkeypatch.setattr(client, "_dataset_call_async", async_capture)
            result = await client.topics.unfeature_topic(
                "topic-1",
                ADMIN_PERMISSIONS,
                mutation_policy(wire.UNFEATURE_TOPIC_OPERATION, "topic-1", destructive=True),
            )
        assert result.receipt.operation == wire.UNFEATURE_TOPIC_OPERATION

    asyncio.run(run_async())


_MALFORMED_OBJECT_PAYLOADS: tuple[object, ...] = ([{"id": "topic-1"}], "topic-1", None, 4, [{"nested": []}, 4])
_TOPIC_READS = (
    ("list_topics", (), "/api/2/topics/?page=1&page_size=20"),
    ("search_topics", (), "/api/2/topics/search/?page=1&page_size=20"),
    ("get_topic", ("topic-1",), "/api/2/topics/topic-1/"),
    ("topic_elements", ("topic-1",), "/api/2/topics/topic-1/elements/?page=1&page_size=20"),
)


@pytest.mark.parametrize("payload", _MALFORMED_OBJECT_PAYLOADS)
def test_malformed_topic_read_bodies_fail_typed_without_a_raw_body(payload: object) -> None:
    routes: RouteTable = {("GET", f"{ORIGIN}{path}"): (200, payload) for _, _, path in _TOPIC_READS}
    with sync_client(sync_route_table(with_site_route(routes)), None) as client:
        for method, arguments, _ in _TOPIC_READS:
            with pytest.raises(CatalogValidationError):
                cast("Any", getattr(client.topics, method))(*arguments)


_NON_FINITE_LITERALS = (b"NaN", b"Infinity", b"-Infinity")


@pytest.mark.parametrize("literal", _NON_FINITE_LITERALS)
@pytest.mark.parametrize(("method", "arguments", "path"), _TOPIC_READS)
def test_non_finite_topic_responses_fail_typed_without_a_raw_body(
    literal: bytes, method: str, arguments: tuple[str, ...], path: str
) -> None:
    """NaN and the infinities are not JSON and must never reach a native record."""
    shape = b'{"data": [{"id": "topic-1", @@}], "page": 1, "total": 1}'
    routes: RouteTable = {
        ("GET", f"{ORIGIN}/api/1/site/"): (200, json.dumps(UDATA_SITE).encode()),
        ("GET", f"{ORIGIN}{path}"): (200, shape.replace(b"@@", literal)),
    }
    with sync_client(sync_route_table(routes), None) as client, pytest.raises(NativeCatalogError) as raised:
        cast("Any", getattr(client.topics, method))(*arguments)
    rendered = repr(raised.value) + str(raised.value.__dict__)
    assert literal.decode() not in rendered
    assert "secret-key" not in rendered


def test_topic_routes_require_a_controlled_origin_for_every_mutation() -> None:
    """PROHIB-04-01: a public origin refuses a topic mutation before any network call."""
    public_origin = "https://www.data.gouv.fr"
    public_router = sync_route_table(
        {("GET", f"{public_origin}/api/1/site/"): (200, UDATA_SITE)},
    )
    with (
        SyncUDataClient(
            public_router,
            declared_udata_profile(),
            origin=public_origin,
            credentials=UDATA_CREDENTIAL,
        ) as client,
        pytest.raises(CatalogValidationError, match="controlled local deployment"),
    ):
        client.topics.create_topic(
            TopicCreateInput(name="Health"), PERMISSIONS, mutation_policy(wire.CREATE_TOPIC_OPERATION, "Health")
        )
    assert _topic_requests(public_router) == []


def test_topic_mutation_failure_after_dispatch_attaches_a_rejected_receipt() -> None:
    """A pre-dispatch policy rejection still yields an immutable receipt bound to the target."""
    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as raised:
        client.topics.delete_topic("topic-1", PERMISSIONS, None)
    receipt = cast("MutationReceipt", raised.value.__dict__["mutation_receipt"])
    assert receipt.outcome == "rejected"
    assert receipt.target.value == "topic-1"
    assert receipt.operation == wire.DELETE_TOPIC_OPERATION
    assert router.requests == []


_TOPIC_STATUS_OUTCOMES = (
    (400, CatalogValidationError, None),
    (401, UnauthenticatedError, "unauthorized"),
    (403, ForbiddenError, "forbidden"),
    (404, CatalogNotFoundError, None),
    (410, CatalogConflictError, "unavailable"),
    (423, CatalogUnavailableError, "deployment-disabled"),
    (429, CatalogRateLimitError, None),
    (500, CatalogUnavailableError, "unavailable"),
    (503, CatalogUnavailableError, "unavailable"),
)


@pytest.mark.parametrize(("status", "error_type", "state"), _TOPIC_STATUS_OUTCOMES)
def test_topic_status_failures_decode_into_the_documented_error_shape(
    status: int, error_type: type[CatalogError], state: str | None
) -> None:
    """Every pinned failure status maps to its normalized error and never leaks the raw body."""
    router = sync_route_table(
        with_site_route({("GET", f"{ORIGIN}/api/2/topics/topic-1/"): (status, {"id": "topic-1"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(error_type) as raised:
        client.topics.get_topic("topic-1")
    assert raised.value.operation == wire.GET_TOPIC_OPERATION
    assert raised.value.__dict__["metadata"]["status_code"] == status
    if state is not None:
        assert raised.value.capability_state == state
    assert "mutation_receipt" not in raised.value.__dict__
    assert "topic-1" not in str(raised.value.metadata)


_LEVELS_URL = f"{ORIGIN}/api/2/topics/?page=1&page_size=20"


class _InterruptingRoutes:
    """Answer the site probe and one topic route, raising the configured interruption while armed."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, body: object) -> None:
        self._route = route
        self._interruption = interruption
        self._body = body
        self.armed = True
        self.requests: list[RuntimeRequest] = []

    def _respond(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if self.armed and (request.method, request.url) == self._route:
            raise self._interruption
        payload = UDATA_SITE if request.url.endswith("/api/1/site/") else self._body
        return RuntimeResponse(200, {"Content-Type": "application/json"}, json.dumps(payload).encode())


class _CancellingSyncTransport(_InterruptingRoutes):
    """Sync transport that counts `close` so close-once ownership is observable."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, body: object) -> None:
        super().__init__(route, interruption, body)
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._respond(request)

    def close(self) -> None:
        self.close_count += 1


class _CancellingAsyncTransport(_InterruptingRoutes):
    """Async transport that counts `aclose` so close-once ownership is observable."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, body: object) -> None:
        super().__init__(route, interruption, body)
        self.aclose_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._respond(request)

    async def aclose(self) -> None:
        self.aclose_count += 1


def _topics_interrupting_sync(
    interruption: BaseException, *, breaker_failure_threshold: int = 2, emitter: EventEmitter | None = None
) -> tuple[SyncUDataClient, _CancellingSyncTransport]:
    transport = _CancellingSyncTransport(("GET", _LEVELS_URL), interruption, _TOPIC_PAGE)
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


def _topics_interrupting_async(
    interruption: BaseException, *, breaker_failure_threshold: int = 2, emitter: EventEmitter | None = None
) -> tuple[AsyncUDataClient, _CancellingAsyncTransport]:
    transport = _CancellingAsyncTransport(("GET", _LEVELS_URL), interruption, _TOPIC_PAGE)
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


def test_topic_sync_cancellation_leaves_post_state_ready_and_closes_the_owned_transport_once() -> None:
    """An interrupted read propagates, leaves the client usable, and still honours close-once ownership."""
    client, transport = _topics_interrupting_sync(KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        client.topics.list_topics()
    assert transport.close_count == 0

    transport.armed = False
    assert _plain(client.topics.list_topics().payload) == _TOPIC_PAGE

    client.close()
    client.close()
    assert transport.close_count == 1


def test_topic_sync_cancellation_never_closes_a_borrowed_transport() -> None:
    """A borrowed transport survives the same interrupted read and the context exit untouched."""
    transport = _CancellingSyncTransport(("GET", _LEVELS_URL), KeyboardInterrupt(), _TOPIC_PAGE)
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
        client.topics.list_topics()
    assert transport.close_count == 0


def test_topic_sync_cancellation_is_never_recorded_as_a_circuit_breaker_failure() -> None:
    """Cancellations beyond the failure threshold still admit the next read, so a cancel is not an origin failure."""
    events = ListSink()
    client, transport = _topics_interrupting_sync(KeyboardInterrupt(), emitter=EventEmitter(sinks=(events,)))

    for _ in range(4):
        with pytest.raises(KeyboardInterrupt):
            client.topics.list_topics()

    transport.armed = False
    assert _plain(client.topics.list_topics().payload) == _TOPIC_PAGE
    assert [event.outcome for event in events.events if "breaker" in event.outcome] == []
    client.close()


def test_topic_transport_failures_still_open_the_circuit_before_the_next_read() -> None:
    """The breaker is live for this family: genuine transport failures still fail the following read closed."""
    client, transport = _topics_interrupting_sync(TransportError("no route"))

    with pytest.raises(TransportError):
        client.topics.list_topics()

    transport.armed = False
    before = len(transport.requests)
    with pytest.raises(CatalogUnavailableError, match="circuit is open"):
        client.topics.list_topics()
    assert len(transport.requests) == before
    client.close()


def test_topic_mutation_cancellation_settles_as_cancelled_not_failed() -> None:
    """An interrupted mutation records the ambiguous-but-cancelled post-state on the raised error."""
    transport = _CancellingSyncTransport(("DELETE", f"{ORIGIN}/api/2/topics/topic-1/"), KeyboardInterrupt(), None)
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        retry_sleep=lambda _: None,
        owns_transport=True,
    )
    with pytest.raises(KeyboardInterrupt) as raised:
        client.topics.delete_topic(
            "topic-1",
            PERMISSIONS,
            mutation_policy(wire.DELETE_TOPIC_OPERATION, "topic-1", destructive=True),
        )
    receipt = cast("MutationReceipt", raised.value.__dict__["mutation_receipt"])
    assert receipt.outcome == "cancelled"
    assert receipt.target.value == "topic-1"
    assert receipt.operation == wire.DELETE_TOPIC_OPERATION
    client.close()


def test_topic_async_cancellation_leaves_post_state_ready_and_closes_the_owned_transport_once() -> None:
    """The async mode matches sync: the cancellation propagates and ownership is still close-once."""
    client, transport = _topics_interrupting_async(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError):
            await client.topics.list_topics()
        assert transport.aclose_count == 0

        transport.armed = False
        assert _plain((await client.topics.list_topics()).payload) == _TOPIC_PAGE

        await client.aclose()
        await client.aclose()
        assert transport.aclose_count == 1

    asyncio.run(run())


def test_topic_async_cancellation_never_closes_a_borrowed_transport() -> None:
    """An interrupted async read against a borrowed transport leaves the caller's transport open."""
    transport = _CancellingAsyncTransport(("GET", _LEVELS_URL), asyncio.CancelledError(), _TOPIC_PAGE)
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
                await client.topics.list_topics()
        assert transport.aclose_count == 0

    asyncio.run(run())


def test_topic_async_cancellation_is_never_recorded_as_a_circuit_breaker_failure() -> None:
    """Async cancellations also leave the circuit closed, so the next read is admitted rather than refused."""
    events = ListSink()
    client, transport = _topics_interrupting_async(asyncio.CancelledError(), emitter=EventEmitter(sinks=(events,)))

    async def run() -> None:
        for _ in range(4):
            with pytest.raises(asyncio.CancelledError):
                await client.topics.list_topics()
        transport.armed = False
        assert _plain((await client.topics.list_topics()).payload) == _TOPIC_PAGE
        await client.aclose()

    asyncio.run(run())
    assert [event.outcome for event in events.events if "breaker" in event.outcome] == []
