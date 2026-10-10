"""Exact wire and safety coverage for the uData taxonomy family."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.connectors.catalog.udata.models.taxonomies import (
    BadgeCreateInput,
    SuggestQuery,
    TaxonomyMutationResult,
    segment,
)
from datasluice.connectors.catalog.udata.services.taxonomies import (
    AsyncTaxonomiesService,
    SyncTaxonomiesService,
)
from datasluice.connectors.catalog.udata.wire import taxonomies as wire
from datasluice.contracts.catalog.native.udata import (
    AsyncUDataTaxonomiesService,
    SyncUDataTaxonomiesService,
)
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError, NativeCatalogError
from tests.helpers.udata_test_support import (
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
    thawed,
    with_site_route,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_PERMISSIONS
_BADGE_REQUIREMENTS = {
    wire.ADD_BADGE_OPERATION: frozenset({"badge-add"}),
    wire.DELETE_BADGE_OPERATION: frozenset({"badge-delete"}),
}


def _badge_permissions(*granted: str) -> EffectivePermissions:
    """Bind both badge requirements to one granted scope set, so the gate can discriminate."""
    return EffectivePermissions.for_credential(
        UDATA_CREDENTIAL,
        platform=CatalogPlatform.UDATA,
        scopes=frozenset(granted),
        operation_scopes=_BADGE_REQUIREMENTS,
    )


ADD_ONLY_PERMISSIONS = _badge_permissions("badge-add")
DELETE_ONLY_PERMISSIONS = _badge_permissions("badge-delete")


def _badge_requests(router: SyncRouteRouter | AsyncRouteRouter) -> list[tuple[str, str]]:
    return [(request.method, request.url) for request in router.requests if "/badges/" in request.url]


@pytest.mark.parametrize("identifier", [".", ".."])
def test_taxonomies_identifiers_reject_dot_segments(identifier: str) -> None:
    """A bare dot segment is removed by RFC 3986 resolution, retargeting the route."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(identifier, "list_taxonomies")


def test_taxonomy_contract_exposes_every_assigned_method_in_both_modes() -> None:
    sync_names = {
        name
        for name in dir(SyncTaxonomiesService)
        if not name.startswith("_") and callable(getattr(SyncTaxonomiesService, name))
    }
    async_names = {
        name
        for name in dir(AsyncTaxonomiesService)
        if not name.startswith("_") and callable(getattr(AsyncTaxonomiesService, name))
    }
    assert sync_names == async_names
    assert sync_names == {
        "available_badges",
        "add_badge",
        "delete_badge",
        "suggest_formats",
        "suggest_mime",
        "licenses",
        "frequencies",
        "extensions",
        "schemas",
        "dataset_schemas",
    }
    with sync_client(sync_route_table(with_site_route({})), None) as client:
        assert isinstance(client.taxonomies, SyncUDataTaxonomiesService)

    async def run() -> None:
        async with async_client(async_route_table(with_site_route({})), None) as client:
            assert isinstance(client.taxonomies, AsyncUDataTaxonomiesService)

    asyncio.run(run())


def test_every_taxonomy_route_has_an_exact_wire_shape() -> None:
    actual = [
        wire.available_badges_request()[0:2],
        wire.add_badge_request("dataset-1", BadgeCreateInput(kind="certified"))[0:2],
        wire.delete_badge_request("dataset-1", "certified")[0:2],
        wire.suggest_request("formats", SuggestQuery("csv"))[0:2],
        wire.suggest_request("mime", SuggestQuery("json", size=2))[0:2],
        wire.licenses_request()[0:2],
        wire.frequencies_request()[0:2],
        wire.extensions_request()[0:2],
        wire.schemas_request()[0:2],
        wire.dataset_schemas_request("dataset-1")[0:2],
    ]
    assert actual == [
        ("GET", "/api/1/datasets/badges/"),
        ("POST", "/api/1/datasets/dataset-1/badges/"),
        ("DELETE", "/api/1/datasets/dataset-1/badges/certified/"),
        ("GET", "/api/1/datasets/suggest/formats/?q=csv&size=10"),
        ("GET", "/api/1/datasets/suggest/mime/?q=json&size=2"),
        ("GET", "/api/1/datasets/licenses/"),
        ("GET", "/api/1/datasets/frequencies/"),
        ("GET", "/api/1/datasets/extensions/"),
        ("GET", "/api/1/datasets/schemas/"),
        ("GET", "/api/2/datasets/dataset-1/schemas/"),
    ]
    with pytest.raises(CatalogValidationError):
        wire.add_badge_request("../secret", BadgeCreateInput(kind="certified"))
    with pytest.raises(CatalogValidationError):
        wire.delete_badge_request("dataset-1", "")


def test_add_badge_wire_contract_is_exact_through_transport() -> None:
    method, path, headers, body = wire.add_badge_request("dataset-1", BadgeCreateInput(kind="certified"))

    assert method == "POST"
    assert path == "/api/1/datasets/dataset-1/badges/"
    assert headers == {}
    assert body == {"kind": "certified"}
    router = sync_route_table(
        with_site_route({("POST", f"{ORIGIN}/api/1/datasets/dataset-1/badges/"): (201, {"kind": "certified"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.taxonomies.add_badge(
            "dataset-1",
            BadgeCreateInput("certified"),
            ADD_ONLY_PERMISSIONS,
            mutation_policy(wire.ADD_BADGE_OPERATION, "dataset-1"),
        )

    request = router.requests[-1]
    assert request.method == "POST"
    assert request.url == f"{ORIGIN}/api/1/datasets/dataset-1/badges/"
    assert dict(request.headers) == {"X-API-KEY": "secret-key", "Content-Type": "application/json"}
    assert request.body == b'{"kind": "certified"}'


def test_taxonomy_reads_decode_losslessly_and_fail_typed_in_both_modes() -> None:
    responses = {
        "available_badges": {"certified": "Certified"},
        "suggest_formats": [{"text": "csv"}, {"text": "tsv"}],
        "suggest_mime": [{"text": "application/json"}],
        "licenses": [{"id": "lov2", "title": "Licence Ouverte", "flags": []}],
        "frequencies": [{"id": "punctual", "label": "Punctual"}],
        "extensions": ["csv", "tsv"],
        "schemas": [{"name": "etalab/schema-irve", "version": "2.0.0"}],
        "dataset_schemas": [{"name": "etalab/schema-irve", "version": None, "url": "https://schema.test"}],
    }
    routes: RouteTable = {
        ("GET", f"{ORIGIN}/api/1/datasets/badges/"): (200, responses["available_badges"]),
        ("GET", f"{ORIGIN}/api/1/datasets/suggest/formats/?q=csv&size=10"): (200, responses["suggest_formats"]),
        ("GET", f"{ORIGIN}/api/1/datasets/suggest/mime/?q=json&size=10"): (200, responses["suggest_mime"]),
        ("GET", f"{ORIGIN}/api/1/datasets/licenses/"): (200, responses["licenses"]),
        ("GET", f"{ORIGIN}/api/1/datasets/frequencies/"): (200, responses["frequencies"]),
        ("GET", f"{ORIGIN}/api/1/datasets/extensions/"): (200, responses["extensions"]),
        ("GET", f"{ORIGIN}/api/1/datasets/schemas/"): (200, responses["schemas"]),
        ("GET", f"{ORIGIN}/api/2/datasets/dataset-1/schemas/"): (200, responses["dataset_schemas"]),
    }
    with sync_client(sync_route_table(with_site_route(routes)), None) as client:
        service = client.taxonomies
        assert service.available_badges().payload == thawed(responses["available_badges"])
        assert thawed([record.payload for record in service.suggest_formats(SuggestQuery("csv"))]) == thawed(
            responses["suggest_formats"]
        )
        assert thawed([record.payload for record in service.suggest_mime(SuggestQuery("json"))]) == thawed(
            responses["suggest_mime"]
        )
        assert thawed([record.payload for record in service.licenses()]) == thawed(responses["licenses"])
        assert thawed([record.payload for record in service.frequencies()]) == thawed(responses["frequencies"])
        assert service.extensions() == ("csv", "tsv")
        assert thawed([record.payload for record in service.schemas()]) == thawed(responses["schemas"])
        assert thawed([record.payload for record in service.dataset_schemas("dataset-1")]) == thawed(
            responses["dataset_schemas"]
        )

    async def run() -> None:
        async with async_client(async_route_table(with_site_route(routes)), None) as client:
            assert await client.taxonomies.extensions() == ("csv", "tsv")
            records = await client.taxonomies.dataset_schemas("dataset-1")
            expected = cast("list[Mapping[str, object]]", thawed(responses["dataset_schemas"]))[0]
            assert any(record.payload == expected for record in records)

    asyncio.run(run())


_MALFORMED_SUGGEST_SIZES = (0, -1, 101, True, False, 1.0, 10.5, "10", None, [10], b"10")
_MALFORMED_SUGGEST_QUERIES = (5, None, b"csv", ["csv"], {"q": "csv"}, True)
_MALFORMED_BADGE_KINDS = ("", "a/b", "a?b", "a#b", 'a"b', "a'b", 4, None, True)
_ROUTE_ONLY_BADGE_KINDS = (".", "..", "a\x00b", "a\nb")
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
    ["dataset-1"],
)
_MALFORMED_OBJECT_LISTS: tuple[object, ...] = (
    ["object", 4],
    [4, "object"],
    {},
    {"id": "lov2"},
    "lov2",
    None,
    [{"nested": []}, "object"],
)
_MALFORMED_STRING_LISTS: tuple[object, ...] = (["csv", 4], ["csv", ""], ["csv", None], {}, "csv", None, [b"csv"])
_NON_FINITE_LITERALS = (b"NaN", b"Infinity", b"-Infinity")
_TAXONOMY_READ_PATHS = (
    ("available_badges", (), "/api/1/datasets/badges/", b'{"certified": @@}'),
    ("suggest_formats", (SuggestQuery("csv"),), "/api/1/datasets/suggest/formats/?q=csv&size=10", b'[{"text": @@}]'),
    ("suggest_mime", (SuggestQuery("csv"),), "/api/1/datasets/suggest/mime/?q=csv&size=10", b'[{"text": @@}]'),
    ("licenses", (), "/api/1/datasets/licenses/", b'[{"id": "lov2", "title": @@}]'),
    ("frequencies", (), "/api/1/datasets/frequencies/", b'[{"id": "punctual", "label": @@}]'),
    ("extensions", (), "/api/1/datasets/extensions/", b'["csv", @@]'),
    ("schemas", (), "/api/1/datasets/schemas/", b'[{"name": "etalab/schema-irve", "version": @@}]'),
    (
        "dataset_schemas",
        ("dataset-1",),
        "/api/2/datasets/dataset-1/schemas/",
        b'[{"name": "etalab/schema-irve", "version": @@}]',
    ),
)


@pytest.mark.parametrize("size", _MALFORMED_SUGGEST_SIZES)
def test_malformed_suggest_sizes_are_rejected_before_dispatch(size: object) -> None:
    """`size` must be a real integer in 1..100; bool, float, and str are all malformed."""
    with pytest.raises(ValueError, match="size"):
        SuggestQuery("csv", size=cast("int", size))


@pytest.mark.parametrize("query", _MALFORMED_SUGGEST_QUERIES)
def test_malformed_suggest_queries_are_rejected_before_dispatch(query: object) -> None:
    """`q` reaches the query string verbatim, so a non-string must never be coerced."""
    with pytest.raises(ValueError, match="query"):
        SuggestQuery(cast("str", query))


@pytest.mark.parametrize("kind", _MALFORMED_BADGE_KINDS)
def test_malformed_badge_kinds_are_rejected_before_dispatch(kind: object) -> None:
    """The badge kind is a path segment on delete and a body value on add."""
    with pytest.raises(ValueError, match="kind"):
        BadgeCreateInput(kind=cast("str", kind))


@pytest.mark.parametrize("kind", _ROUTE_ONLY_BADGE_KINDS)
def test_dot_and_control_badge_kinds_are_stopped_at_the_route_boundary(kind: str) -> None:
    """`BadgeCreateInput` only screens separators; the shared segment guard screens the rest."""
    assert BadgeCreateInput(kind=kind).payload() == {"kind": kind}
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        wire.delete_badge_request("dataset-1", kind)
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        wire.add_badge_request(kind, BadgeCreateInput("certified"))


@pytest.mark.parametrize("identifier", _MALFORMED_SEGMENTS)
def test_malformed_taxonomy_segments_never_reach_a_route(identifier: object) -> None:
    """Dot segments, separators, and control characters must be refused, not quoted."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(cast("str", identifier), wire.DELETE_BADGE_OPERATION)


@pytest.mark.parametrize("payload", _MALFORMED_OBJECT_LISTS)
def test_malformed_object_lists_fail_the_taxonomy_decoder(payload: object) -> None:
    with pytest.raises(CatalogValidationError):
        wire.parse_objects(payload, operation=wire.TAXONOMIES_OPERATION)


@pytest.mark.parametrize("payload", _MALFORMED_STRING_LISTS)
def test_malformed_string_lists_fail_the_taxonomy_decoder(payload: object) -> None:
    with pytest.raises(CatalogValidationError):
        wire.parse_strings(payload, operation=wire.EXTENSIONS_OPERATION)


@pytest.mark.parametrize("literal", _NON_FINITE_LITERALS)
@pytest.mark.parametrize(("read", "arguments", "path", "shape"), _TAXONOMY_READ_PATHS)
def test_non_finite_taxonomy_responses_fail_typed_without_a_raw_body(
    literal: bytes, read: str, arguments: tuple[object, ...], path: str, shape: bytes
) -> None:
    """NaN and the infinities are not JSON and must never reach a native record."""
    router = sync_route_table(
        {
            ("GET", f"{ORIGIN}/api/1/site/"): (200, json.dumps(UDATA_SITE).encode()),
            ("GET", f"{ORIGIN}{path}"): (200, shape.replace(b"@@", literal)),
        }
    )
    with sync_client(router, None) as client, pytest.raises(NativeCatalogError) as raised:
        getattr(client.taxonomies, read)(*arguments)
    rendered = repr(raised.value) + str(raised.value.__dict__)
    assert literal.decode() not in rendered
    assert "secret-key" not in rendered


def test_invalid_taxonomy_values_fail_before_or_at_decode_without_raw_body() -> None:
    with pytest.raises(ValueError, match="kind"):
        BadgeCreateInput(kind="")
    with pytest.raises(ValueError, match="query"):
        SuggestQuery("")
    with pytest.raises(CatalogValidationError):
        wire.parse_objects(["object", 4], operation=wire.TAXONOMIES_OPERATION)
    with pytest.raises(CatalogValidationError):
        wire.parse_strings(["csv", 4], operation=wire.EXTENSIONS_OPERATION)
    nan_site = (
        b'{"version": "17.6.0", "id": "site", "title": "uData", "feed_size": 0, '
        b'"keywords": [], "metrics": {"widgets": NaN}}'
    )
    with (
        pytest.raises(NativeCatalogError),
        sync_client(
            sync_route_table(
                {
                    ("GET", f"{ORIGIN}/api/1/site/"): (200, nan_site),
                    ("GET", f"{ORIGIN}/api/1/datasets/extensions/"): (200, nan_site),
                }
            ),
            None,
        ) as client,
    ):
        client.taxonomies.extensions()


def test_badge_mutations_are_permission_guarded_and_return_redacted_receipts() -> None:
    badge = {"kind": "certified", "label": "Certified"}
    routes: RouteTable = {
        ("POST", f"{ORIGIN}/api/1/datasets/dataset-1/badges/"): (201, badge),
        ("DELETE", f"{ORIGIN}/api/1/datasets/dataset-1/badges/certified/"): (204, None),
    }
    add_policy = mutation_policy("udata/api-v1.add-dataset-badge", "dataset-1")
    delete_policy = mutation_policy("udata/api-v1.delete-dataset-badge", "dataset-1:certified", destructive=True)
    with sync_client(sync_route_table(with_site_route(routes)), UDATA_CREDENTIAL) as client:
        added = client.taxonomies.add_badge("dataset-1", BadgeCreateInput("certified"), PERMISSIONS, add_policy)
        removed = client.taxonomies.delete_badge("dataset-1", "certified", PERMISSIONS, delete_policy)
        assert isinstance(added, TaxonomyMutationResult)
        assert added.record is not None
        assert added.record.payload == badge
        added_receipt = added.receipt.to_dict()
        added_metadata = cast("dict[str, object]", added_receipt["audit_metadata"])
        assert added_metadata["mutation"] == "added"
        assert removed.record is None
        removed_receipt = removed.receipt.to_dict()
        removed_metadata = cast("dict[str, object]", removed_receipt["audit_metadata"])
        assert removed_metadata["mutation"] == "deleted"
        assert b"secret-key" not in json.dumps(added.receipt.to_dict()).encode()


def test_badge_permissions_discriminate_add_from_delete_in_both_modes() -> None:
    routes = with_site_route(
        {
            ("POST", f"{ORIGIN}/api/1/datasets/dataset-1/badges/"): (201, {"kind": "certified"}),
            ("DELETE", f"{ORIGIN}/api/1/datasets/dataset-1/badges/certified/"): (204, None),
        }
    )
    add_policy = mutation_policy(wire.ADD_BADGE_OPERATION, "dataset-1")
    delete_policy = mutation_policy(wire.DELETE_BADGE_OPERATION, "dataset-1:certified", destructive=True)

    def run_sync() -> None:
        router = sync_route_table(routes)
        with sync_client(router, UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied_delete:
                client.taxonomies.delete_badge("dataset-1", "certified", ADD_ONLY_PERMISSIONS, delete_policy)
            with pytest.raises(ForbiddenError) as denied_add:
                client.taxonomies.add_badge(
                    "dataset-1", BadgeCreateInput("certified"), DELETE_ONLY_PERMISSIONS, add_policy
                )
            assert denied_delete.value.operation == wire.DELETE_BADGE_OPERATION
            assert denied_add.value.operation == wire.ADD_BADGE_OPERATION
            assert denied_delete.value.capability_state == "forbidden"
            assert denied_add.value.capability_state == "forbidden"
            assert router.requests == []
            allowed_add = client.taxonomies.add_badge(
                "dataset-1", BadgeCreateInput("certified"), ADD_ONLY_PERMISSIONS, add_policy
            )
            allowed_delete = client.taxonomies.delete_badge(
                "dataset-1", "certified", DELETE_ONLY_PERMISSIONS, delete_policy
            )
        assert allowed_add.receipt.outcome == "succeeded"
        assert allowed_delete.receipt.outcome == "succeeded"
        assert _badge_requests(router) == [
            ("POST", f"{ORIGIN}/api/1/datasets/dataset-1/badges/"),
            ("DELETE", f"{ORIGIN}/api/1/datasets/dataset-1/badges/certified/"),
        ]

    async def run_async() -> None:
        router = async_route_table(routes)
        async with async_client(router, UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied_delete:
                await client.taxonomies.delete_badge("dataset-1", "certified", ADD_ONLY_PERMISSIONS, delete_policy)
            with pytest.raises(ForbiddenError) as denied_add:
                await client.taxonomies.add_badge(
                    "dataset-1", BadgeCreateInput("certified"), DELETE_ONLY_PERMISSIONS, add_policy
                )
            assert denied_delete.value.operation == wire.DELETE_BADGE_OPERATION
            assert denied_add.value.operation == wire.ADD_BADGE_OPERATION
            assert denied_delete.value.capability_state == "forbidden"
            assert denied_add.value.capability_state == "forbidden"
            assert router.requests == []
            allowed_add = await client.taxonomies.add_badge(
                "dataset-1", BadgeCreateInput("certified"), ADD_ONLY_PERMISSIONS, add_policy
            )
            allowed_delete = await client.taxonomies.delete_badge(
                "dataset-1", "certified", DELETE_ONLY_PERMISSIONS, delete_policy
            )
        assert allowed_add.receipt.outcome == "succeeded"
        assert allowed_delete.receipt.outcome == "succeeded"
        assert _badge_requests(router) == [
            ("POST", f"{ORIGIN}/api/1/datasets/dataset-1/badges/"),
            ("DELETE", f"{ORIGIN}/api/1/datasets/dataset-1/badges/certified/"),
        ]

    run_sync()
    asyncio.run(run_async())


def test_badge_dispatch_uses_exact_operation_in_both_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    add_calls: list[dict[str, object]] = []
    delete_calls: list[dict[str, object]] = []

    def record(target: list[dict[str, object]], status: int):
        def capture(**kwargs: object) -> tuple[int, object, object]:
            target.append(kwargs)
            return status, {"kind": "certified"} if status == 201 else None, object()

        return capture

    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        monkeypatch.setattr(client, "_dataset_call", record(add_calls, 201))
        added = client.taxonomies.add_badge(
            "dataset-1",
            BadgeCreateInput("certified"),
            ADD_ONLY_PERMISSIONS,
            mutation_policy(wire.ADD_BADGE_OPERATION, "dataset-1"),
        )
        monkeypatch.setattr(client, "_dataset_call", record(delete_calls, 204))
        deleted = client.taxonomies.delete_badge(
            "dataset-1",
            "certified",
            DELETE_ONLY_PERMISSIONS,
            mutation_policy(wire.DELETE_BADGE_OPERATION, "dataset-1:certified", destructive=True),
        )

    assert add_calls[0]["owning_operation"] == wire.ADD_BADGE_OPERATION
    assert delete_calls[0]["owning_operation"] == wire.DELETE_BADGE_OPERATION
    assert added.receipt.operation == wire.ADD_BADGE_OPERATION
    assert deleted.receipt.operation == wire.DELETE_BADGE_OPERATION

    async def run_async() -> None:
        async def capture(**kwargs: object) -> tuple[int, object, object]:
            assert kwargs["owning_operation"] == wire.DELETE_BADGE_OPERATION
            return 204, None, object()

        async with async_client(async_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            monkeypatch.setattr(client, "_dataset_call_async", capture)
            deleted = await client.taxonomies.delete_badge(
                "dataset-1",
                "certified",
                DELETE_ONLY_PERMISSIONS,
                mutation_policy(wire.DELETE_BADGE_OPERATION, "dataset-1:certified", destructive=True),
            )

        assert deleted.receipt.operation == wire.DELETE_BADGE_OPERATION

    asyncio.run(run_async())
