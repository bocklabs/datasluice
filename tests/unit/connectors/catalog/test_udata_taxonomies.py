"""Exact wire and safety coverage for the uData taxonomy family."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    declared_udata_profile,
)
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
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError, NativeCatalogError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

ORIGIN = "http://127.0.0.1:5640"
SITE = {"feed_size": 0, "id": "site", "keywords": [], "metrics": {}, "title": "uData", "version": "17.6.0"}
CREDENTIAL = UDataCredential(api_key="secret-key")
PERMISSIONS = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA)
ADD_ONLY_PERMISSIONS = EffectivePermissions.for_credential(
    CREDENTIAL,
    platform=CatalogPlatform.UDATA,
    scopes=frozenset({"add-badge"}),
    operation_scopes={wire.ADD_BADGE_OPERATION: frozenset({"add-badge"})},
)
DELETE_ONLY_PERMISSIONS = EffectivePermissions.for_credential(
    CREDENTIAL,
    platform=CatalogPlatform.UDATA,
    scopes=frozenset({"delete-badge"}),
    operation_scopes={wire.DELETE_BADGE_OPERATION: frozenset({"delete-badge"})},
)


class _Router:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, object]]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        status, payload = self.routes[(request.method, request.url)]
        if payload is None:
            body = b""
        elif isinstance(payload, bytes):
            body = payload
        else:
            body = json.dumps(payload, separators=(",", ":")).encode()
        return RuntimeResponse(status_code=status, headers={"Content-Type": "application/json"}, body=body)

    def close(self) -> None:
        return None


class _AsyncRouter:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, object]]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        status, payload = self.routes[(request.method, request.url)]
        if payload is None:
            body = b""
        elif isinstance(payload, bytes):
            body = payload
        else:
            body = json.dumps(payload, separators=(",", ":")).encode()
        return RuntimeResponse(status_code=status, headers={"Content-Type": "application/json"}, body=body)

    async def aclose(self) -> None:
        return None


def _routes(
    routes: dict[tuple[str, str], tuple[int, object]],
) -> dict[tuple[str, str], tuple[int, object]]:
    return {("GET", f"{ORIGIN}/api/1/site/"): (200, SITE), **routes}


def _policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _thawed(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thawed(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_thawed(item) for item in value]
    return value


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
    with SyncUDataClient(_Router(_routes({})), declared_udata_profile(), origin=ORIGIN) as client:
        assert isinstance(client.taxonomies, SyncUDataTaxonomiesService)

    async def run() -> None:
        async with AsyncUDataClient(_AsyncRouter(_routes({})), declared_udata_profile(), origin=ORIGIN) as client:
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
    router = _Router(_routes({("POST", f"{ORIGIN}/api/1/datasets/dataset-1/badges/"): (201, {"kind": "certified"})}))
    with SyncUDataClient(
        router,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=CREDENTIAL,
    ) as client:
        client.taxonomies.add_badge(
            "dataset-1",
            BadgeCreateInput("certified"),
            ADD_ONLY_PERMISSIONS,
            MutationPolicy(
                confirmation=ConfirmationPolicy(confirmed=True, operation=wire.ADD_BADGE_OPERATION, target="dataset-1"),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
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
    routes: dict[tuple[str, str], tuple[int, object]] = {
        ("GET", f"{ORIGIN}/api/1/datasets/badges/"): (200, responses["available_badges"]),
        ("GET", f"{ORIGIN}/api/1/datasets/suggest/formats/?q=csv&size=10"): (200, responses["suggest_formats"]),
        ("GET", f"{ORIGIN}/api/1/datasets/suggest/mime/?q=json&size=10"): (200, responses["suggest_mime"]),
        ("GET", f"{ORIGIN}/api/1/datasets/licenses/"): (200, responses["licenses"]),
        ("GET", f"{ORIGIN}/api/1/datasets/frequencies/"): (200, responses["frequencies"]),
        ("GET", f"{ORIGIN}/api/1/datasets/extensions/"): (200, responses["extensions"]),
        ("GET", f"{ORIGIN}/api/1/datasets/schemas/"): (200, responses["schemas"]),
        ("GET", f"{ORIGIN}/api/2/datasets/dataset-1/schemas/"): (200, responses["dataset_schemas"]),
    }
    with SyncUDataClient(_Router(_routes(routes)), declared_udata_profile(), origin=ORIGIN) as client:
        service = client.taxonomies
        assert service.available_badges().payload == _thawed(responses["available_badges"])
        assert _thawed([record.payload for record in service.suggest_formats(SuggestQuery("csv"))]) == _thawed(
            responses["suggest_formats"]
        )
        assert _thawed([record.payload for record in service.suggest_mime(SuggestQuery("json"))]) == _thawed(
            responses["suggest_mime"]
        )
        assert _thawed([record.payload for record in service.licenses()]) == _thawed(responses["licenses"])
        assert _thawed([record.payload for record in service.frequencies()]) == _thawed(responses["frequencies"])
        assert service.extensions() == ("csv", "tsv")
        assert _thawed([record.payload for record in service.schemas()]) == _thawed(responses["schemas"])
        assert _thawed([record.payload for record in service.dataset_schemas("dataset-1")]) == _thawed(
            responses["dataset_schemas"]
        )

    async def run() -> None:
        async with AsyncUDataClient(_AsyncRouter(_routes(routes)), declared_udata_profile(), origin=ORIGIN) as client:
            assert await client.taxonomies.extensions() == ("csv", "tsv")
            records = await client.taxonomies.dataset_schemas("dataset-1")
            expected = cast(list[Mapping[str, object]], _thawed(responses["dataset_schemas"]))[0]
            assert any(record.payload == expected for record in records)

    asyncio.run(run())


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
    with pytest.raises(NativeCatalogError):
        with SyncUDataClient(
            _Router(
                {
                    ("GET", f"{ORIGIN}/api/1/site/"): (200, nan_site),
                    ("GET", f"{ORIGIN}/api/1/datasets/extensions/"): (200, nan_site),
                }
            ),
            declared_udata_profile(),
            origin=ORIGIN,
        ) as client:
            client.taxonomies.extensions()


def test_badge_mutations_are_permission_guarded_and_return_redacted_receipts() -> None:
    badge = {"kind": "certified", "label": "Certified"}
    routes: dict[tuple[str, str], tuple[int, object]] = {
        ("POST", f"{ORIGIN}/api/1/datasets/dataset-1/badges/"): (201, badge),
        ("DELETE", f"{ORIGIN}/api/1/datasets/dataset-1/badges/certified/"): (204, None),
    }
    add_policy = _policy("udata/api-v1.add-dataset-badge", "dataset-1")
    delete_policy = _policy("udata/api-v1.delete-dataset-badge", "dataset-1:certified", destructive=True)
    with SyncUDataClient(
        _Router(_routes(routes)), declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL
    ) as client:
        added = client.taxonomies.add_badge("dataset-1", BadgeCreateInput("certified"), PERMISSIONS, add_policy)
        removed = client.taxonomies.delete_badge("dataset-1", "certified", PERMISSIONS, delete_policy)
        assert isinstance(added, TaxonomyMutationResult)
        assert added.record is not None
        assert added.record.payload == badge
        added_receipt = added.receipt.to_dict()
        added_metadata = cast(dict[str, object], added_receipt["audit_metadata"])
        assert added_metadata["mutation"] == "added"
        assert removed.record is None
        removed_receipt = removed.receipt.to_dict()
        removed_metadata = cast(dict[str, object], removed_receipt["audit_metadata"])
        assert removed_metadata["mutation"] == "deleted"
        assert b"secret-key" not in json.dumps(added.receipt.to_dict()).encode()


def test_badge_permissions_discriminate_add_from_delete_in_both_modes() -> None:
    routes = _routes(
        {
            ("POST", f"{ORIGIN}/api/1/datasets/dataset-1/badges/"): (201, {"kind": "certified"}),
            ("DELETE", f"{ORIGIN}/api/1/datasets/dataset-1/badges/certified/"): (204, None),
        }
    )

    router = _Router(routes)

    def run_sync() -> None:
        with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied_delete:
                client.taxonomies.delete_badge("dataset-1", "certified", ADD_ONLY_PERMISSIONS)
            with pytest.raises(ForbiddenError) as denied_add:
                client.taxonomies.add_badge("dataset-1", BadgeCreateInput("certified"), DELETE_ONLY_PERMISSIONS)
            assert denied_delete.value.operation == wire.DELETE_BADGE_OPERATION
            assert denied_add.value.operation == wire.ADD_BADGE_OPERATION
            assert router.requests == []

    async def run_async() -> None:
        async with AsyncUDataClient(
            _AsyncRouter(routes), declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL
        ) as client:
            with pytest.raises(ForbiddenError) as denied_delete:
                await client.taxonomies.delete_badge("dataset-1", "certified", ADD_ONLY_PERMISSIONS)
            with pytest.raises(ForbiddenError) as denied_add:
                await client.taxonomies.add_badge("dataset-1", BadgeCreateInput("certified"), DELETE_ONLY_PERMISSIONS)
            assert denied_delete.value.operation == wire.DELETE_BADGE_OPERATION
            assert denied_add.value.operation == wire.ADD_BADGE_OPERATION
            assert router.requests == []

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

    router = _Router(_routes({}))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        monkeypatch.setattr(client, "_dataset_call", record(add_calls, 201))
        added = client.taxonomies.add_badge(
            "dataset-1",
            BadgeCreateInput("certified"),
            ADD_ONLY_PERMISSIONS,
            _policy(wire.ADD_BADGE_OPERATION, "dataset-1"),
        )
        monkeypatch.setattr(client, "_dataset_call", record(delete_calls, 204))
        deleted = client.taxonomies.delete_badge(
            "dataset-1",
            "certified",
            DELETE_ONLY_PERMISSIONS,
            _policy(wire.DELETE_BADGE_OPERATION, "dataset-1:certified", destructive=True),
        )

    assert add_calls[0]["owning_operation"] == wire.ADD_BADGE_OPERATION
    assert delete_calls[0]["owning_operation"] == wire.DELETE_BADGE_OPERATION
    assert added.receipt.operation == wire.ADD_BADGE_OPERATION
    assert deleted.receipt.operation == wire.DELETE_BADGE_OPERATION

    async def run_async() -> None:
        async def capture(**kwargs: object) -> tuple[int, object, object]:
            assert kwargs["owning_operation"] == wire.DELETE_BADGE_OPERATION
            return 204, None, object()

        async with AsyncUDataClient(
            _AsyncRouter(_routes({})), declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL
        ) as client:
            monkeypatch.setattr(client, "_dataset_call_async", capture)
            deleted = await client.taxonomies.delete_badge(
                "dataset-1",
                "certified",
                DELETE_ONLY_PERMISSIONS,
                _policy(wire.DELETE_BADGE_OPERATION, "dataset-1:certified", destructive=True),
            )

        assert deleted.receipt.operation == wire.DELETE_BADGE_OPERATION

    asyncio.run(run_async())
