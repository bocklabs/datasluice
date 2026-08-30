"""Exact-wire and failure evidence for the complete uData dataset family."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.datasets import (
    DatasetCreateInput,
    DatasetDeleteOptions,
    DatasetExtrasDelete,
    DatasetExtrasUpdate,
    DatasetListQuery,
    DatasetMutationOutcome,
    DatasetSearchQuery,
    DatasetSuggestQuery,
    DatasetUpdateInput,
)
from datasluice.connectors.catalog.udata.wire import datasets as wire
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import NativeRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, IdempotencyPolicy, MutationPolicy
from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogError,
    CatalogNotFoundError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
)
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure

_USER_CREDENTIAL = UDataCredential(api_key="secret-key")
_ADMIN_CREDENTIAL = UDataCredential(api_key="admin-key")
_USER_KEY_CREDENTIAL = UDataCredential(api_key="user-key")
_USER_PERMISSIONS = EffectivePermissions.for_credential(_USER_CREDENTIAL, platform=CatalogPlatform.UDATA)
_ADMIN_PERMISSIONS = EffectivePermissions.for_credential(
    _ADMIN_CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
)
_USER_KEY_ADMIN_PERMISSIONS = EffectivePermissions.for_credential(
    _USER_KEY_CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
)


def _mutation_policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _permissions_for(credential: UDataCredential, *, admin: bool = False) -> EffectivePermissions:
    return EffectivePermissions.for_credential(
        credential,
        platform=CatalogPlatform.UDATA,
        roles=frozenset({"admin"}) if admin else frozenset(),
    )


def _receipt_from(error: BaseException) -> MutationReceipt:
    receipt = getattr(error, "mutation_receipt", None)
    assert isinstance(receipt, MutationReceipt)
    return receipt


def _respond(body: object) -> RuntimeResponse:
    if isinstance(body, bytes):
        return RuntimeResponse(status_code=200, headers={"Content-Type": "application/atom+xml"}, body=body)
    if isinstance(body, tuple) and body and isinstance(body[0], int):
        status, payload = body[0], body[1]
        headers = body[2] if len(body) > 2 else {}
    else:
        status, payload, headers = 200, body, {}
    encoded = b"" if payload is None else payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return RuntimeResponse(status_code=status, headers=headers, body=encoded)


def _dataset_doc(dataset_id: str = "abc") -> dict[str, object]:
    return {"id": dataset_id, "title": "Title", "slug": "title", "description": "d", "private": False}


def _page_body(items: list[dict[str, object]] | None = None) -> bytes:
    return json.dumps(
        {
            "data": [_dataset_doc()] if items is None else items,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "previous_page": None,
            "total": 1,
        }
    ).encode()


class RouterTransport:
    """A transport routing canned bodies by (method, path) with request capture."""

    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self._routes: dict[tuple[str, str], object] = dict(routes)
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0
        self.aclose_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        key = (request.method, request.url)
        if key not in self._routes:
            raise AssertionError(f"unexpected request {key}")
        return _respond(self._routes[key])

    def close(self) -> None:
        self.close_count += 1


class RouterAsyncTransport:
    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self._routes: dict[tuple[str, str], object] = dict(routes)
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0
        self.aclose_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return _respond(self._routes[(request.method, request.url)])

    async def aclose(self) -> None:
        self.aclose_count += 1


def _sync_client(routes: dict[tuple[str, str], object]) -> SyncUDataClient:
    transport, client = _sync_client_with_transport(routes)
    return client


def _sync_client_with_transport(
    routes: dict[tuple[str, str], object],
    credential: UDataCredential | None = None,
) -> tuple[RouterTransport, SyncUDataClient]:
    transport = RouterTransport(routes)
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )
    return transport, client


def _async_client(routes: dict[tuple[str, str], object]) -> AsyncUDataClient:
    return AsyncUDataClient(
        RouterAsyncTransport(routes),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    )


def _site_first(routes: dict[tuple[str, str], object]) -> dict[tuple[str, str], object]:
    routes = dict(routes)
    site = json.dumps(
        {"feed_size": 0, "id": "s", "keywords": [], "metrics": {}, "title": "uData", "version": "17.6.0"}
    ).encode()
    routes[("GET", "http://127.0.0.1:5640/api/1/site/")] = site
    return routes


def test_row39_list_datasets_exact_wire_and_projection() -> None:
    routes = _site_first(
        {("GET", "http://127.0.0.1:5640/api/1/datasets/?page=2&page_size=5&sort=-views"): _page_body()}
    )
    with _sync_client(routes) as client:
        envelope = client.datasets.list(DatasetListQuery(page=2, page_size=5, sort="-views"))

    assert len(envelope.items) == 1
    assert isinstance(envelope.items[0], NativeRecord)
    assert envelope.items[0].id.value == "abc"
    assert envelope.page is not None and envelope.page.total_items == 1


def test_row40_create_dataset_posts_exact_body_and_decodes_201() -> None:
    routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/"): (201, _dataset_doc("new"))})
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)
    with client:
        record = client.datasets.create(
            DatasetCreateInput(title="T", description="D", private=True),
            permissions=_USER_PERMISSIONS,
            mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
        )

    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"title": "T", "description": "D", "private": True}
    assert request.headers.get("Content-Type") == "application/json"
    assert request.headers.get("X-API-KEY") == "secret-key"
    record_value = record.record
    assert record_value is not None and record_value.id.value == "new"
    assert record.receipt.outcome == "succeeded"
    assert record.receipt.audit_metadata["mutation"] == "created"
    assert record.receipt.audit_metadata["status_code"] == 201


def test_row41_recent_atom_returns_typed_text_document() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/recent.atom?page=1&page_size=20"): b"<feed/>"})
    with _sync_client(routes) as client:
        record = client.datasets.recent_atom()

    assert record.payload["media_type"] == "application/atom+xml"
    assert record.payload["size_bytes"] == len("<feed/>")
    assert record.payload["sha256"]


def test_row42_get_dataset_exact_path() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/abc/"): _dataset_doc()})
    with _sync_client(routes) as client:
        record = client.datasets.get("abc")

    assert record.id.value == "abc"


def test_row43_update_dataset_omits_absent_fields() -> None:
    routes = _site_first({("PUT", "http://127.0.0.1:5640/api/1/datasets/abc/"): _dataset_doc()})
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)
    with client:
        client.datasets.update(
            "abc",
            DatasetUpdateInput(title="New"),
            permissions=_USER_PERMISSIONS,
            mutation_policy=_mutation_policy("udata/api-v1.update-dataset", "abc"),
        )

    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"title": "New"}


def test_row44_delete_dataset_returns_redacted_receipt() -> None:
    routes = _site_first({("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/?send_legal_notice=true"): (204, None)})
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)
    with client:
        outcome = client.datasets.delete(
            "abc",
            _USER_PERMISSIONS,
            DatasetDeleteOptions(send_legal_notice=True),
            mutation_policy=_mutation_policy("udata/api-v1.delete-dataset", "abc", destructive=True),
        ).receipt

    assert isinstance(outcome, DatasetMutationOutcome)
    assert outcome.outcome == "succeeded"
    assert outcome.audit_metadata["mutation"] == "deleted"
    assert outcome.audit_metadata["status_code"] == 204
    assert outcome.target.value == "abc"


def test_rows45_46_feature_transitions_use_exact_methods() -> None:
    routes = _site_first(
        {
            ("POST", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): _dataset_doc(),
            ("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): _dataset_doc(),
        }
    )
    transport, client = _sync_client_with_transport(routes, _ADMIN_CREDENTIAL)
    with client:
        featured = client.datasets.feature(
            "abc",
            permissions=_ADMIN_PERMISSIONS,
            mutation_policy=_mutation_policy("udata/api-v1.feature-dataset", "abc"),
        )
        unfeatured = client.datasets.unfeature(
            "abc",
            permissions=_ADMIN_PERMISSIONS,
            mutation_policy=_mutation_policy("udata/api-v1.unfeature-dataset", "abc", destructive=True),
        )

    assert featured.record is not None and unfeatured.record is not None
    featured_id = featured.record.id
    unfeatured_id = unfeatured.record.id
    assert featured_id.value == unfeatured_id.value == "abc"
    methods = [r.method for r in transport.requests[-2:]]
    assert methods == ["POST", "DELETE"]


def test_row47_rdf_dataset_returns_typed_redirect_outcome() -> None:
    routes = _site_first(
        {
            (
                "GET",
                "http://127.0.0.1:5640/api/1/datasets/abc/rdf",
            ): (302, None, {"Location": "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl"}),
            ("GET", "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl"): b"<rdf/>",
        }
    )
    with _sync_client(routes) as client:
        outcome = client.datasets.rdf("abc")

    assert isinstance(outcome, DatasetMutationOutcome)
    assert outcome.outcome == "skipped"
    assert outcome.audit_metadata["mutation"] == "rdf_redirect"
    assert outcome.audit_metadata["status_code"] == 302
    assert outcome.target.value == "abc"


def test_row48_rdf_format_returns_bounded_document_metadata() -> None:
    routes = _site_first(
        {
            ("GET", "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl"): (
                200,
                b"<rdf/>",
                {"Content-Type": "text/turtle"},
            )
        }
    )
    with _sync_client(routes) as client:
        record = client.datasets.rdf_format("abc", "ttl")

    assert record.payload["media_type"] == "text/turtle"
    assert record.payload["size_bytes"] == len("<rdf/>")
    assert record.payload["sha256"]
    assert "body" not in record.payload


def test_row67_suggest_encodes_required_query() -> None:
    routes = _site_first(
        {("GET", "http://127.0.0.1:5640/api/1/datasets/suggest/?q=ab&size=3"): [{"id": "abc", "title": "T"}]}
    )
    with _sync_client(routes) as client:
        suggestions = client.datasets.suggest(DatasetSuggestQuery(q="ab", size=3))

    assert len(suggestions) == 1
    assert suggestions[0].payload["title"] == "T"


def test_row75_v2_search_uses_search_endpoint() -> None:
    routes = _site_first(
        {("GET", "http://127.0.0.1:5640/api/2/datasets/search/?page=1&page_size=20&q=x"): _page_body()}
    )
    with _sync_client(routes) as client:
        envelope = client.datasets.search_v2(DatasetSearchQuery(q="x"))

    assert envelope.items[0].id.value == "abc"


def test_row76_v2_list_uses_v2_endpoint() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/2/datasets/?page=1&page_size=20"): _page_body()})
    with _sync_client(routes) as client:
        envelope = client.datasets.list_v2()

    assert envelope.page is not None


def test_row77_v2_get_dataset_exact_path() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/2/datasets/abc/"): _dataset_doc()})
    with _sync_client(routes) as client:
        assert client.datasets.get_v2("abc").id.value == "abc"


def test_rows78_79_extras_read_and_null_delete_semantics() -> None:
    routes = _site_first(
        {
            ("GET", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): {"keep": "v"},
            ("PUT", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (200, {"keep": "v", "added": 1}),
        }
    )
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)
    with client:
        assert client.datasets.get_extras_v2("abc") == {"keep": "v"}
        result = client.datasets.update_extras_v2(
            "abc",
            DatasetExtrasUpdate({"added": 1, "gone": None}),
            permissions=_USER_PERMISSIONS,
            mutation_policy=_mutation_policy("udata/api-v2.update-dataset-extras", "abc"),
        )

    assert result.extras == {"keep": "v", "added": 1}
    assert result.receipt.outcome == "succeeded"
    assert result.receipt.audit_metadata["mutation"] == "extras_updated"
    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"added": 1, "gone": None}


def test_row80_extras_delete_returns_receipt_from_204_with_body() -> None:
    routes = _site_first({("DELETE", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (204, {"keep": "v"})})
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)
    with client:
        outcome = client.datasets.delete_extras_v2(
            "abc",
            DatasetExtrasDelete(keys=("gone",)),
            permissions=_USER_PERMISSIONS,
            mutation_policy=_mutation_policy("udata/api-v2.delete-dataset-extras", "abc", destructive=True),
        ).receipt

    assert outcome.audit_metadata["status_code"] == 204
    assert outcome.outcome == "succeeded"
    assert outcome.audit_metadata["mutation"] == "extras_deleted"


def test_dataset_failures_map_to_typed_errors_without_retry_on_client_errors() -> None:
    routes = _site_first(
        {
            ("GET", "http://127.0.0.1:5640/api/1/datasets/missing/"): (404, {"message": "Unknown dataset"}),
            ("GET", "http://127.0.0.1:5640/api/1/datasets/gone/"): (410, {"message": "Dataset has been deleted"}),
            ("POST", "http://127.0.0.1:5640/api/1/datasets/"): (400, {"errors": {"title": "required"}}),
        }
    )
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)
    with client:
        with pytest.raises(CatalogNotFoundError):
            client.datasets.get("missing")
        with pytest.raises(CatalogConflictError) as gone:
            client.datasets.get("gone")
        with pytest.raises(CatalogValidationError) as invalid:
            client.datasets.create(
                DatasetCreateInput(title="T", description="D"),
                permissions=_USER_PERMISSIONS,
                mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
            )

    assert gone.value.capability_state == "unavailable"
    invalid_receipt = cast(dict[str, object], invalid.value.metadata["receipt"])
    audit_metadata = cast(dict[str, object], invalid_receipt["audit_metadata"])
    assert audit_metadata["status_code"] == 400
    assert invalid_receipt["outcome"] == "failed"
    assert len([r for r in transport.requests if r.url.endswith("/api/1/site/")]) == 1
    assert len([r for r in transport.requests if r.url.endswith("/datasets/missing/")]) == 1
    assert len([r for r in transport.requests if r.url.endswith("/datasets/gone/")]) == 1
    assert len([r for r in transport.requests if r.method == "POST"]) == 1


def test_invalid_inputs_are_rejected_before_any_dispatch() -> None:
    routes = _site_first({})
    transport, client = _sync_client_with_transport(routes)
    with client:
        with pytest.raises(ValueError):
            client.datasets.create(DatasetCreateInput(title="", description="D"), permissions=_USER_PERMISSIONS)
        with pytest.raises(ValueError):
            client.datasets.list(DatasetListQuery(sort="-nope"))
        with pytest.raises(CatalogValidationError):
            client.datasets.get("")

    assert [r.url for r in transport.requests if "/api/" in r.url] == []


PARITY_ROUTES: dict[str, dict[tuple[str, str], object]] = {
    "list": {("GET", "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20"): _page_body()},
    "create": {("POST", "http://127.0.0.1:5640/api/1/datasets/"): (201, _dataset_doc("new"))},
    "atom": {("GET", "http://127.0.0.1:5640/api/1/datasets/recent.atom?page=1&page_size=20"): b"<feed/>"},
    "get": {("GET", "http://127.0.0.1:5640/api/1/datasets/abc/"): _dataset_doc()},
    "update": {("PUT", "http://127.0.0.1:5640/api/1/datasets/abc/"): _dataset_doc()},
    "delete": {("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/"): (204, None)},
    "feature": {("POST", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): _dataset_doc()},
    "unfeature": {("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): _dataset_doc()},
    "rdf_redirect": {
        (
            "GET",
            "http://127.0.0.1:5640/api/1/datasets/abc/rdf",
        ): (302, None, {"Location": "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl"}),
        ("GET", "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl"): b"<rdf/>",
    },
    "rdf_format": {
        ("GET", "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl"): (
            200,
            b"<rdf/>",
            {"Content-Type": "text/turtle"},
        )
    },
    "suggest": {("GET", "http://127.0.0.1:5640/api/1/datasets/suggest/?q=ab&size=3"): [{"id": "abc", "title": "T"}]},
    "search_v2": {("GET", "http://127.0.0.1:5640/api/2/datasets/search/?page=1&page_size=20&q=x"): _page_body()},
    "list_v2": {("GET", "http://127.0.0.1:5640/api/2/datasets/?page=1&page_size=20"): _page_body()},
    "get_v2": {("GET", "http://127.0.0.1:5640/api/2/datasets/abc/"): _dataset_doc()},
    "get_extras_v2": {("GET", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): {"keep": "v"}},
    "update_extras_v2": {("PUT", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (200, {"keep": "v"})},
    "delete_extras_v2": {("DELETE", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (204, {"keep": "v"})},
}


def _sync_calls(client: SyncUDataClient) -> dict[str, object]:
    credential = client.credentials
    assert isinstance(credential, UDataCredential)
    user_permissions = _permissions_for(credential)
    admin_permissions = _permissions_for(credential, admin=True)
    results: dict[str, object] = {}
    results["list"] = client.datasets.list().items[0].id.value
    results["create"] = client.datasets.create(
        DatasetCreateInput(title="T", description="D"),
        permissions=user_permissions,
        mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
    ).receipt.outcome
    results["atom"] = client.datasets.recent_atom().payload["media_type"]
    results["get"] = client.datasets.get("abc").id.value
    results["update"] = client.datasets.update(
        "abc",
        DatasetUpdateInput(title="New"),
        permissions=user_permissions,
        mutation_policy=_mutation_policy("udata/api-v1.update-dataset", "abc"),
    ).receipt.outcome
    results["delete"] = client.datasets.delete(
        "abc",
        permissions=user_permissions,
        mutation_policy=_mutation_policy("udata/api-v1.delete-dataset", "abc", destructive=True),
    ).receipt.outcome
    results["feature"] = client.datasets.feature(
        "abc",
        permissions=admin_permissions,
        mutation_policy=_mutation_policy("udata/api-v1.feature-dataset", "abc"),
    ).receipt.outcome
    results["unfeature"] = client.datasets.unfeature(
        "abc",
        permissions=admin_permissions,
        mutation_policy=_mutation_policy("udata/api-v1.unfeature-dataset", "abc", destructive=True),
    ).receipt.outcome
    rdf_result = client.datasets.rdf("abc")
    assert isinstance(rdf_result, DatasetMutationOutcome)
    results["rdf"] = rdf_result.outcome
    results["rdf_format"] = client.datasets.rdf_format("abc", "ttl").payload["media_type"]
    results["suggest"] = client.datasets.suggest(DatasetSuggestQuery(q="ab", size=3))[0].id.value
    results["search_v2"] = client.datasets.search_v2(DatasetSearchQuery(q="x")).items[0].id.value
    results["list_v2"] = client.datasets.list_v2().items[0].id.value
    results["get_v2"] = client.datasets.get_v2("abc").id.value
    results["get_extras_v2"] = client.datasets.get_extras_v2("abc")["keep"]
    results["update_extras_v2"] = client.datasets.update_extras_v2(
        "abc",
        DatasetExtrasUpdate({"added": 1, "gone": None}),
        permissions=user_permissions,
        mutation_policy=_mutation_policy("udata/api-v2.update-dataset-extras", "abc"),
    ).receipt.outcome
    results["delete_extras_v2"] = client.datasets.delete_extras_v2(
        "abc",
        DatasetExtrasDelete(keys=("gone",)),
        permissions=user_permissions,
        mutation_policy=_mutation_policy("udata/api-v2.delete-dataset-extras", "abc", destructive=True),
    ).receipt.outcome
    return results


def test_async_dataset_service_matches_sync_wire_exactly() -> None:
    routes = _site_first(dict((key, value) for family in PARITY_ROUTES.values() for key, value in family.items()))
    transport = RouterAsyncTransport(routes)
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=_USER_CREDENTIAL,
        owns_transport=False,
    )

    async def run() -> dict[str, object]:
        async with client as active:
            results: dict[str, object] = {}
            results["list"] = (await active.datasets.list()).items[0].id.value
            user_permissions = _permissions_for(_USER_CREDENTIAL)
            admin_permissions = _permissions_for(_USER_CREDENTIAL, admin=True)
            results["create"] = (
                await active.datasets.create(
                    DatasetCreateInput(title="T", description="D"),
                    permissions=user_permissions,
                    mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
                )
            ).receipt.outcome
            results["atom"] = (await active.datasets.recent_atom()).payload["media_type"]
            results["get"] = (await active.datasets.get("abc")).id.value
            results["update"] = (
                await active.datasets.update(
                    "abc",
                    DatasetUpdateInput(title="New"),
                    permissions=user_permissions,
                    mutation_policy=_mutation_policy("udata/api-v1.update-dataset", "abc"),
                )
            ).receipt.outcome
            results["delete"] = (
                await active.datasets.delete(
                    "abc",
                    permissions=user_permissions,
                    mutation_policy=_mutation_policy("udata/api-v1.delete-dataset", "abc", destructive=True),
                )
            ).receipt.outcome
            results["feature"] = (
                await active.datasets.feature(
                    "abc",
                    permissions=admin_permissions,
                    mutation_policy=_mutation_policy("udata/api-v1.feature-dataset", "abc"),
                )
            ).receipt.outcome
            results["unfeature"] = (
                await active.datasets.unfeature(
                    "abc",
                    permissions=admin_permissions,
                    mutation_policy=_mutation_policy("udata/api-v1.unfeature-dataset", "abc", destructive=True),
                )
            ).receipt.outcome
            rdf_result = await active.datasets.rdf("abc")
            assert isinstance(rdf_result, DatasetMutationOutcome)
            results["rdf"] = rdf_result.outcome
            results["rdf_format"] = (await active.datasets.rdf_format("abc", "ttl")).payload["media_type"]
            results["suggest"] = (await active.datasets.suggest(DatasetSuggestQuery(q="ab", size=3)))[0].id.value
            results["search_v2"] = (await active.datasets.search_v2(DatasetSearchQuery(q="x"))).items[0].id.value
            results["list_v2"] = (await active.datasets.list_v2()).items[0].id.value
            results["get_v2"] = (await active.datasets.get_v2("abc")).id.value
            results["get_extras_v2"] = (await active.datasets.get_extras_v2("abc"))["keep"]
            results["update_extras_v2"] = (
                await active.datasets.update_extras_v2(
                    "abc",
                    DatasetExtrasUpdate({"added": 1, "gone": None}),
                    permissions=user_permissions,
                    mutation_policy=_mutation_policy("udata/api-v2.update-dataset-extras", "abc"),
                )
            ).receipt.outcome
            results["delete_extras_v2"] = (
                await active.datasets.delete_extras_v2(
                    "abc",
                    DatasetExtrasDelete(keys=("gone",)),
                    permissions=user_permissions,
                    mutation_policy=_mutation_policy("udata/api-v2.delete-dataset-extras", "abc", destructive=True),
                )
            ).receipt.outcome
            return results

    async_results = asyncio.run(run())

    sync_routes = _site_first(dict((key, value) for family in PARITY_ROUTES.values() for key, value in family.items()))
    sync_transport, sync_client = _sync_client_with_transport(sync_routes, _USER_CREDENTIAL)
    with sync_client:
        sync_results = _sync_calls(sync_client)

    async_urls = [r.url for r in transport.requests if not r.url.endswith("/api/1/site/")]
    sync_urls = [r.url for r in sync_transport.requests if not r.url.endswith("/api/1/site/")]
    assert async_urls == sync_urls
    assert len(async_urls) == 17
    assert async_results == sync_results


def test_unimplemented_native_operation_returns_typed_error() -> None:
    routes = _site_first({})
    transport, client = _sync_client_with_transport(routes)
    with client:
        resources_op = next(op for op in declared_udata_profile().operations if "resource" in op.method)
        with pytest.raises(NativeCatalogError, match="tracer slice"):
            client.datasets_list(
                CatalogOperationRequest(operation_id=resources_op, payload={}),
                CatalogOperationGuard(operation_id=resources_op),
            )
        assert [r.url for r in transport.requests if "resources" in r.url] == []


def test_cr01_mutations_without_credentials_fail_closed_before_dispatch() -> None:
    routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/"): (201, _dataset_doc("new"))})
    with _sync_client(routes) as client:
        with pytest.raises(UnauthenticatedError) as excinfo:
            client.datasets.create(DatasetCreateInput(title="T", description="D"), permissions=_USER_PERMISSIONS)
        receipt = _receipt_from(excinfo.value)
        assert receipt.outcome == "rejected"
        assert receipt.target.value.startswith("request:")

    assert [r.url for r in transport_requests(client) if "/api/1/datasets/" in r.url] == []


def test_cr01_feature_requires_admin_role_evidence() -> None:
    routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): _dataset_doc()})
    transport, client = _sync_client_with_transport(routes, _USER_KEY_CREDENTIAL)
    with client:
        with pytest.raises(ForbiddenError) as excinfo:
            client.datasets.feature(
                "abc",
                permissions=EffectivePermissions.for_credential(_USER_KEY_CREDENTIAL, platform=CatalogPlatform.UDATA),
            )

    receipt = _receipt_from(excinfo.value)
    assert receipt.outcome == "rejected"
    assert [r for r in transport.requests if "/featured/" in r.url] == []


def test_cr02_rejected_mutations_carry_redacted_receipts() -> None:
    routes = _site_first({})
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)
    with client:
        with pytest.raises(ForbiddenError) as excinfo:
            client.datasets.feature("abc", permissions=_USER_PERMISSIONS)

    receipt = _receipt_from(excinfo.value)
    assert receipt.operation == "udata/api-v1.feature-dataset"
    assert receipt.target.value == "abc"
    assert receipt.outcome == "rejected"


def test_cr03_destructive_calls_are_never_auto_retried() -> None:
    class FlakyTransport(RouterTransport):
        def __init__(self, routes: dict[tuple[str, str], object]) -> None:
            super().__init__(routes)
            self.delete_sends = 0

        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            if request.method == "DELETE" and request.url.endswith("/api/1/datasets/abc/"):
                self.delete_sends += 1
                return RuntimeResponse(status_code=500, headers={}, body=b"{}")
            return super().send(request)

    transport = FlakyTransport(_site_first({}))
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=UDataCredential(api_key="secret-key"),
        max_attempts=3,
        owns_transport=False,
    )
    with client:
        with pytest.raises(CatalogUnavailableError) as excinfo:
            client.datasets.delete(
                "abc",
                _USER_PERMISSIONS,
                mutation_policy=_mutation_policy("udata/api-v1.delete-dataset", "abc", destructive=True),
            )

    assert transport.delete_sends == 1
    assert _receipt_from(excinfo.value).outcome == "failed"


@pytest.mark.parametrize(
    "idempotency",
    [
        pytest.param(IdempotencyPolicy(safe=True), id="safe"),
        pytest.param(IdempotencyPolicy(explicit_retry_opt_in=True), id="explicit-retry-opt-in"),
    ],
)
def test_cr01_retry_enabled_idempotency_is_rejected_before_sync_mutation_dispatch(
    idempotency: IdempotencyPolicy,
) -> None:
    delete_url = "http://127.0.0.1:5640/api/1/datasets/abc/"
    transport, client = _sync_client_with_transport(
        _site_first({("DELETE", delete_url): (503, {"message": "retryable"})}), _USER_CREDENTIAL
    )
    policy = MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.delete-dataset", target="abc"),
        concurrency=ConcurrencyPolicy(overwrite=True),
        idempotency=idempotency,
    )

    with client:
        with pytest.raises(ForbiddenError) as raised:
            client.datasets.delete("abc", _USER_PERMISSIONS, mutation_policy=policy)

    assert _receipt_from(raised.value).outcome == "rejected"
    assert [request for request in transport.requests if request.url == delete_url] == []


@pytest.mark.parametrize(
    "idempotency",
    [
        pytest.param(IdempotencyPolicy(safe=True), id="safe"),
        pytest.param(IdempotencyPolicy(explicit_retry_opt_in=True), id="explicit-retry-opt-in"),
    ],
)
def test_cr01_retry_enabled_idempotency_is_rejected_before_async_mutation_dispatch(
    idempotency: IdempotencyPolicy,
) -> None:
    delete_url = "http://127.0.0.1:5640/api/1/datasets/abc/"
    transport = RouterAsyncTransport(_site_first({("DELETE", delete_url): (503, {"message": "retryable"})}))
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=_USER_CREDENTIAL,
        owns_transport=False,
    )
    policy = MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.delete-dataset", target="abc"),
        concurrency=ConcurrencyPolicy(overwrite=True),
        idempotency=idempotency,
    )

    async def run() -> ForbiddenError:
        async with client:
            with pytest.raises(ForbiddenError) as raised:
                await client.datasets.delete("abc", _USER_PERMISSIONS, mutation_policy=policy)
        return raised.value

    error = asyncio.run(run())

    assert _receipt_from(error).outcome == "rejected"
    assert [request for request in transport.requests if request.url == delete_url] == []


def test_cr04_capability_evidence_stays_scoped_to_its_route() -> None:
    routes = _site_first(
        {
            ("POST", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): (403, {"message": "admin only"}),
            ("GET", "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20"): _page_body(),
        }
    )
    transport, client = _sync_client_with_transport(routes, _USER_KEY_CREDENTIAL)
    with client:
        with pytest.raises(ForbiddenError) as excinfo:
            client.datasets.feature(
                "abc",
                permissions=_USER_KEY_ADMIN_PERMISSIONS,
                mutation_policy=_mutation_policy("udata/api-v1.feature-dataset", "abc"),
            )
        envelope = client.datasets.list()

    assert len(envelope.items) == 1
    assert _receipt_from(excinfo.value).operation == "udata/api-v1.feature-dataset"


def test_wr03_read_only_mutation_status_maps_to_deployment_disabled() -> None:
    routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/"): (423, {"message": "read-only"})})
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)

    with client:
        with pytest.raises(CatalogUnavailableError) as raised:
            client.datasets.create(
                DatasetCreateInput(title="T", description="D"),
                permissions=_USER_PERMISSIONS,
                mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
            )

    assert raised.value.capability_state == "deployment-disabled"
    receipt = _receipt_from(raised.value)
    assert receipt.outcome == "failed"
    assert receipt.audit_metadata["status_code"] == 423


def test_cr05_configured_probe_runner_is_used_for_effective_evidence() -> None:
    from datasluice.domain.catalog.operations import OperationId
    from datasluice.domain.catalog.profiles import (
        CredentialClassification,
        ProbeEvidence,
        ProbeResponseClass,
        RoleClassification,
    )

    op = OperationId(platform="udata", service="api-v1", method="list-datasets")
    calls: list[OperationId] = []

    class Runner:
        def probe(self, operation_id: OperationId) -> ProbeEvidence:
            calls.append(operation_id)
            return ProbeEvidence(
                operation_id=operation_id,
                deployment_url="https://datasets.example.com/api/1/datasets/",
                credential_classification=CredentialClassification.ANONYMOUS,
                role_classification=RoleClassification.ANONYMOUS,
                observed_response_class=ProbeResponseClass.SUCCESS,
            )

    routes: dict[tuple[str, str], object] = {
        (
            "GET",
            "https://datasets.example.com/api/1/site/",
        ): json.dumps(
            {"feed_size": 0, "id": "s", "keywords": [], "metrics": {}, "title": "uData", "version": "17.6.0"}
        ).encode(),
        ("GET", "https://datasets.example.com/api/1/datasets/?page=1&page_size=20"): _page_body(),
    }
    transport = RouterTransport(routes)
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="https://datasets.example.com",
        probe_runner=Runner(),
        owns_transport=False,
    )
    with client:
        client.datasets.list()

    assert calls == [op]


def test_cr05_foreign_origin_probe_evidence_is_rejected() -> None:
    from datasluice.domain.catalog.profiles import (
        CredentialClassification,
        ProbeEvidence,
        ProbeResponseClass,
        RoleClassification,
    )

    class ForeignRunner:
        def probe(self, operation_id: OperationId) -> ProbeEvidence:
            return ProbeEvidence(
                operation_id=operation_id,
                deployment_url="https://elsewhere.example.com/api/1/datasets/",
                credential_classification=CredentialClassification.ANONYMOUS,
                role_classification=RoleClassification.ANONYMOUS,
                observed_response_class=ProbeResponseClass.SUCCESS,
            )

    routes: dict[tuple[str, str], object] = {
        (
            "GET",
            "https://datasets.example.com/api/1/site/",
        ): json.dumps(
            {"feed_size": 0, "id": "s", "keywords": [], "metrics": {}, "title": "uData", "version": "17.6.0"}
        ).encode()
    }
    client = SyncUDataClient(
        RouterTransport(routes),
        declared_udata_profile(),
        origin="https://datasets.example.com",
        probe_runner=ForeignRunner(),
        owns_transport=False,
    )
    with client:
        with pytest.raises(CatalogError, match="deployment origin"):
            client.datasets.list()


def test_cr05_capability_evidence_is_bound_to_deployment_and_credential_scope() -> None:
    from datasluice.domain.catalog.profiles import (
        CredentialClassification,
        ProbeEvidence,
        ProbeResponseClass,
        RoleClassification,
    )
    from datasluice.runtime.capability import EffectiveCapabilityCache

    operation_id = next(
        operation_id for operation_id in declared_udata_profile().operations if operation_id.method == "list-datasets"
    )
    calls = 0

    class Runner:
        def probe(self, operation_id: OperationId) -> ProbeEvidence:
            nonlocal calls
            calls += 1
            return ProbeEvidence(
                operation_id=operation_id,
                deployment_url="https://datasets.example.com/api/1/datasets/",
                credential_classification=CredentialClassification.AUTHENTICATED,
                role_classification=RoleClassification.USER,
                observed_response_class=ProbeResponseClass.SUCCESS,
                credential_scope="scope-a",
            )

    cache = EffectiveCapabilityCache(
        declared_udata_profile(),
        Runner(),
        namespace="https://datasets.example.com",
        deployment_origin="https://datasets.example.com",
    )
    cache.resolve(operation_id, credential_scope="scope-a")
    with pytest.raises(CatalogValidationError, match="invalid evidence"):
        cache.resolve(operation_id, credential_scope="scope-b")

    assert calls == 2


def test_wr01_identifiers_and_query_values_are_encoded() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/a%20b/"): _dataset_doc()})
    with _sync_client(routes) as client:
        record = client.datasets.get("a b")

    assert record.id.value == "abc"


def test_wr02_repeated_tags_encode_as_repeated_keys() -> None:
    from urllib.parse import parse_qs

    routes = _site_first(
        {("GET", "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20&tag=a&tag=b"): _page_body()}
    )
    transport, client = _sync_client_with_transport(routes)
    with client:
        client.datasets.list(DatasetListQuery(filters={"tag": ("a", "b")}))

    parsed = parse_qs(transport.requests[-1].url.split("?", 1)[1])
    assert parsed["tag"] == ["a", "b"]


def test_wr05_v2_search_retains_facets_and_native_links() -> None:
    body = json.dumps(
        {
            "data": [_dataset_doc()],
            "next_page": "http://127.0.0.1:5640/api/2/datasets/search/?page=2",
            "page": 1,
            "page_size": 20,
            "previous_page": None,
            "total": 1,
            "facets": {"tag": {"open": 1}},
        }
    ).encode()
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/2/datasets/search/?page=1&page_size=20"): body})
    with _sync_client(routes) as client:
        envelope = client.datasets.search_v2(DatasetSearchQuery())

    assert envelope.platform is not None
    extensions = envelope.platform.extensions
    assert extensions["udata.facets"] == {"tag": {"open": 1}}
    assert "page=2" in str(extensions["udata.nextpage"])


def test_wr08_v2_search_rejects_undocumented_filters_and_ranges() -> None:
    with pytest.raises(ValueError, match="Unknown dataset search filters"):
        DatasetSearchQuery(filters={"private": True})
    with pytest.raises(ValueError, match="last_update_range"):
        DatasetSearchQuery(filters={"last_update_range": "yesterday"})


def test_cr01_mutation_permission_evidence_must_match_platform_identity() -> None:
    routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/"): (201, _dataset_doc("new"))})
    wrong_platform = EffectivePermissions.for_credential(_USER_CREDENTIAL, platform=CatalogPlatform.CKAN)
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)

    with client:
        with pytest.raises(ForbiddenError) as raised:
            client.datasets.create(
                DatasetCreateInput(title="T", description="D"),
                permissions=wrong_platform,
                mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
            )

    assert _receipt_from(raised.value).outcome == "rejected"
    assert not [request for request in transport.requests if request.method == "POST"]


def test_cr01_unauthenticated_permission_claims_cannot_authorize_mutation() -> None:
    permissions = EffectivePermissions.for_credential(
        _USER_CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"}), authenticated=False
    )
    transport, client = _sync_client_with_transport(_site_first({}), _USER_CREDENTIAL)

    with client:
        with pytest.raises(ForbiddenError) as raised:
            client.datasets.update(
                "abc",
                DatasetUpdateInput(title="New"),
                permissions=permissions,
                mutation_policy=_mutation_policy("udata/api-v1.update-dataset", "abc"),
            )

    assert _receipt_from(raised.value).outcome == "rejected"
    assert [request for request in transport.requests if "/api/" in request.url] == []


def test_cr02_malformed_successful_mutation_is_ambiguous_and_receipt_bearing() -> None:
    routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/"): (200, b"not-json")})
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)

    with client:
        with pytest.raises(NativeCatalogError) as raised:
            client.datasets.create(
                DatasetCreateInput(title="T", description="D"),
                permissions=_USER_PERMISSIONS,
                mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
            )

    receipt = _receipt_from(raised.value)
    assert receipt.outcome == "ambiguous"
    assert receipt.audit_metadata["status_code"] == 200
    assert receipt.target.value.startswith("request:")
    assert len([request for request in transport.requests if request.method == "POST"]) == 1


def test_cr02_transport_failure_keeps_an_ambiguous_receipt_on_the_original_error() -> None:
    class FailingTransport(RouterTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            if request.method == "POST":
                self.requests.append(request)
                raise TransportFailure("connection dropped after dispatch")
            return super().send(request)

    transport = FailingTransport(_site_first({}))
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=_USER_CREDENTIAL,
        owns_transport=False,
    )

    with client:
        with pytest.raises(TransportFailure) as raised:
            client.datasets.create(
                DatasetCreateInput(title="T", description="D"),
                permissions=_USER_PERMISSIONS,
                mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
            )

    assert _receipt_from(raised.value).outcome == "ambiguous"


def test_cr02_policy_rejection_has_a_shared_receipt_without_site_probe() -> None:
    transport, client = _sync_client_with_transport(_site_first({}), _USER_CREDENTIAL)

    with client:
        with pytest.raises(ForbiddenError) as raised:
            client.datasets.update("abc", DatasetUpdateInput(title="New"), permissions=_USER_PERMISSIONS)

    assert _receipt_from(raised.value).outcome == "rejected"
    assert [request for request in transport.requests if "/api/" in request.url] == []


def test_cr03_idempotency_key_is_not_silently_treated_as_retry_authorization() -> None:
    policy = MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.delete-dataset", target="abc"),
        concurrency=ConcurrencyPolicy(overwrite=True),
        idempotency=IdempotencyPolicy(key="request-once"),
    )
    transport, client = _sync_client_with_transport(_site_first({}), _USER_CREDENTIAL)

    with client:
        with pytest.raises(ForbiddenError) as raised:
            client.datasets.delete("abc", _USER_PERMISSIONS, mutation_policy=policy)

    assert _receipt_from(raised.value).outcome == "rejected"
    assert [request for request in transport.requests if request.method == "DELETE"] == []


def test_cr06_create_receipt_uses_server_id_and_redacts_request_target() -> None:
    title = "Bearer topsecretvalue"
    routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/"): (201, _dataset_doc("server-id"))})
    transport, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)

    with client:
        result = client.datasets.create(
            DatasetCreateInput(title=title, description="D"),
            permissions=_USER_PERMISSIONS,
            mutation_policy=_mutation_policy("udata/api-v1.create-dataset", title),
        )

    assert result.receipt.target.value == "server-id"
    assert title not in str(result.receipt.to_dict())
    assert transport.requests[-1].body is not None


def test_wr01_rdf_encodes_once_and_rejects_dot_segments() -> None:
    method, path, _, _ = wire.rdf_request("a b", None)
    assert method == "GET"
    assert path == "/api/1/datasets/a%20b/rdf"

    with pytest.raises(CatalogValidationError):
        wire.rdf_request("..", None)


def test_wr01_rdf_format_rejects_overlong_credential_shaped_extensions_without_echo() -> None:
    credential_shaped_format = "api_key_" + "A" * 128

    with pytest.raises(CatalogValidationError) as raised:
        wire.media_type_for_format(credential_shaped_format)

    message = str(raised.value)
    assert message == "The uData RDF format is invalid."
    assert credential_shaped_format not in message


def test_wr01_suggestion_ids_remain_raw_for_later_request_encoding() -> None:
    routes = _site_first(
        {("GET", "http://127.0.0.1:5640/api/1/datasets/suggest/?q=ab&size=3"): [{"id": "a b", "title": "T"}]}
    )
    with _sync_client(routes) as client:
        suggestions = client.datasets.suggest(DatasetSuggestQuery(q="ab", size=3))

    assert suggestions[0].id.value == "a b"


def test_wr03_wr04_final_rdf_is_decoded_from_bytes_with_case_insensitive_media_type() -> None:
    routes = _site_first(
        {
            (
                "GET",
                "http://127.0.0.1:5640/api/1/datasets/abc/rdf",
            ): (200, b"<rdf/>", {"cOnTeNt-TyPe": "application/rdf+xml; charset=utf-8"})
        }
    )
    with _sync_client(routes) as client:
        record = client.datasets.rdf("abc")

    assert isinstance(record, NativeRecord)
    assert record.payload["media_type"] == "application/rdf+xml"
    assert "body" not in record.payload


def test_wr04_invalid_text_bytes_raise_a_typed_native_error() -> None:
    routes = _site_first(
        {
            (
                "GET",
                "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl",
            ): (200, b"\xff", {"Content-Type": "text/turtle"})
        }
    )
    with _sync_client(routes) as client:
        with pytest.raises(NativeCatalogError, match="UTF-8"):
            client.datasets.rdf_format("abc", "ttl")


def test_wr05_v1_page_retains_previous_link_and_field_presence() -> None:
    body = {
        "data": [_dataset_doc()],
        "page": 2,
        "page_size": 20,
        "previous_page": "http://127.0.0.1:5640/api/1/datasets/?page=1",
        "next_page": None,
        "total": 3,
    }
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20"): body})
    with _sync_client(routes) as client:
        envelope = client.datasets.list()

    assert envelope.native_page.previous_page == "http://127.0.0.1:5640/api/1/datasets/?page=1"
    assert "previous_page" in envelope.native_page.present_fields
    assert envelope.platform is not None
    metadata = cast(dict[str, object], envelope.platform.extensions["udata.page"])
    assert metadata["previous_page"] == "http://127.0.0.1:5640/api/1/datasets/?page=1"
    assert "previous_page" in cast(list[str], metadata["present_fields"])


def test_wr06_malformed_documented_dataset_fields_fail_with_route_identity() -> None:
    malformed = {**_dataset_doc(), "private": "false"}
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/abc/"): malformed})
    with _sync_client(routes) as client:
        with pytest.raises(CatalogValidationError) as raised:
            client.datasets.get("abc")

    assert raised.value.operation == "udata/api-v1.get-dataset"
    with pytest.raises(CatalogValidationError):
        wire.parse_suggestions([{"id": "abc"}], operation="udata/api-v1.suggest-datasets")


def test_wr07_nested_extras_inputs_and_results_are_json_safe_and_immutable() -> None:
    update = DatasetExtrasUpdate({"nested": {"items": [1, 2]}})
    assert update.payload() == {"nested": {"items": [1, 2]}}
    with pytest.raises(ValueError):
        DatasetExtrasUpdate({"not-finite": float("nan")})

    routes = _site_first(
        {("PUT", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (200, {"nested": {"items": [1, 2]}})}
    )
    _, client = _sync_client_with_transport(routes, _USER_CREDENTIAL)
    with client:
        result = client.datasets.update_extras_v2(
            "abc",
            update,
            permissions=_USER_PERMISSIONS,
            mutation_policy=_mutation_policy("udata/api-v2.update-dataset-extras", "abc"),
        )

    with pytest.raises(TypeError):
        cast(dict[str, object], result.extras)["nested"] = {}
    nested = cast(Mapping[str, object], result.extras)["nested"]
    with pytest.raises(TypeError):
        cast(dict[str, object], nested)["items"] = []


def test_wr08_async_mutations_use_async_credential_resolution() -> None:
    class AsyncOnlyProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def resolve_async(self) -> UDataCredential:
            self.calls += 1
            return _USER_CREDENTIAL

    async def run() -> None:
        provider = AsyncOnlyProvider()
        routes = _site_first(
            {
                ("POST", "http://127.0.0.1:5640/api/1/datasets/"): (201, _dataset_doc("new")),
                ("DELETE", "http://127.0.0.1:5640/api/1/datasets/new/"): (204, None),
            }
        )
        transport = RouterAsyncTransport(routes)
        client = AsyncUDataClient(
            transport,
            declared_udata_profile(),
            origin="http://127.0.0.1:5640",
            credentials=provider,
            owns_transport=False,
        )
        async with client:
            result = await client.datasets.create(
                DatasetCreateInput(title="T", description="D"),
                permissions=_USER_PERMISSIONS,
                mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
            )
            deleted = await client.datasets.delete(
                "new",
                permissions=_USER_PERMISSIONS,
                mutation_policy=_mutation_policy("udata/api-v1.delete-dataset", "new", destructive=True),
            )
        assert result.record is not None
        assert deleted.receipt.audit_metadata["mutation"] == "deleted"
        assert provider.calls >= 1
        assert [request.method for request in transport.requests if request.method == "DELETE"] == ["DELETE"]

    asyncio.run(run())


def test_wr09_async_mutation_failure_preserves_the_same_ambiguous_receipt() -> None:
    async def run() -> None:
        routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/"): (200, b"not-json")})
        transport = RouterAsyncTransport(routes)
        client = AsyncUDataClient(
            transport,
            declared_udata_profile(),
            origin="http://127.0.0.1:5640",
            credentials=_USER_CREDENTIAL,
            owns_transport=False,
        )
        async with client:
            with pytest.raises(NativeCatalogError) as raised:
                await client.datasets.create(
                    DatasetCreateInput(title="T", description="D"),
                    permissions=_USER_PERMISSIONS,
                    mutation_policy=_mutation_policy("udata/api-v1.create-dataset", "T"),
                )
        receipt = _receipt_from(raised.value)
        assert receipt.outcome == "ambiguous"
        assert receipt.audit_metadata["status_code"] == 200
        assert [request.method for request in transport.requests if request.method == "POST"] == ["POST"]

    asyncio.run(run())


def test_wr10_query_models_reject_non_scalar_filter_values_at_construction() -> None:
    with pytest.raises(ValueError):
        DatasetListQuery(sort=cast(str, 1))
    with pytest.raises(ValueError):
        DatasetListQuery(filters=cast(Mapping[str, str | bool | tuple[str, ...]], {"tag": 1}))
    with pytest.raises(ValueError, match="last_update_range"):
        DatasetSearchQuery(
            filters=cast(Mapping[str, str | bool | tuple[str, ...]], {"last_update_range": ("last_30_days",)})
        )


def test_wr13_error_metadata_is_deeply_immutable_and_finite() -> None:
    error = NativeCatalogError(
        "bad response", operation="udata/api-v1.get-dataset", platform="udata", metadata={"nested": {"value": 1}}
    )
    nested = cast(Mapping[str, object], error.metadata["nested"])
    with pytest.raises(TypeError):
        cast(dict[str, object], nested)["value"] = 2
    coerced = NativeCatalogError(
        "bad response", operation="udata/api-v1.get-dataset", platform="udata", metadata={"value": float("nan")}
    )
    assert isinstance(coerced.metadata["value"], str)


def transport_requests(client: SyncUDataClient) -> list[RuntimeRequest]:
    transport = client._transport
    assert isinstance(transport, RouterTransport)
    return transport.requests
