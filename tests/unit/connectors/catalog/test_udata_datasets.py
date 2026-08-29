"""Exact-wire and failure evidence for the complete uData dataset family."""

from __future__ import annotations

import asyncio
import json
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
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import NativeRecord
from datasluice.domain.catalog.safety import (
    ConcurrencyPolicy,
    ConfirmationPolicy,
    MutationPolicy,
)
from datasluice.errors.catalog import (
    CatalogError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    UnauthenticatedError,
)
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

_ADMIN_PERMISSIONS = EffectivePermissions(
    platform=CatalogPlatform.UDATA, authenticated=True, roles=frozenset({"admin"})
)
_USER_PERMISSIONS = EffectivePermissions(platform=CatalogPlatform.UDATA, authenticated=True)
_CONFIRMED_POLICY = MutationPolicy(
    confirmation=ConfirmationPolicy(confirmed=True),
    concurrency=ConcurrencyPolicy(overwrite=True),
)


def _respond(body: object) -> RuntimeResponse:
    if isinstance(body, bytes):
        return RuntimeResponse(status_code=200, headers={"Content-Type": "text/xml"}, body=body)
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

    def _respond(self, body: object) -> RuntimeResponse:
        if isinstance(body, bytes):
            return RuntimeResponse(status_code=200, headers={"Content-Type": "text/xml"}, body=body)
        if isinstance(body, tuple) and body and isinstance(body[0], int):
            status, payload = body[0], body[1]
            headers = body[2] if len(body) > 2 else {}
        else:
            status, payload, headers = 200, body, {}
        encoded = b"" if payload is None else payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        return RuntimeResponse(status_code=status, headers=headers, body=encoded)

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
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="secret-key"))
    with client:
        record = client.datasets.create(
            DatasetCreateInput(title="T", description="D", private=True), permissions=_USER_PERMISSIONS
        )

    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"title": "T", "description": "D", "private": True}
    assert request.headers.get("Content-Type") == "application/json"
    assert request.headers.get("X-API-KEY") == "secret-key"
    record_value = record.record
    assert record_value is not None and record_value.id.value == "new"
    assert record.receipt.outcome == "created" and record.receipt.status_code == 201


def test_row41_recent_atom_returns_typed_text_document() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/recent.atom?page=1&page_size=20"): b"<feed/>"})
    with _sync_client(routes) as client:
        record = client.datasets.recent_atom()

    assert record.payload["media_type"] == "application/atom+xml"
    assert record.payload["body"] == "<feed/>"
    assert record.payload["sha256"]


def test_row42_get_dataset_exact_path() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/abc/"): _dataset_doc()})
    with _sync_client(routes) as client:
        record = client.datasets.get("abc")

    assert record.id.value == "abc"


def test_row43_update_dataset_omits_absent_fields() -> None:
    routes = _site_first({("PUT", "http://127.0.0.1:5640/api/1/datasets/abc/"): _dataset_doc()})
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="secret-key"))
    with client:
        client.datasets.update("abc", DatasetUpdateInput(title="New"), permissions=_USER_PERMISSIONS)

    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"title": "New"}


def test_row44_delete_dataset_returns_redacted_receipt() -> None:
    routes = _site_first({("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/?send_legal_notice=true"): (204, None)})
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="secret-key"))
    with client:
        outcome = client.datasets.delete(
            "abc",
            _USER_PERMISSIONS,
            DatasetDeleteOptions(send_legal_notice=True),
            mutation_policy=_CONFIRMED_POLICY,
        ).receipt

    assert isinstance(outcome, DatasetMutationOutcome)
    assert outcome.status_code == 204
    assert outcome.outcome == "deleted"
    assert outcome.dataset_id == "abc"


def test_rows45_46_feature_transitions_use_exact_methods() -> None:
    routes = _site_first(
        {
            ("POST", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): _dataset_doc(),
            ("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): _dataset_doc(),
        }
    )
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="admin-key"))
    with client:
        featured = client.datasets.feature("abc", permissions=_ADMIN_PERMISSIONS)
        unfeatured = client.datasets.unfeature("abc", permissions=_ADMIN_PERMISSIONS)

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
    assert outcome.status_code == 302
    assert outcome.outcome == "redirect:/api/1/datasets/abc/rdf.ttl"


def test_row48_rdf_format_returns_document() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl"): b"<rdf/>"})
    with _sync_client(routes) as client:
        record = client.datasets.rdf_format("abc", "ttl")

    assert record.payload["body"] == "<rdf/>"


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
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="secret-key"))
    with client:
        assert client.datasets.get_extras_v2("abc") == {"keep": "v"}
        result = client.datasets.update_extras_v2(
            "abc", DatasetExtrasUpdate({"added": 1, "gone": None}), permissions=_USER_PERMISSIONS
        )

    assert result.extras == {"keep": "v", "added": 1}
    assert result.receipt.outcome == "extras_updated"
    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"added": 1, "gone": None}


def test_row80_extras_delete_returns_receipt_from_204_with_body() -> None:
    routes = _site_first({("DELETE", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (204, {"keep": "v"})})
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="secret-key"))
    with client:
        outcome = client.datasets.delete_extras_v2(
            "abc", DatasetExtrasDelete(keys=("gone",)), permissions=_USER_PERMISSIONS, mutation_policy=_CONFIRMED_POLICY
        ).receipt

    assert outcome.status_code == 204
    assert outcome.outcome == "extras_deleted"


def test_dataset_failures_map_to_typed_errors_without_retry_on_client_errors() -> None:
    routes = _site_first(
        {
            ("GET", "http://127.0.0.1:5640/api/1/datasets/missing/"): (404, {"message": "Unknown dataset"}),
            ("GET", "http://127.0.0.1:5640/api/1/datasets/gone/"): (410, {"message": "Dataset has been deleted"}),
            ("POST", "http://127.0.0.1:5640/api/1/datasets/"): (400, {"errors": {"title": "required"}}),
        }
    )
    from datasluice.errors.catalog import CatalogConflictError, CatalogNotFoundError, CatalogValidationError

    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="secret-key"))
    with client:
        with pytest.raises(CatalogNotFoundError):
            client.datasets.get("missing")
        with pytest.raises(CatalogConflictError) as gone:
            client.datasets.get("gone")
        with pytest.raises(CatalogValidationError) as invalid:
            client.datasets.create(DatasetCreateInput(title="T", description="D"), permissions=_USER_PERMISSIONS)

    assert gone.value.capability_state == "unavailable"
    invalid_receipt = cast(dict[str, object], invalid.value.metadata["receipt"])
    assert invalid_receipt["status_code"] == 400
    assert invalid_receipt["outcome"] == "failed"
    probes = [r for r in transport.requests if r.url.endswith("/api/1/site/")]
    assert len(probes) == 1
    probes = [r for r in transport.requests if r.url.endswith("/api/1/site/")]
    assert len(probes) == 1


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
    "rdf_format": {("GET", "http://127.0.0.1:5640/api/1/datasets/abc/rdf.ttl"): b"<rdf/>"},
    "suggest": {("GET", "http://127.0.0.1:5640/api/1/datasets/suggest/?q=ab&size=3"): [{"id": "abc", "title": "T"}]},
    "search_v2": {("GET", "http://127.0.0.1:5640/api/2/datasets/search/?page=1&page_size=20&q=x"): _page_body()},
    "list_v2": {("GET", "http://127.0.0.1:5640/api/2/datasets/?page=1&page_size=20"): _page_body()},
    "get_v2": {("GET", "http://127.0.0.1:5640/api/2/datasets/abc/"): _dataset_doc()},
    "get_extras_v2": {("GET", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): {"keep": "v"}},
    "update_extras_v2": {("PUT", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (200, {"keep": "v"})},
    "delete_extras_v2": {("DELETE", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (204, {"keep": "v"})},
}


def _sync_calls(client: SyncUDataClient) -> None:
    client.datasets.list()
    client.datasets.create(DatasetCreateInput(title="T", description="D"), permissions=_USER_PERMISSIONS)
    client.datasets.recent_atom()
    client.datasets.get("abc")
    client.datasets.update("abc", DatasetUpdateInput(title="New"), permissions=_USER_PERMISSIONS)
    client.datasets.delete("abc", permissions=_USER_PERMISSIONS, mutation_policy=_CONFIRMED_POLICY)
    client.datasets.feature("abc", permissions=_ADMIN_PERMISSIONS)
    client.datasets.unfeature("abc", permissions=_ADMIN_PERMISSIONS)
    client.datasets.rdf("abc")
    client.datasets.rdf_format("abc", "ttl")
    client.datasets.suggest(DatasetSuggestQuery(q="ab", size=3))
    client.datasets.search_v2(DatasetSearchQuery(q="x"))
    client.datasets.list_v2()
    client.datasets.get_v2("abc")
    client.datasets.get_extras_v2("abc")
    client.datasets.update_extras_v2(
        "abc", DatasetExtrasUpdate({"added": 1, "gone": None}), permissions=_USER_PERMISSIONS
    )
    client.datasets.delete_extras_v2(
        "abc", DatasetExtrasDelete(keys=("gone",)), permissions=_USER_PERMISSIONS, mutation_policy=_CONFIRMED_POLICY
    )


def test_async_dataset_service_matches_sync_wire_exactly() -> None:
    routes = _site_first(dict((key, value) for family in PARITY_ROUTES.values() for key, value in family.items()))
    transport = RouterAsyncTransport(routes)
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=UDataCredential(api_key="secret-key"),
        owns_transport=False,
    )

    async def run() -> None:
        async with client as active:
            await active.datasets.list()
            await active.datasets.create(DatasetCreateInput(title="T", description="D"), permissions=_USER_PERMISSIONS)
            await active.datasets.recent_atom()
            await active.datasets.get("abc")
            await active.datasets.update("abc", DatasetUpdateInput(title="New"), permissions=_USER_PERMISSIONS)
            await active.datasets.delete("abc", permissions=_USER_PERMISSIONS, mutation_policy=_CONFIRMED_POLICY)
            await active.datasets.feature("abc", permissions=_ADMIN_PERMISSIONS)
            await active.datasets.unfeature("abc", permissions=_ADMIN_PERMISSIONS)
            await active.datasets.rdf("abc")
            await active.datasets.rdf_format("abc", "ttl")
            await active.datasets.suggest(DatasetSuggestQuery(q="ab", size=3))
            await active.datasets.search_v2(DatasetSearchQuery(q="x"))
            await active.datasets.list_v2()
            await active.datasets.get_v2("abc")
            await active.datasets.get_extras_v2("abc")
            await active.datasets.update_extras_v2(
                "abc", DatasetExtrasUpdate({"added": 1, "gone": None}), permissions=_USER_PERMISSIONS
            )
            await active.datasets.delete_extras_v2(
                "abc",
                DatasetExtrasDelete(keys=("gone",)),
                permissions=_USER_PERMISSIONS,
                mutation_policy=_CONFIRMED_POLICY,
            )

    asyncio.run(run())

    sync_routes = _site_first(dict((key, value) for family in PARITY_ROUTES.values() for key, value in family.items()))
    sync_transport, sync_client = _sync_client_with_transport(sync_routes, UDataCredential(api_key="secret-key"))
    with sync_client:
        _sync_calls(sync_client)

    async_urls = [r.url for r in transport.requests if not r.url.endswith("/api/1/site/")]
    sync_urls = [r.url for r in sync_transport.requests if not r.url.endswith("/api/1/site/")]
    assert async_urls == sync_urls
    assert len(async_urls) == 17


def test_unimplemented_native_operation_returns_typed_error() -> None:
    routes = _site_first({})
    transport, client = _sync_client_with_transport(routes)
    with client:
        from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
        from datasluice.errors.catalog import NativeCatalogError

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
        with pytest.raises(UnauthenticatedError):
            client.datasets.create(DatasetCreateInput(title="T", description="D"), permissions=_USER_PERMISSIONS)

    assert [r.url for r in transport_requests(client) if "/api/1/datasets/" in r.url] == []


def test_cr01_feature_requires_admin_role_evidence() -> None:
    routes = _site_first({("POST", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): _dataset_doc()})
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="user-key"))
    with client:
        with pytest.raises(ForbiddenError) as excinfo:
            client.datasets.feature("abc", permissions=_USER_PERMISSIONS)

    receipt = cast(dict[str, object], excinfo.value.metadata["receipt"])
    assert receipt["outcome"] == "rejected"
    assert [r for r in transport.requests if "/featured/" in r.url] == []


def test_cr02_rejected_mutations_carry_redacted_receipts() -> None:
    routes = _site_first({})
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="secret-key"))
    with client:
        with pytest.raises(ForbiddenError) as excinfo:
            client.datasets.feature("abc", permissions=_USER_PERMISSIONS)

    receipt = cast(dict[str, object], excinfo.value.metadata["receipt"])
    assert receipt["operation_id"] == "udata/api-v1.feature-dataset"
    assert receipt["dataset_id"] == "abc"
    assert receipt["outcome"] == "rejected"


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
        with pytest.raises(CatalogUnavailableError):
            client.datasets.delete("abc", _USER_PERMISSIONS, mutation_policy=_CONFIRMED_POLICY)

    assert transport.delete_sends == 1


def test_cr04_capability_evidence_stays_scoped_to_its_route() -> None:
    routes = _site_first(
        {
            ("POST", "http://127.0.0.1:5640/api/1/datasets/abc/featured/"): (403, {"message": "admin only"}),
            ("GET", "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20"): _page_body(),
        }
    )
    transport, client = _sync_client_with_transport(routes, UDataCredential(api_key="user-key"))
    with client:
        with pytest.raises(ForbiddenError):
            client.datasets.feature("abc", permissions=_ADMIN_PERMISSIONS)
        envelope = client.datasets.list()

    assert len(envelope.items) == 1


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
        probe_runner=Runner(),  # type: ignore[arg-type]
        owns_transport=False,
    )
    with client:
        client.datasets.list()

    assert calls == [op]


def test_cr05_foreign_origin_probe_evidence_is_rejected() -> None:
    from datasluice.domain.catalog.operations import OperationId
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
        probe_runner=ForeignRunner(),  # type: ignore[arg-type]
        owns_transport=False,
    )
    with client:
        with pytest.raises(CatalogError, match="deployment origin"):
            client.datasets.list()


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


def transport_requests(client: SyncUDataClient) -> list[RuntimeRequest]:
    transport = client._transport
    assert isinstance(transport, RouterTransport)
    return transport.requests
