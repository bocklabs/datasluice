"""Exact-wire and failure evidence for the complete uData dataset family."""

from __future__ import annotations

import asyncio
import json

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.datasets import (
    DatasetCreateInput,
    DatasetDeleteOptions,
    DatasetExtrasDelete,
    DatasetExtrasUpdate,
    DatasetListQuery,
    DatasetMutationOutcome,
    DatasetSuggestQuery,
    DatasetUpdateInput,
)
from datasluice.domain.catalog.models import NativeRecord
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse


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
        self._routes = routes
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0
        self.aclose_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        key = (request.method, request.url)
        if key not in self._routes:
            raise AssertionError(f"unexpected request {key}")
        body = self._routes[key]
        if isinstance(body, bytes):
            return RuntimeResponse(status_code=200, headers={"Content-Type": "text/xml"}, body=body)
        status, payload = body if isinstance(body, tuple) else (200, body)
        encoded = b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(status_code=status, headers={}, body=encoded)

    def close(self) -> None:
        self.close_count += 1


class RouterAsyncTransport:
    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self._routes = routes
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0
        self.aclose_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        body = self._routes[(request.method, request.url)]
        if isinstance(body, bytes):
            return RuntimeResponse(status_code=200, headers={"Content-Type": "text/xml"}, body=body)
        status, payload = body if isinstance(body, tuple) else (200, body)
        encoded = b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(status_code=status, headers={}, body=encoded)

    async def aclose(self) -> None:
        self.aclose_count += 1


def _sync_client(routes: dict[tuple[str, str], object]) -> SyncUDataClient:
    transport, client = _sync_client_with_transport(routes)
    return client


def _sync_client_with_transport(
    routes: dict[tuple[str, str], object],
) -> tuple[RouterTransport, SyncUDataClient]:
    transport = RouterTransport(routes)
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
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
    transport, client = _sync_client_with_transport(routes)
    with client:
        record = client.datasets.create(DatasetCreateInput(title="T", description="D", private=True))

    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"title": "T", "description": "D", "private": True}
    assert request.headers.get("Content-Type") == "application/json"
    assert record.id.value == "new"


def test_row41_recent_atom_returns_typed_text_document() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/recent.atom?page=1&page_size=20"): b"<feed/>"})
    with _sync_client(routes) as client:
        record = client.datasets.recent_atom()

    assert record.payload["content_type"] == "application/atom+xml"
    assert record.payload["body"] == "<feed/>"


def test_row42_get_dataset_exact_path() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/abc/"): _dataset_doc()})
    with _sync_client(routes) as client:
        record = client.datasets.get("abc")

    assert record.id.value == "abc"


def test_row43_update_dataset_omits_absent_fields() -> None:
    routes = _site_first({("PUT", "http://127.0.0.1:5640/api/1/datasets/abc/"): _dataset_doc()})
    transport, client = _sync_client_with_transport(routes)
    with client:
        client.datasets.update("abc", DatasetUpdateInput(title="New"))

    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"title": "New"}


def test_row44_delete_dataset_returns_redacted_receipt() -> None:
    routes = _site_first({("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/?send_legal_notice=true"): (204, None)})
    with _sync_client(routes) as client:
        outcome = client.datasets.delete("abc", DatasetDeleteOptions(send_legal_notice=True))

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
    transport, client = _sync_client_with_transport(routes)
    with client:
        featured = client.datasets.feature("abc")
        unfeatured = client.datasets.unfeature("abc")

    assert featured.id.value == unfeatured.id.value == "abc"
    methods = [r.method for r in transport.requests[-2:]]
    assert methods == ["POST", "DELETE"]


def test_row47_rdf_dataset_expects_redirect_status() -> None:
    routes = _site_first({("GET", "http://127.0.0.1:5640/api/1/datasets/abc/rdf"): (302, None)})
    with _sync_client(routes) as client:
        assert client.datasets.rdf("abc") == 302


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
        envelope = client.datasets.search_v2(DatasetListQuery(q="x"))

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
    transport, client = _sync_client_with_transport(routes)
    with client:
        assert client.datasets.get_extras_v2("abc") == {"keep": "v"}
        extras = client.datasets.update_extras_v2("abc", DatasetExtrasUpdate({"added": 1, "gone": None}))

    assert extras == {"keep": "v", "added": 1}
    request = transport.requests[-1]
    assert request.body is not None
    assert json.loads(request.body) == {"added": 1, "gone": None}


def test_row80_extras_delete_returns_receipt_from_204_with_body() -> None:
    routes = _site_first({("DELETE", "http://127.0.0.1:5640/api/2/datasets/abc/extras/"): (204, {"keep": "v"})})
    with _sync_client(routes) as client:
        outcome = client.datasets.delete_extras_v2("abc", DatasetExtrasDelete(keys=("gone",)))

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

    transport, client = _sync_client_with_transport(routes)
    with client:
        with pytest.raises(CatalogNotFoundError):
            client.datasets.get("missing")
        with pytest.raises(CatalogConflictError) as gone:
            client.datasets.get("gone")
        with pytest.raises(CatalogValidationError):
            client.datasets.create(DatasetCreateInput(title="T", description="D"))

    assert gone.value.capability_state == "unavailable"
    probes = [r for r in transport.requests if r.url.endswith("/api/1/site/")]
    assert len(probes) == 1
    probes = [r for r in transport.requests if r.url.endswith("/api/1/site/")]
    assert len(probes) == 1


def test_invalid_inputs_are_rejected_before_any_dispatch() -> None:
    routes = _site_first({})
    transport, client = _sync_client_with_transport(routes)
    with client:
        with pytest.raises(ValueError):
            client.datasets.create(DatasetCreateInput(title="", description="D"))
        with pytest.raises(ValueError):
            client.datasets.list(DatasetListQuery(sort="-nope"))
        with pytest.raises(CatalogValidationError):
            client.datasets.get("")

    assert [r.url for r in transport.requests if "/api/" in r.url] == []


def test_async_dataset_service_matches_sync_wire_exactly() -> None:
    routes = _site_first(
        {
            ("GET", "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20"): _page_body(),
            ("POST", "http://127.0.0.1:5640/api/1/datasets/"): (201, _dataset_doc("new")),
            ("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/"): (204, None),
        }
    )
    transport = RouterAsyncTransport(routes)
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        owns_transport=False,
    )

    async def run() -> None:
        async with client as active:
            envelope = await active.datasets.list()
            record = await active.datasets.create(DatasetCreateInput(title="T", description="D"))
            outcome = await active.datasets.delete("abc")
            assert len(envelope.items) == 1
            assert record.id.value == "new"
            assert outcome.status_code == 204

    asyncio.run(run())

    sync_routes = _site_first(
        {
            ("GET", "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20"): _page_body(),
            ("POST", "http://127.0.0.1:5640/api/1/datasets/"): (201, _dataset_doc("new")),
            ("DELETE", "http://127.0.0.1:5640/api/1/datasets/abc/"): (204, None),
        }
    )
    sync_transport, sync_client = _sync_client_with_transport(sync_routes)
    with sync_client:
        sync_client.datasets.list()
        sync_client.datasets.create(DatasetCreateInput(title="T", description="D"))
        sync_client.datasets.delete("abc")

    async_urls = [r.url for r in transport.requests if not r.url.endswith("/api/1/site/")]
    sync_urls = [r.url for r in sync_transport.requests if not r.url.endswith("/api/1/site/")]
    assert async_urls == sync_urls


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
