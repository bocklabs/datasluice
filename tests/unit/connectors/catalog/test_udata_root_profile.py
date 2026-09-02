"""Exact-wire and safety evidence for the uData root-profile family."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
from collections.abc import AsyncIterator, Callable, Generator, Mapping
from contextlib import ExitStack
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest

from datasluice.connectors.catalog.udata import clients as udata_clients
from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    _create_controlled_sync_client,
    create_async_client,
    create_sync_client,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.root_profile import (
    SiteCatalogQuery,
    SiteDataserviceCsvQuery,
    SiteDatasetCsvQuery,
    SiteDocument,
    SiteMutationResult,
    SiteOrganizationCsvQuery,
    SitePatchInput,
    SiteProfile,
    SiteReuseCsvQuery,
)
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.connectors.catalog.udata.wire import root_profile as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import NativeRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import (
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
)
from datasluice.runtime.events import EventEmitter
from datasluice.runtime.transport.base import (
    AsyncRuntimeStreamResponse,
    RuntimeRequest,
    RuntimeResponse,
    RuntimeStreamResponse,
    TransportFailure,
)

_ORIGIN = "http://127.0.0.1:5640"
_SITE_URL = f"{_ORIGIN}/api/1/site/"
_CREDENTIAL = UDataCredential(api_key="site-key")
_PERMISSIONS = EffectivePermissions.for_credential(
    _CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
)


@pytest.fixture(autouse=True)
def _reload_real_root_profile_service() -> None:
    importlib.reload(importlib.import_module("datasluice.connectors.catalog.udata.services.root_profile"))


def _controlled_evidence(site_id: str = "site", *, nonce: str = "unit-test-stack") -> Any:
    return udata_clients._ControlledStackEvidence(
        nonce_sha256=hashlib.sha256(nonce.encode()).hexdigest(), site_id=site_id
    )


def _controlled_compose_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    env_file = tmp_path / ".env"
    compose_file.write_text("compose", encoding="utf-8")
    env_file.write_text("env", encoding="utf-8")
    monkeypatch.setattr(udata_clients, "_CONTROLLED_COMPOSE_FILE", compose_file)
    monkeypatch.setattr(udata_clients, "_CONTROLLED_ENV_FILE", env_file)


def test_controlled_command_reaps_a_process_after_pipe_eof(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _controlled_compose_files(monkeypatch, tmp_path)
    reads = iter((b"ok", b""))
    waits = iter(((0, 0), (7, 0)))

    monkeypatch.setattr(udata_clients.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(udata_clients.os, "posix_spawnp", lambda *args, **kwargs: 7)
    monkeypatch.setattr(udata_clients.os, "close", lambda _: None)
    monkeypatch.setattr(udata_clients.os, "read", lambda *_: next(reads))
    monkeypatch.setattr(udata_clients.os, "waitpid", lambda *_: next(waits))
    monkeypatch.setattr(udata_clients.os, "waitstatus_to_exitcode", lambda _: 0)
    monkeypatch.setattr(udata_clients.select, "select", lambda readers, *_: ([10], [], []) if readers else ([], [], []))

    assert udata_clients._compose_read("ps") == "ok"


def test_controlled_command_decode_failure_is_redacted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _controlled_compose_files(monkeypatch, tmp_path)
    reads = iter((b"\xff", b""))

    monkeypatch.setattr(udata_clients.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(udata_clients.os, "posix_spawnp", lambda *args, **kwargs: 7)
    monkeypatch.setattr(udata_clients.os, "close", lambda _: None)
    monkeypatch.setattr(udata_clients.os, "read", lambda *_: next(reads))
    monkeypatch.setattr(udata_clients.os, "waitpid", lambda *_: (7, 0))
    monkeypatch.setattr(udata_clients.os, "waitstatus_to_exitcode", lambda _: 0)
    monkeypatch.setattr(udata_clients.select, "select", lambda readers, *_: ([10], [], []) if readers else ([], [], []))

    with pytest.raises(CatalogValidationError) as excinfo:
        udata_clients._compose_read("ps")

    assert excinfo.value.__cause__ is None


def test_controlled_peer_decode_failure_is_redacted() -> None:
    response = RuntimeResponse(status_code=200, headers={"Content-Type": "application/json"}, body=b"not-json")

    with pytest.raises(CatalogValidationError) as excinfo:
        udata_clients._controlled_peer_evidence(response)

    assert excinfo.value.__cause__ is None


def test_root_document_decode_failure_is_redacted() -> None:
    with pytest.raises(NativeCatalogError) as excinfo:
        wire.parse_document(
            b"\xffpayload",
            endpoint=_SITE_URL,
            expected_media_type="text/csv",
            response_media_type="text/csv",
        )

    assert excinfo.value.__cause__ is None


def _site_body(*, title: str = "uData") -> dict[str, object]:
    return {
        "id": "site",
        "title": title,
        "keywords": ["open", "data"],
        "feed_size": 20,
        "configs": {"default_language": "en"},
        "themes": {"name": "default"},
        "settings": {"home_datasets": [], "home_reuses": []},
        "datasets_blocs": [],
        "reuses_blocs": [],
        "dataservices_blocs": [],
        "metrics": {"datasets": 1},
        "version": "17.6.0",
        "portal_extension": {"enabled": True},
    }


def _json_response(status: int, payload: object, headers: Mapping[str, str] | None = None) -> RuntimeResponse:
    body = b"" if payload is None else payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return RuntimeResponse(status_code=status, headers=dict(headers or {}), body=body)


class RouterTransport:
    """A deterministic transport that records every wire request."""

    def __init__(self, routes: Mapping[tuple[str, str], RuntimeResponse]) -> None:
        self.routes = dict(routes)
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        try:
            return self.routes[(request.method, request.url)]
        except KeyError as error:
            raise AssertionError(f"unexpected request {(request.method, request.url)}") from error

    def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
        response = self.send(request)
        return RuntimeStreamResponse(
            response.status_code,
            response.headers,
            iter((response.body,)),
            lambda: None,
            response.retry_after,
        )

    def close(self) -> None:
        self.close_count += 1


class AsyncRouterTransport:
    """An asynchronous deterministic transport with the same request map."""

    def __init__(self, routes: Mapping[tuple[str, str], RuntimeResponse]) -> None:
        self.routes = dict(routes)
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        try:
            return self.routes[(request.method, request.url)]
        except KeyError as error:
            raise AssertionError(f"unexpected request {(request.method, request.url)}") from error

    async def send_stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
        response = await self.send(request)

        async def chunks() -> AsyncIterator[bytes]:
            yield response.body

        return AsyncRuntimeStreamResponse(
            response.status_code,
            response.headers,
            chunks(),
            lambda: None,
            response.retry_after,
        )

    async def aclose(self) -> None:
        self.close_count += 1


def _sync_client(
    routes: Mapping[tuple[str, str], RuntimeResponse],
    *,
    origin: str = _ORIGIN,
    credential: UDataCredential | None = None,
    revalidate: Callable[..., bool] | None = None,
    emitter: EventEmitter | None = None,
    root_export_max_bytes: int = 8 * 1024 * 1024,
    test_transport: RouterTransport | None = None,
) -> tuple[RouterTransport, SyncUDataClient]:
    transport = test_transport or RouterTransport(routes)
    settings = UDataClientSettings(
        base_url=origin,
        credential=credential,
        sync_transport=transport,
        root_export_max_bytes=root_export_max_bytes,
    )
    client = create_sync_client(settings)
    stack = ExitStack()
    stack.enter_context(patch.object(SyncUDataClient, "_has_controlled_stack_authority", new=lambda _: True))
    stack.enter_context(
        patch.object(SyncUDataClient, "_controlled_evidence_digest", new=lambda _: _controlled_evidence().digest)
    )
    stack.enter_context(patch.object(SyncUDataClient, "_controlled_site_id", new=lambda _: "site"))

    def fake_revalidate(_: object, *, site_id: str) -> bool:
        return revalidate(site_id=site_id) if revalidate is not None else True

    stack.enter_context(patch.object(SyncUDataClient, "_revalidate_controlled_sync_stack", new=fake_revalidate))
    importlib.reload(importlib.import_module("datasluice.connectors.catalog.udata.services.root_profile"))
    original_close = client.close
    closed = False

    def close() -> None:
        nonlocal closed
        try:
            original_close()
        finally:
            if not closed:
                stack.close()
                closed = True

    cast(Any, client).close = close
    if emitter is not None:
        client._emitter = emitter
    return transport, client


def _async_client(
    routes: Mapping[tuple[str, str], RuntimeResponse],
    *,
    origin: str = _ORIGIN,
    credential: UDataCredential | None = None,
    emitter: EventEmitter | None = None,
    root_export_max_bytes: int = 8 * 1024 * 1024,
) -> tuple[AsyncRouterTransport, AsyncUDataClient]:
    transport = AsyncRouterTransport(routes)
    settings = UDataClientSettings(
        base_url=origin,
        credential=credential,
        async_transport=transport,
        root_export_max_bytes=root_export_max_bytes,
    )
    client = create_async_client(settings)

    stack = ExitStack()
    stack.enter_context(patch.object(AsyncUDataClient, "_has_controlled_stack_authority", new=lambda _: True))
    stack.enter_context(
        patch.object(AsyncUDataClient, "_controlled_evidence_digest", new=lambda _: _controlled_evidence().digest)
    )
    stack.enter_context(patch.object(AsyncUDataClient, "_controlled_site_id", new=lambda _: "site"))

    async def revalidate(_: object, *, site_id: str) -> bool:
        return True

    stack.enter_context(patch.object(AsyncUDataClient, "_revalidate_controlled_async_stack", new=revalidate))
    importlib.reload(importlib.import_module("datasluice.connectors.catalog.udata.services.root_profile"))
    original_aclose = client.aclose
    closed = False

    async def aclose() -> None:
        nonlocal closed
        try:
            await original_aclose()
        finally:
            if not closed:
                stack.close()
                closed = True

    cast(Any, client).aclose = aclose
    if emitter is not None:
        client._emitter = emitter
    return transport, client


def _routes(*responses: tuple[str, str, RuntimeResponse]) -> dict[tuple[str, str], RuntimeResponse]:
    result = {(method, url): response for method, url, response in responses}
    result.setdefault(("GET", _SITE_URL), _json_response(200, _site_body(), {"Content-Type": "application/json"}))
    return result


def _site_policy(*, target: str = "site") -> MutationPolicy:
    return MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation=wire.ROOT_OPERATIONS["set_site"],
            target=target,
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _receipt_from(error: BaseException) -> MutationReceipt:
    receipt = vars(error).get("mutation_receipt")
    assert isinstance(receipt, MutationReceipt)
    return receipt


def test_row183_get_site_decodes_a_lossless_typed_profile() -> None:
    transport, client = _sync_client(
        _routes(("GET", _SITE_URL, _json_response(200, _site_body(), {"Content-Type": "application/json"})))
    )
    with client:
        profile = client.root_profile.get()

    assert isinstance(profile, SiteProfile)
    assert profile.id == "site"
    assert profile.title == "uData"
    assert profile.version == "17.6.0"
    assert profile.payload["portal_extension"] == {"enabled": True}
    profile_payload = cast(dict[str, object], profile.to_dict()["payload"])
    assert profile_payload["metrics"] == {"datasets": 1}
    assert [request.url for request in transport.requests] == [_SITE_URL, _SITE_URL]


def test_row184_set_site_uses_patch_presence_and_exact_confirmation() -> None:
    patch_url = _SITE_URL
    transport, client = _sync_client(
        _routes(
            (
                "PATCH",
                patch_url,
                _json_response(200, _site_body(title="Changed"), {"Content-Type": "application/json"}),
            ),
        ),
        credential=_CREDENTIAL,
    )
    with client:
        result = client.root_profile.set_site(
            SitePatchInput(title="Changed", configs=None),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert isinstance(result, SiteMutationResult)
    assert result.profile is not None and result.profile.title == "Changed"
    assert result.receipt.outcome == "succeeded"
    assert result.receipt.target.value == "site"
    assert result.receipt.audit_metadata["controlled_evidence_digest"] == _controlled_evidence().digest
    patch_request = transport.requests[-1]
    assert patch_request.method == "PATCH"
    assert json.loads(patch_request.body or b"") == {"title": "Changed", "configs": None}
    assert patch_request.headers["Content-Type"] == "application/json"
    assert patch_request.headers["X-API-KEY"] == "site-key"
    assert len(transport.requests) == 3


def test_site_patch_omits_unset_fields_but_retains_explicit_null() -> None:
    body = SitePatchInput(configs=None, keywords=("one", "two")).payload()

    assert body == {"keywords": ["one", "two"], "configs": None}
    assert "title" not in body
    assert SitePatchInput().payload() == {}


def test_site_data_portal_redirect_is_typed_and_same_origin() -> None:
    location = f"{_ORIGIN}/api/1/site/catalog.json"
    transport, client = _sync_client(
        _routes(
            (
                "GET",
                f"{_ORIGIN}/api/1/site/data.json",
                _json_response(302, None, {"Location": location}),
            )
        )
    )
    with client:
        redirect = client.root_profile.data_portal("json")

    assert isinstance(redirect, SiteDocument)
    assert redirect.location == location
    assert redirect.status_code == 302
    assert redirect.payload["location"] == location
    assert [request.url for request in transport.requests] == [_SITE_URL, f"{_ORIGIN}/api/1/site/data.json"]


def test_site_rdf_catalog_preserves_accept_and_catalog_pagination_query() -> None:
    catalog_url = f"{_ORIGIN}/api/1/site/catalog?page=2&page_size=5&q=air+quality"
    location = f"{_ORIGIN}/api/1/site/catalog.json?page=2&page_size=5&q=air+quality"
    transport, client = _sync_client(_routes(("GET", catalog_url, _json_response(302, None, {"Location": location}))))
    with client:
        redirect = client.root_profile.rdf_catalog(
            SiteCatalogQuery(page=2, page_size=5, q="air quality"), accept="application/ld+json"
        )

    request = transport.requests[-1]
    assert request.headers["Accept"] == "application/ld+json"
    assert request.url == catalog_url
    assert redirect.location == location


def test_site_rdf_catalog_accepts_upstream_default_query_order() -> None:
    catalog_url = f"{_ORIGIN}/api/1/site/catalog"
    location = f"{_ORIGIN}/api/1/site/catalog.json?page_size=100&page=1"
    transport, client = _sync_client(_routes(("GET", catalog_url, _json_response(302, None, {"Location": location}))))
    with client:
        redirect = client.root_profile.rdf_catalog()

    assert redirect.location == location
    assert transport.requests[-1].url == catalog_url


def test_site_rdf_catalog_format_returns_bounded_document_metadata() -> None:
    url = f"{_ORIGIN}/api/1/site/catalog.json?page=1&page_size=100"
    body = b'{"@context": {}}'
    transport, client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "application/ld+json"})))
    )
    with client:
        document = client.root_profile.rdf_catalog_format("json", SiteCatalogQuery())

    assert document.media_type == "application/ld+json"
    assert document.size_bytes == len(body)
    assert document.sha256
    assert "body" not in document.to_dict()


def test_root_export_forwards_chunks_to_the_caller_sink_and_enforces_the_limit() -> None:
    url = f"{_ORIGIN}/api/1/site/datasets.csv"
    body = b"id\n123\n"
    received: list[bytes] = []
    _, client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"}))),
        root_export_max_bytes=len(body),
    )
    with client:
        document = client.root_profile.datasets_csv(sink=received.append)

    assert b"".join(received) == body
    assert document.size_bytes == len(body)

    _, limited_client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"}))),
        root_export_max_bytes=len(body) - 1,
    )
    with limited_client, pytest.raises(NativeCatalogError, match="byte limit"):
        limited_client.root_profile.datasets_csv()


def _assert_csv_export(method_name: str, path: str) -> None:
    url = f"{_ORIGIN}{path}"
    body = b'"id";"title"\n"one";"A"\n'
    transport, client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv; charset=utf-8"})))
    )
    with client:
        document = getattr(client.root_profile, method_name)()

    assert isinstance(document, SiteDocument)
    assert document.media_type == "text/csv"
    assert document.payload["size_bytes"] == len(body)
    assert transport.requests[-1].url == url


def test_root_export_emits_failure_only_after_stream_consumption_fails() -> None:
    class FailingStreamTransport(RouterTransport):
        def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
            self.requests.append(request)

            def chunks() -> Generator[bytes, None, None]:
                yield b"id\n"
                raise TransportFailure("stream interrupted")

            return RuntimeStreamResponse(
                200,
                {"Content-Type": "text/csv"},
                chunks(),
                lambda: None,
            )

    events = []
    transport = FailingStreamTransport(_routes())
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=_ORIGIN,
        emitter=EventEmitter(sinks=(events.append,)),
        owns_transport=False,
    )
    with client, pytest.raises(TransportFailure):
        client.root_profile.datasets_csv()

    assert events[-1].outcome == "failed"
    assert not any(event.outcome == "succeeded" for event in events if event.operation_id == wire.ROOT_OPERATION)


def test_async_site_patch_matches_sync_target_and_receipt_contract() -> None:
    transport, client = _async_client(
        _routes(
            (
                "PATCH",
                _SITE_URL,
                _json_response(200, _site_body(title="Changed"), {"Content-Type": "application/json"}),
            )
        ),
        credential=_CREDENTIAL,
    )

    async def run() -> SiteMutationResult:
        async with client:
            return await client.root_profile.set_site(
                SitePatchInput(title="Changed"),
                permissions=_PERMISSIONS,
                mutation_policy=_site_policy(),
            )

    result = asyncio.run(run())
    assert result.profile is not None and result.profile.site_id == "site"
    assert result.receipt.target.value == "site"
    assert [request.method for request in transport.requests] == ["GET", "GET", "PATCH"]


def test_root_profile_rejects_oversized_buffered_json() -> None:
    payload = _site_body()
    transport, client = _sync_client(
        _routes(("GET", _SITE_URL, _json_response(200, payload, {"Content-Type": "application/json"}))),
        root_export_max_bytes=4,
    )
    with client, pytest.raises(NativeCatalogError, match="byte limit"):
        client.root_profile.get()

    assert [request.url for request in transport.requests] == [_SITE_URL, _SITE_URL]


@pytest.mark.parametrize(
    "headers",
    ({}, {"Content-Type": ""}, {"Content-Type": "application/json,text/plain"}, {"Content-Type": "text/plain"}),
)
def test_site_profile_requires_one_exact_response_media_type(headers: Mapping[str, str]) -> None:
    transport, client = _sync_client(_routes(("GET", _SITE_URL, _json_response(200, _site_body(), headers))))
    with client, pytest.raises(NativeCatalogError):
        client.root_profile.get()

    assert [request.url for request in transport.requests] == [_SITE_URL, _SITE_URL]


def test_row188_site_datasets_csv() -> None:
    _assert_csv_export("datasets_csv", "/api/1/site/datasets.csv")


def test_row189_site_resources_csv() -> None:
    _assert_csv_export("resources_csv", "/api/1/site/resources.csv")


def test_row190_site_organizations_csv() -> None:
    _assert_csv_export("organizations_csv", "/api/1/site/organizations.csv")


def test_row191_site_reuses_csv() -> None:
    _assert_csv_export("reuses_csv", "/api/1/site/reuses.csv")


def test_row192_site_dataservices_csv() -> None:
    _assert_csv_export("dataservices_csv", "/api/1/site/dataservices.csv")


def test_row193_site_harvests_csv() -> None:
    _assert_csv_export("harvests_csv", "/api/1/site/harvests.csv")


def test_row194_site_tags_csv() -> None:
    _assert_csv_export("tags_csv", "/api/1/site/tags.csv")


def test_row195_jsonld_context_decodes_json_without_retaining_raw_bytes() -> None:
    url = f"{_ORIGIN}/api/1/site/context.jsonld"
    payload = {"@vocab": "http://www.w3.org/ns/dcat#", "title": "dct:title"}
    body = json.dumps(payload).encode()
    transport, client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "application/ld+json"})))
    )
    with client:
        document = client.root_profile.jsonld_context()

    assert document.payload["@vocab"] == payload["@vocab"]
    assert document.media_type == "application/ld+json"
    assert "body" not in document.to_dict()
    assert transport.requests[-1].url == url


@pytest.mark.parametrize("fmt", ("rdf", "owl"))
def test_rdf_xml_aliases_are_supported(fmt: str) -> None:
    assert wire.media_type_for_format(fmt) == "application/rdf+xml"


def test_route_specific_csv_query_models_do_not_share_dataset_filters() -> None:
    with pytest.raises(TypeError):
        cast(Any, SiteOrganizationCsvQuery)(name="org", page_size=2)
    with pytest.raises(ValueError):
        SiteDatasetCsvQuery(filters={"last_update_range": "not-a-range"})
    assert wire.reuses_csv_request(SiteReuseCsvQuery(filters={"tag": ("one", "two")}))[1].endswith("tag=one&tag=two")
    assert wire.dataservices_csv_request(SiteDataserviceCsvQuery(filters={"tag": ("one", "two")}))[1].endswith(
        "tag=one&tag=two"
    )


def test_root_invalid_format_is_rejected_before_site_probe() -> None:
    transport, client = _sync_client(_routes())
    with client, pytest.raises(CatalogValidationError):
        client.root_profile.data_portal("yaml")

    assert transport.requests == []


def test_root_malformed_profile_maps_to_typed_error() -> None:
    payload = _site_body()
    payload["title"] = 42
    transport, client = _sync_client(
        _routes(("GET", _SITE_URL, _json_response(200, payload, {"Content-Type": "application/json"})))
    )
    with client, pytest.raises(CatalogValidationError) as excinfo:
        client.root_profile.get()

    assert excinfo.value.operation == wire.ROOT_OPERATION
    assert [request.url for request in transport.requests] == [_SITE_URL, _SITE_URL]


def test_root_external_redirect_is_rejected_without_following() -> None:
    url = f"{_ORIGIN}/api/1/site/data.json"
    transport, client = _sync_client(
        _routes(("GET", url, _json_response(302, None, {"Location": "https://other.example/site/catalog.json"})))
    )
    with client, pytest.raises(NativeCatalogError):
        client.root_profile.data_portal("json")

    assert [request.url for request in transport.requests] == [_SITE_URL, url]


def test_set_site_rejects_public_origins_before_any_dispatch_and_keeps_receipt() -> None:
    transport = RouterTransport({("PATCH", "https://public.example/api/1/site/"): _json_response(200, _site_body())})
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="https://public.example",
        credentials=_CREDENTIAL,
        owns_transport=False,
    )
    with client, pytest.raises(CatalogValidationError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="unsafe"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    receipt = _receipt_from(excinfo.value)
    assert receipt.outcome == "rejected"
    assert transport.requests == []


def test_set_site_rejects_wrong_confirmation_without_dispatch() -> None:
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(200, _site_body()))),
        credential=_CREDENTIAL,
    )
    with client, pytest.raises(ForbiddenError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="unsafe"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(target="other-site"),
        )

    assert _receipt_from(excinfo.value).outcome == "rejected"
    assert transport.requests == []


def test_set_site_maps_423_to_non_retryable_deployment_disabled_with_receipt() -> None:
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(423, None))),
        credential=_CREDENTIAL,
    )
    with client, pytest.raises(CatalogUnavailableError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="read-only"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    error = excinfo.value
    assert error.capability_state == "deployment-disabled"
    assert error.metadata["status_code"] == 423
    assert _receipt_from(error).outcome == "failed"
    assert [request.method for request in transport.requests] == ["GET", "GET", "PATCH"]


def test_site_patch_post_dispatch_media_failure_is_ambiguous() -> None:
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(200, _site_body(), {"Content-Type": "text/plain"}))),
        credential=_CREDENTIAL,
    )
    with client, pytest.raises(NativeCatalogError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="possibly-written"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    receipt = _receipt_from(excinfo.value)
    assert receipt.outcome == "ambiguous"
    assert receipt.audit_metadata["status_code"] == 200


def test_set_site_denial_does_not_poison_the_root_read_capability() -> None:
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(423, None))),
        credential=_CREDENTIAL,
    )
    with client:
        with pytest.raises(CatalogUnavailableError):
            client.root_profile.set_site(
                SitePatchInput(title="read-only"),
                permissions=_PERMISSIONS,
                mutation_policy=_site_policy(),
            )
        assert client.root_profile.get().id == "site"

    assert [request.method for request in transport.requests] == ["GET", "GET", "PATCH", "GET"]


def test_set_site_requires_controlled_factory_before_any_dispatch() -> None:
    transport = RouterTransport(_routes())
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=_ORIGIN,
        credentials=_CREDENTIAL,
        owns_transport=False,
    )
    with client, pytest.raises(CatalogValidationError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="unattested"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert _receipt_from(excinfo.value).outcome == "rejected"
    assert transport.requests == []


def test_fabricated_controlled_evidence_cannot_authorize_an_injected_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = RouterTransport(_routes())
    monkeypatch.setattr(udata_clients, "_verify_controlled_sync_stack", lambda _: _controlled_evidence("forged"))
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, udata_clients._ControlledSyncTransport)(transport=transport)
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, UDataClientSettings)(
            base_url=_ORIGIN,
            credential=_CREDENTIAL,
            sync_transport=transport,
            controlled_stack_attestation=object(),
        )

    fabricated = object.__new__(udata_clients._ControlledSyncTransport)
    with pytest.raises(AttributeError):
        object.__setattr__(fabricated, "_transport", transport)
    client = SyncUDataClient(
        fabricated,
        declared_udata_profile(),
        origin=_ORIGIN,
        credentials=_CREDENTIAL,
        owns_transport=False,
    )
    with client, pytest.raises((CatalogValidationError, ForbiddenError)):
        client.root_profile.set_site(
            SitePatchInput(title="unattested"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
        )

    hooked_client = create_sync_client(
        UDataClientSettings(base_url=_ORIGIN, credential=_CREDENTIAL, sync_transport=transport)
    )
    cast(Any, hooked_client)._has_controlled_stack_authority = lambda: True
    cast(Any, hooked_client)._controlled_evidence_digest = lambda: _controlled_evidence().digest
    cast(Any, hooked_client)._controlled_site_id = lambda: "site"
    cast(Any, hooked_client)._revalidate_controlled_sync_stack = lambda *, site_id: True
    with hooked_client, pytest.raises(CatalogValidationError):
        hooked_client.root_profile.set_site(
            SitePatchInput(title="unattested"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
        )
    assert transport.requests == []


def test_injected_transport_remains_available_for_read_only_behavior() -> None:
    transport = RouterTransport(_routes())
    client = create_sync_client(UDataClientSettings(base_url=_ORIGIN, sync_transport=transport))

    with client:
        assert client.root_profile.get().id == "site"

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_controlled_factory_rejects_injected_transport_before_any_dispatch() -> None:
    transport = RouterTransport(_routes())
    with pytest.raises(CatalogValidationError):
        _create_controlled_sync_client(
            UDataClientSettings(
                base_url=_ORIGIN,
                credential=_CREDENTIAL,
                sync_transport=transport,
            )
        )

    assert transport.requests == []


def test_root_mutation_does_not_retry_after_transport_failure() -> None:
    class FailingTransport(RouterTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            self.requests.append(request)
            if request.method == "PATCH":
                raise TransportFailure("connection dropped after dispatch")
            return _json_response(200, _site_body(), {"Content-Type": "application/json"})

    transport, client = _sync_client(
        {},
        credential=_CREDENTIAL,
        test_transport=FailingTransport({}),
    )
    with client, pytest.raises(TransportFailure):
        client.root_profile.set_site(
            SitePatchInput(title="once"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert [request.method for request in transport.requests] == ["GET", "GET", "PATCH"]


def test_unrelated_local_listener_loses_authority_before_patch_dispatch() -> None:
    transport, client = _sync_client(
        _routes(),
        credential=_CREDENTIAL,
        revalidate=lambda *, site_id: False,
    )

    with client, pytest.raises(CatalogValidationError):
        client.root_profile.set_site(
            SitePatchInput(title="unchanged"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
        )

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_forwarding_listener_loses_authority_before_patch_dispatch() -> None:
    transport, client = _sync_client(
        _routes(),
        credential=_CREDENTIAL,
        revalidate=lambda *, site_id: False,
    )

    with client, pytest.raises(CatalogValidationError):
        client.root_profile.set_site(
            SitePatchInput(title="unchanged"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
        )

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_async_root_service_matches_sync_wire_and_result_shapes() -> None:
    url = f"{_ORIGIN}/api/1/site/datasets.csv"
    body = b'"id";"title"\n'
    routes = _routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"})))
    sync_transport, sync_client = _sync_client(routes)
    with sync_client:
        sync_document = sync_client.root_profile.datasets_csv()

    transport, client = _async_client(_routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"}))))

    async def run() -> SiteDocument:
        async with client:
            return await client.root_profile.datasets_csv()

    document = asyncio.run(run())
    assert isinstance(document, SiteDocument)
    assert document.sha256
    assert [request.url for request in transport.requests] == [_SITE_URL, url]
    assert document == sync_document
    assert [request.url for request in sync_transport.requests] == [_SITE_URL, url]


def test_root_profile_wire_operations_use_the_existing_broad_capability_identity() -> None:
    assert wire.ROOT_OPERATIONS["set_site"] == "udata/api-v1.set_site"
    assert all(
        operation == wire.ROOT_OPERATION for name, operation in wire.ROOT_OPERATIONS.items() if name != "set_site"
    )
    assert next(
        operation
        for operation in declared_udata_profile().operations
        if operation.method == "root-and-effective-profile-probe"
    )


def test_root_profile_models_are_typed_and_immutable() -> None:
    profile = SiteProfile.from_payload(_site_body())
    with pytest.raises(TypeError):
        dict.__setitem__(cast(dict[str, object], profile.payload), "title", "changed")

    assert isinstance(profile.catalog_id.value, str)
    assert isinstance(profile.to_dict(), dict)
    assert isinstance(SitePatchInput(title="x"), SitePatchInput)
    assert NativeRecord is not SiteProfile
