"""Exact-wire and safety evidence for the uData root-profile family."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.root_profile import (
    SiteCatalogQuery,
    SiteDocument,
    SiteMutationResult,
    SitePatchInput,
    SiteProfile,
)
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
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure

_ORIGIN = "http://127.0.0.1:5640"
_SITE_URL = f"{_ORIGIN}/api/1/site/"
_CREDENTIAL = UDataCredential(api_key="site-key")
_PERMISSIONS = EffectivePermissions.for_credential(
    _CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
)


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

    async def aclose(self) -> None:
        self.close_count += 1


def _sync_client(
    routes: Mapping[tuple[str, str], RuntimeResponse],
    *,
    origin: str = _ORIGIN,
    credential: UDataCredential | None = None,
) -> tuple[RouterTransport, SyncUDataClient]:
    transport = RouterTransport(routes)
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=origin,
        credentials=credential,
        owns_transport=False,
    )
    return transport, client


def _async_client(
    routes: Mapping[tuple[str, str], RuntimeResponse],
    *,
    origin: str = _ORIGIN,
    credential: UDataCredential | None = None,
) -> tuple[AsyncRouterTransport, AsyncUDataClient]:
    transport = AsyncRouterTransport(routes)
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=origin,
        credentials=credential,
        owns_transport=False,
    )
    return transport, client


def _routes(*responses: tuple[str, str, RuntimeResponse]) -> dict[tuple[str, str], RuntimeResponse]:
    result = {(method, url): response for method, url, response in responses}
    result.setdefault(("GET", _SITE_URL), _json_response(200, _site_body()))
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
    transport, client = _sync_client(_routes(("GET", _SITE_URL, _json_response(200, _site_body()))))
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
            ("PATCH", patch_url, _json_response(200, _site_body(title="Changed"))),
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
    patch_request = transport.requests[-1]
    assert patch_request.method == "PATCH"
    assert json.loads(patch_request.body or b"") == {"title": "Changed", "configs": None}
    assert patch_request.headers["Content-Type"] == "application/json"
    assert patch_request.headers["X-API-KEY"] == "site-key"
    assert len(transport.requests) == 2


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


@pytest.mark.parametrize(
    ("method_name", "path"),
    [
        ("datasets_csv", "/api/1/site/datasets.csv"),
        ("resources_csv", "/api/1/site/resources.csv"),
        ("organizations_csv", "/api/1/site/organizations.csv"),
        ("reuses_csv", "/api/1/site/reuses.csv"),
        ("dataservices_csv", "/api/1/site/dataservices.csv"),
        ("harvests_csv", "/api/1/site/harvests.csv"),
        ("tags_csv", "/api/1/site/tags.csv"),
    ],
)
def test_site_csv_exports_have_exact_paths_and_bounded_stream_metadata(method_name: str, path: str) -> None:
    _assert_csv_export(method_name, path)


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


def test_root_invalid_format_is_rejected_before_site_probe() -> None:
    transport, client = _sync_client(_routes())
    with client, pytest.raises(CatalogValidationError):
        client.root_profile.data_portal("yaml")

    assert transport.requests == []


def test_root_malformed_profile_maps_to_typed_error() -> None:
    payload = _site_body()
    payload["title"] = 42
    transport, client = _sync_client(_routes(("GET", _SITE_URL, _json_response(200, payload))))
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
    transport, client = _sync_client(
        {("PATCH", "https://public.example/api/1/site/"): _json_response(200, _site_body())},
        origin="https://public.example",
        credential=_CREDENTIAL,
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
    assert [request.method for request in transport.requests] == ["GET", "PATCH"]


def test_root_mutation_does_not_retry_after_transport_failure() -> None:
    class FailingTransport(RouterTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            self.requests.append(request)
            if request.method == "PATCH":
                raise TransportFailure("connection dropped after dispatch")
            return _json_response(200, _site_body())

    transport = FailingTransport({})
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=_ORIGIN,
        credentials=_CREDENTIAL,
        owns_transport=False,
    )
    with client, pytest.raises(TransportFailure):
        client.root_profile.set_site(
            SitePatchInput(title="once"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert [request.method for request in transport.requests] == ["GET", "PATCH"]


def test_async_root_service_matches_sync_wire_and_result_shapes() -> None:
    url = f"{_ORIGIN}/api/1/site/datasets.csv"
    body = b'"id";"title"\n'
    transport, client = _async_client(_routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"}))))

    async def run() -> SiteDocument:
        async with client:
            return await client.root_profile.datasets_csv()

    document = asyncio.run(run())
    assert isinstance(document, SiteDocument)
    assert document.sha256
    assert [request.url for request in transport.requests] == [_SITE_URL, url]


def test_root_profile_wire_operations_use_the_existing_broad_capability_identity() -> None:
    assert wire.ROOT_OPERATIONS["set_site"] == "udata/api-v1.set-site"
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
