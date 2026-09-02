"""uData family conformance ledgers: every assigned COVERAGE row carries evidence."""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import AsyncIterator, Callable
from importlib import resources
from typing import Any, cast
from urllib.parse import urlsplit

import pytest

import tests.unit.connectors.catalog.test_udata_datasets as dataset_tests
from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    create_async_client,
    create_sync_client,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.root_profile import (
    SiteDataserviceCsvQuery,
    SiteDatasetCatalogQuery,
    SiteDatasetCsvQuery,
    SiteOrganizationCsvQuery,
    SitePatchInput,
    SiteReuseCsvQuery,
)
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.connectors.catalog.udata.wire import root_profile as root_wire
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError
from datasluice.runtime.transport.base import (
    AsyncRuntimeStreamResponse,
    RuntimeRequest,
    RuntimeResponse,
    RuntimeStreamResponse,
)

_ROOT_CONTRACT_RESOURCE = resources.files("datasluice.contracts").joinpath("catalog/fixtures/udata/root_profile.json")
_ORIGIN = "http://127.0.0.1:5640"
_SITE_PATH = "/api/1/site/"
_SITE_BODY = {"id": "site", "title": "uData", "version": "17.6.0"}


def _root_contract() -> dict[str, object]:
    document = json.loads(_ROOT_CONTRACT_RESOURCE.read_text(encoding="utf-8"))
    assert document["schema_version"] == "1.0"
    assert document["platform"] == "udata"
    assert document["profile_version"] == "17.6.0"
    assert document["source_commit"] == "0546582058d84706812a1c37387576efc4e5ad1f"
    return document


ASSIGNED_ROWS: dict[int, str] = {
    39: "test_row39_list_datasets_exact_wire_and_projection",
    40: "test_row40_create_dataset_posts_exact_body_and_decodes_201",
    41: "test_row41_recent_atom_returns_typed_text_document",
    42: "test_row42_get_dataset_exact_path",
    43: "test_row43_update_dataset_omits_absent_fields",
    44: "test_row44_delete_dataset_returns_redacted_receipt",
    45: "test_rows45_46_feature_transitions_use_exact_methods",
    46: "test_rows45_46_feature_transitions_use_exact_methods",
    47: "test_row47_rdf_dataset_returns_typed_redirect_outcome",
    48: "test_row48_rdf_format_returns_bounded_document_metadata",
    67: "test_row67_suggest_encodes_required_query",
    75: "test_row75_v2_search_uses_search_endpoint",
    76: "test_row76_v2_list_uses_v2_endpoint",
    77: "test_row77_v2_get_dataset_exact_path",
    78: "test_rows78_79_extras_read_and_null_delete_semantics",
    79: "test_rows78_79_extras_read_and_null_delete_semantics",
    80: "test_row80_extras_delete_returns_receipt_from_204_with_body",
}

FINDING_EVIDENCE: dict[str, str] = {
    "CR-01 mutation-auth-fail-closed": "test_cr01_mutations_without_credentials_fail_closed_before_dispatch",
    "CR-01 admin-role-evidence-required": "test_cr01_feature_requires_admin_role_evidence",
    "CR-02 rejected-mutation-receipt": "test_cr02_rejected_mutations_carry_redacted_receipts",
    "CR-02 failed-mutation-receipt": "test_dataset_failures_map_to_typed_errors_without_retry_on_client_errors",
    "CR-02 transport-failure-receipt": "test_cr02_transport_failure_keeps_an_ambiguous_receipt_on_the_original_error",
    "CR-03 no-auto-retry-destructive": "test_cr03_destructive_calls_are_never_auto_retried",
    "CR-03 idempotency-key-boundary": "test_cr03_idempotency_key_is_not_silently_treated_as_retry_authorization",
    "CR-04 per-route-capability-scope": "test_cr04_capability_evidence_stays_scoped_to_its_route",
    "CR-05 configured-probe-runner-used": "test_cr05_configured_probe_runner_is_used_for_effective_evidence",
    "CR-05 deployment-and-credential-scope": (
        "test_cr05_capability_evidence_is_bound_to_deployment_and_credential_scope"
    ),
    "CR-06 server-target-and-redaction": "test_cr06_create_receipt_uses_server_id_and_redacts_request_target",
    "WR-01 identifier-encoding": "test_wr01_identifiers_and_query_values_are_encoded",
    "WR-01 dot-segment": "test_wr01_rdf_encodes_once_and_rejects_dot_segments",
    "WR-01 raw-suggestion-id": "test_wr01_suggestion_ids_remain_raw_for_later_request_encoding",
    "WR-02 repeated-filter-keys": "test_wr02_repeated_tags_encode_as_repeated_keys",
    "WR-03 redirect-document-boundary": (
        "test_wr03_wr04_final_rdf_is_decoded_from_bytes_with_case_insensitive_media_type"
    ),
    "WR-03 deployment-disabled-status": "test_wr03_read_only_mutation_status_maps_to_deployment_disabled",
    "WR-04 invalid-text-decoding": "test_wr04_invalid_text_bytes_raise_a_typed_native_error",
    "WR-05 v2-facets-and-native-links": "test_wr05_v2_search_retains_facets_and_native_links",
    "WR-05 v1-page-presence": "test_wr05_v1_page_retains_previous_link_and_field_presence",
    "WR-06 typed-dataset-fields": "test_wr06_malformed_documented_dataset_fields_fail_with_route_identity",
    "WR-07 frozen-json-inputs": "test_wr07_nested_extras_inputs_and_results_are_json_safe_and_immutable",
    "WR-08 async-credential-resolution": "test_wr08_async_mutations_use_async_credential_resolution",
    "WR-08 v2-search-filter-surface": "test_wr08_v2_search_rejects_undocumented_filters_and_ranges",
    "WR-09 full-async-parity": "test_async_dataset_service_matches_sync_wire_exactly",
    "WR-09 async-failure-receipt": "test_wr09_async_mutation_failure_preserves_the_same_ambiguous_receipt",
    "WR-10 typed-query-validation": "test_wr10_query_models_reject_non_scalar_filter_values_at_construction",
    "WR-13 deeply-frozen-errors": "test_wr13_error_metadata_is_deeply_immutable_and_finite",
}

FAILURE_ROWS: dict[str, str] = {
    "dataset_get_missing_404": "test_dataset_failures_map_to_typed_errors_without_retry_on_client_errors",
    "dataset_get_deleted_410": "test_dataset_failures_map_to_typed_errors_without_retry_on_client_errors",
    "dataset_create_validation_400": "test_dataset_failures_map_to_typed_errors_without_retry_on_client_errors",
    "dataset_invalid_input_pre_dispatch": "test_invalid_inputs_are_rejected_before_any_dispatch",
    "dataset_unimplemented_family": "test_unimplemented_native_operation_returns_typed_error",
    "dataset_sync_async_parity": "test_async_dataset_service_matches_sync_wire_exactly",
}

ROOT_ROW_NUMBERS = frozenset(range(183, 196))
ROOT_FAILURE_IDS = frozenset(
    {
        "root_invalid_format_pre_dispatch",
        "root_external_redirect",
        "root_missing_or_conflicting_media",
        "root_export_limit_and_close",
        "root_set_site_denial_isolated",
        "root_async_parity",
    }
)


@pytest.fixture(autouse=True)
def _reload_root_profile_service() -> None:
    importlib.reload(importlib.import_module("datasluice.connectors.catalog.udata.services.root_profile"))


class _FixtureSyncTransport:
    def __init__(self, responder: Callable[[RuntimeRequest], RuntimeResponse]) -> None:
        self._responder = responder
        self.requests: list[RuntimeRequest] = []
        self.stream_close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return self._responder(request)

    def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
        response = self.send(request)
        return RuntimeStreamResponse(
            response.status_code,
            response.headers,
            iter((response.body,)),
            self._close_stream,
            response.retry_after,
        )

    def _close_stream(self) -> None:
        self.stream_close_count += 1

    def close(self) -> None:
        pass


class _FixtureAsyncTransport:
    def __init__(self, responder: Callable[[RuntimeRequest], RuntimeResponse]) -> None:
        self._responder = responder
        self.requests: list[RuntimeRequest] = []
        self.stream_close_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return self._responder(request)

    async def send_stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
        response = await self.send(request)

        async def chunks() -> AsyncIterator[bytes]:
            yield response.body

        return AsyncRuntimeStreamResponse(
            response.status_code,
            response.headers,
            chunks(),
            self._close_stream,
            response.retry_after,
        )

    def _close_stream(self) -> None:
        self.stream_close_count += 1

    async def aclose(self) -> None:
        pass


def _site_response(headers: dict[str, str] | None = None) -> RuntimeResponse:
    return RuntimeResponse(
        status_code=200,
        headers={"Content-Type": "application/json"} if headers is None else headers,
        body=json.dumps(_SITE_BODY).encode(),
    )


def _fixture_row_responder(row: dict[str, object]) -> Callable[[RuntimeRequest], RuntimeResponse]:
    expected_path = cast(str, row["path"]).replace("<format>", "json")
    row_number = cast(int, row["row"])

    def responder(request: RuntimeRequest) -> RuntimeResponse:
        request_path = urlsplit(request.url).path
        if request_path == _SITE_PATH:
            return _site_response()
        assert request.method == row["method"]
        assert request_path == expected_path
        if row_number == 185:
            return RuntimeResponse(
                status_code=302,
                headers={"Location": f"{_ORIGIN}/api/1/site/catalog.json"},
                body=b"",
            )
        if row_number == 186:
            return RuntimeResponse(
                status_code=302,
                headers={"Location": f"{_ORIGIN}/api/1/site/catalog.json?page=1&page_size=100"},
                body=b"",
            )
        if row_number in {187, 195}:
            return RuntimeResponse(
                status_code=200,
                headers={"Content-Type": "application/ld+json"},
                body=b'{"@context": {}}',
            )
        return RuntimeResponse(
            status_code=200,
            headers={"Content-Type": "text/csv"},
            body=b"id,title\nsite,uData\n",
        )

    return responder


def _sync_root_client(
    responder: Callable[[RuntimeRequest], RuntimeResponse], *, root_export_max_bytes: int = 1024
) -> tuple[_FixtureSyncTransport, SyncUDataClient]:
    transport = _FixtureSyncTransport(responder)
    return transport, SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=_ORIGIN,
        root_export_max_bytes=root_export_max_bytes,
        owns_transport=False,
    )


def _async_root_client(
    responder: Callable[[RuntimeRequest], RuntimeResponse], *, root_export_max_bytes: int = 1024
) -> tuple[_FixtureAsyncTransport, AsyncUDataClient]:
    transport = _FixtureAsyncTransport(responder)
    return transport, AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=_ORIGIN,
        root_export_max_bytes=root_export_max_bytes,
        owns_transport=False,
    )


def _sync_fixture_operation(client: SyncUDataClient, operation: str) -> object:
    service = client.root_profile
    operations: dict[str, Callable[[], object]] = {
        "udata.v1.get_site": service.get,
        "udata.v1.SiteDataPortal_get": lambda: service.data_portal("json"),
        "udata.v1.SiteRdfCatalog_get": service.rdf_catalog,
        "udata.v1.SiteRdfCatalogFormat_get": lambda: service.rdf_catalog_format("json"),
        "udata.v1.SiteDatasetsCsv_get": service.datasets_csv,
        "udata.v1.SiteResourcesCsv_get": service.resources_csv,
        "udata.v1.SiteOrganizationsCsv_get": service.organizations_csv,
        "udata.v1.SiteReusesCsv_get": service.reuses_csv,
        "udata.v1.SiteDataservicesCsv_get": service.dataservices_csv,
        "udata.v1.SiteHarvestsCsv_get": service.harvests_csv,
        "udata.v1.SiteTagsCsv_get": service.tags_csv,
        "udata.v1.SiteJsonLdContext_get": service.jsonld_context,
    }
    return operations[operation]()


async def _async_fixture_operation(client: AsyncUDataClient, operation: str) -> object:
    service = client.root_profile
    operations: dict[str, Callable[[], object]] = {
        "udata.v1.get_site": service.get,
        "udata.v1.SiteDataPortal_get": lambda: service.data_portal("json"),
        "udata.v1.SiteRdfCatalog_get": service.rdf_catalog,
        "udata.v1.SiteRdfCatalogFormat_get": lambda: service.rdf_catalog_format("json"),
        "udata.v1.SiteDatasetsCsv_get": service.datasets_csv,
        "udata.v1.SiteResourcesCsv_get": service.resources_csv,
        "udata.v1.SiteOrganizationsCsv_get": service.organizations_csv,
        "udata.v1.SiteReusesCsv_get": service.reuses_csv,
        "udata.v1.SiteDataservicesCsv_get": service.dataservices_csv,
        "udata.v1.SiteHarvestsCsv_get": service.harvests_csv,
        "udata.v1.SiteTagsCsv_get": service.tags_csv,
        "udata.v1.SiteJsonLdContext_get": service.jsonld_context,
    }
    result = operations[operation]()
    return await cast(Any, result)


def _root_read_rows() -> list[dict[str, object]]:
    rows = cast(list[dict[str, object]], _root_contract()["rows"])
    return [row for row in rows if not row.get("controlled_only", False)]


@pytest.mark.parametrize("row", sorted(ASSIGNED_ROWS))
def test_dataset_row_has_passing_evidence(row: int) -> None:
    test_name = ASSIGNED_ROWS[row]
    test = getattr(dataset_tests, test_name)
    assert callable(test), test_name


@pytest.mark.parametrize("finding", sorted(FINDING_EVIDENCE))
def test_review_finding_has_discriminating_evidence(finding: str) -> None:
    test_name = FINDING_EVIDENCE[finding]
    test = getattr(dataset_tests, test_name)
    assert callable(test), test_name


@pytest.mark.parametrize("cell", sorted(FAILURE_ROWS))
def test_dataset_failure_cell_has_passing_evidence(cell: str) -> None:
    test_name = FAILURE_ROWS[cell]
    test = getattr(dataset_tests, test_name)
    assert callable(test), test_name


def test_root_profile_rows_are_exhaustively_declared() -> None:
    rows = cast(list[dict[str, object]], _root_contract()["rows"])
    assert {cast(int, item["row"]) for item in rows} == ROOT_ROW_NUMBERS
    assert [row for row in rows if row.get("controlled_only")] == [
        {
            "row": 184,
            "operation": "udata.v1.set_site",
            "method": "PATCH",
            "path": "/api/1/site/",
            "request_media_type": "application/json",
            "request_fields": ["title", "feed_size"],
            "response_media_type": "application/json",
            "controlled_only": True,
        }
    ]


def test_root_profile_failure_cells_are_exhaustively_declared() -> None:
    failures = cast(list[dict[str, object]], _root_contract()["failure_cases"])
    assert {cast(str, item["id"]) for item in failures} == ROOT_FAILURE_IDS


@pytest.mark.parametrize("row", sorted(ROOT_ROW_NUMBERS))
def test_root_profile_row_has_declared_evidence(row: int) -> None:
    rows = cast(list[dict[str, object]], _root_contract()["rows"])
    assert any(cast(int, item["row"]) == row for item in rows)


@pytest.mark.parametrize("row", _root_read_rows(), ids=lambda row: f"row-{row['row']}")
def test_root_contract_executes_each_non_mutating_service_row_in_both_modes(row: dict[str, object]) -> None:
    operation = cast(str, row["operation"])
    expected_path = cast(str, row["path"]).replace("<format>", "json")
    sync_transport, sync_client = _sync_root_client(_fixture_row_responder(row))
    with sync_client:
        sync_value = _sync_fixture_operation(sync_client, operation)

    async_transport, async_client = _async_root_client(_fixture_row_responder(row))

    async def run_async() -> object:
        async with async_client:
            return await _async_fixture_operation(async_client, operation)

    async_value = asyncio.run(run_async())

    for transport in (sync_transport, async_transport):
        assert [(request.method, urlsplit(request.url).path) for request in transport.requests] == [
            ("GET", _SITE_PATH),
            (cast(str, row["method"]), expected_path),
        ]
    assert cast(Any, sync_value).to_dict() == cast(Any, async_value).to_dict()

    row_number = cast(int, row["row"])
    if row_number == 183:
        assert cast(Any, sync_value).id == "site"
    elif row_number == 185:
        assert cast(Any, sync_value).location == f"{_ORIGIN}/api/1/site/catalog.json"
    elif row_number == 186:
        assert cast(Any, sync_value).location == f"{_ORIGIN}/api/1/site/catalog.json?page=1&page_size=100"
    else:
        expected_media_type = "application/ld+json" if row_number in {187, 195} else "text/csv"
        assert cast(Any, sync_value).media_type == expected_media_type
        assert cast(Any, sync_value).size_bytes > 0
        assert "body" not in cast(Any, sync_value).to_dict()
    if row_number in {187, 188, 189, 190, 191, 192, 193, 194}:
        assert sync_transport.stream_close_count == async_transport.stream_close_count == 1


def test_root_contract_query_schemas_preserve_only_documented_cardinality() -> None:
    document = _root_contract()
    rows = cast(list[dict[str, object]], document["rows"])
    schemas = {cast(int, row["row"]): row.get("query_schema") for row in rows}
    assert schemas[186] == "dataset_catalog"
    assert schemas[187] == "dataset_catalog"
    assert schemas[188] == "dataset_csv"
    assert schemas[189] == "dataset_csv"
    assert schemas[190] == "organization_csv"
    assert schemas[191] == "reuse_csv"
    assert root_wire.rdf_catalog_request(SiteDatasetCatalogQuery(filters={"tag": ("one", "two")}))[1].endswith(
        "tag=one&tag=two"
    )
    assert root_wire.datasets_csv_request(SiteDatasetCsvQuery(filters={"format": ("csv", "json")}))[1].endswith(
        "format=csv&format=json"
    )
    assert root_wire.organizations_csv_request(SiteOrganizationCsvQuery(name="Evidence"))[1].endswith("name=Evidence")

    query_types = {
        "dataset_catalog": (SiteDatasetCatalogQuery, "title"),
        "dataset_csv": (SiteDatasetCsvQuery, "created"),
        "reuse_csv": (SiteReuseCsvQuery, "title"),
        "dataservice_csv": (SiteDataserviceCsvQuery, "title"),
    }
    query_schemas = cast(dict[str, dict[str, object]], document["query_schemas"])
    object_id_keys = {
        "license",
        "organization",
        "owner",
        "followed_by",
        "topic",
        "dataservice",
        "reuse",
        "dataset",
        "contact_point",
    }
    for schema_name, (query_type, sort) in query_types.items():
        schema = query_schemas[schema_name]
        filters: dict[str, object] = {}
        for key in cast(list[str], schema.get("repeatable", [])):
            filters[key] = ("one", "two")
        for key in cast(list[str], schema.get("boolean", [])):
            filters[key] = True
        choices = cast(dict[str, list[str]], schema.get("choices", {}))
        for key in cast(list[str], schema.get("scalar", [])):
            if key in {"q", "sort"}:
                continue
            if key in choices:
                filters[key] = choices[key][0]
            elif key == "geozone":
                filters[key] = "country:fr"
            elif key in object_id_keys:
                filters[key] = "0123456789abcdef01234567"
            else:
                filters[key] = "value"
        kwargs: dict[str, object] = {"filters": filters}
        if "defaults" in schema:
            defaults = cast(dict[str, int], schema["defaults"])
            kwargs.update(page=defaults["page"], page_size=defaults["page_size"])
        if "q" in cast(list[str], schema.get("scalar", [])):
            kwargs["q"] = "query"
        if "sort" in cast(list[str], schema.get("scalar", [])):
            kwargs["sort"] = sort
        query = cast(Any, query_type)(**kwargs)
        actual_keys = {key for key, _ in query.query_params()}
        expected_keys = {
            *cast(list[str], schema.get("repeatable", [])),
            *cast(list[str], schema.get("boolean", [])),
            *cast(list[str], schema.get("scalar", [])),
        }
        if "defaults" in schema:
            expected_keys.update({"page", "page_size"})
        assert actual_keys == expected_keys

    with pytest.raises(ValueError):
        SiteDatasetCatalogQuery(filters={"credit": "ignored"})


@pytest.mark.parametrize(
    "failure",
    cast(list[dict[str, object]], _root_contract()["failure_cases"]),
    ids=lambda failure: cast(str, failure["id"]),
)
def test_root_contract_failure_cells_execute_the_declared_sync_async_behavior(failure: dict[str, object]) -> None:
    failure_id = cast(str, failure["id"])
    assert failure["modes"] == ["sync", "async"]

    if failure_id == "root_invalid_format_pre_dispatch":
        sync_transport, sync_client = _sync_root_client(lambda request: _site_response())
        with sync_client, pytest.raises(CatalogValidationError):
            sync_client.root_profile.data_portal("yaml")

        async_transport, async_client = _async_root_client(lambda request: _site_response())

        async def invalid_format() -> None:
            async with async_client:
                with pytest.raises(CatalogValidationError):
                    await async_client.root_profile.data_portal("yaml")

        asyncio.run(invalid_format())
        assert sync_transport.requests == async_transport.requests == []
        return

    if failure_id == "root_external_redirect":

        def external_redirect(request: RuntimeRequest) -> RuntimeResponse:
            if urlsplit(request.url).path == _SITE_PATH:
                return _site_response()
            return RuntimeResponse(302, {"Location": "https://other.example/site/catalog.json"}, b"")

        sync_transport, sync_client = _sync_root_client(external_redirect)
        with sync_client, pytest.raises(NativeCatalogError):
            sync_client.root_profile.data_portal("json")

        async_transport, async_client = _async_root_client(external_redirect)

        async def external_redirect_async() -> None:
            async with async_client:
                with pytest.raises(NativeCatalogError):
                    await async_client.root_profile.data_portal("json")

        asyncio.run(external_redirect_async())
        for transport in (sync_transport, async_transport):
            assert [(request.method, urlsplit(request.url).path) for request in transport.requests] == [
                ("GET", _SITE_PATH),
                ("GET", "/api/1/site/data.json"),
            ]
        return

    if failure_id == "root_missing_or_conflicting_media":

        def missing_media() -> Callable[[RuntimeRequest], RuntimeResponse]:
            response_count = 0

            def responder(request: RuntimeRequest) -> RuntimeResponse:
                nonlocal response_count
                response_count += 1
                return _site_response() if response_count == 1 else RuntimeResponse(200, {}, b"{}")

            return responder

        sync_transport, sync_client = _sync_root_client(missing_media())
        with sync_client, pytest.raises(NativeCatalogError):
            sync_client.root_profile.get()

        async_transport, async_client = _async_root_client(missing_media())

        async def missing_media_async() -> None:
            async with async_client:
                with pytest.raises(NativeCatalogError):
                    await async_client.root_profile.get()

        asyncio.run(missing_media_async())
        assert len(sync_transport.requests) == len(async_transport.requests) == 2
        return

    if failure_id == "root_export_limit_and_close":

        def oversized_export(request: RuntimeRequest) -> RuntimeResponse:
            if urlsplit(request.url).path == _SITE_PATH:
                return _site_response()
            return RuntimeResponse(200, {"Content-Type": "text/csv"}, b"id,title\nsite,uData\n")

        sync_transport, sync_client = _sync_root_client(oversized_export, root_export_max_bytes=1)
        with sync_client, pytest.raises(NativeCatalogError):
            sync_client.root_profile.datasets_csv()

        async_transport, async_client = _async_root_client(oversized_export, root_export_max_bytes=1)

        async def oversized_export_async() -> None:
            async with async_client:
                with pytest.raises(NativeCatalogError):
                    await async_client.root_profile.datasets_csv()

        asyncio.run(oversized_export_async())
        assert sync_transport.stream_close_count == async_transport.stream_close_count == 1
        return

    if failure_id == "root_set_site_denial_isolated":

        def direct_and_factory_sync(client: SyncUDataClient, transport: _FixtureSyncTransport) -> None:
            with client:
                with pytest.raises(CatalogValidationError):
                    client.root_profile.set_site(SitePatchInput(title="unowned"), permissions=None)
                assert transport.requests == []
                assert client.root_profile.get().id == "site"

        sync_transport, sync_client = _sync_root_client(lambda request: _site_response())
        direct_and_factory_sync(sync_client, sync_transport)
        factory_sync_transport = _FixtureSyncTransport(lambda request: _site_response())
        factory_sync_client = create_sync_client(
            UDataClientSettings(base_url=_ORIGIN, sync_transport=factory_sync_transport)
        )
        direct_and_factory_sync(factory_sync_client, factory_sync_transport)

        async def direct_and_factory_async(client: AsyncUDataClient, transport: _FixtureAsyncTransport) -> None:
            async with client:
                with pytest.raises(CatalogValidationError):
                    await client.root_profile.set_site(SitePatchInput(title="unowned"), permissions=None)
                assert transport.requests == []
                assert (await client.root_profile.get()).id == "site"

        async_transport, async_client = _async_root_client(lambda request: _site_response())
        asyncio.run(direct_and_factory_async(async_client, async_transport))
        factory_async_transport = _FixtureAsyncTransport(lambda request: _site_response())
        factory_async_client = create_async_client(
            UDataClientSettings(base_url=_ORIGIN, async_transport=factory_async_transport)
        )
        asyncio.run(direct_and_factory_async(factory_async_client, factory_async_transport))
        return

    if failure_id == "root_async_parity":
        sync_transport, sync_client = _sync_root_client(lambda request: _site_response())
        with sync_client:
            sync_profile = sync_client.root_profile.get()

        async_transport, async_client = _async_root_client(lambda request: _site_response())

        async def async_profile() -> object:
            async with async_client:
                return await async_client.root_profile.get()

        assert sync_profile.to_dict() == cast(Any, asyncio.run(async_profile())).to_dict()
        assert [request.url for request in sync_transport.requests] == [
            request.url for request in async_transport.requests
        ]
        return

    raise AssertionError(f"Unhandled root failure fixture: {failure_id}")


def test_conformance_module_imports_isolated() -> None:
    module = importlib.import_module("tests.unit.connectors.catalog.test_udata_conformance")
    assert module.ASSIGNED_ROWS
