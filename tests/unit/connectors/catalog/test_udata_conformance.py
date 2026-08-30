"""uData family conformance ledgers: every assigned COVERAGE row carries evidence."""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest

import tests.unit.connectors.catalog.test_udata_datasets as dataset_tests
from datasluice.connectors.catalog.udata.models.root_profile import (
    SiteDataserviceCsvQuery,
    SiteDatasetCatalogQuery,
    SiteDatasetCsvQuery,
    SiteDocument,
    SiteOrganizationCsvQuery,
    SitePatchInput,
    SiteReuseCsvQuery,
)
from datasluice.connectors.catalog.udata.wire import root_profile as root_wire
from datasluice.runtime.transport.base import AsyncRuntimeStreamResponse, RuntimeStreamResponse

_ROOT_CONTRACT_PATH = (
    Path(__file__).parents[4]
    / "src"
    / "datasluice"
    / "contracts"
    / "catalog"
    / "fixtures"
    / "udata"
    / "root_profile.json"
)


def _root_contract() -> dict[str, object]:
    document = json.loads(_ROOT_CONTRACT_PATH.read_text(encoding="utf-8"))
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

ROOT_ROWS: dict[int, str] = {
    183: "test_row183_get_site_decodes_a_lossless_typed_profile",
    184: "test_row184_set_site_uses_patch_presence_and_exact_confirmation",
    185: "test_site_data_portal_redirect_is_typed_and_same_origin",
    186: "test_site_rdf_catalog_preserves_accept_and_catalog_pagination_query",
    187: "test_site_rdf_catalog_format_returns_bounded_document_metadata",
    188: "test_row188_site_datasets_csv",
    189: "test_row189_site_resources_csv",
    190: "test_row190_site_organizations_csv",
    191: "test_row191_site_reuses_csv",
    192: "test_row192_site_dataservices_csv",
    193: "test_row193_site_harvests_csv",
    194: "test_row194_site_tags_csv",
    195: "test_row195_jsonld_context_decodes_json_without_retaining_raw_bytes",
}

ROOT_FAILURE_ROWS: dict[str, str] = {
    "root_invalid_format_pre_dispatch": "test_root_invalid_format_is_rejected_before_site_probe",
    "root_external_redirect": "test_root_external_redirect_is_rejected_without_following",
    "root_missing_or_conflicting_media": "test_site_profile_requires_one_exact_response_media_type",
    "root_export_limit_and_close": "test_root_export_forwards_chunks_to_the_caller_sink_and_enforces_the_limit",
    "root_set_site_denial_isolated": "test_set_site_denial_does_not_poison_the_root_read_capability",
    "root_async_parity": "test_async_site_patch_matches_sync_target_and_receipt_contract",
}


@pytest.mark.parametrize("row", sorted(ASSIGNED_ROWS))
def test_dataset_row_has_passing_evidence(row: int) -> None:
    test_name = ASSIGNED_ROWS[row]
    test = getattr(dataset_tests, test_name)
    assert callable(test), test_name
    test()


@pytest.mark.parametrize("finding", sorted(FINDING_EVIDENCE))
def test_review_finding_has_discriminating_evidence(finding: str) -> None:
    test_name = FINDING_EVIDENCE[finding]
    test = getattr(dataset_tests, test_name)
    assert callable(test), test_name
    test()


@pytest.mark.parametrize("cell", sorted(FAILURE_ROWS))
def test_dataset_failure_cell_has_passing_evidence(cell: str) -> None:
    test_name = FAILURE_ROWS[cell]
    test = getattr(dataset_tests, test_name)
    assert callable(test), test_name
    test()


def test_root_profile_rows_are_exhaustively_declared() -> None:
    rows = cast(list[dict[str, object]], _root_contract()["rows"])
    assert {cast(int, item["row"]) for item in rows} == set(ROOT_ROWS)


def test_root_profile_failure_cells_are_exhaustively_declared() -> None:
    failures = cast(list[dict[str, object]], _root_contract()["failure_cases"])
    assert {cast(str, item["id"]) for item in failures} == set(ROOT_FAILURE_ROWS)


def test_assigned_rows_match_the_coverage_dataset_scope() -> None:
    assert sorted(ASSIGNED_ROWS) == [39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 67, 75, 76, 77, 78, 79, 80]


def test_root_rows_match_the_coverage_root_profile_scope() -> None:
    assert sorted(ROOT_ROWS) == list(range(183, 196))


def test_root_contract_drives_independent_wire_shape_evidence() -> None:
    document = _root_contract()
    rows = cast(list[dict[str, object]], document["rows"])
    assert [cast(int, row["row"]) for row in rows] == list(range(183, 196))
    builders = {
        183: lambda: root_wire.get_site_request(),
        184: lambda: root_wire.set_site_request(SitePatchInput(title="contract")),
        185: lambda: root_wire.data_portal_request("json"),
        186: lambda: root_wire.rdf_catalog_request(),
        187: lambda: root_wire.rdf_catalog_format_request("json"),
        188: lambda: root_wire.datasets_csv_request(),
        189: lambda: root_wire.resources_csv_request(),
        190: lambda: root_wire.organizations_csv_request(),
        191: lambda: root_wire.reuses_csv_request(),
        192: lambda: root_wire.dataservices_csv_request(),
        193: lambda: root_wire.harvests_csv_request(),
        194: lambda: root_wire.tags_csv_request(),
        195: lambda: root_wire.jsonld_context_request(),
    }
    for row in rows:
        row_number = cast(int, row["row"])
        built = builders[row_number]()
        expected_path = cast(str, row["path"]).replace("<format>", "json")
        assert built[0] == row["method"]
        assert built[1] == expected_path


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


def test_root_contract_lists_every_required_failure_cell_in_both_modes() -> None:
    failures = cast(list[dict[str, object]], _root_contract()["failure_cases"])
    expected = set(ROOT_FAILURE_ROWS)
    assert {cast(str, item["id"]) for item in failures} == expected
    assert all(cast(list[str], item["modes"]) == ["sync", "async"] for item in failures)


def test_root_contract_executes_independent_profile_redirect_and_export_decoders() -> None:
    document = _root_contract()
    rows = {cast(int, row["row"]): row for row in cast(list[dict[str, object]], document["rows"])}
    profile = root_wire.parse_site_profile({"id": "site", "title": "uData", "version": "17.6.0"})
    assert profile.id == "site"

    redirect_row = rows[185]
    location = "http://127.0.0.1:5640/api/1/site/catalog.json"
    redirect = root_wire.parse_redirect(
        status_code=302,
        headers={"Location": location},
        endpoint=cast(str, redirect_row["path"]).replace("<format>", "json"),
        origin="http://127.0.0.1:5640",
        expected_path=cast(str, redirect_row["redirect_path"]).replace("<format>", "json"),
        operation=root_wire.ROOT_OPERATION,
    )
    assert redirect.location == location

    sync_response = RuntimeStreamResponse(
        status_code=200,
        headers={"Content-Type": "text/csv"},
        chunks=iter((b"id\nsite\n",)),
        close_callback=lambda: None,
    )
    sync_document = root_wire.digest_stream_document(
        sync_response,
        endpoint=cast(str, rows[188]["path"]),
        expected_media_type="text/csv",
        max_bytes=1024,
    )

    async def decode_async() -> SiteDocument:
        async def chunks() -> AsyncIterator[bytes]:
            yield b"id\nsite\n"

        async_response = AsyncRuntimeStreamResponse(
            status_code=200,
            headers={"Content-Type": "text/csv"},
            chunks=chunks(),
            close_callback=lambda: None,
        )
        return await root_wire.digest_stream_document_async(
            async_response,
            endpoint=cast(str, rows[188]["path"]),
            expected_media_type="text/csv",
            max_bytes=1024,
        )

    async_document = asyncio.run(decode_async())
    assert sync_document.sha256 == async_document.sha256


def test_conformance_module_imports_isolated() -> None:
    module = importlib.import_module("tests.unit.connectors.catalog.test_udata_conformance")
    assert module.ASSIGNED_ROWS
