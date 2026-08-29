"""Dataset-family conformance ledger: every assigned COVERAGE row carries evidence."""

from __future__ import annotations

import importlib

import pytest

import tests.unit.connectors.catalog.test_udata_datasets as dataset_tests

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


def test_assigned_rows_match_the_coverage_dataset_scope() -> None:
    assert sorted(ASSIGNED_ROWS) == [39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 67, 75, 76, 77, 78, 79, 80]


def test_conformance_module_imports_isolated() -> None:
    module = importlib.import_module("tests.unit.connectors.catalog.test_udata_conformance")
    assert module.ASSIGNED_ROWS
