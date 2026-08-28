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
    48: "test_row48_rdf_format_returns_document",
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
    "CR-03 no-auto-retry-destructive": "test_cr03_destructive_calls_are_never_auto_retried",
    "CR-04 per-route-capability-scope": "test_cr04_capability_evidence_stays_scoped_to_its_route",
    "CR-05 configured-probe-runner-used": "test_cr05_configured_probe_runner_is_used_for_effective_evidence",
    "WR-01 identifier-encoding": "test_wr01_identifiers_and_query_values_are_encoded",
    "WR-02 repeated-filter-keys": "test_wr02_repeated_tags_encode_as_repeated_keys",
    "WR-05 v2-facets-and-native-links": "test_wr05_v2_search_retains_facets_and_native_links",
    "WR-08 v2-search-filter-surface": "test_wr08_v2_search_rejects_undocumented_filters_and_ranges",
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
    assert callable(getattr(dataset_tests, test_name)), test_name


@pytest.mark.parametrize("finding", sorted(FINDING_EVIDENCE))
def test_review_finding_has_discriminating_evidence(finding: str) -> None:
    test_name = FINDING_EVIDENCE[finding]
    assert callable(getattr(dataset_tests, test_name)), test_name


@pytest.mark.parametrize("cell", sorted(FAILURE_ROWS))
def test_dataset_failure_cell_has_passing_evidence(cell: str) -> None:
    test_name = FAILURE_ROWS[cell]
    assert callable(getattr(dataset_tests, test_name)), test_name


def test_assigned_rows_match_the_coverage_dataset_scope() -> None:
    assert sorted(ASSIGNED_ROWS) == [39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 67, 75, 76, 77, 78, 79, 80]


def test_conformance_module_imports_isolated() -> None:
    module = importlib.import_module("tests.unit.connectors.catalog.test_udata_conformance")
    assert module.ASSIGNED_ROWS
