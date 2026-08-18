"""Tests for exhaustive catalog contract case execution."""

from __future__ import annotations

from typing import cast

from datasluice.contracts.catalog.fakes import AsyncReferenceConnector, SyncReferenceConnector
from datasluice.contracts.catalog.fixtures import ReferenceCase, load_reference_fixture_set
from datasluice.contracts.catalog.protocols import AsyncCatalogClient, SyncCatalogClient
from datasluice.contracts.catalog.runner import catalog_contract_cases, run_catalog_contract
from datasluice.domain.catalog.models import NativeRecord, ResultEnvelope


class FailingSyncReferenceConnector(SyncReferenceConnector):
    """Reference connector that reports an unexpected case failure."""

    def execute_case(self, case: ReferenceCase) -> ResultEnvelope[NativeRecord]:
        """Fail one declared case after runner setup succeeds."""
        raise AssertionError("wrong result")


def test_catalog_contract_cases_are_deterministic_and_cover_each_declared_fixture_case_in_both_modes() -> None:
    """Fixture cases produce stable pytest IDs for both execution modes."""
    fixture_set = load_reference_fixture_set("socrata")

    cases = catalog_contract_cases(fixture_set)

    assert cases == catalog_contract_cases(fixture_set)
    assert len(cases) == len(fixture_set.cases) * 2
    assert {case.mode for case in cases} == {"sync", "async"}
    assert {case.outcome for case in cases} >= {
        "core",
        "optional",
        "authenticated-success",
        "missing-credentials",
        "invalid-credentials",
        "forbidden",
        "deployment-disabled",
        "unavailable",
        "async-pending",
        "rate-limited",
    }
    assert len({case.pytest_id for case in cases}) == len(cases)


def test_runner_accumulates_declared_success_and_rejection_outcomes_without_stopping() -> None:
    """Expected authorization and availability outcomes remain passing evidence."""
    fixture_set = load_reference_fixture_set("ckan")
    sync_client = SyncReferenceConnector(fixture_set)
    async_client = AsyncReferenceConnector(fixture_set)

    report = run_catalog_contract(
        catalog_contract_cases(fixture_set),
        sync_client=cast(SyncCatalogClient, sync_client),
        async_client=cast(AsyncCatalogClient, async_client),
        fixture_set=fixture_set,
    )

    assert len(report.outcomes) == len(fixture_set.cases) * 2
    assert {outcome.state for outcome in report.outcomes} == {"passed"}
    assert sync_client.closed
    assert async_client.closed


def test_runner_retains_a_failed_case_and_exposes_its_pytest_identifier() -> None:
    """Unexpected reference behavior becomes focused failure evidence, not a crash."""
    fixture_set = load_reference_fixture_set("ckan")
    sync_client = FailingSyncReferenceConnector(fixture_set)
    async_client = AsyncReferenceConnector(fixture_set)
    case = next(case for case in catalog_contract_cases(fixture_set) if case.mode == "sync" and case.outcome == "core")

    report = run_catalog_contract(
        (case,),
        sync_client=cast(SyncCatalogClient, sync_client),
        async_client=cast(AsyncCatalogClient, async_client),
        fixture_set=fixture_set,
    )

    assert report.outcomes[0].state == "failed"
    assert report.outcomes[0].warnings == (case.pytest_id, "wrong result")


def test_contract_cases_include_anonymous_mutation_capability_bulk_and_both_mode_workflows() -> None:
    """The fixture matrix keeps all D-29 workflow categories executable."""
    fixture_set = load_reference_fixture_set("socrata")

    cases = catalog_contract_cases(fixture_set)
    outcomes = {case.outcome for case in cases}

    assert {"core", "authenticated-success", "forbidden", "deployment-disabled", "unavailable"} <= outcomes
    assert {case.mode for case in cases} == {"sync", "async"}
    assert any("row-create-update-upsert-delete" in case.operation_id for case in cases)
    assert any("async-request-status" in case.operation_id for case in cases)
