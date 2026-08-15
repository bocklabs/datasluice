"""End-to-end tests for the catalog contract tracer."""

from __future__ import annotations

import json

import pytest

from datasluice.contracts.catalog.fakes import AsyncReferenceConnector, SyncReferenceConnector
from datasluice.contracts.catalog.report import ComplianceReport
from datasluice.contracts.catalog.runner import (
    CatalogContractCase,
    UnsupportedCatalogOperationError,
    run_catalog_contract,
)


def test_catalog_contract_runs_dataset_get_in_both_modes_and_returns_json_report() -> None:
    """The same normalized case uses independent sync and async fake paths."""
    sync_client = SyncReferenceConnector()
    async_client = AsyncReferenceConnector()

    report = run_catalog_contract(
        CatalogContractCase(operation_id="datasets.get", dataset_id="fixture-dataset"),
        sync_client=sync_client,
        async_client=async_client,
    )

    assert [outcome.mode for outcome in report.outcomes] == ["sync", "async"]
    assert {outcome.operation_id for outcome in report.outcomes} == {"datasets.get"}
    assert {outcome.capability for outcome in report.outcomes} == {"available"}
    assert {outcome.state for outcome in report.outcomes} == {"passed"}
    assert sync_client.dispatches == ["datasets.get"]
    assert async_client.dispatches == ["datasets.get"]
    assert sync_client.closed
    assert async_client.closed

    payload = report.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert ComplianceReport.from_dict(payload) == report
    assert payload is not report.to_dict()
    platform_metadata = payload["platform_metadata"]
    assert isinstance(platform_metadata, dict)
    assert "access_token" not in platform_metadata
    assert "raw_body" not in platform_metadata


def test_unavailable_case_is_rejected_before_either_fake_dispatches() -> None:
    """Unavailable capabilities provide typed pre-dispatch evidence."""
    sync_client = SyncReferenceConnector(capability="unavailable")
    async_client = AsyncReferenceConnector(capability="unavailable")

    with pytest.raises(UnsupportedCatalogOperationError) as exc_info:
        run_catalog_contract(
            CatalogContractCase(operation_id="datasets.get", dataset_id="fixture-dataset"),
            sync_client=sync_client,
            async_client=async_client,
        )

    error = exc_info.value
    assert error.operation_id == "datasets.get"
    assert error.capability == "unavailable"
    assert error.safe_action == "Inspect the deployment capability profile before retrying."
    assert sync_client.dispatches == []
    assert async_client.dispatches == []
