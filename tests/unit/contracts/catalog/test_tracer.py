from __future__ import annotations

import asyncio
import json
from threading import get_ident
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.contracts.catalog.fakes import AsyncReferenceConnector, SyncReferenceConnector
from datasluice.contracts.catalog.fixtures import load_reference_fixture_set
from datasluice.contracts.catalog.report import ComplianceReport
from datasluice.contracts.catalog.runner import (
    CatalogContractCase,
    UnsupportedCatalogOperationError,
    catalog_contract_cases,
    run_catalog_contract,
    run_catalog_contract_async,
)

if TYPE_CHECKING:
    from datasluice.contracts.catalog.fixtures import ReferenceCase, ReferenceFixtureSet
    from datasluice.contracts.catalog.protocols import (
        AsyncCatalogClient,
        CatalogOperationGuard,
        CatalogOperationRequest,
        SyncCatalogClient,
    )
    from datasluice.domain.catalog.models import DatasetRecord, NativeRecord, ResultEnvelope


class ThreadRecordingSyncReferenceConnector(SyncReferenceConnector):
    """Reference connector that records which thread executes its sync work."""

    def __init__(self, fixture_set: ReferenceFixtureSet | None = None) -> None:
        super().__init__(fixture_set)
        self.thread_ids: list[int] = []

    def get(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[DatasetRecord]:
        self.thread_ids.append(get_ident())
        return super().get(operation, guard)

    def execute_case(self, case: ReferenceCase) -> ResultEnvelope[NativeRecord]:
        self.thread_ids.append(get_ident())
        return super().execute_case(case)


def test_catalog_contract_runs_dataset_get_in_both_modes_and_returns_json_report() -> None:
    """The same normalized case uses independent sync and async fake paths."""
    sync_client = SyncReferenceConnector()
    async_client = AsyncReferenceConnector()

    report = run_catalog_contract(
        CatalogContractCase(operation_id="datasets.get", dataset_id="fixture-dataset"),
        sync_client=cast("SyncCatalogClient", sync_client),
        async_client=cast("AsyncCatalogClient", async_client),
    )

    assert [outcome.mode for outcome in report.outcomes] == ["sync", "async"]
    assert {outcome.operation_id for outcome in report.outcomes} == {"datasets.get"}
    assert {outcome.capability for outcome in report.outcomes} == {"available"}
    assert {outcome.state for outcome in report.outcomes} == {"passed"}
    assert sync_client.dispatches == ["datasets.get"]
    assert async_client.dispatches == ["datasets.get"]
    assert sync_client.closed
    assert async_client.closed

    async_outcome = next(outcome for outcome in report.outcomes if outcome.mode == "async")
    async_metadata = dict(async_outcome.platform_metadata)
    assert "access_token" not in async_metadata
    assert "raw_body" not in async_metadata
    assert set(async_metadata) <= {"platform", "fixture", "environment", "profile_version"}

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

    catalog_contract_case = CatalogContractCase(operation_id="datasets.get", dataset_id="fixture-dataset")
    typed_value = cast("SyncCatalogClient", sync_client)
    typed_value_2 = cast("AsyncCatalogClient", async_client)
    with pytest.raises(UnsupportedCatalogOperationError) as exc_info:
        run_catalog_contract(catalog_contract_case, sync_client=typed_value, async_client=typed_value_2)

    error = exc_info.value
    assert error.operation_id == "datasets.get"
    assert error.capability == "unavailable"
    assert error.safe_action == "Inspect the deployment capability profile before retrying."
    assert sync_client.dispatches == []
    assert async_client.dispatches == []


def test_missing_dataset_id_is_rejected_before_dispatch() -> None:
    sync_client = SyncReferenceConnector()
    async_client = AsyncReferenceConnector()

    with pytest.raises(ValueError, match="dataset ID"):
        run_catalog_contract(
            CatalogContractCase(operation_id="datasets.get"),
            sync_client=cast("SyncCatalogClient", sync_client),
            async_client=cast("AsyncCatalogClient", async_client),
        )

    assert sync_client.dispatches == []
    assert async_client.dispatches == []


def test_async_runner_works_inside_an_active_event_loop() -> None:
    sync_client = SyncReferenceConnector()
    async_client = AsyncReferenceConnector()

    async def execute() -> ComplianceReport:
        return await run_catalog_contract_async(
            CatalogContractCase(operation_id="datasets.get", dataset_id="fixture-dataset"),
            sync_client=cast("SyncCatalogClient", sync_client),
            async_client=cast("AsyncCatalogClient", async_client),
        )

    report = asyncio.run(execute())

    assert report.is_compliant
    assert sync_client.closed
    assert async_client.closed


def test_tracer_sync_phase_runs_off_the_event_loop_thread() -> None:
    sync_client = ThreadRecordingSyncReferenceConnector()
    async_client = AsyncReferenceConnector()

    async def execute() -> int:
        loop_thread = get_ident()
        await run_catalog_contract_async(
            CatalogContractCase(operation_id="datasets.get", dataset_id="fixture-dataset"),
            sync_client=cast("SyncCatalogClient", sync_client),
            async_client=cast("AsyncCatalogClient", async_client),
        )
        return loop_thread

    loop_thread = asyncio.run(execute())

    assert sync_client.thread_ids
    assert all(thread_id != loop_thread for thread_id in sync_client.thread_ids)


def test_reference_sync_phase_runs_off_the_event_loop_thread() -> None:
    fixture_set = load_reference_fixture_set("ckan")
    sync_client = ThreadRecordingSyncReferenceConnector(fixture_set)
    async_client = AsyncReferenceConnector(fixture_set)
    cases = tuple(case for case in catalog_contract_cases(fixture_set) if case.mode == "sync")

    async def execute() -> int:
        loop_thread = get_ident()
        await run_catalog_contract_async(
            cases,
            sync_client=cast("SyncCatalogClient", sync_client),
            async_client=cast("AsyncCatalogClient", async_client),
            fixture_set=fixture_set,
        )
        return loop_thread

    loop_thread = asyncio.run(execute())

    assert sync_client.thread_ids
    assert all(thread_id != loop_thread for thread_id in sync_client.thread_ids)
