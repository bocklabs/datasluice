from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datasluice.contracts.catalog.fakes import AsyncReferenceConnector, SyncReferenceConnector
from datasluice.contracts.catalog.fixtures import load_reference_fixture_set
from datasluice.contracts.catalog.runner import catalog_contract_cases, run_catalog_contract

if TYPE_CHECKING:
    from datasluice.contracts.catalog.protocols import AsyncCatalogClient, SyncCatalogClient


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_unexpected_execution_errors_keep_the_mode_and_case_warning(mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture_set = load_reference_fixture_set("ckan")
    sync_client = SyncReferenceConnector(fixture_set)
    async_client = AsyncReferenceConnector(fixture_set)
    case = next(case for case in catalog_contract_cases(fixture_set) if case.mode == mode and case.outcome == "core")

    def fail(*args: object) -> None:
        raise AssertionError("wrong result")

    async def fail_async(*args: object) -> None:
        raise AssertionError("wrong result")

    monkeypatch.setattr(sync_client, "execute_case", fail)
    monkeypatch.setattr(async_client, "execute_case", fail_async)
    report = run_catalog_contract(
        (case,),
        sync_client=cast("SyncCatalogClient", sync_client),
        async_client=cast("AsyncCatalogClient", async_client),
        fixture_set=fixture_set,
    )

    assert len(report.outcomes) == 1
    assert report.outcomes[0].mode == mode
    assert report.outcomes[0].state == "failed"
    assert report.outcomes[0].warnings == (case.pytest_id, "wrong result")
    assert sync_client.closed
    assert async_client.closed
