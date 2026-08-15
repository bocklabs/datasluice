"""Executable normalized catalog contract tracer."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from datasluice.contracts.catalog.protocols import AsyncCatalogClient, CapabilityState, SyncCatalogClient
from datasluice.contracts.catalog.report import CaseOutcome, ComplianceReport
from datasluice.domain import Dataset
from datasluice.exceptions import DataSluiceError


class UnsupportedCatalogOperationError(DataSluiceError):
    """Raised when a contract operation is unavailable before dispatch."""

    def __init__(self, *, operation_id: str, capability: CapabilityState) -> None:
        self.operation_id = operation_id
        self.capability = capability
        self.safe_action = "Inspect the deployment capability profile before retrying."
        super().__init__(f"Operation {operation_id!r} is {capability}. {self.safe_action}")


@dataclass(frozen=True)
class CatalogContractCase:
    """One normalized catalog operation to execute in both client modes."""

    operation_id: str
    dataset_id: str

    def __post_init__(self) -> None:
        if self.operation_id != "datasets.get" or not self.dataset_id:
            raise ValueError("The tracer accepts exactly one non-empty datasets.get case.")


def _require_available(operation_id: str, capability: CapabilityState) -> None:
    if capability != "available":
        raise UnsupportedCatalogOperationError(operation_id=operation_id, capability=capability)


def _assert_dataset(result: Dataset, case: CatalogContractCase) -> None:
    if not isinstance(result, Dataset) or result.id != case.dataset_id:
        raise AssertionError(f"{case.operation_id} returned an unexpected normalized dataset.")


async def _run_async_case(case: CatalogContractCase, client: AsyncCatalogClient) -> CaseOutcome:
    async with client:
        result = await client.datasets.get(case.dataset_id)
        _assert_dataset(result, case)
        return CaseOutcome(
            operation_id=case.operation_id,
            mode="async",
            capability="available",
            state="passed",
            platform_metadata=client.platform_metadata(),
        )


def run_catalog_contract(
    case: CatalogContractCase,
    *,
    sync_client: SyncCatalogClient,
    async_client: AsyncCatalogClient,
) -> ComplianceReport:
    """Execute one normalized case in both modes and return compliance evidence."""
    _require_available(case.operation_id, sync_client.capability(case.operation_id))
    _require_available(case.operation_id, async_client.capability(case.operation_id))
    with sync_client:
        sync_result = sync_client.datasets.get(case.dataset_id)
        _assert_dataset(sync_result, case)
        sync_outcome = CaseOutcome(
            operation_id=case.operation_id,
            mode="sync",
            capability="available",
            state="passed",
            platform_metadata=sync_client.platform_metadata(),
        )
    async_outcome = asyncio.run(_run_async_case(case, async_client))
    return ComplianceReport(
        outcomes=(sync_outcome, async_outcome),
        platform_metadata=sync_client.platform_metadata(),
    )
