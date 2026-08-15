"""Executable normalized catalog contract tracer."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from datasluice.contracts.catalog.protocols import (
    AsyncCatalogClient,
    CapabilityState,
    CatalogOperationGuard,
    CatalogOperationRequest,
    SyncCatalogClient,
)
from datasluice.contracts.catalog.report import CaseOutcome, ComplianceReport
from datasluice.domain.catalog.models import DatasetRecord, ResultEnvelope
from datasluice.domain.catalog.operations import OperationId
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


def _assert_dataset(result: ResultEnvelope[DatasetRecord], case: CatalogContractCase) -> None:
    if len(result.items) != 1 or result.items[0].id.value != case.dataset_id:
        raise AssertionError(f"{case.operation_id} returned an unexpected normalized dataset.")


def _dataset_get_call(case: CatalogContractCase) -> tuple[CatalogOperationRequest, CatalogOperationGuard]:
    operation_id = OperationId(platform="catalog", service="datasets", method="get")
    return CatalogOperationRequest(operation_id=operation_id, payload={"id": case.dataset_id}), CatalogOperationGuard(
        operation_id=operation_id
    )


async def _run_async_case(case: CatalogContractCase, client: AsyncCatalogClient) -> CaseOutcome:
    async with client:
        operation, guard = _dataset_get_call(case)
        result = await client.datasets.get(operation, guard)
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
        operation, guard = _dataset_get_call(case)
        sync_result = sync_client.datasets.get(operation, guard)
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
