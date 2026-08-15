"""Executable catalog conformance cases and deterministic runner."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol, cast

from datasluice.contracts.catalog.fixtures import ReferenceCase, ReferenceFixtureSet
from datasluice.contracts.catalog.protocols import (
    AsyncCatalogClient,
    CapabilityState,
    CatalogOperationGuard,
    CatalogOperationRequest,
    SyncCatalogClient,
)
from datasluice.contracts.catalog.report import CaseOutcome, ComplianceReport
from datasluice.domain.catalog.models import DatasetRecord, NativeRecord, ResultEnvelope
from datasluice.domain.catalog.operations import OperationId
from datasluice.errors.catalog import (
    CatalogRateLimitError,
    ForbiddenError,
    UnauthenticatedError,
    UnsupportedCapabilityError,
)
from datasluice.exceptions import DataSluiceError

type ContractMode = Literal["sync", "async"]
type FixtureOutcome = Literal[
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
]

_OUTCOMES = frozenset(
    {
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
)
_SUCCESS_OUTCOMES = frozenset({"core", "optional", "authenticated-success", "async-pending"})
_EXPECTED_ERRORS: dict[str, tuple[type[Exception], ...]] = {
    "missing-credentials": (UnauthenticatedError,),
    "invalid-credentials": (UnauthenticatedError,),
    "forbidden": (ForbiddenError,),
    "deployment-disabled": (UnsupportedCapabilityError,),
    "unavailable": (UnsupportedCapabilityError,),
    "rate-limited": (CatalogRateLimitError,),
}


class UnsupportedCatalogOperationError(DataSluiceError):
    """Raised when a contract operation is unavailable before dispatch."""

    def __init__(self, *, operation_id: str, capability: CapabilityState) -> None:
        self.operation_id = operation_id
        self.capability = capability
        self.safe_action = "Inspect the deployment capability profile before retrying."
        super().__init__(f"Operation {operation_id!r} is {capability}. {self.safe_action}")


class _SyncReferenceCaseClient(Protocol):
    """Minimal synchronous fixture-case executor used by the public runner."""

    def execute_case(self, case: ReferenceCase) -> ResultEnvelope[NativeRecord]:
        """Execute one declared fixture case."""

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe platform metadata."""


class _AsyncReferenceCaseClient(Protocol):
    """Minimal asynchronous fixture-case executor used by the public runner."""

    async def execute_case(self, case: ReferenceCase) -> ResultEnvelope[NativeRecord]:
        """Execute one declared fixture case."""

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe platform metadata."""


@dataclass(frozen=True, slots=True)
class CatalogContractCase:
    """One deterministic catalog operation/state/mode contract case."""

    operation_id: str
    outcome: FixtureOutcome = "core"
    mode: ContractMode = "sync"
    dataset_id: str | None = None

    def __post_init__(self) -> None:
        if not self.operation_id or self.outcome not in _OUTCOMES or self.mode not in {"sync", "async"}:
            raise ValueError("Catalog contract cases require a declared operation, outcome, and mode.")
        if self.dataset_id is not None and not self.dataset_id:
            raise ValueError("Catalog contract dataset IDs must be non-empty when supplied.")

    @property
    def pytest_id(self) -> str:
        """Return the stable identifier used by parametrized pytest cases."""
        return f"{self.operation_id}[{self.outcome}][{self.mode}]"


def catalog_contract_cases(fixture_set: ReferenceFixtureSet) -> tuple[CatalogContractCase, ...]:
    """Generate stable sync and async cases from one pinned fixture set."""
    if not isinstance(fixture_set, ReferenceFixtureSet):
        raise TypeError("Catalog contract cases require a pinned reference fixture set.")
    cases = [
        CatalogContractCase(
            operation_id=str(reference_case.operation_id),
            outcome=cast(FixtureOutcome, reference_case.outcome),
            mode=mode,
        )
        for reference_case in fixture_set.cases
        for mode in ("sync", "async")
    ]
    return tuple(sorted(cases, key=lambda case: (case.operation_id, case.outcome, case.mode)))


def _require_available(operation_id: str, capability: CapabilityState) -> None:
    if capability != "available":
        raise UnsupportedCatalogOperationError(operation_id=operation_id, capability=capability)


def _assert_dataset(result: ResultEnvelope[DatasetRecord], case: CatalogContractCase) -> None:
    if case.dataset_id is None or len(result.items) != 1 or result.items[0].id.value != case.dataset_id:
        raise AssertionError(f"{case.operation_id} returned an unexpected normalized dataset.")


def _dataset_get_call(case: CatalogContractCase) -> tuple[CatalogOperationRequest, CatalogOperationGuard]:
    operation_id = OperationId(platform="catalog", service="datasets", method="get")
    return CatalogOperationRequest(operation_id=operation_id, payload={"id": case.dataset_id}), CatalogOperationGuard(
        operation_id=operation_id
    )


async def _run_tracer_async_case(case: CatalogContractCase, client: AsyncCatalogClient) -> CaseOutcome:
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


def _reference_case(case: CatalogContractCase, fixture_set: ReferenceFixtureSet) -> ReferenceCase:
    return next(
        reference_case
        for reference_case in fixture_set.cases
        if str(reference_case.operation_id) == case.operation_id and reference_case.outcome == case.outcome
    )


def _outcome(case: CatalogContractCase, metadata: Mapping[str, object], error: Exception | None = None) -> CaseOutcome:
    if error is None:
        return CaseOutcome(
            operation_id=case.operation_id,
            mode=case.mode,
            capability="available" if case.outcome in _SUCCESS_OUTCOMES else "unavailable",
            state="passed",
            tier=case.outcome,
            warnings=(),
            platform_metadata=metadata,
        )
    return CaseOutcome(
        operation_id=case.operation_id,
        mode=case.mode,
        capability="available" if case.outcome in _SUCCESS_OUTCOMES else "unavailable",
        state="failed",
        tier=case.outcome,
        warnings=(case.pytest_id, str(error)[:256]),
        platform_metadata=metadata,
    )


def _run_reference_sync_case(
    case: CatalogContractCase, client: _SyncReferenceCaseClient, fixture_set: ReferenceFixtureSet
) -> CaseOutcome:
    reference_case = _reference_case(case, fixture_set)
    metadata = client.platform_metadata()
    try:
        result = client.execute_case(reference_case)
        if case.outcome not in _SUCCESS_OUTCOMES or not result.items:
            raise AssertionError("The connector accepted a case that must be rejected.")
    except Exception as error:
        if isinstance(error, _EXPECTED_ERRORS.get(case.outcome, ())):
            return _outcome(case, metadata)
        return _outcome(case, metadata, error)
    return _outcome(case, metadata)


async def _run_reference_async_case(
    case: CatalogContractCase, client: _AsyncReferenceCaseClient, fixture_set: ReferenceFixtureSet
) -> CaseOutcome:
    reference_case = _reference_case(case, fixture_set)
    metadata = client.platform_metadata()
    try:
        result = await client.execute_case(reference_case)
        if case.outcome not in _SUCCESS_OUTCOMES or not result.items:
            raise AssertionError("The connector accepted a case that must be rejected.")
    except Exception as error:
        if isinstance(error, _EXPECTED_ERRORS.get(case.outcome, ())):
            return _outcome(case, metadata)
        return _outcome(case, metadata, error)
    return _outcome(case, metadata)


def _run_reference_cases(
    cases: tuple[CatalogContractCase, ...], sync_client: object, async_client: object, fixture_set: ReferenceFixtureSet
) -> tuple[CaseOutcome, ...]:
    outcomes: list[CaseOutcome] = []
    with cast(SyncCatalogClient, sync_client):
        for case in cases:
            if case.mode == "sync":
                outcomes.append(
                    _run_reference_sync_case(case, cast(_SyncReferenceCaseClient, sync_client), fixture_set)
                )

    async def execute_async() -> None:
        async with cast(AsyncCatalogClient, async_client):
            for case in cases:
                if case.mode == "async":
                    outcomes.append(
                        await _run_reference_async_case(
                            case, cast(_AsyncReferenceCaseClient, async_client), fixture_set
                        )
                    )

    asyncio.run(execute_async())
    return tuple(sorted(outcomes, key=lambda outcome: (outcome.operation_id, outcome.mode, outcome.warnings)))


def _run_tracer_case(
    case: CatalogContractCase, sync_client: SyncCatalogClient, async_client: AsyncCatalogClient
) -> ComplianceReport:
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
    async_outcome = asyncio.run(_run_tracer_async_case(case, async_client))
    return ComplianceReport(outcomes=(sync_outcome, async_outcome), platform_metadata=sync_client.platform_metadata())


def run_catalog_contract(
    case: CatalogContractCase | Iterable[CatalogContractCase],
    *,
    sync_client: SyncCatalogClient,
    async_client: AsyncCatalogClient,
    fixture_set: ReferenceFixtureSet | None = None,
) -> ComplianceReport:
    """Execute a finite catalog contract matrix and retain every case outcome."""
    if isinstance(case, CatalogContractCase):
        if fixture_set is None:
            return _run_tracer_case(case, sync_client, async_client)
        cases = (case,)
    else:
        cases = tuple(case)
    if not cases or not all(isinstance(contract_case, CatalogContractCase) for contract_case in cases):
        raise ValueError("Catalog contract execution requires one or more declared cases.")
    if fixture_set is None:
        raise ValueError("An exhaustive catalog contract matrix requires its pinned fixture set.")
    expected = catalog_contract_cases(fixture_set)
    if not set(cases) <= set(expected):
        raise ValueError("Catalog contract cases must be generated from the pinned fixture set.")
    outcomes = _run_reference_cases(cases, sync_client, async_client, fixture_set)
    return ComplianceReport(
        outcomes=outcomes,
        connector_id=f"datasluice/{fixture_set.platform}",
        manifest_version="reference-v1",
        profile_version=fixture_set.profile_version,
        fixture_fingerprint=fixture_set.fingerprint,
        contract_schema_version=str(ComplianceReport.SCHEMA_VERSION),
        expected_case_ids=tuple(contract_case.pytest_id for contract_case in cases),
        platform_metadata=sync_client.platform_metadata(),
    )
