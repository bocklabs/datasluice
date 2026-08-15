"""Deterministic fixture-backed reference clients for catalog contract tests."""

from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType

from datasluice.contracts.catalog.fixtures import ReferenceCase, ReferenceFixtureSet
from datasluice.contracts.catalog.protocols import CapabilityState, CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import DatasetRecord, NativeRecord, PlatformMetadata, ResultEnvelope
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import BulkCheckpoint, BulkItemReceipt, BulkPlan, MutationReceipt
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import (
    CatalogError,
    CatalogRateLimitError,
    CatalogValidationError,
    ForbiddenError,
    UnauthenticatedError,
    UnsupportedCapabilityError,
)

_FIXTURE_DATASETS = {"fixture-dataset": {"id": "fixture-dataset", "title": "Fixture dataset"}}


class SyncReferenceConnector:
    """Independent synchronous fixture-backed catalog reference client."""

    def __init__(
        self, fixture_set: ReferenceFixtureSet | None = None, *, capability: CapabilityState = "available"
    ) -> None:
        self._capability = capability
        self._fixture_set = fixture_set
        self._datasets = {dataset_id: dict(value) for dataset_id, value in _FIXTURE_DATASETS.items()}
        self.dispatches: list[str] = []
        self.closed = False

    @property
    def datasets(self) -> SyncReferenceConnector:
        """Return the synchronous dataset service."""
        return self

    def capability(self, operation_id: str) -> CapabilityState:
        """Return the deterministic capability classification."""
        if self._fixture_set is not None:
            return (
                "available"
                if any(str(case.operation_id) == operation_id for case in self._fixture_set.cases)
                else "unavailable"
            )
        if operation_id != "datasets.get":
            return "unavailable"
        return self._capability

    def platform_metadata(self) -> Mapping[str, object]:
        """Return metadata that proves report sanitization."""
        return {
            "platform": self._fixture_set.platform if self._fixture_set is not None else "reference",
            "fixture": self._fixture_set.fingerprint if self._fixture_set is not None else "dataset-v1",
            "environment": "deterministic",
            "access_token": "never-report-this",
            "raw_body": "never-report-this",
        }

    def get(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[DatasetRecord]:
        """Return one deterministic fixture dataset."""
        guard.require_allowed()
        self.dispatches.append("datasets.get")
        payload = self._datasets[str(operation.payload["id"])]
        return ResultEnvelope(
            items=(
                DatasetRecord(
                    id=CatalogId(CatalogPlatform("reference"), ResourceKind.DATASET, payload["id"]),
                    name=payload["title"],
                ),
            )
        )

    def execute_case(self, case: ReferenceCase) -> ResultEnvelope[NativeRecord]:
        """Execute one declared outcome without allowing network or random state."""
        self._require_declared_case(case)
        self._guard_case(case)
        return self._result(case)

    def execute_operation(self, operation_id: OperationId, outcome: str) -> ResultEnvelope[NativeRecord]:
        """Execute one declared operation/outcome pair."""
        if self._fixture_set is None or str(operation_id) not in {
            str(declared) for declared in self._fixture_set.declared_operations
        }:
            raise ValueError("Reference operation is undeclared by the pinned profile.")
        case = next(
            (
                item
                for item in self._fixture_set.cases
                if str(item.operation_id) == str(operation_id) and item.outcome == outcome
            ),
            None,
        )
        if case is None:
            raise ValueError("Reference operation outcome is not declared by the fixture set.")
        return self.execute_case(case)

    def execute_mutation(
        self, case: ReferenceCase, policy: MutationPolicy
    ) -> tuple[ResultEnvelope[NativeRecord], MutationReceipt]:
        """Execute a policy-guarded deterministic mutation with a redacted receipt."""
        if not isinstance(policy, MutationPolicy) or not policy.allows_execution() or policy.concurrency is None:
            raise CatalogValidationError(
                "Reference mutations require confirmation and a concurrency instruction.",
                operation=str(case.operation_id),
                platform=case.operation_id.platform,
                safe_action="Provide an explicit mutation policy before dispatching.",
            )
        result = self.execute_case(case)
        target = result.items[0].id
        return result, MutationReceipt(
            operation=str(case.operation_id),
            outcome="succeeded",
            target=target,
            version_token=policy.concurrency.token or "overwrite",
            request_id="fixture-0001",
            audit_metadata={"fixture": self._fixture_set.fingerprint if self._fixture_set else "dataset-v1"},
        )

    def execute_bulk(self, case: ReferenceCase, policy: MutationPolicy, *, count: int) -> BulkCheckpoint:
        """Return ordered, resumable receipts for a bounded reference bulk execution."""
        if type(count) is not int or count < 1:
            raise ValueError("Reference bulk execution count must be positive.")
        _, receipt = self.execute_mutation(case, policy)
        target = receipt.target
        plan = BulkPlan(
            operation=receipt.operation,
            items=tuple(CatalogId(target.platform, target.resource_kind, f"fixture-{index}") for index in range(count)),
        )
        receipts = tuple(
            BulkItemReceipt(
                index=index,
                receipt=MutationReceipt(
                    operation=receipt.operation,
                    outcome="succeeded",
                    target=item,
                    version_token=receipt.version_token,
                    request_id=f"fixture-{index + 1:04d}",
                    audit_metadata=receipt.audit_metadata,
                ),
            )
            for index, item in enumerate(plan.items)
        )
        return BulkCheckpoint(plan=plan, item_receipts=receipts, resumption_cursor=f"fixture-{count:04d}")

    def _require_declared_case(self, case: ReferenceCase) -> None:
        if self._fixture_set is None or case not in self._fixture_set.cases:
            raise ValueError("Reference cases must be loaded from the pinned fixture set.")

    def _guard_case(self, case: ReferenceCase) -> None:
        errors: dict[str, type[CatalogError]] = {
            "missing-credentials": UnauthenticatedError,
            "invalid-credentials": UnauthenticatedError,
            "forbidden": ForbiddenError,
            "deployment-disabled": UnsupportedCapabilityError,
            "unavailable": UnsupportedCapabilityError,
        }
        error_type = errors.get(case.outcome)
        if error_type is not None:
            raise error_type(
                "Reference fixture state rejected the operation before dispatch.",
                operation=str(case.operation_id),
                platform=case.operation_id.platform,
                capability_state=case.outcome,
                safe_action="Inspect the declared reference case before retrying.",
            )
        if case.outcome == "rate-limited":
            raise CatalogRateLimitError(
                "Reference fixture rate limit.",
                operation=str(case.operation_id),
                platform=case.operation_id.platform,
                safe_action="Wait for Retry-After before retrying a safe operation.",
                retry_after=1,
            )
        self.dispatches.append(str(case.operation_id))

    def _result(self, case: ReferenceCase) -> ResultEnvelope[NativeRecord]:
        platform = CatalogPlatform(case.operation_id.platform)
        target = CatalogId(platform, ResourceKind.DATASET, "fixture-dataset")
        return ResultEnvelope(
            items=(
                NativeRecord(
                    platform=platform,
                    resource_kind=ResourceKind.DATASET,
                    id=target,
                    payload={"operation": str(case.operation_id), "outcome": case.outcome},
                ),
            ),
            platform=PlatformMetadata(
                platform=platform, api_version=self._fixture_set.profile_version if self._fixture_set else None
            ),
        )

    def close(self) -> None:
        """Close the synchronous reference client exactly once."""
        self.closed = True

    def __enter__(self) -> SyncReferenceConnector:
        """Enter the synchronous reference client context."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the synchronous reference client context."""
        self.close()


class AsyncReferenceConnector:
    """Independent asynchronous fixture-backed catalog reference client."""

    def __init__(
        self, fixture_set: ReferenceFixtureSet | None = None, *, capability: CapabilityState = "available"
    ) -> None:
        self._capability = capability
        self._sync = SyncReferenceConnector(fixture_set, capability=capability)
        self._fixture_set = fixture_set
        self._datasets = {dataset_id: dict(value) for dataset_id, value in _FIXTURE_DATASETS.items()}
        self.dispatches: list[str] = []
        self.closed = False

    @property
    def datasets(self) -> AsyncReferenceConnector:
        """Return the asynchronous dataset service."""
        return self

    def capability(self, operation_id: str) -> CapabilityState:
        """Return the deterministic capability classification."""
        if self._fixture_set is not None:
            return self._sync.capability(operation_id)
        if operation_id != "datasets.get":
            return "unavailable"
        return self._capability

    def platform_metadata(self) -> Mapping[str, object]:
        """Return metadata that proves report sanitization."""
        return {
            "platform": self._fixture_set.platform if self._fixture_set is not None else "reference",
            "fixture": self._fixture_set.fingerprint if self._fixture_set is not None else "dataset-v1",
            "environment": "deterministic",
            "access_token": "never-report-this",
            "raw_body": "never-report-this",
        }

    async def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[DatasetRecord]:
        """Return one deterministic fixture dataset without sync delegation."""
        guard.require_allowed()
        self.dispatches.append("datasets.get")
        payload = self._datasets[str(operation.payload["id"])]
        return ResultEnvelope(
            items=(
                DatasetRecord(
                    id=CatalogId(CatalogPlatform("reference"), ResourceKind.DATASET, payload["id"]),
                    name=payload["title"],
                ),
            )
        )

    async def execute_case(self, case: ReferenceCase) -> ResultEnvelope[NativeRecord]:
        """Execute a declared fixture case without delegating asynchronous I/O."""
        self._sync._require_declared_case(case)
        self._sync._guard_case(case)
        self.dispatches.append(str(case.operation_id))
        self._sync.dispatches.pop()
        return self._sync._result(case)

    async def aclose(self) -> None:
        """Close the asynchronous reference client exactly once."""
        self.closed = True

    async def __aenter__(self) -> AsyncReferenceConnector:
        """Enter the asynchronous reference client context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the asynchronous reference client context."""
        await self.aclose()
