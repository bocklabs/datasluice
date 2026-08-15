"""Deterministic fixture-backed reference clients for catalog contract tests."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import TracebackType
from urllib.parse import urlsplit, urlunsplit

from datasluice.contracts.catalog.fixtures import ReferenceCase, ReferenceFixtureSet
from datasluice.contracts.catalog.protocols import CapabilityState, CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import DatasetRecord, NativeRecord, PlatformMetadata, ResultEnvelope
from datasluice.domain.catalog.observability import DiagnosticPolicy, StructuredEvent, TelemetryPolicy, TLSPolicy
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import BulkCheckpoint, BulkItemReceipt, BulkPlan, MutationReceipt
from datasluice.domain.catalog.resilience import CircuitKey, CircuitState, RetryDecision, TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy, MutationPolicy
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
        self,
        fixture_set: ReferenceFixtureSet | None = None,
        *,
        capability: CapabilityState = "available",
        diagnostic_policy: DiagnosticPolicy | None = None,
        event_sink: Callable[[StructuredEvent], None] | None = None,
    ) -> None:
        self._capability = capability
        self._fixture_set = fixture_set
        self._diagnostic_policy = diagnostic_policy or DiagnosticPolicy()
        self._event_sink = event_sink
        self._circuit = CircuitState()
        self._datasets = {dataset_id: dict(value) for dataset_id, value in _FIXTURE_DATASETS.items()}
        self.dispatches: list[str] = []
        self.events: list[StructuredEvent] = []
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
        }

    @property
    def circuit(self) -> CircuitState:
        """Return the typed in-memory circuit state for this fake instance."""
        return self._circuit

    @property
    def circuit_key(self) -> CircuitKey:
        """Return a credential-scoped synthetic circuit identity without a live endpoint."""
        return CircuitKey(
            origin="https://reference.invalid",
            credential_scope=self._fixture_set.platform if self._fixture_set else "reference",
        )

    @property
    def tls_policy(self) -> TLSPolicy:
        """Return the secure default TLS policy represented by the fake."""
        return TLSPolicy()

    @property
    def telemetry_policy(self) -> TelemetryPolicy:
        """Return inactive-by-default telemetry policy for deterministic fakes."""
        return TelemetryPolicy()

    def diagnostic(self, body: bytes) -> bytes | None:
        """Return caller-opted-in raw diagnostics bounded by its explicit policy."""
        if not self._diagnostic_policy.include_raw_body:
            return None
        return self._diagnostic_policy.bound_raw_body(body)

    def record_event(self, name: str, metadata: Mapping[str, object]) -> StructuredEvent:
        """Record one locally redacted event and optionally notify a caller sink."""
        event = StructuredEvent(name=name, metadata=_sanitize_event_metadata(metadata))
        self.events.append(event)
        if self._event_sink is not None:
            self._event_sink(event)
        return event

    def retry_decision(self, policy: IdempotencyPolicy) -> RetryDecision:
        """Derive a typed retry decision for a deterministic rate-limited response."""
        return RetryDecision.for_response(
            attempt=1,
            max_attempts=2,
            status_code=429,
            retry_after=1,
            idempotency=policy,
            budget=TimeBudget(),
        )

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
            self.record_event(
                "catalog.reference.rejected", {"operation": str(case.operation_id), "outcome": case.outcome}
            )
            raise error_type(
                "Reference fixture state rejected the operation before dispatch.",
                operation=str(case.operation_id),
                platform=case.operation_id.platform,
                capability_state=case.outcome,
                safe_action="Inspect the declared reference case before retrying.",
            )
        if case.outcome == "rate-limited":
            self._circuit = self._circuit.record_failure()
            self.record_event("catalog.reference.rate_limited", {"operation": str(case.operation_id), "retry_after": 1})
            raise CatalogRateLimitError(
                "Reference fixture rate limit.",
                operation=str(case.operation_id),
                platform=case.operation_id.platform,
                safe_action="Wait for Retry-After before retrying a safe operation.",
                retry_after=1,
            )
        self.dispatches.append(str(case.operation_id))
        self.record_event(
            "catalog.reference.dispatched", {"operation": str(case.operation_id), "outcome": case.outcome}
        )

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
        self,
        fixture_set: ReferenceFixtureSet | None = None,
        *,
        capability: CapabilityState = "available",
        diagnostic_policy: DiagnosticPolicy | None = None,
        event_sink: Callable[[StructuredEvent], None] | None = None,
    ) -> None:
        self._capability = capability
        self._sync = SyncReferenceConnector(
            fixture_set,
            capability=capability,
            diagnostic_policy=diagnostic_policy,
            event_sink=event_sink,
        )
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


def _sanitize_event_metadata(metadata: Mapping[str, object]) -> dict[str, object]:
    """Remove query-bearing URLs before structured-event redaction runs."""
    sanitized: dict[str, object] = {}
    for key, value in metadata.items():
        if isinstance(value, Mapping):
            sanitized[key] = _sanitize_event_metadata(value)
        elif key.lower().replace("-", "_") in {"url", "uri", "signed_url"} and isinstance(value, str):
            parsed = urlsplit(value)
            sanitized[key] = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        else:
            sanitized[key] = value
    return sanitized
