"""Policy-enforced mutation dispatch with safe, redacted receipts."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from time import monotonic
from time import sleep as default_sleep

from datasluice.domain.catalog.ids import CatalogId
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError, map_catalog_error
from datasluice.runtime.redaction import redact_event_metadata
from datasluice.runtime.resilience import DeadlineMonitor, RetryLoop
from datasluice.runtime.transport.base import RuntimeResponse


@dataclass(frozen=True, slots=True)
class MutationDispatchRequest:
    """One policy-derived request supplied to a mutation dispatch callable."""

    operation_id: OperationId
    target: CatalogId
    concurrency_token: str | None
    overwrite: bool
    idempotency_key: str | None


class MutationEnforcer:
    """Enforce mutation policy before dispatching one catalog operation."""

    def __init__(
        self,
        send: Callable[[MutationDispatchRequest], RuntimeResponse],
        *,
        budget: TimeBudget | None = None,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        sleep: Callable[[float], None] = default_sleep,
    ) -> None:
        if not callable(send) or not callable(clock) or not callable(sleep):
            raise TypeError("Mutation enforcement requires callable dispatch, clock, and sleep dependencies.")
        if budget is not None and not isinstance(budget, TimeBudget):
            raise TypeError("Mutation enforcement budgets must use TimeBudget.")
        self._send = send
        self._budget = budget or TimeBudget(connect=10, read=30, write=30, total=60)
        self._max_attempts = max_attempts
        self._clock = clock
        self._sleep = sleep
        self.last_receipt: MutationReceipt | None = None

    def execute(
        self,
        operation_id: OperationId,
        target: CatalogId,
        policy: MutationPolicy | None,
        *,
        audit_metadata: Mapping[str, object] | None = None,
    ) -> MutationReceipt:
        """Dispatch one confirmed mutation and return its redacted receipt."""
        self._validate(operation_id, target, policy)
        assert policy is not None
        metadata = dict(audit_metadata or {})
        if policy.dry_run.requested:
            return self._receipt(operation_id, target, policy, "skipped", {**metadata, "dry_run": True})

        request = MutationDispatchRequest(
            operation_id=operation_id,
            target=target,
            concurrency_token=policy.concurrency.token if policy.concurrency is not None else None,
            overwrite=policy.concurrency.overwrite if policy.concurrency is not None else False,
            idempotency_key=policy.idempotency.key,
        )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(operation_id), operation_id.platform)
        try:
            response = RetryLoop(
                budget=self._budget,
                idempotency=policy.idempotency,
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=self._sleep,
            ).run(lambda: self._send(request))
            if not 200 <= response.status_code < 300:
                raise map_catalog_error(
                    NativeCatalogError(
                        "Catalog mutation returned an unsuccessful HTTP status.",
                        operation=str(operation_id),
                        platform=operation_id.platform,
                        status_code=response.status_code,
                        retry_after=response.retry_after,
                    )
                )
        except Exception:
            self._receipt(operation_id, target, policy, "failed", metadata)
            raise
        return self._receipt(operation_id, target, policy, "succeeded", metadata)

    def _validate(self, operation_id: OperationId, target: CatalogId, policy: MutationPolicy | None) -> None:
        if not isinstance(operation_id, OperationId) or not isinstance(target, CatalogId):
            raise TypeError("Mutation enforcement requires typed operation and target values.")
        if (
            not isinstance(policy, MutationPolicy)
            or not policy.allows_execution()
            or policy.concurrency is None
            or not policy.concurrency.allows_execution()
        ):
            raise CatalogValidationError(
                "Catalog mutations require a confirmed policy and concurrency instruction before dispatch.",
                operation=str(operation_id),
                platform=operation_id.platform,
                safe_action="Provide a confirmed mutation policy with a version token or explicit overwrite.",
            )

    def _receipt(
        self,
        operation_id: OperationId,
        target: CatalogId,
        policy: MutationPolicy,
        outcome: str,
        metadata: Mapping[str, object],
    ) -> MutationReceipt:
        redacted = redact_event_metadata(metadata)
        receipt = MutationReceipt(
            operation=str(operation_id),
            outcome=outcome,
            target=target,
            version_token=policy.concurrency.token if policy.concurrency is not None else None,
            atomicity="independent",
            operation_atomicity="independent",
            audit_metadata=redacted,
        )
        self.last_receipt = receipt
        return receipt
