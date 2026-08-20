"""Runtime-owned deadlines, retry decisions, and circuit state holders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from threading import RLock
from time import monotonic

from datasluice.domain.catalog.resilience import CircuitKey, CircuitState, RetryDecision, TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import BudgetExhaustedError
from datasluice.logging import get_logger
from datasluice.runtime.transport.base import RuntimeResponse, TransportFailure

logger = get_logger("runtime.resilience")


class DeadlineMonitor:
    """Track one operation's finite total budget against a monotonic clock."""

    def __init__(self, budget: TimeBudget, *, clock: Callable[[], float] = monotonic) -> None:
        if not isinstance(budget, TimeBudget):
            raise TypeError("Deadline monitors require a TimeBudget.")
        if not callable(clock):
            raise TypeError("Deadline monitors require a monotonic clock callable.")
        self._budget = budget
        self._clock = clock
        self._started_at = clock()
        self._operation: str | None = None
        self._platform: str | None = None

    @property
    def budget(self) -> TimeBudget:
        """Return the immutable operation budget."""
        return self._budget

    def remaining(self) -> float:
        """Return the remaining total operation time without clipping negatives."""
        return self._budget.total - (self._clock() - self._started_at)

    def assert_dispatchable(self, operation: str, platform: str) -> None:
        """Raise a typed error when the operation deadline has expired."""
        self._operation = operation
        self._platform = platform
        if self.remaining() <= 0:
            self._raise_exhausted({"phase": "dispatch"})

    def check_wait(self, delay: float, *, retry_state: Mapping[str, object] | None = None) -> None:
        """Reject a retry wait that would exceed the remaining operation time."""
        if (type(delay) is not int and type(delay) is not float) or delay < 0:
            raise ValueError("Retry delays must be non-negative numbers.")
        if delay > self.remaining():
            state = {"phase": "retry-wait", "delay_seconds": float(delay)}
            if retry_state is not None:
                state.update(retry_state)
            self._raise_exhausted(state)

    def _raise_exhausted(self, retry_state: Mapping[str, object]) -> None:
        operation = self._operation or "runtime.dispatch"
        platform = self._platform or "runtime"
        elapsed = max(0.0, self._clock() - self._started_at)
        raise BudgetExhaustedError(
            f"Catalog operation {operation} on {platform} exhausted its total time budget.",
            operation=operation,
            platform=platform,
            capability_state="unavailable",
            safe_action="Increase the operation budget or retry after the deployment is available.",
            elapsed_seconds=elapsed,
            budget_seconds=self._budget.total,
            retry_state=retry_state,
        )


class RetryLoop:
    """Apply frozen retry decisions while enforcing a runtime deadline."""

    def __init__(
        self,
        *,
        budget: TimeBudget,
        idempotency: IdempotencyPolicy,
        deadline: DeadlineMonitor,
        max_attempts: int = 3,
        sleep: Callable[[float], None],
    ) -> None:
        if not isinstance(budget, TimeBudget) or not isinstance(idempotency, IdempotencyPolicy):
            raise TypeError("Retry loops require typed budgets and idempotency policies.")
        if not isinstance(deadline, DeadlineMonitor):
            raise TypeError("Retry loops require a DeadlineMonitor.")
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("Retry loops require at least one attempt.")
        if not callable(sleep):
            raise TypeError("Retry loops require a sleep callable.")
        self._budget = budget
        self._idempotency = idempotency
        self._deadline = deadline
        self._max_attempts = max_attempts
        self._sleep = sleep

    def run(self, send: Callable[[], RuntimeResponse]) -> RuntimeResponse:
        """Send until a terminal response or transport failure is reached."""
        last_failure: TransportFailure | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = send()
                status_code = response.status_code
                retry_after = response.retry_after
            except TransportFailure as exc:
                last_failure = exc
                response = None
                status_code = None
                retry_after = None
            decision = RetryDecision.for_response(
                attempt=attempt,
                max_attempts=self._max_attempts,
                status_code=status_code,
                retry_after=retry_after,
                idempotency=self._idempotency,
                budget=self._budget,
            )
            if not decision.retry:
                if response is not None:
                    return response
                assert last_failure is not None
                raise last_failure
            delay = decision.delay or 0.0
            self._deadline.check_wait(
                delay,
                retry_state={"attempt": attempt, "max_attempts": self._max_attempts, "reason": decision.reason},
            )
            logger.warning(
                "Attempt %d/%d received retryable runtime outcome — retrying in %.1fs",
                attempt,
                self._max_attempts,
                delay,
            )
            self._sleep(delay)
        raise RuntimeError("Retry loop exhausted without a terminal runtime outcome.")


class BreakerRegistry:
    """Keep per-origin credential-scoped circuit snapshots and half-open trials."""

    def __init__(
        self,
        *,
        failure_threshold: int,
        cooldown: float,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._initial_state = CircuitState(failure_threshold=failure_threshold, cooldown=cooldown)
        if not callable(clock):
            raise TypeError("Breaker registries require a monotonic clock callable.")
        self._clock = clock
        self._states: dict[CircuitKey, CircuitState] = {}
        self._opened_at: dict[CircuitKey, float] = {}
        self._trial_in_flight: set[CircuitKey] = set()
        self._lock = RLock()

    def admit(self, key: CircuitKey) -> bool:
        """Return whether one dispatch may proceed through the circuit."""
        with self._lock:
            state = self._state_for(key)
            if not state.open:
                return True
            opened_at = self._opened_at.get(key, self._clock())
            if self._clock() - opened_at < state.cooldown or key in self._trial_in_flight:
                return False
            self._trial_in_flight.add(key)
            return True

    def record_success(self, key: CircuitKey) -> CircuitState:
        """Close a circuit and clear failures after a successful trial or response."""
        with self._lock:
            self._states[key] = self._state_for(key).reset()
            self._opened_at.pop(key, None)
            self._trial_in_flight.discard(key)
            return self._states[key]

    def record_transport_failure(self, key: CircuitKey) -> CircuitState:
        """Record one transport-level or 5xx origin-health failure."""
        with self._lock:
            state = self._state_for(key).record_failure()
            self._states[key] = state
            self._trial_in_flight.discard(key)
            if state.open:
                self._opened_at[key] = self._clock()
            return state

    def record_response(self, key: CircuitKey, status_code: int) -> CircuitState:
        """Record response health without treating 4xx outcomes as origin failures."""
        if type(status_code) is not int or not 100 <= status_code <= 599:
            raise ValueError("Circuit response statuses must be valid HTTP status codes.")
        if status_code >= 500:
            return self.record_transport_failure(key)
        if 200 <= status_code < 400:
            return self.record_success(key)
        return self.inspect(key)

    def inspect(self, key: CircuitKey) -> CircuitState:
        """Return the immutable circuit snapshot for one identity."""
        with self._lock:
            return self._state_for(key)

    def reset(self, key: CircuitKey) -> CircuitState:
        """Explicitly reset one circuit for operational recovery."""
        return self.record_success(key)

    def _state_for(self, key: CircuitKey) -> CircuitState:
        if not isinstance(key, CircuitKey):
            raise TypeError("Circuit operations require a CircuitKey.")
        return self._states.setdefault(key, self._initial_state)
