"""Runtime-owned deadlines, retry decisions, and circuit state holders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from time import monotonic

from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import BudgetExhaustedError


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
