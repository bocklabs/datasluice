"""Tests for finite runtime operation deadlines."""

from __future__ import annotations

import pytest

from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import BudgetExhaustedError
from datasluice.runtime.constants import (
    DEFAULT_CONNECT_BUDGET_SECONDS,
    DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    DEFAULT_READ_BUDGET_SECONDS,
    DEFAULT_WRITE_BUDGET_SECONDS,
)
from datasluice.runtime.resilience import DeadlineMonitor


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


def _budget(*, total: float = DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS) -> TimeBudget:
    return TimeBudget(
        connect=DEFAULT_CONNECT_BUDGET_SECONDS,
        read=DEFAULT_READ_BUDGET_SECONDS,
        write=DEFAULT_WRITE_BUDGET_SECONDS,
        total=total,
    )


def test_deadline_exhaustion_identifies_operation_platform_and_safe_remedy() -> None:
    clock = _Clock()
    monitor = DeadlineMonitor(_budget(), clock=clock)
    clock.value = DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS

    with pytest.raises(BudgetExhaustedError) as raised:
        monitor.assert_dispatchable("reference/datasets.get", "ckan")

    error = raised.value
    assert "reference/datasets.get" in str(error)
    assert "ckan" in str(error)
    assert error.platform == "ckan"
    assert error.safe_action
    assert error.elapsed_seconds == DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS
    assert error.budget_seconds == DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS


def test_retry_wait_compares_against_remaining_budget_not_total() -> None:
    clock = _Clock()
    monitor = DeadlineMonitor(_budget(total=40.0), clock=clock)
    monitor.assert_dispatchable("reference/datasets.get", "reference")
    clock.value = 25.0

    monitor.check_wait(14.0)
    monitor.check_wait(15.0)
    with pytest.raises(BudgetExhaustedError):
        monitor.check_wait(16.0)


def test_caller_supplied_budget_controls_deadline() -> None:
    clock = _Clock()
    monitor = DeadlineMonitor(_budget(total=35.0), clock=clock)
    monitor.assert_dispatchable("reference/datasets.get", "reference")
    clock.value = 34.0

    assert monitor.remaining() == 1.0
