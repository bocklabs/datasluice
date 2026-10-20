from __future__ import annotations

from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.resilience import TimeBudget

_MAX_POLLS = 1024
_MAX_POLL_INTERVAL = 300.0


def _validate_lease(
    budget: TimeBudget,
    poll_interval: float,
    max_polls: int,
    polls: int,
    exhausted: bool,
    abandoned: bool,
) -> float:
    if not isinstance(budget, TimeBudget):
        raise ValueError("uData job handle wait budgets must use TimeBudget.")
    if type(poll_interval) not in (int, float) or not 0 <= float(poll_interval) <= _MAX_POLL_INTERVAL:
        raise ValueError(f"uData job handle poll intervals must be between 0 and {_MAX_POLL_INTERVAL} seconds.")
    interval = float(poll_interval)
    if type(max_polls) is not int or not 1 <= max_polls <= _MAX_POLLS:
        raise ValueError(f"uData job handle poll ceilings must be an integer from 1 through {_MAX_POLLS}.")
    if type(polls) is not int or polls < 0:
        raise ValueError("uData job handle poll counts must be a non-negative integer.")
    if not all(type(state) is bool for state in (exhausted, abandoned)):
        raise ValueError("uData job handle lease state must use booleans.")
    return interval


def _validate_wait_result(handle: object, record: MappingRecord | None, handle_type: type[object]) -> None:
    if not isinstance(handle, handle_type):
        raise ValueError("uData job wait results require a typed job handle.")
    if record is not None and not isinstance(record, MappingRecord):
        raise ValueError("uData job wait result records require a mapping record.")
