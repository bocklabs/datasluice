"""Immutable uData worker job, task, and budgeted task-wait records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from datasluice.connectors.catalog.udata.models._segment import path_segment
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.models import MappingRecord, _freeze_json
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.exceptions import DataSluiceError

PLATFORM = "udata"
WORKER_JOB_KIND = ResourceKind("worker-job")
WORKER_JOBS_COLLECTION = "worker-jobs-collection"

PERIODS = frozenset({"days", "hours", "minutes", "seconds", "microseconds"})
CRON_FIELDS = ("minute", "hour", "day_of_week", "day_of_month", "month_of_year")
WORKER_TASK_STATES = frozenset({"PENDING", "RECEIVED", "STARTED", "SUCCESS", "FAILURE", "RETRY", "REVOKED"})
WORKER_TERMINAL_TASK_STATES = frozenset({"SUCCESS", "FAILURE", "REVOKED"})
WORKER_EXCEPTION_TASK_STATES = frozenset({"RETRY", "FAILURE", "REVOKED"})

_MAX_TEXT = 255
_MAX_POLLS = 1024
_MAX_POLL_INTERVAL = 300.0
_MAX_ARGS = 64


def _validated_text(value: object, label: str, *, required: bool = False) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"uData worker job {label} must be a non-empty string.")
    if len(value) > _MAX_TEXT:
        raise ValueError(f"uData worker job {label} must be at most {_MAX_TEXT} characters.")
    return value


def _json_safe(value: object, label: str) -> object:
    try:
        return _freeze_json(value, f"udata.worker-job-{label}")
    except DataSluiceError as error:
        raise ValueError(f"uData worker job {label} must contain JSON-safe values only.") from error


def job_segment(value: str, operation: str) -> str:
    """Return *value* encoded as one URL-safe worker job path segment."""
    return path_segment(value, operation, "uData worker job")


def task_segment(value: str, operation: str) -> str:
    """Return *value* encoded as one URL-safe worker task path segment."""
    return path_segment(value, operation, "uData worker task")


@dataclass(frozen=True, slots=True)
class CrontabSchedule:
    """The pinned five-field crontab schedule one periodic job may declare."""

    minute: str = "*"
    hour: str = "*"
    day_of_week: str = "*"
    day_of_month: str = "*"
    month_of_year: str = "*"

    def __post_init__(self) -> None:
        for field in CRON_FIELDS:
            if _validated_text(getattr(self, field), field, required=True) is None:
                raise ValueError(f"uData worker job crontab {field} must be a non-empty string.")

    def payload(self) -> dict[str, object]:
        """Return the exact five-key crontab write body."""
        return {field: getattr(self, field) for field in CRON_FIELDS}


@dataclass(frozen=True, slots=True)
class IntervalSchedule:
    """The pinned every/period interval schedule one periodic job may declare."""

    every: int
    period: str

    def __post_init__(self) -> None:
        if type(self.every) is not int or self.every < 0:
            raise ValueError("uData worker job interval every must be a non-negative integer.")
        if self.period not in PERIODS:
            raise ValueError(f"uData worker job interval period must be one of {sorted(PERIODS)}.")

    def payload(self) -> dict[str, object]:
        """Return the exact interval write body."""
        return {"every": self.every, "period": self.period}


@dataclass(frozen=True, slots=True)
class JobScheduleInput:
    """One typed create or update write for a periodic worker job.

    The pinned ``JobsAPI`` refuses a body carrying both schedule forms with a
    400 and picks the form from whichever key the body carries, so the typed
    input enforces exactly one schedule before any dispatch. The stock PUT
    treats an omitted ``enabled`` key as false, so updating a schedule without
    ``enabled=True`` disables the job; the typed input mirrors that wire rule.
    """

    name: str
    task: str
    crontab: CrontabSchedule | None = None
    interval: IntervalSchedule | None = None
    description: str | None = None
    args: tuple[object, ...] = ()
    kwargs: Mapping[str, object] | None = None
    enabled: bool = False

    def __post_init__(self) -> None:
        if (
            _validated_text(self.name, "name", required=True) is None
            or _validated_text(self.task, "task", required=True) is None
        ):
            raise ValueError("uData worker job name and task must be non-empty strings.")
        if (self.crontab is None) == (self.interval is None):
            raise ValueError("uData worker job input requires exactly one of crontab or interval.")
        _validated_text(self.description, "description")
        if type(self.enabled) is not bool:
            raise ValueError("uData worker job enabled must be a boolean.")
        if self.args:
            if len(self.args) > _MAX_ARGS:
                raise ValueError(f"uData worker job args must hold at most {_MAX_ARGS} values.")
            _json_safe(list(self.args), "args")
        if self.kwargs is not None and not isinstance(self.kwargs, Mapping):
            raise ValueError("uData worker job kwargs must be a mapping when supplied.")

    def payload(self) -> dict[str, object]:
        """Return the exact write body, omitting every absent optional field."""
        body: dict[str, object] = {"name": self.name, "task": self.task}
        if self.description is not None:
            body["description"] = self.description
        if self.crontab is not None:
            body["crontab"] = self.crontab.payload()
        else:
            body["interval"] = self.interval.payload()  # type: ignore[union-attr]
        if self.args:
            safe_args = _json_safe(list(self.args), "args")
            body["args"] = list(safe_args) if isinstance(safe_args, list | tuple) else safe_args
        if self.kwargs:
            body["kwargs"] = _json_safe(dict(self.kwargs), "kwargs")
        if self.enabled:
            body["enabled"] = True
        return body


@dataclass(frozen=True, slots=True)
class UDataJobHandle:
    """Immutable typed lease for one queued worker task and its bounded wait.

    Worker dispatch is asynchronous on the deployment, so a task can only be
    awaited under this explicit finite lease; the handle owns the caller's wait
    budget, the poll interval, and the poll ceiling. This is the worker-task
    lease and is distinct from the harvest job handle carried by harvest runs.
    """

    task_id: str
    status: str = "PENDING"
    budget: TimeBudget = TimeBudget()
    poll_interval: float = 1.0
    max_polls: int = 16
    polls: int = 0
    exhausted: bool = False
    abandoned: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id:
            raise ValueError("uData job handles require a non-empty task identifier.")
        if self.status not in WORKER_TASK_STATES:
            raise ValueError(f"uData job handle status must be one of {sorted(WORKER_TASK_STATES)}.")
        if not isinstance(self.budget, TimeBudget):
            raise ValueError("uData job handle wait budgets must use TimeBudget.")
        if type(self.poll_interval) not in (int, float) or not 0 <= float(self.poll_interval) <= _MAX_POLL_INTERVAL:
            raise ValueError(f"uData job handle poll intervals must be between 0 and {_MAX_POLL_INTERVAL} seconds.")
        object.__setattr__(self, "poll_interval", float(self.poll_interval))
        if type(self.max_polls) is not int or not 1 <= self.max_polls <= _MAX_POLLS:
            raise ValueError(f"uData job handle poll ceilings must be an integer from 1 through {_MAX_POLLS}.")
        if type(self.polls) is not int or self.polls < 0:
            raise ValueError("uData job handle poll counts must be a non-negative integer.")
        if not all(type(state) is bool for state in (self.exhausted, self.abandoned)):
            raise ValueError("uData job handle lease state must use booleans.")

    @property
    def terminal(self) -> bool:
        """Return whether the observed task status is terminal."""
        return self.status in WORKER_TERMINAL_TASK_STATES

    def observed(self, status: str) -> UDataJobHandle:
        """Return the handle advanced by one bounded observation."""
        if status not in WORKER_TASK_STATES:
            raise ValueError(f"uData job handle status must be one of {sorted(WORKER_TASK_STATES)}.")
        return UDataJobHandle(
            task_id=self.task_id,
            status=status,
            budget=self.budget,
            poll_interval=self.poll_interval,
            max_polls=self.max_polls,
            polls=self.polls + 1,
            exhausted=self.exhausted,
            abandoned=self.abandoned,
        )

    def abandoned_lease(self, *, exhausted: bool = False) -> UDataJobHandle:
        """Return the handle after the caller stopped watching a still-running task.

        A caller lease is finite by construction, so releasing it mid-run is a
        normal outcome rather than a failure. Recording it keeps the outstanding
        remote work detectable; the abandoned handle never polls again, and
        watching the same task requires a fresh handle for its task id.

        Args:
            exhausted: Whether the lease ended because its time budget ran out.

        Returns:
            The same handle with its outstanding work marked abandoned.
        """
        return UDataJobHandle(
            task_id=self.task_id,
            status=self.status,
            budget=self.budget,
            poll_interval=self.poll_interval,
            max_polls=self.max_polls,
            polls=self.polls,
            exhausted=exhausted,
            abandoned=True,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the handle as a JSON-safe projection without endpoints or secrets."""
        return {
            "task_id": self.task_id,
            "status": self.status,
            "terminal": self.terminal,
            "polls": self.polls,
            "max_polls": self.max_polls,
            "poll_interval": self.poll_interval,
            "budget_seconds": self.budget.total,
            "exhausted": self.exhausted,
            "abandoned": self.abandoned,
        }


@dataclass(frozen=True, slots=True)
class WorkerTaskWaitResult:
    """The outcome of one bounded worker-task wait."""

    handle: UDataJobHandle
    record: MappingRecord | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.handle, UDataJobHandle):
            raise ValueError("uData job wait results require a typed job handle.")
        if self.record is not None and not isinstance(self.record, MappingRecord):
            raise ValueError("uData job wait result records require a mapping record.")

    def to_dict(self) -> dict[str, object]:
        """Return the wait outcome as a JSON-safe projection."""
        return {
            "handle": self.handle.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }


@dataclass(frozen=True, slots=True)
class JobMutationResult:
    """Worker job mutation output: a redacted receipt plus any returned record."""

    receipt: MutationReceipt
    record: MappingRecord | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, MutationReceipt):
            raise ValueError("uData worker job mutation results require a mutation receipt.")
        if self.record is not None and not isinstance(self.record, MappingRecord):
            raise ValueError("uData worker job mutation result records require a mapping record.")

    def to_dict(self) -> dict[str, object]:
        """Return the mutation outcome as a JSON-safe projection."""
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }
