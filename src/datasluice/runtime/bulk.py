"""Bounded, resumable bulk mutation execution."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from inspect import isawaitable
from threading import Event
from time import monotonic
from typing import Protocol

from datasluice.domain.catalog.ids import CatalogId
from datasluice.domain.catalog.receipts import BulkCheckpoint, BulkItemReceipt, BulkPlan, MutationReceipt
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import BulkExecutionPolicy
from datasluice.errors.catalog import BudgetExhaustedError, CatalogValidationError
from datasluice.runtime.constants import DEFAULT_BULK_MAX_PARALLELISM
from datasluice.runtime.resilience import DeadlineMonitor


class CheckpointSink(Protocol):
    """Persist a caller-owned bulk checkpoint at each item boundary."""

    def __call__(self, checkpoint: BulkCheckpoint, /) -> Awaitable[object] | object:
        """Store one complete immutable checkpoint snapshot."""


@dataclass(frozen=True, slots=True)
class BulkSummary:
    """Terminal aggregate emitted after a bulk run settles or drains."""

    total: int
    succeeded: int
    failed: int
    skipped: int
    settled: int | None = None
    outstanding: int | None = None
    dispatches: int = 0
    state: str = "completed"
    cancelled: bool = False
    budget_exhausted: bool = False
    elapsed_seconds: float = 0.0
    budget_seconds: float | None = None
    reason: str | None = None
    budget_error: BudgetExhaustedError | None = None

    def __post_init__(self) -> None:
        settled = self.settled
        outstanding = self.outstanding
        if settled is None and outstanding is None:
            settled = self.succeeded + self.failed + self.skipped
            outstanding = self.total - settled
        elif settled is None:
            assert outstanding is not None
            settled = self.total - outstanding
        elif outstanding is None:
            outstanding = self.total - settled
        if type(settled) is not int or type(outstanding) is not int or settled < 0 or outstanding < 0:
            raise ValueError("Bulk summary counts must be non-negative integers.")
        object.__setattr__(self, "settled", settled)
        object.__setattr__(self, "outstanding", outstanding)
        if self.cancelled and self.state == "completed":
            object.__setattr__(self, "state", "cancelled")
        elif self.budget_exhausted and self.state == "completed":
            object.__setattr__(self, "state", "budget_exhausted")

    @property
    def status(self) -> str:
        """Return the terminal state name."""
        return self.state

    @property
    def dispatch_count(self) -> int:
        """Return the number of items dispatched during this run."""
        return self.dispatches


BulkAggregate = BulkSummary


@dataclass(frozen=True, slots=True)
class _ItemResult:
    receipt: MutationReceipt
    budget_error: BudgetExhaustedError | None = None


@dataclass(slots=True)
class _BulkProgress:
    completed: dict[int, BulkItemReceipt]
    pending: list[int]
    stop_reason: str | None
    budget_error: BudgetExhaustedError | None
    item_budget_error: BudgetExhaustedError | None = None
    dispatches: int = 0
    next_emit: int = 0
    last_persisted_count: int = -1

    def ordered_receipts(self) -> list[BulkItemReceipt]:
        receipts = []
        while self.next_emit in self.completed:
            receipts.append(self.completed[self.next_emit])
            self.next_emit += 1
        return receipts


def _bulk_progress(
    plan: BulkPlan,
    checkpoint: BulkCheckpoint | None,
    monitor: DeadlineMonitor | None,
    cancel_source: object | None,
    policy_requested: bool,
) -> _BulkProgress:
    completed = {item.index: item for item in checkpoint.item_receipts} if checkpoint else {}
    pending = [index for index in range(len(plan.items)) if index not in completed]
    stop_reason, budget_error = _stop_state(monitor, plan, cancel_source, policy_requested, check_budget=bool(pending))
    return _BulkProgress(completed, pending, stop_reason, budget_error)


def _parallelism(policy: BulkExecutionPolicy, platform_max_parallelism: int) -> int:
    if type(platform_max_parallelism) is not int or not 1 <= platform_max_parallelism <= 32:
        raise ValueError("Platform bulk parallelism must be between one and 32.")
    return min(policy.max_parallelism, platform_max_parallelism, 32)


def _platform_for(plan: BulkPlan) -> str:
    return str(plan.items[0].platform) if plan.items else "runtime"


def _duplicate_name(item: CatalogId) -> str:
    return f"{item.platform}/{item.resource_kind}/{item.value}"


def _validate_plan(plan: BulkPlan, checkpoint: BulkCheckpoint | None) -> None:
    if not isinstance(plan, BulkPlan):
        raise TypeError("Bulk execution requires a BulkPlan.")
    seen: set[CatalogId] = set()
    duplicates: list[str] = []
    for item in plan.items:
        if item in seen:
            duplicates.append(_duplicate_name(item))
        else:
            seen.add(item)
    if duplicates:
        duplicate_names = ", ".join(duplicates)
        raise CatalogValidationError(
            f"Bulk plan contains duplicate catalog IDs: {duplicate_names}.",
            operation=plan.operation,
            platform=_platform_for(plan),
            safe_action="Remove duplicate catalog IDs before dispatching the bulk plan.",
        )
    if checkpoint is None:
        return
    if checkpoint.plan != plan:
        raise CatalogValidationError(
            "Bulk checkpoint does not belong to the supplied plan.",
            operation=plan.operation,
            platform=_platform_for(plan),
            safe_action="Resume only with a checkpoint created for this exact bulk plan.",
        )
    for item_receipt in checkpoint.item_receipts:
        if item_receipt.receipt.target != plan.items[item_receipt.index]:
            raise CatalogValidationError(
                f"Bulk checkpoint receipt index {item_receipt.index} does not match its catalog ID.",
                operation=plan.operation,
                platform=_platform_for(plan),
                safe_action="Use a checkpoint whose receipt target matches the original plan item.",
            )


def _failure_receipt(plan: BulkPlan, item: CatalogId, *, budget_exhausted: bool = False) -> MutationReceipt:
    metadata: dict[str, object] = {"bulk_failure": True}
    if budget_exhausted:
        metadata["budget_exhausted"] = True
    return MutationReceipt(operation=plan.operation, outcome="failed", target=item, audit_metadata=metadata)


def _cancel_requested(source: object | None, policy_requested: bool) -> bool:
    if policy_requested:
        return True
    if source is None:
        return False
    is_set = getattr(source, "is_set", None)
    if callable(is_set) and is_set():
        return True
    done = getattr(source, "done", None)
    return bool(done()) if callable(done) else False


def _stop_state(
    monitor: DeadlineMonitor | None,
    plan: BulkPlan,
    cancel_source: object | None,
    policy_requested: bool,
    *,
    check_budget: bool,
) -> tuple[str | None, BudgetExhaustedError | None]:
    if _cancel_requested(cancel_source, policy_requested):
        return "cancelled", None
    if monitor is None or not check_budget:
        return None, None
    try:
        monitor.assert_dispatchable(plan.operation, _platform_for(plan))
    except BudgetExhaustedError as exc:
        return "budget_exhausted", exc
    return None, None


def _summary(
    receipts: Iterable[BulkItemReceipt],
    total: int,
    *,
    dispatches: int,
    stop_reason: str | None,
    budget_error: BudgetExhaustedError | None,
    item_budget_error: BudgetExhaustedError | None,
    monitor: DeadlineMonitor | None,
    started_at: float,
    clock: Callable[[], float],
) -> BulkSummary:
    values = tuple(receipts)
    outcomes = tuple(item.receipt.outcome for item in values)
    elapsed: float
    budget_seconds: float | None
    terminal_budget_error = budget_error or item_budget_error
    if terminal_budget_error is not None:
        elapsed = terminal_budget_error.elapsed_seconds
        budget_seconds = terminal_budget_error.budget_seconds
    elif monitor is not None:
        elapsed = max(0.0, monitor.budget.total - monitor.remaining())
        budget_seconds = monitor.budget.total
    else:
        elapsed = max(0.0, clock() - started_at)
        budget_seconds = None
    budget_exhausted = stop_reason == "budget_exhausted" or item_budget_error is not None
    state = stop_reason or ("budget_exhausted" if budget_exhausted else "completed")
    reason: str | None = None
    if stop_reason == "cancelled":
        reason = "Bulk execution was cancelled after in-flight items settled."
    elif terminal_budget_error is not None:
        reason = str(terminal_budget_error)
    return BulkSummary(
        total=total,
        succeeded=outcomes.count("succeeded"),
        failed=outcomes.count("failed"),
        skipped=outcomes.count("skipped"),
        settled=len(values),
        outstanding=total - len(values),
        dispatches=dispatches,
        state=state,
        cancelled=stop_reason == "cancelled",
        budget_exhausted=budget_exhausted,
        elapsed_seconds=elapsed,
        budget_seconds=budget_seconds,
        reason=reason,
        budget_error=terminal_budget_error,
    )


def _validated_dependencies(
    execute_item: Callable[[CatalogId], object],
    checkpoint_sink: CheckpointSink,
    policy: BulkExecutionPolicy | None,
    checkpoint: BulkCheckpoint | None,
    item_budget: TimeBudget | None,
    per_item_budget: TimeBudget | None,
    whole_run_budget: TimeBudget | None,
    clock: Callable[[], float],
) -> tuple[BulkExecutionPolicy, TimeBudget | None, TimeBudget | None]:
    if not callable(execute_item) or not callable(checkpoint_sink) or not callable(clock):
        raise TypeError("Bulk execution requires callable dispatch, checkpoint sink, and clock dependencies.")
    if policy is not None and not isinstance(policy, BulkExecutionPolicy):
        raise TypeError("Bulk execution policy must use BulkExecutionPolicy.")
    if checkpoint is not None and not isinstance(checkpoint, BulkCheckpoint):
        raise TypeError("Bulk resumption requires a BulkCheckpoint.")
    if item_budget is not None and per_item_budget is not None and item_budget != per_item_budget:
        raise ValueError("Item budgets must agree when both budget names are supplied.")
    selected_item_budget = item_budget or per_item_budget
    if selected_item_budget is not None and not isinstance(selected_item_budget, TimeBudget):
        raise TypeError("Bulk item budgets must use TimeBudget.")
    if whole_run_budget is not None and not isinstance(whole_run_budget, TimeBudget):
        raise TypeError("Bulk run budgets must use TimeBudget.")
    selected_policy = policy or BulkExecutionPolicy(max_parallelism=DEFAULT_BULK_MAX_PARALLELISM)
    return selected_policy, selected_item_budget, whole_run_budget


class BulkExecutor:
    """Execute a synchronous bulk plan with bounded concurrent dispatch."""

    def __init__(
        self,
        execute_item: Callable[[CatalogId], MutationReceipt],
        *,
        policy: BulkExecutionPolicy | None = None,
        checkpoint_sink: CheckpointSink,
        checkpoint: BulkCheckpoint | None = None,
        platform_max_parallelism: int = 32,
        item_budget: TimeBudget | None = None,
        per_item_budget: TimeBudget | None = None,
        whole_run_budget: TimeBudget | None = None,
        clock: Callable[[], float] = monotonic,
        cancel_event: Event | None = None,
    ) -> None:
        self._policy, self._item_budget, self._whole_run_budget = _validated_dependencies(
            execute_item, checkpoint_sink, policy, checkpoint, item_budget, per_item_budget, whole_run_budget, clock
        )
        self._execute_item = execute_item
        self._sink = checkpoint_sink
        self._checkpoint = checkpoint
        self._parallelism = _parallelism(self._policy, platform_max_parallelism)
        self._clock = clock
        self._cancel_event = cancel_event if cancel_event is not None else Event()

    def _dispatch_available(
        self,
        plan: BulkPlan,
        progress: _BulkProgress,
        in_flight: dict[Future[_ItemResult], int],
        workers: ThreadPoolExecutor,
        monitor: DeadlineMonitor | None,
    ) -> None:
        while progress.pending and len(in_flight) < self._parallelism and progress.stop_reason is None:
            progress.stop_reason, progress.budget_error = _stop_state(
                monitor,
                plan,
                self._cancel_event,
                self._policy.cancellation_requested,
                check_budget=True,
            )
            if progress.stop_reason is not None:
                return
            index = progress.pending.pop(0)
            in_flight[workers.submit(self._execute_sync_item, plan, plan.items[index])] = index
            progress.dispatches += 1

    def _record_sync_result(
        self,
        future: Future[_ItemResult],
        index: int,
        plan: BulkPlan,
        progress: _BulkProgress,
        monitor: DeadlineMonitor | None,
    ) -> None:
        try:
            result = future.result()
        except Exception:
            result = _ItemResult(_failure_receipt(plan, plan.items[index]))
        progress.completed[index] = BulkItemReceipt(index=index, receipt=result.receipt)
        if result.budget_error is not None and progress.item_budget_error is None:
            progress.item_budget_error = result.budget_error
        if progress.stop_reason is None:
            progress.stop_reason, progress.budget_error = _stop_state(
                monitor,
                plan,
                self._cancel_event,
                self._policy.cancellation_requested,
                check_budget=bool(progress.pending),
            )
        self._persist(plan, progress.completed, cancellation_requested=progress.stop_reason == "cancelled")
        progress.last_persisted_count = len(progress.completed)

    def _drain_sync(
        self,
        plan: BulkPlan,
        progress: _BulkProgress,
        in_flight: dict[Future[_ItemResult], int],
    ) -> None:
        wait(in_flight)
        for future, index in tuple(in_flight.items()):
            in_flight.pop(future)
            try:
                result = future.result()
            except Exception:
                result = _ItemResult(_failure_receipt(plan, plan.items[index]))
            progress.completed[index] = BulkItemReceipt(index=index, receipt=result.receipt)
            if result.budget_error is not None and progress.item_budget_error is None:
                progress.item_budget_error = result.budget_error
        self._persist(plan, progress.completed, cancellation_requested=True)

    def _finish_progress(self, plan: BulkPlan, progress: _BulkProgress, monitor: DeadlineMonitor | None) -> None:
        if progress.stop_reason is None:
            progress.stop_reason, progress.budget_error = _stop_state(
                monitor,
                plan,
                self._cancel_event,
                self._policy.cancellation_requested,
                check_budget=False,
            )
        if progress.stop_reason is not None and progress.last_persisted_count != len(progress.completed):
            self._persist(
                plan,
                progress.completed,
                cancellation_requested=progress.stop_reason == "cancelled",
            )

    def stream(self, plan: BulkPlan) -> Iterator[BulkItemReceipt | BulkSummary]:
        """Yield ordered receipts followed by a terminal aggregate."""
        _validate_plan(plan, self._checkpoint)
        started_at = self._clock()
        monitor = DeadlineMonitor(self._whole_run_budget, clock=self._clock) if self._whole_run_budget else None
        progress = _bulk_progress(
            plan, self._checkpoint, monitor, self._cancel_event, self._policy.cancellation_requested
        )
        in_flight: dict[Future[_ItemResult], int] = {}

        yield from progress.ordered_receipts()

        with ThreadPoolExecutor(max_workers=self._parallelism) as workers:
            try:
                while progress.pending or in_flight:
                    self._dispatch_available(plan, progress, in_flight, workers, monitor)
                    if not in_flight:
                        break
                    done, _ = wait(in_flight, return_when="FIRST_COMPLETED")
                    for future in done:
                        index = in_flight.pop(future)
                        self._record_sync_result(future, index, plan, progress, monitor)
                        yield from progress.ordered_receipts()
            except GeneratorExit:
                self._drain_sync(plan, progress, in_flight)
                raise

        self._finish_progress(plan, progress, monitor)
        yield from progress.ordered_receipts()
        yield _summary(
            progress.completed.values(),
            len(plan.items),
            dispatches=progress.dispatches,
            stop_reason=progress.stop_reason,
            budget_error=progress.budget_error,
            item_budget_error=progress.item_budget_error,
            monitor=monitor,
            started_at=started_at,
            clock=self._clock,
        )

    def execute(self, plan: BulkPlan) -> Iterator[BulkItemReceipt | BulkSummary]:
        """Return the streaming iterator for one bulk plan."""
        return self.stream(plan)

    def _execute_sync_item(self, plan: BulkPlan, item: CatalogId) -> _ItemResult:
        monitor = DeadlineMonitor(self._item_budget, clock=self._clock) if self._item_budget else None
        if monitor is not None:
            try:
                monitor.assert_dispatchable(plan.operation, _platform_for(plan))
            except BudgetExhaustedError as exc:
                return _ItemResult(_failure_receipt(plan, item, budget_exhausted=True), exc)
        try:
            receipt = self._execute_item(item)
        except BudgetExhaustedError as exc:
            return _ItemResult(_failure_receipt(plan, item, budget_exhausted=True), exc)
        except Exception:
            return _ItemResult(_failure_receipt(plan, item))
        if not isinstance(receipt, MutationReceipt):
            return _ItemResult(_failure_receipt(plan, item))
        if monitor is not None:
            try:
                monitor.assert_dispatchable()
            except BudgetExhaustedError as exc:
                return _ItemResult(receipt, exc)
        return _ItemResult(receipt)

    def _persist(self, plan: BulkPlan, completed: dict[int, BulkItemReceipt], *, cancellation_requested: bool) -> None:
        if not self._policy.checkpoint_required:
            return
        resumption_cursor = self._checkpoint.resumption_cursor if self._checkpoint else plan.resumption_cursor
        self._sink(
            BulkCheckpoint(
                plan=plan,
                item_receipts=tuple(completed[index] for index in sorted(completed)),
                cancellation_requested=cancellation_requested,
                resumption_cursor=resumption_cursor,
            )
        )


class AsyncBulkExecutor:
    """Execute an asynchronous bulk plan with bounded concurrent dispatch."""

    def __init__(
        self,
        execute_item: Callable[[CatalogId], Awaitable[MutationReceipt]],
        *,
        policy: BulkExecutionPolicy | None = None,
        checkpoint_sink: CheckpointSink,
        checkpoint: BulkCheckpoint | None = None,
        platform_max_parallelism: int = 32,
        item_budget: TimeBudget | None = None,
        per_item_budget: TimeBudget | None = None,
        whole_run_budget: TimeBudget | None = None,
        clock: Callable[[], float] = monotonic,
        cancel_event: object | None = None,
    ) -> None:
        self._policy, self._item_budget, self._whole_run_budget = _validated_dependencies(
            execute_item, checkpoint_sink, policy, checkpoint, item_budget, per_item_budget, whole_run_budget, clock
        )
        self._execute_item = execute_item
        self._sink = checkpoint_sink
        self._checkpoint = checkpoint
        self._parallelism = _parallelism(self._policy, platform_max_parallelism)
        self._clock = clock
        self._cancel_event = cancel_event if cancel_event is not None else asyncio.Event()

    def _dispatch_available(
        self,
        plan: BulkPlan,
        progress: _BulkProgress,
        in_flight: dict[asyncio.Future[_ItemResult], int],
        monitor: DeadlineMonitor | None,
    ) -> None:
        while progress.pending and len(in_flight) < self._parallelism and progress.stop_reason is None:
            progress.stop_reason, progress.budget_error = _stop_state(
                monitor,
                plan,
                self._cancel_event,
                self._policy.cancellation_requested,
                check_budget=True,
            )
            if progress.stop_reason is not None:
                return
            index = progress.pending.pop(0)
            in_flight[asyncio.ensure_future(self._execute_async_item(plan, plan.items[index]))] = index
            progress.dispatches += 1

    async def _record_result(
        self,
        task: asyncio.Future[_ItemResult],
        index: int,
        plan: BulkPlan,
        progress: _BulkProgress,
        monitor: DeadlineMonitor | None,
    ) -> None:
        result = self._task_result(task, plan, plan.items[index])
        progress.completed[index] = BulkItemReceipt(index=index, receipt=result.receipt)
        if result.budget_error is not None and progress.item_budget_error is None:
            progress.item_budget_error = result.budget_error
        if progress.stop_reason is None:
            progress.stop_reason, progress.budget_error = _stop_state(
                monitor,
                plan,
                self._cancel_event,
                self._policy.cancellation_requested,
                check_budget=bool(progress.pending),
            )
        await self._persist(
            plan,
            progress.completed,
            cancellation_requested=progress.stop_reason == "cancelled",
        )
        progress.last_persisted_count = len(progress.completed)

    async def _finish_progress(self, plan: BulkPlan, progress: _BulkProgress, monitor: DeadlineMonitor | None) -> None:
        if progress.stop_reason is None:
            progress.stop_reason, progress.budget_error = _stop_state(
                monitor,
                plan,
                self._cancel_event,
                self._policy.cancellation_requested,
                check_budget=False,
            )
        if progress.stop_reason is not None and progress.last_persisted_count != len(progress.completed):
            await self._persist(
                plan,
                progress.completed,
                cancellation_requested=progress.stop_reason == "cancelled",
            )

    async def _cancel_stream(
        self,
        plan: BulkPlan,
        progress: _BulkProgress,
        in_flight: dict[asyncio.Future[_ItemResult], int],
    ) -> None:
        current_task = asyncio.current_task()
        while current_task is not None and current_task.cancelling():
            current_task.uncancel()
        cleanup = asyncio.create_task(
            self._finalize_cancelled_stream(plan, in_flight, progress.completed, progress.item_budget_error)
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await asyncio.shield(cleanup)
            raise

    async def stream(self, plan: BulkPlan) -> AsyncIterator[BulkItemReceipt | BulkSummary]:
        """Yield ordered asynchronous receipts followed by a terminal aggregate."""
        _validate_plan(plan, self._checkpoint)
        started_at = self._clock()
        monitor = DeadlineMonitor(self._whole_run_budget, clock=self._clock) if self._whole_run_budget else None
        progress = _bulk_progress(
            plan, self._checkpoint, monitor, self._cancel_event, self._policy.cancellation_requested
        )
        in_flight: dict[asyncio.Future[_ItemResult], int] = {}

        for receipt in progress.ordered_receipts():
            yield receipt

        try:
            while progress.pending or in_flight:
                self._dispatch_available(plan, progress, in_flight, monitor)
                if not in_flight:
                    break
                done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    index = in_flight.pop(task)
                    await self._record_result(task, index, plan, progress, monitor)
                    for receipt in progress.ordered_receipts():
                        yield receipt
        except asyncio.CancelledError:
            await self._cancel_stream(plan, progress, in_flight)
            raise
        except GeneratorExit:
            await self._finalize_cancelled_stream(plan, in_flight, progress.completed, progress.item_budget_error)
            raise

        await self._finish_progress(plan, progress, monitor)
        for receipt in progress.ordered_receipts():
            yield receipt
        yield _summary(
            progress.completed.values(),
            len(plan.items),
            dispatches=progress.dispatches,
            stop_reason=progress.stop_reason,
            budget_error=progress.budget_error,
            item_budget_error=progress.item_budget_error,
            monitor=monitor,
            started_at=started_at,
            clock=self._clock,
        )

    def execute(self, plan: BulkPlan) -> AsyncIterator[BulkItemReceipt | BulkSummary]:
        """Return the asynchronous streaming iterator for one bulk plan."""
        return self.stream(plan)

    async def _execute_async_item(self, plan: BulkPlan, item: CatalogId) -> _ItemResult:
        monitor = DeadlineMonitor(self._item_budget, clock=self._clock) if self._item_budget else None
        if monitor is not None:
            try:
                monitor.assert_dispatchable(plan.operation, _platform_for(plan))
            except BudgetExhaustedError as exc:
                return _ItemResult(_failure_receipt(plan, item, budget_exhausted=True), exc)
        try:
            receipt = await self._execute_item(item)
        except BudgetExhaustedError as exc:
            return _ItemResult(_failure_receipt(plan, item, budget_exhausted=True), exc)
        except Exception:
            return _ItemResult(_failure_receipt(plan, item))
        if not isinstance(receipt, MutationReceipt):
            return _ItemResult(_failure_receipt(plan, item))
        if monitor is not None:
            try:
                monitor.assert_dispatchable()
            except BudgetExhaustedError as exc:
                return _ItemResult(receipt, exc)
        return _ItemResult(receipt)

    async def _drain_in_flight(
        self,
        plan: BulkPlan,
        in_flight: dict[asyncio.Future[_ItemResult], int],
        completed: dict[int, BulkItemReceipt],
        item_budget_error: BudgetExhaustedError | None,
    ) -> BudgetExhaustedError | None:
        """Settle every in-flight task once and record its authoritative receipt."""
        for task, index in tuple(in_flight.items()):
            result = await self._settled_task(task, plan, plan.items[index])
            completed[index] = BulkItemReceipt(index=index, receipt=result.receipt)
            if result.budget_error is not None and item_budget_error is None:
                item_budget_error = result.budget_error
        in_flight.clear()
        return item_budget_error

    async def _finalize_cancelled_stream(
        self,
        plan: BulkPlan,
        in_flight: dict[asyncio.Future[_ItemResult], int],
        completed: dict[int, BulkItemReceipt],
        item_budget_error: BudgetExhaustedError | None,
    ) -> None:
        await self._drain_in_flight(plan, in_flight, completed, item_budget_error)
        await self._persist_final_checkpoint(plan, completed)

    @staticmethod
    async def _settled_task(task: asyncio.Future[_ItemResult], plan: BulkPlan, item: CatalogId) -> _ItemResult:
        """Collect one task outcome while keeping a child cancellation local to that item."""
        settled = asyncio.gather(task, return_exceptions=True)
        try:
            await asyncio.shield(settled)
        except asyncio.CancelledError:
            await asyncio.shield(settled)
            raise
        return AsyncBulkExecutor._task_result(task, plan, item)

    async def _persist_final_checkpoint(self, plan: BulkPlan, completed: dict[int, BulkItemReceipt]) -> None:
        """Persist the terminal cancellation checkpoint exactly once, surviving repeated cancellation."""
        checkpoint = asyncio.create_task(self._persist(plan, completed, cancellation_requested=True))
        try:
            await asyncio.shield(checkpoint)
        except asyncio.CancelledError:
            await asyncio.shield(checkpoint)
            raise

    @staticmethod
    def _task_result(task: asyncio.Future[_ItemResult], plan: BulkPlan, item: CatalogId) -> _ItemResult:
        try:
            return task.result()
        except asyncio.CancelledError:
            return _ItemResult(_failure_receipt(plan, item))
        except Exception:
            return _ItemResult(_failure_receipt(plan, item))

    async def _persist(
        self,
        plan: BulkPlan,
        completed: dict[int, BulkItemReceipt],
        *,
        cancellation_requested: bool,
    ) -> None:
        if not self._policy.checkpoint_required:
            return
        resumption_cursor = self._checkpoint.resumption_cursor if self._checkpoint else plan.resumption_cursor
        result = self._sink(
            BulkCheckpoint(
                plan=plan,
                item_receipts=tuple(completed[index] for index in sorted(completed)),
                cancellation_requested=cancellation_requested,
                resumption_cursor=resumption_cursor,
            )
        )
        if isawaitable(result):
            await result
