"""Tests for bounded, resumable bulk mutation execution."""

from __future__ import annotations

import asyncio
import json
from threading import Event, Lock
from time import sleep

import pytest

from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.receipts import BulkCheckpoint, BulkItemReceipt, BulkPlan, MutationReceipt
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import BulkExecutionPolicy
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.bulk import AsyncBulkExecutor, BulkExecutor, BulkSummary


def _plan(*values: str) -> BulkPlan:
    return BulkPlan(
        operation="ckan/datasets.update",
        items=tuple(CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, value) for value in values),
    )


def _receipt(item: CatalogId) -> MutationReceipt:
    return MutationReceipt(operation="ckan/datasets.update", outcome="succeeded", target=item)


def test_sync_streams_completion_outcomes_in_plan_order_and_persists_each_boundary() -> None:
    plan = _plan("first", "second", "third")
    checkpoints: list[BulkCheckpoint] = []

    def execute(item: CatalogId) -> MutationReceipt:
        sleep({"first": 0.03, "second": 0.01, "third": 0.02}[item.value])
        return _receipt(item)

    outcomes = list(
        BulkExecutor(
            execute,
            policy=BulkExecutionPolicy(max_parallelism=3),
            platform_max_parallelism=3,
            checkpoint_sink=checkpoints.append,
        ).stream(plan)
    )

    receipts = [outcome for outcome in outcomes if isinstance(outcome, BulkItemReceipt)]
    assert [receipt.index for receipt in receipts] == [0, 1, 2]
    assert isinstance(outcomes[-1], BulkSummary)
    assert len(checkpoints) == 3


def test_empty_and_single_item_plans_follow_their_distinct_dispatch_paths() -> None:
    calls: list[str] = []
    empty = list(BulkExecutor(lambda item: _receipt(item), checkpoint_sink=lambda checkpoint: None).stream(_plan()))
    one = list(
        BulkExecutor(
            lambda item: calls.append(item.value) or _receipt(item), checkpoint_sink=lambda checkpoint: None
        ).stream(_plan("only"))
    )

    assert len(empty) == 1 and isinstance(empty[0], BulkSummary)
    assert empty[0].dispatches == 0
    assert empty[0].settled == 0
    assert empty[0].outstanding == 0
    assert calls == ["only"]
    assert [type(value) for value in one] == [BulkItemReceipt, BulkSummary]
    one_summary = one[-1]
    assert isinstance(one_summary, BulkSummary)
    assert one_summary.dispatches == 1


def test_duplicate_ids_fail_before_any_dispatch() -> None:
    plan = _plan("same", "same")
    calls: list[CatalogId] = []
    executor = BulkExecutor(lambda item: calls.append(item) or _receipt(item), checkpoint_sink=lambda checkpoint: None)

    with pytest.raises(CatalogValidationError) as raised:
        list(executor.stream(plan))

    assert calls == []
    assert "ckan/dataset/same" in str(raised.value)
    assert raised.value.operation == plan.operation
    assert raised.value.platform == "ckan"
    assert raised.value.safe_action


def test_parallelism_is_clamped_to_platform_bound() -> None:
    plan = _plan("one", "two", "three", "four")
    active = 0
    peak = 0
    lock = Lock()

    def execute(item: CatalogId) -> MutationReceipt:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        sleep(0.02)
        with lock:
            active -= 1
        return _receipt(item)

    list(
        BulkExecutor(
            execute,
            policy=BulkExecutionPolicy(max_parallelism=4),
            platform_max_parallelism=2,
            checkpoint_sink=lambda checkpoint: None,
        ).stream(plan)
    )
    assert peak == 2


def test_resume_uses_materialized_checkpoint_without_redispatching_completed_items() -> None:
    plan = _plan("one", "two", "three")
    checkpoint = BulkCheckpoint(plan=plan, item_receipts=(BulkItemReceipt(index=0, receipt=_receipt(plan.items[0])),))
    restored = BulkCheckpoint.from_dict(json.loads(json.dumps(checkpoint.to_dict())))
    calls: list[str] = []
    checkpoints: list[BulkCheckpoint] = []

    outcomes = list(
        BulkExecutor(
            lambda item: calls.append(item.value) or _receipt(item),
            checkpoint=restored,
            checkpoint_sink=checkpoints.append,
        ).stream(plan)
    )

    assert calls == ["two", "three"]
    assert [item.index for item in outcomes[:-1] if isinstance(item, BulkItemReceipt)] == [0, 1, 2]
    assert len(checkpoints) == 2


def test_async_stream_has_the_same_deterministic_order() -> None:
    plan = _plan("one", "two")
    checkpoints: list[BulkCheckpoint] = []

    async def run() -> list[BulkItemReceipt | BulkSummary]:
        async def execute(item: CatalogId) -> MutationReceipt:
            await asyncio.sleep(0.01 if item.value == "one" else 0)
            return _receipt(item)

        return [
            outcome
            async for outcome in AsyncBulkExecutor(
                execute,
                policy=BulkExecutionPolicy(max_parallelism=2),
                checkpoint_sink=checkpoints.append,
            ).stream(plan)
        ]

    outcomes = asyncio.run(run())
    assert [item.index for item in outcomes[:-1] if isinstance(item, BulkItemReceipt)] == [0, 1]
    assert isinstance(outcomes[-1], BulkSummary)


def test_sync_cancellation_drains_in_flight_items_and_checkpoints_their_receipts() -> None:
    plan = _plan("one", "two", "three", "four")
    cancel_event = Event()
    checkpoints: list[BulkCheckpoint] = []
    calls: list[str] = []
    active = 0
    peak = 0
    lock = Lock()

    def execute(item: CatalogId) -> MutationReceipt:
        nonlocal active, peak
        with lock:
            calls.append(item.value)
            active += 1
            peak = max(peak, active)
            if active == 2:
                cancel_event.set()
        sleep(0.03)
        with lock:
            active -= 1
        return _receipt(item)

    outcomes = list(
        BulkExecutor(
            execute,
            policy=BulkExecutionPolicy(max_parallelism=2),
            checkpoint_sink=checkpoints.append,
            cancel_event=cancel_event,
        ).stream(plan)
    )

    summary = outcomes[-1]
    assert isinstance(summary, BulkSummary)
    assert summary.cancelled
    assert summary.settled == 2
    assert summary.outstanding == 2
    assert summary.dispatches == 2
    assert peak == 2
    assert active == 0
    assert len(calls) == 2
    assert len(checkpoints) == 2
    assert checkpoints[-1].cancellation_requested
    assert [item.index for item in checkpoints[-1].item_receipts] == [0, 1]


def test_sync_run_budget_drains_in_flight_items_and_surfaces_elapsed_budget_state() -> None:
    plan = _plan("one", "two", "three", "four")
    checkpoints: list[BulkCheckpoint] = []

    def execute(item: CatalogId) -> MutationReceipt:
        sleep(0.03)
        return _receipt(item)

    outcomes = list(
        BulkExecutor(
            execute,
            policy=BulkExecutionPolicy(max_parallelism=2),
            checkpoint_sink=checkpoints.append,
            whole_run_budget=TimeBudget(connect=0.01, read=0.01, write=0.01, total=0.01),
        ).stream(plan)
    )

    summary = outcomes[-1]
    assert isinstance(summary, BulkSummary)
    assert summary.budget_exhausted
    assert not summary.cancelled
    assert summary.settled == 2
    assert summary.outstanding == 2
    assert summary.budget_seconds is not None
    assert summary.elapsed_seconds >= summary.budget_seconds
    assert summary.reason and "exhausted" in summary.reason
    assert len(checkpoints) == 2
    assert not checkpoints[-1].cancellation_requested


def test_async_task_cancellation_drains_in_flight_items_and_persists_the_final_checkpoint() -> None:
    plan = _plan("one", "two", "three", "four")
    checkpoints: list[BulkCheckpoint] = []
    started = asyncio.Event()
    calls: list[str] = []
    active = 0
    peak = 0

    async def execute(item: CatalogId) -> MutationReceipt:
        nonlocal active, peak
        calls.append(item.value)
        active += 1
        peak = max(peak, active)
        if active == 2:
            started.set()
        await asyncio.sleep(0.03)
        active -= 1
        return _receipt(item)

    async def run() -> list[BulkItemReceipt | BulkSummary]:
        return [
            outcome
            async for outcome in AsyncBulkExecutor(
                execute,
                policy=BulkExecutionPolicy(max_parallelism=2),
                checkpoint_sink=checkpoints.append,
            ).stream(plan)
        ]

    async def cancel_after_start() -> list[BulkItemReceipt | BulkSummary]:
        task = asyncio.create_task(run())
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        return await task

    outcomes = asyncio.run(cancel_after_start())
    summary = outcomes[-1]
    assert isinstance(summary, BulkSummary)
    assert summary.cancelled
    assert summary.settled == 2
    assert summary.outstanding == 2
    assert peak == 2
    assert active == 0
    assert len(calls) == 2
    assert len(checkpoints) == 2
    assert checkpoints[-1].cancellation_requested
    assert [item.index for item in checkpoints[-1].item_receipts] == [0, 1]


def test_async_run_budget_drains_in_flight_items_and_surfaces_budget_state() -> None:
    plan = _plan("one", "two", "three", "four")
    checkpoints: list[BulkCheckpoint] = []

    async def execute(item: CatalogId) -> MutationReceipt:
        await asyncio.sleep(0.03)
        return _receipt(item)

    async def run() -> list[BulkItemReceipt | BulkSummary]:
        return [
            outcome
            async for outcome in AsyncBulkExecutor(
                execute,
                policy=BulkExecutionPolicy(max_parallelism=2),
                checkpoint_sink=checkpoints.append,
                whole_run_budget=TimeBudget(connect=0.01, read=0.01, write=0.01, total=0.01),
            ).stream(plan)
        ]

    outcomes = asyncio.run(run())
    summary = outcomes[-1]
    assert isinstance(summary, BulkSummary)
    assert summary.budget_exhausted
    assert not summary.cancelled
    assert summary.settled == 2
    assert summary.outstanding == 2
    assert summary.budget_seconds is not None
    assert summary.elapsed_seconds >= summary.budget_seconds
    assert summary.reason and "exhausted" in summary.reason
    assert len(checkpoints) == 2
    assert not checkpoints[-1].cancellation_requested
