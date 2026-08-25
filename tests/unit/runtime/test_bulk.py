"""Tests for bounded, resumable bulk mutation execution."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import AsyncGenerator, Generator
from threading import Event, Lock
from time import sleep
from typing import cast

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
    assert "duplicate" in raised.value.safe_action.lower()


@pytest.mark.parametrize("platform_max_parallelism", [0, 33])
def test_platform_parallelism_bounds_are_enforced(platform_max_parallelism: int) -> None:
    with pytest.raises(ValueError):
        BulkExecutor(
            lambda item: _receipt(item),
            platform_max_parallelism=platform_max_parallelism,
            checkpoint_sink=lambda checkpoint: None,
        )


def test_resume_rejects_a_checkpoint_created_for_a_different_plan() -> None:
    plan = _plan("one", "two")
    other = _plan("one", "three")
    checkpoint = BulkCheckpoint(
        plan=other,
        item_receipts=(BulkItemReceipt(index=0, receipt=_receipt(other.items[0])),),
    )
    calls: list[str] = []

    with pytest.raises(CatalogValidationError) as raised:
        list(
            BulkExecutor(
                lambda item: calls.append(item.value) or _receipt(item),
                checkpoint=checkpoint,
                checkpoint_sink=lambda checkpoint: None,
            ).stream(plan)
        )

    assert calls == []
    assert "does not belong" in str(raised.value)


def test_resume_rejects_a_receipt_target_mismatching_its_plan_index() -> None:
    plan = _plan("one", "two")
    checkpoint = BulkCheckpoint(
        plan=plan,
        item_receipts=(BulkItemReceipt(index=1, receipt=_receipt(plan.items[0])),),
    )

    with pytest.raises(CatalogValidationError, match="does not match its catalog ID"):
        list(
            BulkExecutor(
                lambda item: _receipt(item),
                checkpoint=checkpoint,
                checkpoint_sink=lambda checkpoint: None,
            ).stream(plan)
        )


def test_sync_item_failures_convert_to_failed_receipts_and_counted() -> None:
    plan = _plan("explodes", "wrong-type", "succeeds")

    def execute(item: CatalogId) -> MutationReceipt:
        if item.value == "explodes":
            raise RuntimeError("dispatch failed")
        if item.value == "wrong-type":
            return cast(MutationReceipt, object())
        return _receipt(item)

    outcomes = list(BulkExecutor(execute, checkpoint_sink=lambda checkpoint: None).stream(plan))
    receipts = [outcome for outcome in outcomes if isinstance(outcome, BulkItemReceipt)]
    summary = outcomes[-1]

    assert [item.receipt.outcome for item in receipts] == ["failed", "failed", "succeeded"]
    assert isinstance(summary, BulkSummary)
    assert summary.failed == 2
    assert summary.succeeded == 1
    assert summary.settled == 3


def test_async_item_failures_convert_to_failed_receipts_and_counted() -> None:
    async def run() -> list[BulkItemReceipt | BulkSummary]:
        plan = _plan("explodes", "wrong-type", "succeeds")

        async def execute(item: CatalogId) -> MutationReceipt:
            if item.value == "explodes":
                raise RuntimeError("dispatch failed")
            if item.value == "wrong-type":
                return cast(MutationReceipt, object())
            return _receipt(item)

        return [
            outcome
            async for outcome in AsyncBulkExecutor(execute, checkpoint_sink=lambda checkpoint: None).stream(plan)
        ]

    outcomes = asyncio.run(run())
    receipts = [outcome for outcome in outcomes if isinstance(outcome, BulkItemReceipt)]
    summary = outcomes[-1]

    assert [item.receipt.outcome for item in receipts] == ["failed", "failed", "succeeded"]
    assert isinstance(summary, BulkSummary)
    assert summary.failed == 2
    assert summary.succeeded == 1


def test_sync_post_execution_budget_expiry_keeps_the_authoritative_receipt() -> None:
    plan = _plan("one")
    checkpoints: list[BulkCheckpoint] = []
    ticks = {"value": 0.0}

    def clock() -> float:
        return ticks["value"]

    def execute(item: CatalogId) -> MutationReceipt:
        ticks["value"] += 5.0
        return _receipt(item)

    outcomes = list(
        BulkExecutor(
            execute,
            per_item_budget=TimeBudget(connect=1.0, read=1.0, write=1.0, total=1.0),
            clock=clock,
            checkpoint_sink=checkpoints.append,
        ).stream(plan)
    )
    receipts = [outcome for outcome in outcomes if isinstance(outcome, BulkItemReceipt)]
    summary = outcomes[-1]

    assert receipts[0].receipt.outcome == "succeeded"
    assert receipts[0].receipt.target == plan.items[0]
    assert checkpoints[-1].item_receipts[0].receipt.outcome == "succeeded"
    assert isinstance(summary, BulkSummary)
    assert summary.budget_exhausted
    assert summary.succeeded == 1
    assert summary.failed == 0


def test_async_post_execution_budget_expiry_keeps_the_authoritative_receipt() -> None:
    async def run() -> tuple[list[BulkCheckpoint], list[BulkItemReceipt | BulkSummary]]:
        plan = _plan("one")
        checkpoints: list[BulkCheckpoint] = []
        ticks = {"value": 0.0}

        def clock() -> float:
            return ticks["value"]

        async def execute(item: CatalogId) -> MutationReceipt:
            ticks["value"] += 5.0
            await asyncio.sleep(0)
            return _receipt(item)

        outcomes = [
            outcome
            async for outcome in AsyncBulkExecutor(
                execute,
                per_item_budget=TimeBudget(connect=1.0, read=1.0, write=1.0, total=1.0),
                clock=clock,
                checkpoint_sink=checkpoints.append,
            ).stream(plan)
        ]
        return checkpoints, outcomes

    checkpoints, outcomes = asyncio.run(run())
    receipts = [outcome for outcome in outcomes if isinstance(outcome, BulkItemReceipt)]
    summary = outcomes[-1]

    assert receipts[0].receipt.outcome == "succeeded"
    assert checkpoints[-1].item_receipts[0].receipt.outcome == "succeeded"
    assert isinstance(summary, BulkSummary)
    assert summary.budget_exhausted
    assert summary.succeeded == 1
    assert summary.failed == 0


def test_sync_pre_dispatch_item_budget_expiry_skips_execution() -> None:
    calls: list[CatalogId] = []
    clock_calls = 0

    def clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return 0.0 if clock_calls <= 2 else 2.0

    outcomes = list(
        BulkExecutor(
            lambda item: calls.append(item) or _receipt(item),
            per_item_budget=TimeBudget(connect=1.0, read=1.0, write=1.0, total=1.0),
            clock=clock,
            checkpoint_sink=lambda checkpoint: None,
        ).stream(_plan("one"))
    )

    receipt = outcomes[0]
    assert isinstance(receipt, BulkItemReceipt)
    assert receipt.receipt.outcome == "failed"
    assert receipt.receipt.audit_metadata["budget_exhausted"] is True
    assert calls == []


def test_async_pre_dispatch_item_budget_expiry_skips_execution() -> None:
    async def run() -> tuple[list[CatalogId], list[BulkItemReceipt | BulkSummary]]:
        calls: list[CatalogId] = []
        clock_calls = 0

        def clock() -> float:
            nonlocal clock_calls
            clock_calls += 1
            return 0.0 if clock_calls <= 2 else 2.0

        async def execute(item: CatalogId) -> MutationReceipt:
            calls.append(item)
            return _receipt(item)

        outcomes = [
            outcome
            async for outcome in AsyncBulkExecutor(
                execute,
                per_item_budget=TimeBudget(connect=1.0, read=1.0, write=1.0, total=1.0),
                clock=clock,
                checkpoint_sink=lambda checkpoint: None,
            ).stream(_plan("one"))
        ]
        return calls, outcomes

    calls, outcomes = asyncio.run(run())
    receipt = outcomes[0]
    assert isinstance(receipt, BulkItemReceipt)
    assert receipt.receipt.outcome == "failed"
    assert receipt.receipt.audit_metadata["budget_exhausted"] is True
    assert calls == []


def test_parallelism_is_clamped_to_platform_bound() -> None:
    plan = _plan("one", "two", "three", "four")
    active = 0
    peak = 0
    lock = Lock()
    both_active = Event()

    def execute(item: CatalogId) -> MutationReceipt:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == 2:
                both_active.set()
        assert both_active.wait(timeout=5.0)
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


def test_async_cooperative_cancellation_drains_in_flight_items() -> None:
    async def run() -> tuple[list[str], list[BulkCheckpoint], list[BulkItemReceipt | BulkSummary]]:
        plan = _plan("one", "two", "three", "four")
        cancel_event = asyncio.Event()
        checkpoints: list[BulkCheckpoint] = []
        calls: list[str] = []
        active = 0

        async def execute(item: CatalogId) -> MutationReceipt:
            nonlocal active
            calls.append(item.value)
            active += 1
            if active == 2:
                cancel_event.set()
            await asyncio.sleep(0.01)
            active -= 1
            return _receipt(item)

        outcomes = [
            outcome
            async for outcome in AsyncBulkExecutor(
                execute,
                policy=BulkExecutionPolicy(max_parallelism=2),
                checkpoint_sink=checkpoints.append,
                cancel_event=cancel_event,
            ).stream(plan)
        ]
        assert active == 0
        return calls, checkpoints, outcomes

    calls, checkpoints, outcomes = asyncio.run(run())
    summary = outcomes[-1]
    assert isinstance(summary, BulkSummary)
    assert summary.cancelled
    assert summary.settled == 2
    assert summary.outstanding == 2
    assert calls == ["one", "two"]
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
    assert len(checkpoints) == 1
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


def test_abandoned_sync_stream_drains_in_flight_items_and_persists_the_final_checkpoint() -> None:
    plan = _plan("one", "two", "three", "four")
    checkpoints: list[BulkCheckpoint] = []
    calls: list[str] = []
    blocker = Event()

    def execute(item: CatalogId) -> MutationReceipt:
        calls.append(item.value)
        if item.value == "two":
            blocker.wait(timeout=2.0)
        sleep(0.01)
        return _receipt(item)

    releaser = threading.Timer(0.2, blocker.set)
    releaser.start()
    generator = cast(
        "Generator[BulkItemReceipt | BulkSummary, None, None]",
        BulkExecutor(
            execute,
            policy=BulkExecutionPolicy(max_parallelism=2),
            checkpoint_sink=checkpoints.append,
        ).stream(plan),
    )

    first = next(generator)
    generator.close()
    releaser.cancel()

    assert isinstance(first, BulkItemReceipt)
    assert first.index == 0
    assert set(calls) == {"one", "two"}
    assert len(checkpoints) == 2
    final = checkpoints[-1]
    assert final.cancellation_requested
    assert [item.index for item in final.item_receipts] == [0, 1]
    assert all(item.receipt.outcome == "succeeded" for item in final.item_receipts)


def test_async_aclose_drains_in_flight_tasks_and_persists_the_final_checkpoint_once() -> None:
    plan = _plan("one", "two", "three", "four")
    checkpoints: list[BulkCheckpoint] = []
    calls: list[str] = []

    async def exercise() -> BulkItemReceipt | BulkSummary:
        async def execute(item: CatalogId) -> MutationReceipt:
            calls.append(item.value)
            await asyncio.sleep(0.25 if item.value == "two" else 0.01)
            return _receipt(item)

        stream = cast(
            "AsyncGenerator[BulkItemReceipt | BulkSummary, None]",
            AsyncBulkExecutor(
                execute,
                policy=BulkExecutionPolicy(max_parallelism=2),
                checkpoint_sink=checkpoints.append,
            ).stream(plan),
        )
        first = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
        await asyncio.wait_for(stream.aclose(), timeout=1.0)
        return first

    first = asyncio.run(exercise())

    assert isinstance(first, BulkItemReceipt)
    assert first.index == 0
    assert set(calls) == {"one", "two"}
    assert len(checkpoints) == 2
    assert not checkpoints[0].cancellation_requested
    final = checkpoints[-1]
    assert final.cancellation_requested
    assert [item.index for item in final.item_receipts] == [0, 1]
    assert all(item.receipt.outcome == "succeeded" for item in final.item_receipts)
