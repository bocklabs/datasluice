"""Tests for bounded, resumable bulk mutation execution."""

from __future__ import annotations

import asyncio
import json
from threading import Lock
from time import sleep

import pytest

from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.receipts import BulkCheckpoint, BulkItemReceipt, BulkPlan, MutationReceipt
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
    assert one[-1].dispatches == 1


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
