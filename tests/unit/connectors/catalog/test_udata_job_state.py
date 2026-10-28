from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.models.jobs import UDataJobHandle, WorkerTaskWaitResult
from datasluice.connectors.catalog.udata.wire import jobs as wire
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_SITE,
    AsyncRouteRouter,
    SyncRouteRouter,
    async_client,
    encoded_body,
    swagger_response,
    sync_client,
)

ORIGIN = UDATA_ORIGIN
ADMIN = UDATA_ADMIN_PERMISSIONS


def short_budget() -> TimeBudget:
    return TimeBudget(connect=0.01, read=0.02, write=0.02, total=0.05)


TASK_URL = f"{ORIGIN}/api/1/workers/tasks/wheel-task/"
_SITE_RESPONSE = RuntimeResponse(
    status_code=200,
    headers={"Content-Type": "application/json"},
    body=encoded_body(UDATA_SITE),
)


def _handle(**overrides: object) -> UDataJobHandle:
    values: dict[str, object] = {"task_id": "wheel-task", "poll_interval": 0}
    values.update(overrides)
    return UDataJobHandle(**values)  # type: ignore[arg-type]


async def _aresponse(response: RuntimeResponse) -> RuntimeResponse:
    """Yield once before returning, so an async client observes a real await point."""
    await asyncio.sleep(0)
    return response


class PollingRouter:
    """Answer each task poll with the next pinned status, then hold the last one."""

    def __init__(self, statuses: list[str], *, hang_after: int | None = None) -> None:
        self.statuses = statuses
        self.polls = 0
        self.hang_after = hang_after
        self.closed = 0

    def _task_response(self) -> RuntimeResponse:
        self.polls += 1
        if self.hang_after is not None and self.polls > self.hang_after:
            time.sleep(0.5)
        status = self.statuses[min(self.polls, len(self.statuses)) - 1]
        return RuntimeResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=encoded_body({"id": "wheel-task", "status": status, "result": None}),
        )

    def route(self, request: RuntimeRequest) -> RuntimeResponse:
        if response := swagger_response(request, {("GET", TASK_URL): None}):
            return response
        if request.url == f"{ORIGIN}/api/1/site/":
            return _SITE_RESPONSE
        if request.url == TASK_URL:
            return self._task_response()
        raise AssertionError(request.url)

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self.route(request)

    def close(self) -> None:
        self.closed += 1


class AsyncPollingRouter(PollingRouter):
    """Expose the sync router to an async client by awaiting each response."""

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:  # type: ignore[override]
        return await _aresponse(self.route(request))

    async def aclose(self) -> None:
        self.close()


class SlowAsyncRouter(PollingRouter):
    async def send(self, request: RuntimeRequest) -> RuntimeResponse:  # type: ignore[override]
        if request.url == TASK_URL:
            await asyncio.sleep(5)
        return await _aresponse(self.route(request))

    async def aclose(self) -> None:
        self.close()


class CancellingRouter(PollingRouter):
    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.url == TASK_URL:
            raise asyncio.CancelledError()
        return self.route(request)


def test_handle_observation_advances_polls_and_terminal_state() -> None:
    """Each bounded observation advances the poll count and records the observed status."""
    handle = _handle()
    assert handle.terminal is False
    started = handle.observed("STARTED")
    assert started.polls == 1
    assert started.status == "STARTED"
    assert started.terminal is False
    done = started.observed("SUCCESS")
    assert done.polls == 2
    assert done.terminal is True


def test_handle_refuses_unknown_states_and_bad_lease_values() -> None:
    """Handle state stays inside the pinned vocabulary and bounded ranges."""
    with pytest.raises(ValueError, match="status"):
        _handle(status="finished")
    with pytest.raises(ValueError, match="task"):
        UDataJobHandle(task_id="")
    with pytest.raises(ValueError, match="poll intervals"):
        _handle(poll_interval=301.0)
    with pytest.raises(ValueError, match="poll ceilings"):
        _handle(max_polls=0)
    with pytest.raises(ValueError, match="poll counts"):
        _handle(polls=-1)


def test_abandoned_lease_records_why_the_wait_stopped() -> None:
    """Abandonment keeps the remote work visible and never marks terminal success."""
    abandoned = _handle().abandoned_lease()
    assert abandoned.abandoned is True
    assert abandoned.exhausted is False
    exhausted = _handle().abandoned_lease(exhausted=True)
    assert exhausted.abandoned is True
    assert exhausted.exhausted is True
    assert abandoned.terminal is False


def test_wait_result_serialization_stays_bounded() -> None:
    """The wait result is a JSON-safe projection of the handle and its last record."""
    result = WorkerTaskWaitResult(handle=_handle().observed("SUCCESS"))
    serialized = result.to_dict()
    handle = cast("dict[str, object]", serialized["handle"])
    assert handle["status"] == "SUCCESS"
    assert serialized["record"] is None


def test_a_terminal_handle_never_polls_again() -> None:
    """Waiting on an already-terminal handle is an immediate, zero-dispatch return."""
    router = PollingRouter(["SUCCESS"])
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        result = client.worker_jobs.wait_for_task(_handle().observed("SUCCESS"), ADMIN)
    assert result.handle.terminal is True
    assert router.polls == 0


def test_sync_wait_stops_at_the_terminal_status_without_extra_polls() -> None:
    """A terminal status ends the lease immediately with the last record attached."""
    router = PollingRouter(["PENDING", "SUCCESS"])
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        result = client.worker_jobs.wait_for_task(_handle(), ADMIN)
    assert result.handle.status == "SUCCESS"
    assert result.handle.terminal is True
    assert result.handle.abandoned is False
    assert result.record is not None
    assert result.record.payload["id"] == "wheel-task"
    assert router.polls == 2


def test_sync_wait_abandons_at_the_poll_ceiling() -> None:
    """The poll ceiling is a deterministic abandon point, not an error."""
    router = PollingRouter(["PENDING"])
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        result = client.worker_jobs.wait_for_task(_handle(max_polls=3), ADMIN)
    assert result.handle.abandoned is True
    assert result.handle.polls == 3
    assert result.handle.terminal is False
    assert router.polls == 3


def test_sync_wait_abandons_when_the_budget_is_exhausted() -> None:
    """An exhausted budget abandons the lease before the next dispatch."""
    ticks = iter(range(1000))

    def clock() -> float:
        return float(next(ticks) * 100)

    router = PollingRouter(["PENDING"])
    short_budget = TimeBudget(connect=0.01, read=0.02, write=0.02, total=1.0)
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        result = client.worker_jobs.wait_for_task(_handle(max_polls=8), ADMIN, budget=short_budget, clock=clock)
    assert result.handle.exhausted is True
    assert result.handle.abandoned is True


def test_sync_wait_cancellation_leaves_the_lease_stopped_and_post_state_ready() -> None:
    """A cancelled observation propagates and the client stays usable."""
    router = CancellingRouter(["PENDING"])
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        handle = _handle(max_polls=4)
        with pytest.raises(asyncio.CancelledError):
            client.worker_jobs.wait_for_task(handle, ADMIN)
        assert client.site_version().version == "17.6.0"
    assert router.closed == 1


def test_sync_wait_observation_timeout_abandons_without_killing_the_caller() -> None:
    """An observation that outlives the remaining budget abandons the lease."""
    router = PollingRouter(["PENDING"], hang_after=1)
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        result = client.worker_jobs.wait_for_task(_handle(max_polls=4, poll_interval=0.0), ADMIN, budget=short_budget())
    assert result.handle.exhausted is True
    assert result.handle.abandoned is True


def test_async_wait_mirrors_the_sync_lease() -> None:
    """The async waiter enforces the same bounded lease and terminal stop."""

    async def run() -> WorkerTaskWaitResult:
        router = AsyncPollingRouter(["PENDING", "STARTED", "SUCCESS"])
        async with async_client(cast("AsyncRouteRouter", router), UDATA_CREDENTIAL) as client:
            return await client.worker_jobs.wait_for_task(_handle(max_polls=8), ADMIN)

    result = asyncio.run(run())
    assert result.handle.status == "SUCCESS"
    assert result.handle.terminal is True
    assert result.handle.abandoned is False


def test_async_wait_observation_timeout_abandons_the_lease() -> None:
    """The async observation timeout abandons instead of hanging the caller."""

    async def run() -> WorkerTaskWaitResult:
        router = SlowAsyncRouter(["PENDING"])
        async with async_client(cast("AsyncRouteRouter", router), UDATA_CREDENTIAL) as client:
            return await client.worker_jobs.wait_for_task(_handle(max_polls=4), ADMIN, budget=short_budget())

    result = asyncio.run(run())
    assert result.handle.exhausted is True
    assert result.handle.abandoned is True


def test_parallel_task_reads_preserve_results_and_post_state() -> None:
    """Concurrent task reads are order-preserved and leave the client ready."""
    router = PollingRouter(["SUCCESS"])
    with (
        sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client,
        ThreadPoolExecutor(max_workers=4) as pool,
    ):
        records = list(pool.map(lambda _: client.worker_jobs.get_task("wheel-task", ADMIN), range(8)))
    assert [str(record.payload["status"]) for record in records] == ["SUCCESS"] * 8


def test_async_parallel_task_reads_preserve_results() -> None:
    """Concurrent async task reads gather without cross-talk."""

    async def run() -> list[str]:
        router = AsyncPollingRouter(["SUCCESS"])
        async with async_client(cast("AsyncRouteRouter", router), UDATA_CREDENTIAL) as client:
            records = await asyncio.gather(*(client.worker_jobs.get_task("wheel-task", ADMIN) for _ in range(8)))
        return [str(record.payload["status"]) for record in records]

    assert asyncio.run(run()) == ["SUCCESS"] * 8


def test_unknown_task_status_during_a_wait_fails_typed() -> None:
    """A deployment answering an undocumented status is a typed error, never a terminal state."""
    router = PollingRouter(["WHATEVER"])
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        handle = _handle(max_polls=4)
        with pytest.raises(CatalogValidationError):
            client.worker_jobs.wait_for_task(handle, ADMIN)


def test_invalid_poll_interval_is_refused_before_any_dispatch() -> None:
    """Lease bounds are validated at handle construction, before transport work."""
    with pytest.raises(ValueError, match="poll intervals"):
        _handle(poll_interval=-1.0)
    assert wire.TASKS_PATH == "/api/1/workers/tasks/"
