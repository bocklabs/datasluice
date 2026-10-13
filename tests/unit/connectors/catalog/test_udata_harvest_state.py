"""Budgeted job-wait lease and state-transition coverage for the uData harvest family."""

from __future__ import annotations

import asyncio
import time
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.models.harvest import HarvestJobWaitResult, UDataJobHandle
from datasluice.domain.catalog.resilience import TimeBudget
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
    sync_client,
)

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_ADMIN_PERMISSIONS


def short_budget() -> TimeBudget:
    return TimeBudget(connect=0.01, read=0.02, write=0.02, total=0.05)


JOB_URL = f"{ORIGIN}/api/1/harvest/job/job-9/"
_SITE_RESPONSE = RuntimeResponse(
    status_code=200,
    headers={"Content-Type": "application/json"},
    body=encoded_body(UDATA_SITE),
)


def _handle(**overrides: object) -> UDataJobHandle:
    values: dict[str, object] = {"source_id": "source-1", "job_id": "job-9", "poll_interval": 0}
    values.update(overrides)
    return UDataJobHandle(**values)  # type: ignore[arg-type]


async def _aresponse(response: RuntimeResponse) -> RuntimeResponse:
    """Yield once before returning, so an async client observes a real await point."""
    await asyncio.sleep(0)
    return response


class PollingRouter:
    """Answer each job poll with the next pinned status, then hold the last one."""

    def __init__(self, statuses: list[str], *, hang_after: int | None = None) -> None:
        self.statuses = statuses
        self.polls = 0
        self.hang_after = hang_after
        self.closed = 0

    def _job_response(self) -> RuntimeResponse:
        self.polls += 1
        if self.hang_after is not None and self.polls > self.hang_after:
            time.sleep(0.5)
        status = self.statuses[min(self.polls, len(self.statuses)) - 1]
        return RuntimeResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=encoded_body({"id": "job-9", "status": status}),
        )

    def route(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.url == f"{ORIGIN}/api/1/site/":
            return _SITE_RESPONSE
        if request.url == JOB_URL:
            return self._job_response()
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
        if request.url == JOB_URL:
            await asyncio.sleep(5)
        return await _aresponse(self.route(request))

    async def aclose(self) -> None:
        self.close()


class CancellingRouter(PollingRouter):
    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.url == JOB_URL:
            raise asyncio.CancelledError()
        return self.route(request)


def test_handle_observation_advances_polls_and_terminal_state() -> None:
    """Each bounded observation advances the poll count and records the observed status."""
    handle = _handle()
    assert handle.terminal is False
    started = handle.observed("processing")
    assert started.polls == 1 and started.status == "processing" and started.terminal is False
    done = started.observed("done")
    assert done.polls == 2 and done.terminal is True


def test_handle_refuses_unknown_states_and_bad_lease_values() -> None:
    """Handle state stays inside the pinned harvest vocabulary and bounded ranges."""
    with pytest.raises(ValueError, match="status"):
        _handle(status="finished")
    with pytest.raises(ValueError, match="source identifier"):
        UDataJobHandle(source_id="")
    with pytest.raises(ValueError, match="poll intervals"):
        _handle(poll_interval=301.0)
    with pytest.raises(ValueError, match="poll ceilings"):
        _handle(max_polls=0)
    with pytest.raises(ValueError, match="poll counts"):
        _handle(polls=-1)


def test_abandoned_lease_records_why_the_wait_stopped() -> None:
    """Abandonment keeps the remote job visible and never marks terminal success."""
    abandoned = _handle().abandoned_lease()
    assert abandoned.abandoned is True and abandoned.exhausted is False
    exhausted = _handle().abandoned_lease(exhausted=True)
    assert exhausted.abandoned is True and exhausted.exhausted is True
    assert abandoned.terminal is False


def test_wait_result_serialization_stays_bounded() -> None:
    """The wait result is a JSON-safe projection of the handle and its last record."""
    result = HarvestJobWaitResult(handle=_handle().observed("done"))
    serialized = result.to_dict()
    handle = cast("dict[str, object]", serialized["handle"])
    assert handle["status"] == "done"
    assert serialized["record"] is None


def test_a_terminal_handle_never_polls_again() -> None:
    """Waiting on an already-terminal handle is an immediate, zero-dispatch return."""
    router = PollingRouter(["done"])
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        result = client.harvest_moderation_admin.wait_for_job(_handle().observed("done"), PERMISSIONS)
    assert result.handle.terminal is True
    assert router.polls == 0


def test_sync_wait_stops_at_the_terminal_status_without_extra_polls() -> None:
    """A terminal status ends the lease immediately with the last record attached."""
    router = PollingRouter(["processing", "done"])
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        result = client.harvest_moderation_admin.wait_for_job(_handle(), PERMISSIONS)
    assert result.handle.status == "done"
    assert result.handle.terminal is True
    assert result.handle.abandoned is False
    assert result.record is not None and result.record.payload["id"] == "job-9"
    assert router.polls == 2


def test_sync_wait_observation_timeout_abandons_without_killing_the_caller() -> None:
    """An observation that outlives the remaining budget abandons the lease."""
    router = PollingRouter(["processing"], hang_after=1)
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        result = client.harvest_moderation_admin.wait_for_job(
            _handle(max_polls=4, poll_interval=0.0), PERMISSIONS, budget=short_budget()
        )
    assert result.handle.exhausted is True
    assert result.handle.abandoned is True


def test_sync_wait_cancellation_leaves_the_lease_stopped_and_post_state_ready() -> None:
    """A cancelled observation propagates and the client stays usable."""
    router = CancellingRouter(["processing"])
    with sync_client(cast("SyncRouteRouter", router), UDATA_CREDENTIAL) as client:
        with pytest.raises(asyncio.CancelledError):
            client.harvest_moderation_admin.wait_for_job(_handle(max_polls=4), PERMISSIONS)
        assert client.site_version().version == "17.6.0"
    assert router.closed == 1


def test_async_wait_mirrors_the_sync_lease() -> None:
    """The async waiter enforces the same bounded lease and terminal stop."""

    async def run() -> HarvestJobWaitResult:
        router = AsyncPollingRouter(["pending", "processing", "done"])
        async with async_client(cast("AsyncRouteRouter", router), UDATA_CREDENTIAL) as client:
            return await client.harvest_moderation_admin.wait_for_job(_handle(max_polls=8), PERMISSIONS)

    result = asyncio.run(run())
    assert result.handle.status == "done"
    assert result.handle.terminal is True
    assert result.handle.abandoned is False


def test_async_wait_observation_timeout_abandons_the_lease() -> None:
    """The async observation timeout abandons instead of hanging the caller."""

    async def run() -> HarvestJobWaitResult:
        router = SlowAsyncRouter(["processing"])
        async with async_client(cast("AsyncRouteRouter", router), UDATA_CREDENTIAL) as client:
            return await client.harvest_moderation_admin.wait_for_job(
                _handle(max_polls=4), PERMISSIONS, budget=short_budget()
            )

    result = asyncio.run(run())
    assert result.handle.exhausted is True
    assert result.handle.abandoned is True
