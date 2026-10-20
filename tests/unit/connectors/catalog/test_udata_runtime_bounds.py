from __future__ import annotations

import asyncio
from io import BytesIO
from threading import get_ident
from typing import Any, cast

import pytest

from datasluice.connectors.catalog.udata.models.contact_visualization import VisualizationImageInput
from datasluice.connectors.catalog.udata.models.harvest import UDataJobHandle as HarvestHandle
from datasluice.connectors.catalog.udata.models.jobs import UDataJobHandle as TaskHandle
from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput, UploadDeadlineExceededError
from datasluice.connectors.catalog.udata.services.harvest import _wait_for_job, _wait_for_job_async
from datasluice.connectors.catalog.udata.services.jobs import _wait_for_task, _wait_for_task_async
from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.resilience import DeadlineMonitor
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_SITE,
    async_client,
    async_route_table,
    sync_client,
    sync_route_table,
    with_site_route,
)


@pytest.mark.parametrize("chunk", [b"a", b""])
def test_upload_checks_deadline_after_data_and_eof_reads(chunk: bytes) -> None:
    now = [0.0]

    class SlowSource(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            now[0] = 2.0
            return chunk

    source = SlowSource()
    upload = ResourceUploadInput(source, "file", 1, budget=TimeBudget(1, 1, 1, 1), clock=lambda: now[0])
    try:
        part = upload.part()
        assert not isinstance(part.data, bytes)
        with pytest.raises(UploadDeadlineExceededError):
            part.data.read()
    finally:
        upload.close()
    assert source.closed


def test_visualization_image_rejects_oversize_before_copy() -> None:
    class NoCopy(bytearray):
        def __bytes__(self) -> bytes:
            raise AssertionError("Oversize input must not be copied.")

    image = cast("bytes", NoCopy(1_048_577))
    with pytest.raises(ValueError, match="byte limit"):
        VisualizationImageInput(image, "image/png")


def test_visualization_image_freezes_mutable_input_and_accepts_exact_limit() -> None:
    data = bytearray(1_048_576)
    image = VisualizationImageInput(cast("bytes", data), "image/png")
    data.append(1)
    assert isinstance(image.data, bytes)
    part = image.part()
    assert isinstance(part.data, bytes)
    assert len(part.data) == 1_048_576


@pytest.mark.parametrize("family", ["harvest", "task"])
def test_sync_wait_observes_on_caller_thread_and_rejects_late_success(family: str) -> None:
    caller = get_ident()
    threads: list[int] = []
    now = [0.0]
    monitor = DeadlineMonitor(TimeBudget(1, 1, 1, 1), clock=lambda: now[0])

    def observe_harvest(handle: HarvestHandle) -> tuple[HarvestHandle, MappingRecord]:
        threads.append(get_ident())
        now[0] = 2.0
        return handle.observed("done"), MappingRecord({"status": "done"})

    def observe_task(handle: TaskHandle) -> tuple[TaskHandle, MappingRecord]:
        threads.append(get_ident())
        now[0] = 2.0
        return handle.observed("SUCCESS"), MappingRecord({"status": "SUCCESS"})

    stopped: HarvestHandle | TaskHandle
    if family == "harvest":
        harvest_result = _wait_for_job(
            HarvestHandle("source", "job", poll_interval=0), monitor, observe_harvest, lambda _: None
        )
        stopped = harvest_result.handle
    else:
        task_result = _wait_for_task(TaskHandle("task", poll_interval=0), monitor, observe_task, lambda _: None)
        stopped = task_result.handle
    assert threads == [caller]
    assert stopped.exhausted
    assert stopped.abandoned
    assert not stopped.terminal


def _wait_via(client: Any, family: str, budget: TimeBudget) -> Any:
    """Invoke the family wait on either mode's client."""
    if family == "harvest":
        return client.harvest_moderation_admin.wait_for_job(
            HarvestHandle("source", "job", poll_interval=0), UDATA_ADMIN_PERMISSIONS, budget=budget
        )
    return client.worker_jobs.wait_for_task(TaskHandle("job", poll_interval=0), UDATA_ADMIN_PERMISSIONS, budget=budget)


def _capture_deadlines(deadlines: list[DeadlineMonitor], family: str) -> Any:
    """Return one dataset-call stub recording the deadline it was handed."""

    def capture(**kwargs: object) -> tuple[int, object, RuntimeResponse]:
        deadline = kwargs.get("deadline")
        assert isinstance(deadline, DeadlineMonitor)
        deadlines.append(deadline)
        status = (
            ("processing" if family == "harvest" else "PENDING")
            if len(deadlines) == 1
            else ("done" if family == "harvest" else "SUCCESS")
        )
        return 200, {"id": "job", "status": status}, RuntimeResponse(200, {}, b"")

    return capture


@pytest.mark.parametrize("family", ["harvest", "task"])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_wait_passes_one_absolute_deadline_to_every_poll(
    family: str, asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    deadlines: list[DeadlineMonitor] = []
    budget = TimeBudget(1, 1, 1, 1)
    capture = _capture_deadlines(deadlines, family)

    async def run() -> None:
        async def capture_async(**kwargs: object) -> tuple[int, object, RuntimeResponse]:
            return capture(**kwargs)

        async with async_client(async_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            monkeypatch.setattr(client, "_dataset_call_async", capture_async)
            await _wait_via(client, family, budget)

    if asynchronous:
        asyncio.run(run())
    else:
        with sync_client(sync_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            monkeypatch.setattr(client, "_dataset_call", capture)
            _wait_via(client, family, budget)
    assert len(deadlines) == 2
    assert deadlines[0] is deadlines[1]
    assert deadlines[0].budget is budget


@pytest.mark.parametrize("family", ["harvest", "task"])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_wait_transport_uses_the_lease_deadline(
    family: str, asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    now = [0.0]
    observed: list[DeadlineMonitor] = []

    def response(request: RuntimeRequest) -> RuntimeResponse:
        if request.url.endswith("/api/1/site/"):
            return RuntimeResponse(200, {}, json.dumps(UDATA_SITE).encode())
        assert request.deadline is not None
        observed.append(request.deadline)
        now[0] += 0.5
        request.deadline.assert_dispatchable()
        status = "processing" if family == "harvest" else "PENDING"
        return RuntimeResponse(200, {}, json.dumps({"id": "job", "status": status}).encode())

    budget = TimeBudget(1, 1, 1, 1)

    async def run() -> None:
        async def send(request: RuntimeRequest) -> RuntimeResponse:
            await asyncio.sleep(0)
            return response(request)

        router = async_route_table(with_site_route({}))
        monkeypatch.setattr(router, "send", send)
        async with async_client(router, UDATA_CREDENTIAL) as client:
            if family == "harvest":
                result = await client.harvest_moderation_admin.wait_for_job(
                    HarvestHandle("source", "job", poll_interval=0),
                    UDATA_ADMIN_PERMISSIONS,
                    budget=budget,
                    clock=lambda: now[0],
                )
                assert result.handle.exhausted
            else:
                task_result = await client.worker_jobs.wait_for_task(
                    TaskHandle("job", poll_interval=0),
                    UDATA_ADMIN_PERMISSIONS,
                    budget=budget,
                    clock=lambda: now[0],
                )
                assert task_result.handle.exhausted

    if asynchronous:
        asyncio.run(run())
    else:
        router = sync_route_table(with_site_route({}))
        monkeypatch.setattr(router, "send", response)
        with sync_client(router, UDATA_CREDENTIAL) as client:
            if family == "harvest":
                result = client.harvest_moderation_admin.wait_for_job(
                    HarvestHandle("source", "job", poll_interval=0),
                    UDATA_ADMIN_PERMISSIONS,
                    budget=budget,
                    clock=lambda: now[0],
                )
                assert result.handle.exhausted
            else:
                task_result = client.worker_jobs.wait_for_task(
                    TaskHandle("job", poll_interval=0),
                    UDATA_ADMIN_PERMISSIONS,
                    budget=budget,
                    clock=lambda: now[0],
                )
                assert task_result.handle.exhausted
    assert len(observed) == 2
    assert observed[0] is observed[1]


@pytest.mark.parametrize("family", ["harvest", "task"])
def test_async_wait_cancellation_finishes_the_observation(family: str) -> None:
    async def run() -> None:
        started = asyncio.Event()
        finished = asyncio.Event()
        monitor = DeadlineMonitor(TimeBudget())

        async def stop_when_cancelled() -> None:
            started.set()
            try:
                await asyncio.Future()
            finally:
                finished.set()

        async def observe_harvest(handle: HarvestHandle) -> tuple[HarvestHandle, MappingRecord]:
            await stop_when_cancelled()
            return handle, MappingRecord({})

        async def observe_task(handle: TaskHandle) -> tuple[TaskHandle, MappingRecord]:
            await stop_when_cancelled()
            return handle, MappingRecord({})

        async def wait() -> None:
            if family == "harvest":
                await _wait_for_job_async(
                    HarvestHandle("source", "job", poll_interval=0), monitor, observe_harvest, asyncio.sleep
                )
            else:
                await _wait_for_task_async(TaskHandle("task", poll_interval=0), monitor, observe_task, asyncio.sleep)

        task = asyncio.create_task(wait())
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

    asyncio.run(run())
