"""Dual-mode uData worker job, task, and budgeted task-wait service."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from functools import partial
from time import monotonic, sleep
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.jobs import (
    PLATFORM,
    WORKER_JOB_KIND,
    WORKER_JOBS_COLLECTION,
    JobMutationResult,
    JobScheduleInput,
    UDataJobHandle,
    WorkerTaskWaitResult,
)
from datasluice.connectors.catalog.udata.services.datasets import _require_mutation_permission
from datasluice.connectors.catalog.udata.services.taxonomies import (
    AsyncCatalogService,
    Permissions,
    Policy,
    Request,
    Response,
    SyncCatalogService,
    _dataset_mutate,
    _parsed,
    _run_mutation,
    _run_mutation_async,
)
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import jobs as wire
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import BudgetExhaustedError, NativeCatalogError
from datasluice.runtime.resilience import DeadlineMonitor

if TYPE_CHECKING:
    from typing import Protocol, Self

    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
    from datasluice.domain.catalog.resilience import TimeBudget

    class _WaitableHandle(Protocol):
        @property
        def terminal(self) -> bool: ...
        @property
        def abandoned(self) -> bool: ...
        @property
        def polls(self) -> int: ...
        @property
        def max_polls(self) -> int: ...

        def abandoned_lease(self, *, exhausted: bool = False) -> Self: ...


type Clock = Callable[[], float]
type Sleeper = Callable[[float], None]
type AsyncSleeper = Callable[[float], Awaitable[None]]
type Observe = Callable[[UDataJobHandle], tuple[UDataJobHandle, MappingRecord]]
type AsyncObserve = Callable[[UDataJobHandle], Awaitable[tuple[UDataJobHandle, MappingRecord]]]

_mutation = partial(_run_mutation, SETTLEMENT_ERRORS, JobMutationResult, WORKER_JOB_KIND)
_mutation_async = partial(_run_mutation_async, ASYNC_SETTLEMENT_ERRORS, JobMutationResult, WORKER_JOB_KIND)


def _created_identifier(payload: object) -> str:
    """Return the server identifier one created job document carries, else the collection."""
    if isinstance(payload, Mapping) and isinstance(payload.get("id"), str) and payload["id"]:
        return payload["id"]
    return WORKER_JOBS_COLLECTION


def _wait_step[H: _WaitableHandle](
    handle: H,
    monitor: DeadlineMonitor,
    record: MappingRecord | None,
    delay: float,
) -> tuple[bool, H, MappingRecord | None]:
    """Decide whether the current lease may take one more bounded poll."""
    if handle.terminal or handle.abandoned:
        return False, handle, record
    if handle.polls >= handle.max_polls:
        return False, handle.abandoned_lease(), record
    if delay >= monitor.remaining():
        return False, handle.abandoned_lease(exhausted=True), record
    return True, handle, record


def _observe_task(
    observe: Observe,
    handle: UDataJobHandle,
    monitor: DeadlineMonitor,
) -> tuple[UDataJobHandle, MappingRecord]:
    """Take one bounded observation of the queued task this handle names."""
    try:
        monitor.assert_dispatchable(wire.GET_TASK_OPERATION, PLATFORM)
    except BudgetExhaustedError:
        return handle.abandoned_lease(exhausted=True), _status_only_record(handle)
    return observe(handle)


def _status_only_record(handle: UDataJobHandle) -> MappingRecord:
    return MappingRecord({"id": handle.task_id, "status": handle.status})


def _wait_for_task(
    handle: UDataJobHandle,
    monitor: DeadlineMonitor,
    observe: Observe,
    sleeper: Sleeper,
) -> WorkerTaskWaitResult:
    """Poll one queued task under the caller's finite lease and poll ceiling."""
    current = handle
    record: MappingRecord | None = None
    while True:
        proceed, stopped, record = _wait_step(current, monitor, record, current.poll_interval)
        if not proceed:
            return WorkerTaskWaitResult(handle=stopped, record=record)
        if current.poll_interval > 0:
            sleeper(current.poll_interval)
        try:
            monitor.assert_dispatchable(wire.GET_TASK_OPERATION, PLATFORM)
            observed, observed_record = observe(current)
            monitor.assert_dispatchable(wire.GET_TASK_OPERATION, PLATFORM)
        except BudgetExhaustedError:
            return WorkerTaskWaitResult(handle=current.abandoned_lease(exhausted=True), record=record)
        current, record = observed, observed_record


async def _wait_for_task_async(
    handle: UDataJobHandle,
    monitor: DeadlineMonitor,
    observe: AsyncObserve,
    sleeper: AsyncSleeper,
) -> WorkerTaskWaitResult:
    """Await one queued task under the same bounded lease as the sync surface."""
    current = handle
    record: MappingRecord | None = None
    while True:
        proceed, stopped, record = _wait_step(current, monitor, record, current.poll_interval)
        if not proceed:
            return WorkerTaskWaitResult(handle=stopped, record=record)
        if current.poll_interval > 0:
            await sleeper(current.poll_interval)
        try:
            async with asyncio.timeout(max(0.0, monitor.remaining())) as deadline:
                observed, observed_record = await observe(current)
                monitor.assert_dispatchable(wire.GET_TASK_OPERATION, PLATFORM)
        except BudgetExhaustedError:
            return WorkerTaskWaitResult(handle=current.abandoned_lease(exhausted=True), record=record)
        except TimeoutError:
            if not deadline.expired():
                raise
            return WorkerTaskWaitResult(handle=current.abandoned_lease(exhausted=True), record=record)
        current, record = observed, observed_record


class SyncWorkerJobsService(SyncCatalogService):
    """Typed synchronous worker job, task, and schedulable operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def _read_admin(
        self, request: Request, permissions: Permissions, operation: str, *, deadline: DeadlineMonitor | None = None
    ) -> Response:
        """Run one bounded workers read behind explicit admin permission evidence."""
        method, path, headers, body = request
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
            permissions=permissions,
            deadline=deadline,
            credential=_require_mutation_permission(
                self._client._resolved_credential(), operation, permissions, admin=True
            ),
        )

    def _mutate_admin(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        """Run one workers write behind explicit admin permission evidence."""
        return _dataset_mutate(
            self._client._dataset_call,
            self._client._resolved_credential(),
            permissions,
            policy,
            operation,
            request,
            admin=True,
        )

    def list_jobs(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        """GET /api/1/workers/jobs/."""
        return _parsed(
            self._read_admin(wire.list_jobs_request(), permissions, wire.LIST_JOBS_OPERATION),
            wire.LIST_JOBS_OPERATION,
            wire.parse_job_sequence,
        )

    def get_job(self, job_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/workers/jobs/<id>/."""
        return _parsed(
            self._read_admin(wire.get_job_request(job_id), permissions, wire.GET_JOB_OPERATION),
            wire.GET_JOB_OPERATION,
            wire.parse_record,
        )

    def get_task(self, task_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/workers/tasks/<id>/."""
        return _parsed(
            self._read_admin(wire.get_task_request(task_id), permissions, wire.GET_TASK_OPERATION),
            wire.GET_TASK_OPERATION,
            wire.parse_record,
        )

    def list_schedulables(self, permissions: Permissions) -> tuple[str, ...]:
        """GET /api/1/workers/jobs/schedulables/."""
        return _parsed(
            self._read_admin(wire.list_schedulables_request(), permissions, wire.LIST_SCHEDULABLES_OPERATION),
            wire.LIST_SCHEDULABLES_OPERATION,
            wire.parse_schedulables,
        )

    def create_job(
        self,
        client_input: JobScheduleInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> JobMutationResult:
        """POST /api/1/workers/jobs/."""
        return _mutation(
            client_input.name,
            mutation_policy,
            "created",
            wire.CREATE_JOB_OPERATION,
            lambda: wire.create_job_request(client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy, wire.CREATE_JOB_OPERATION),
            success_target=_created_identifier,
        )

    def update_job(
        self,
        job_id: str,
        client_input: JobScheduleInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> JobMutationResult:
        """PUT /api/1/workers/jobs/<id>/."""
        return _mutation(
            job_id,
            mutation_policy,
            "updated",
            wire.UPDATE_JOB_OPERATION,
            lambda: wire.update_job_request(job_id, client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy, wire.UPDATE_JOB_OPERATION),
        )

    def delete_job(
        self,
        job_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> JobMutationResult:
        """DELETE /api/1/workers/jobs/<id>/."""
        return _mutation(
            job_id,
            mutation_policy,
            "deleted",
            wire.DELETE_JOB_OPERATION,
            lambda: wire.delete_job_request(job_id),
            lambda request: self._mutate_admin(request, permissions, mutation_policy, wire.DELETE_JOB_OPERATION),
            destructive=True,
        )

    def wait_for_task(
        self,
        handle: UDataJobHandle,
        permissions: Permissions,
        *,
        budget: TimeBudget | None = None,
        clock: Clock = monotonic,
        sleeper: Sleeper = sleep,
    ) -> WorkerTaskWaitResult:
        """Poll GET /api/1/workers/tasks/<id>/ under the caller's finite lease."""
        monitor = DeadlineMonitor(budget or handle.budget, clock=clock)

        def observe(lease: UDataJobHandle) -> tuple[UDataJobHandle, MappingRecord]:
            response = self._read_admin(
                wire.get_task_request(lease.task_id), permissions, wire.GET_TASK_OPERATION, deadline=monitor
            )
            record = wire.parse_record(response[1], wire.GET_TASK_OPERATION)
            return lease.observed(wire.parse_task_status(record.payload, wire.GET_TASK_OPERATION)), record

        def bounded(lease: UDataJobHandle) -> tuple[UDataJobHandle, MappingRecord]:
            return _observe_task(observe, lease, monitor)

        return _wait_for_task(handle, monitor, bounded, sleeper)


class AsyncWorkerJobsService(AsyncCatalogService):
    """Typed asynchronous worker job, task, and schedulable operations."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def _read_admin(
        self, request: Request, permissions: Permissions, operation: str, *, deadline: DeadlineMonitor | None = None
    ) -> Response:
        """Run one bounded workers read behind explicit async admin permission evidence."""
        method, path, headers, body = request
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
            permissions=permissions,
            deadline=deadline,
            credential=_require_mutation_permission(
                await self._client._resolved_credential_async(), operation, permissions, admin=True
            ),
        )

    async def _mutate_admin(
        self, request: Request, permissions: Permissions, policy: Policy, operation: str
    ) -> Response:
        """Run one workers write behind explicit async admin permission evidence."""
        return await _dataset_mutate(
            self._client._dataset_call_async,
            await self._client._resolved_credential_async(),
            permissions,
            policy,
            operation,
            request,
            admin=True,
        )

    async def list_jobs(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        """GET /api/1/workers/jobs/."""
        return _parsed(
            await self._read_admin(wire.list_jobs_request(), permissions, wire.LIST_JOBS_OPERATION),
            wire.LIST_JOBS_OPERATION,
            wire.parse_job_sequence,
        )

    async def get_job(self, job_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/workers/jobs/<id>/."""
        return _parsed(
            await self._read_admin(wire.get_job_request(job_id), permissions, wire.GET_JOB_OPERATION),
            wire.GET_JOB_OPERATION,
            wire.parse_record,
        )

    async def get_task(self, task_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/workers/tasks/<id>/."""
        return _parsed(
            await self._read_admin(wire.get_task_request(task_id), permissions, wire.GET_TASK_OPERATION),
            wire.GET_TASK_OPERATION,
            wire.parse_record,
        )

    async def list_schedulables(self, permissions: Permissions) -> tuple[str, ...]:
        """GET /api/1/workers/jobs/schedulables/."""
        return _parsed(
            await self._read_admin(wire.list_schedulables_request(), permissions, wire.LIST_SCHEDULABLES_OPERATION),
            wire.LIST_SCHEDULABLES_OPERATION,
            wire.parse_schedulables,
        )

    async def create_job(
        self,
        client_input: JobScheduleInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> JobMutationResult:
        """POST /api/1/workers/jobs/."""
        return await _mutation_async(
            client_input.name,
            mutation_policy,
            "created",
            wire.CREATE_JOB_OPERATION,
            lambda: wire.create_job_request(client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy, wire.CREATE_JOB_OPERATION),
            success_target=_created_identifier,
        )

    async def update_job(
        self,
        job_id: str,
        client_input: JobScheduleInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> JobMutationResult:
        """PUT /api/1/workers/jobs/<id>/."""
        return await _mutation_async(
            job_id,
            mutation_policy,
            "updated",
            wire.UPDATE_JOB_OPERATION,
            lambda: wire.update_job_request(job_id, client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy, wire.UPDATE_JOB_OPERATION),
        )

    async def delete_job(
        self,
        job_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> JobMutationResult:
        """DELETE /api/1/workers/jobs/<id>/."""
        return await _mutation_async(
            job_id,
            mutation_policy,
            "deleted",
            wire.DELETE_JOB_OPERATION,
            lambda: wire.delete_job_request(job_id),
            lambda request: self._mutate_admin(request, permissions, mutation_policy, wire.DELETE_JOB_OPERATION),
            destructive=True,
        )

    async def wait_for_task(
        self,
        handle: UDataJobHandle,
        permissions: Permissions,
        *,
        budget: TimeBudget | None = None,
        clock: Clock = monotonic,
        sleeper: AsyncSleeper = asyncio.sleep,
    ) -> WorkerTaskWaitResult:
        """Await GET /api/1/workers/tasks/<id>/ under the caller's finite lease."""
        monitor = DeadlineMonitor(budget or handle.budget, clock=clock)

        async def observe(lease: UDataJobHandle) -> tuple[UDataJobHandle, MappingRecord]:
            response = await self._read_admin(
                wire.get_task_request(lease.task_id), permissions, wire.GET_TASK_OPERATION, deadline=monitor
            )
            record = wire.parse_record(response[1], wire.GET_TASK_OPERATION)
            return lease.observed(wire.parse_task_status(record.payload, wire.GET_TASK_OPERATION)), record

        async def bounded(lease: UDataJobHandle) -> tuple[UDataJobHandle, MappingRecord]:
            try:
                monitor.assert_dispatchable(wire.GET_TASK_OPERATION, PLATFORM)
            except BudgetExhaustedError:
                return lease.abandoned_lease(exhausted=True), _status_only_record(lease)
            return await observe(lease)

        return await _wait_for_task_async(handle, monitor, bounded, sleeper)
