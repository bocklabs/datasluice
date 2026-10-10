"""Dual-mode uData harvest source, job, and bounded admin bulk service.

Harvest is the one uData family where a caller-supplied value becomes a URL the
*deployment* fetches, so every write re-applies the crawl-target policy at the
request boundary, and every queued or bulk action is bounded by a caller-owned
lease instead of an unbounded wait.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from functools import partial
from inspect import isawaitable
from time import monotonic, sleep
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.harvest import (
    HARVEST_DESTRUCTIVE_BULK_ACTIONS,
    HARVEST_OPERATION,
    PLATFORM,
    HarvestBulkPreview,
    HarvestBulkResult,
    HarvestBulkTargets,
    HarvestJobItemsQuery,
    HarvestJobQuery,
    HarvestJobWaitResult,
    HarvestMutationResult,
    HarvestPage,
    HarvestPreviewResult,
    HarvestScheduleInput,
    HarvestSourceInput,
    HarvestSourceQuery,
    HarvestValidationInput,
    UDataJobHandle,
    bulk_plan,
)
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import harvest as wire
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.receipts import BulkCheckpoint, MutationReceipt
from datasluice.domain.catalog.safety import BulkExecutionPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import BudgetExhaustedError, CatalogValidationError, NativeCatalogError
from datasluice.runtime.bulk import AsyncBulkExecutor, BulkExecutor, BulkItemReceipt, BulkPlan, BulkSummary
from datasluice.runtime.resilience import DeadlineMonitor

from .datasets import _enforce_mutation_policy, _error_status, _require_mutation_permission
from .resources import _attach, _receipt
from .taxonomies import (
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

if TYPE_CHECKING:
    from threading import Event

    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
    from datasluice.domain.catalog.resilience import TimeBudget

HARVEST_SOURCE_KIND = ResourceKind("harvest-source")
_MAX_HARVEST_READ_BYTES = 1_048_576
_MAX_BULK_PARALLELISM = 8
_CREATE_TARGET = "harvest-source:create"
_PREVIEW_TARGET = "harvest-source:preview"
_WAIT_REQUIREMENT = "The harvest job wait requires a queued job identifier."
_WAIT_ACTION = "Pass the handle returned by run_source, or resolve the queued job first."

type Clock = Callable[[], float]
type AsyncSleep = Callable[[float], Awaitable[None]]
type CheckpointSink = Callable[[BulkCheckpoint], object]
type Observe = Callable[[UDataJobHandle], tuple[UDataJobHandle, MappingRecord]]
type AsyncObserve = Callable[[UDataJobHandle], Awaitable[tuple[UDataJobHandle, MappingRecord]]]

_BULK_REQUESTS: dict[str, Callable[[str], Request]] = {
    "delete-source": wire.delete_source_request,
    "unschedule-source": wire.unschedule_source_request,
    "run-source": wire.run_source_request,
}

_mutation = partial(_run_mutation, SETTLEMENT_ERRORS, HarvestMutationResult, HARVEST_SOURCE_KIND)
_preview_mutation = partial(_run_mutation, SETTLEMENT_ERRORS, HarvestPreviewResult, HARVEST_SOURCE_KIND)
_mutation_async = partial(_run_mutation_async, ASYNC_SETTLEMENT_ERRORS, HarvestMutationResult, HARVEST_SOURCE_KIND)
_preview_mutation_async = partial(
    _run_mutation_async, ASYNC_SETTLEMENT_ERRORS, HarvestPreviewResult, HARVEST_SOURCE_KIND
)


def _plan_receipt(policy: Policy, action: str, target: str, outcome: str, status: int) -> MutationReceipt:
    """Return the shared receipt one admin bulk plan or item settles against."""
    return _receipt(
        policy,
        target,
        outcome,
        status,
        wire.BULK_MUTATIONS[action],
        resource_kind=HARVEST_SOURCE_KIND,
        operation=HARVEST_OPERATION,
    )


def _item_policy(action: str, source_id: str, policy: Policy) -> Policy:
    """Bind one confirmed plan-level policy to a single exact bulk item target.

    A bulk plan is confirmed once, against the action and the digest of the
    exact ordered source set, so every item inherits that confirmation while
    carrying its own identifier in its own receipt.

    Args:
        action: The confirmed bulk action name.
        source_id: The exact source identifier this item targets.
        policy: The confirmed plan-level policy.

    Returns:
        A policy bound to this item's exact target.

    Raises:
        CatalogValidationError: If no confirmed plan-level policy was supplied.
    """
    if policy is None or policy.confirmation is None or not policy.confirmation.confirmed:
        raise CatalogValidationError(
            "Bulk items require a confirmed plan-level policy bound to this action.",
            operation=HARVEST_OPERATION,
            platform=PLATFORM,
            capability_state="forbidden",
            safe_action="Confirm the plan against its previewed confirmation target before running it.",
        )
    return MutationPolicy(
        destructive=action in HARVEST_DESTRUCTIVE_BULK_ACTIONS,
        confirmation=ConfirmationPolicy(confirmed=True, operation=HARVEST_OPERATION, target=source_id),
        concurrency=policy.concurrency,
        idempotency=policy.idempotency,
    )


def _checkpoint_projection(checkpoint: BulkCheckpoint) -> dict[str, object]:
    """Return bounded metadata for one checkpoint, never a payload or an endpoint."""
    return {
        "settled": len(checkpoint.item_receipts),
        "completed_indexes": [item.index for item in checkpoint.item_receipts],
        "outcomes": [item.receipt.outcome for item in checkpoint.item_receipts],
        "cancellation_requested": checkpoint.cancellation_requested,
        "resumption_cursor": checkpoint.resumption_cursor,
    }


def _bulk_result(
    action: str,
    plan: BulkPlan,
    entries: tuple[BulkItemReceipt | BulkSummary, ...],
    checkpoints: list[BulkCheckpoint],
    resumed: BulkCheckpoint | None,
) -> HarvestBulkResult:
    """Fold one drained bulk stream into its typed terminal record."""
    receipts = tuple(entry for entry in entries if isinstance(entry, BulkItemReceipt))
    summaries = [entry for entry in entries if isinstance(entry, BulkSummary)]
    summary = (
        summaries[-1]
        if summaries
        else BulkSummary(total=len(plan.items), succeeded=0, failed=len(plan.items), skipped=0)
    )
    return HarvestBulkResult(
        action=action,
        plan=plan,
        receipts=receipts,
        summary=summary,
        checkpoints=tuple(_checkpoint_projection(checkpoint) for checkpoint in checkpoints),
        resume=checkpoints[-1] if checkpoints else resumed,
    )


def _created_identifier(payload: object) -> str:
    """Return the identifier the deployment assigned to a new harvest source."""
    identifier = payload.get("id") if isinstance(payload, Mapping) else None
    return identifier if isinstance(identifier, str) and identifier else _CREATE_TARGET


def _queued_result(source_id: str) -> Callable[[MutationReceipt, MappingRecord | None], HarvestMutationResult]:
    """Return the result builder that turns one run dispatch into a typed handle."""

    def build(receipt: MutationReceipt, record: MappingRecord | None) -> HarvestMutationResult:
        return HarvestMutationResult(
            receipt=receipt,
            record=record,
            handle=UDataJobHandle(
                source_id=source_id,
                job_id=wire.parse_job_identifier(record.payload if record is not None else None),
            ),
        )

    return build


def _wait_requirement() -> CatalogValidationError:
    """Return the typed refusal for a wait that names no queued job."""
    return CatalogValidationError(
        _WAIT_REQUIREMENT,
        operation=HARVEST_OPERATION,
        platform=PLATFORM,
        safe_action=_WAIT_ACTION,
    )


def _waitable_handle(handle: UDataJobHandle) -> UDataJobHandle:
    """Return the handle only when a queued job identifier makes a wait possible.

    Raises:
        CatalogValidationError: If the handle carries no queued job identifier.
    """
    if not isinstance(handle, UDataJobHandle) or handle.job_id is None:
        raise _wait_requirement()
    return handle


def _wait_step(
    handle: UDataJobHandle,
    monitor: DeadlineMonitor,
    record: MappingRecord | None,
    delay: float,
) -> tuple[bool, UDataJobHandle, MappingRecord | None]:
    """Decide whether the current lease may take one more bounded poll."""
    if handle.terminal or handle.abandoned:
        return False, handle, record
    if handle.polls >= handle.max_polls:
        return False, handle.abandoned_lease(), record
    if delay >= monitor.remaining():
        return False, handle.abandoned_lease(exhausted=True), record
    return True, handle, record


def _wait_for_job(
    handle: UDataJobHandle,
    monitor: DeadlineMonitor,
    observe: Observe,
    sleeper: Callable[[float], None],
) -> HarvestJobWaitResult:
    """Poll one queued job under the caller's finite lease and poll ceiling."""
    current = handle
    record: MappingRecord | None = None
    while True:
        proceed, stopped, record = _wait_step(current, monitor, record, current.poll_interval)
        if not proceed:
            return HarvestJobWaitResult(handle=stopped, record=record)
        if current.poll_interval > 0:
            sleeper(current.poll_interval)
        current, record = observe(current)


async def _wait_for_job_async(
    handle: UDataJobHandle,
    monitor: DeadlineMonitor,
    observe: AsyncObserve,
    sleeper: AsyncSleep,
) -> HarvestJobWaitResult:
    """Await one queued job under the same bounded lease as the sync surface."""
    current = handle
    record: MappingRecord | None = None
    while True:
        proceed, stopped, record = _wait_step(current, monitor, record, current.poll_interval)
        if not proceed:
            return HarvestJobWaitResult(handle=stopped, record=record)
        if current.poll_interval > 0:
            await sleeper(current.poll_interval)
        current, record = await observe(current)


class SyncHarvestService(SyncCatalogService):
    """Typed synchronous harvest source, job, and bounded admin bulk operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        """Bind the service to one strict sync client."""
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the typed error every harvest failure is raised as."""
        return NativeCatalogError

    def list_sources(self, query: HarvestSourceQuery, permissions: Permissions) -> HarvestPage:
        """GET /api/1/harvest/sources/."""
        return _parsed(
            self._read_admin(wire.list_sources_request(query), permissions), HARVEST_OPERATION, wire.parse_page
        )

    def get_source(self, source_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/harvest/source/<id>/."""
        return _parsed(
            self._read_admin(wire.get_source_request(source_id), permissions), HARVEST_OPERATION, wire.parse_record
        )

    def preview_source(self, source_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/harvest/source/<id>/preview/."""
        return _parsed(
            self._read_admin(wire.preview_source_request(source_id), permissions), HARVEST_OPERATION, wire.parse_record
        )

    def list_jobs(self, source_id: str, query: HarvestJobQuery, permissions: Permissions) -> HarvestPage:
        """GET /api/1/harvest/source/<id>/jobs/."""
        return _parsed(
            self._read_admin(wire.list_jobs_request(source_id, query), permissions), HARVEST_OPERATION, wire.parse_page
        )

    def get_job(self, job_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/harvest/job/<ident>/."""
        return _parsed(
            self._read_admin(wire.get_job_request(job_id), permissions), HARVEST_OPERATION, wire.parse_record
        )

    def list_job_items(self, job_id: str, query: HarvestJobItemsQuery, permissions: Permissions) -> HarvestPage:
        """GET /api/1/harvest/job/<ident>/items/."""
        return _parsed(
            self._read_admin(wire.list_job_items_request(job_id, query), permissions),
            HARVEST_OPERATION,
            wire.parse_page,
        )

    def backends(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        """GET /api/1/harvest/backends/."""
        return _parsed(self._read_admin(wire.backends_request(), permissions), HARVEST_OPERATION, wire.parse_backends)

    def create_source(
        self,
        client_input: HarvestSourceInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestMutationResult:
        """POST /api/1/harvest/sources/."""
        return _mutation(
            _CREATE_TARGET,
            mutation_policy,
            "created",
            HARVEST_OPERATION,
            lambda: wire.create_source_request(client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
            success_target=_created_identifier,
        )

    def update_source(
        self,
        source_id: str,
        client_input: HarvestSourceInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestMutationResult:
        """PUT /api/1/harvest/source/<id>/."""
        return _mutation(
            source_id,
            mutation_policy,
            "updated",
            HARVEST_OPERATION,
            lambda: wire.update_source_request(source_id, client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    def delete_source(
        self, source_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> HarvestMutationResult:
        """DELETE /api/1/harvest/source/<id>/; requires destructive confirmation."""
        return _mutation(
            source_id,
            mutation_policy,
            "deleted",
            HARVEST_OPERATION,
            lambda: wire.delete_source_request(source_id),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
            destructive=True,
        )

    def validate_source(
        self,
        source_id: str,
        client_input: HarvestValidationInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestMutationResult:
        """POST /api/1/harvest/source/<id>/validate/."""
        return _mutation(
            source_id,
            mutation_policy,
            "validated",
            HARVEST_OPERATION,
            lambda: wire.validate_source_request(source_id, client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    def run_source(
        self, source_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> HarvestMutationResult:
        """POST /api/1/harvest/source/<id>/run/; returns a typed job handle."""
        return _run_mutation(
            SETTLEMENT_ERRORS,
            _queued_result(source_id),
            HARVEST_SOURCE_KIND,
            source_id,
            mutation_policy,
            "run",
            HARVEST_OPERATION,
            lambda: wire.run_source_request(source_id),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    def schedule_source(
        self,
        source_id: str,
        client_input: HarvestScheduleInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestMutationResult:
        """POST /api/1/harvest/source/<id>/schedule/."""
        return _mutation(
            source_id,
            mutation_policy,
            "scheduled",
            HARVEST_OPERATION,
            lambda: wire.schedule_source_request(source_id, client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    def unschedule_source(
        self, source_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> HarvestMutationResult:
        """DELETE /api/1/harvest/source/<id>/schedule/; requires destructive confirmation."""
        return _mutation(
            source_id,
            mutation_policy,
            "unscheduled",
            HARVEST_OPERATION,
            lambda: wire.unschedule_source_request(source_id),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
            destructive=True,
        )

    def preview_source_config(
        self,
        client_input: HarvestSourceInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestPreviewResult:
        """POST /api/1/harvest/source/preview/; a server dry run, never a write."""
        return _preview_mutation(
            _PREVIEW_TARGET,
            mutation_policy,
            "previewed",
            HARVEST_OPERATION,
            lambda: wire.preview_source_config_request(client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    def preview_bulk(self, action: str, targets: HarvestBulkTargets) -> HarvestBulkPreview:
        """Return the local non-dispatching preview of one admin bulk plan."""
        return HarvestBulkPreview(
            action=action, targets=targets, destructive=action in HARVEST_DESTRUCTIVE_BULK_ACTIONS
        )

    def wait_for_job(
        self,
        handle: UDataJobHandle,
        permissions: Permissions,
        *,
        budget: TimeBudget | None = None,
        clock: Clock = monotonic,
        sleeper: Callable[[float], None] = sleep,
    ) -> HarvestJobWaitResult:
        """Wait for one queued job under an explicit finite lease and poll ceiling."""
        lease = _waitable_handle(handle)
        monitor = DeadlineMonitor(budget or lease.budget, clock=clock)
        return _wait_for_job(
            lease,
            monitor,
            lambda current: self._observe_job(current, permissions, monitor),
            sleeper,
        )

    def run_bulk(
        self,
        action: str,
        targets: HarvestBulkTargets,
        permissions: Permissions,
        mutation_policy: Policy = None,
        *,
        execution_policy: BulkExecutionPolicy | None = None,
        checkpoint_sink: CheckpointSink,
        checkpoint: BulkCheckpoint | None = None,
        whole_run_budget: TimeBudget | None = None,
        item_budget: TimeBudget | None = None,
        cancel_event: Event | None = None,
        clock: Clock = monotonic,
    ) -> HarvestBulkResult:
        """Run one confirmed, bounded, resumable admin harvest bulk plan."""
        preview = self.preview_bulk(action, targets)
        self._authorize_plan(action, preview, permissions, mutation_policy)
        plan = bulk_plan(action, targets.source_ids, preview=False)
        recorded: list[BulkCheckpoint] = []

        def sink(item: BulkCheckpoint) -> None:
            recorded.append(item)
            checkpoint_sink(item)

        executor = BulkExecutor(
            lambda item: self._bulk_item(action, item.value, permissions, mutation_policy),
            policy=execution_policy,
            checkpoint_sink=sink,
            checkpoint=checkpoint,
            platform_max_parallelism=_MAX_BULK_PARALLELISM,
            whole_run_budget=whole_run_budget,
            item_budget=item_budget,
            clock=clock,
            cancel_event=cancel_event,
        )
        entries = tuple(executor.stream(plan))
        return _bulk_result(action, plan, entries, recorded, checkpoint)

    def _bulk_item(self, action: str, source_id: str, permissions: Permissions, policy: Policy) -> MutationReceipt:
        """Settle one bulk item into a per-item receipt without raising."""
        try:
            result = _mutation(
                source_id,
                _item_policy(action, source_id, policy),
                wire.BULK_MUTATIONS[action],
                HARVEST_OPERATION,
                lambda: _BULK_REQUESTS[action](source_id),
                lambda request: self._mutate_admin(request, permissions, policy),
                destructive=action in HARVEST_DESTRUCTIVE_BULK_ACTIONS,
            )
        except SETTLEMENT_ERRORS as error:
            receipt = getattr(error, "mutation_receipt", None)
            if isinstance(receipt, MutationReceipt):
                return receipt
            return _plan_receipt(policy, action, source_id, "failed", 0)
        return result.receipt

    def _authorize_plan(
        self, action: str, preview: HarvestBulkPreview, permissions: Permissions, policy: Policy
    ) -> None:
        """Refuse an unauthorized bulk plan before any item is dispatched."""
        try:
            _enforce_mutation_policy(
                HARVEST_OPERATION, preview.confirmation_target, policy, destructive=preview.destructive
            )
            _require_mutation_permission(
                self._client._resolved_credential(), HARVEST_OPERATION, permissions, admin=True
            )
        except SETTLEMENT_ERRORS as error:
            _attach(error, _plan_receipt(policy, action, preview.confirmation_target, "rejected", _error_status(error)))
            raise

    def _observe_job(
        self, handle: UDataJobHandle, permissions: Permissions, monitor: DeadlineMonitor
    ) -> tuple[UDataJobHandle, MappingRecord]:
        """Take one bounded observation of the queued job this handle names."""
        job_id = handle.job_id
        if job_id is None:
            raise _wait_requirement()
        try:
            monitor.assert_dispatchable(HARVEST_OPERATION, PLATFORM)
        except BudgetExhaustedError:
            return handle.abandoned_lease(exhausted=True), MappingRecord({"status": handle.status})
        _, payload, _ = self._read_admin(wire.get_job_request(job_id), permissions)
        record = wire.parse_record(payload, HARVEST_OPERATION)
        return handle.observed(wire.parse_job_status(record.payload, HARVEST_OPERATION)), record

    def _read_admin(self, request: Request, permissions: Permissions) -> Response:
        """Run one bounded harvest read behind explicit admin permission evidence."""
        method, path, headers, body = request
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=HARVEST_OPERATION,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=_require_mutation_permission(
                self._client._resolved_credential(), HARVEST_OPERATION, permissions, admin=True
            ),
            max_response_bytes=_MAX_HARVEST_READ_BYTES,
        )

    def _mutate_admin(self, request: Request, permissions: Permissions, policy: Policy) -> Response:
        """Run one harvest write behind explicit admin permission evidence."""
        return _dataset_mutate(
            self._client._dataset_call,
            self._client._resolved_credential(),
            permissions,
            policy,
            HARVEST_OPERATION,
            request,
            admin=True,
        )


class AsyncHarvestService(AsyncCatalogService):
    """Typed asynchronous harvest operations mirroring the sync surface."""

    def __init__(self, client: AsyncUDataClient) -> None:
        """Bind the service to one strict async client."""
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the typed error every harvest failure is raised as."""
        return NativeCatalogError

    async def list_sources(self, query: HarvestSourceQuery, permissions: Permissions) -> HarvestPage:
        """GET /api/1/harvest/sources/."""
        response = await self._read_admin(wire.list_sources_request(query), permissions)
        return _parsed(response, HARVEST_OPERATION, wire.parse_page)

    async def get_source(self, source_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/harvest/source/<id>/."""
        response = await self._read_admin(wire.get_source_request(source_id), permissions)
        return _parsed(response, HARVEST_OPERATION, wire.parse_record)

    async def preview_source(self, source_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/harvest/source/<id>/preview/."""
        response = await self._read_admin(wire.preview_source_request(source_id), permissions)
        return _parsed(response, HARVEST_OPERATION, wire.parse_record)

    async def list_jobs(self, source_id: str, query: HarvestJobQuery, permissions: Permissions) -> HarvestPage:
        """GET /api/1/harvest/source/<id>/jobs/."""
        response = await self._read_admin(wire.list_jobs_request(source_id, query), permissions)
        return _parsed(response, HARVEST_OPERATION, wire.parse_page)

    async def get_job(self, job_id: str, permissions: Permissions) -> MappingRecord:
        """GET /api/1/harvest/job/<ident>/."""
        response = await self._read_admin(wire.get_job_request(job_id), permissions)
        return _parsed(response, HARVEST_OPERATION, wire.parse_record)

    async def list_job_items(self, job_id: str, query: HarvestJobItemsQuery, permissions: Permissions) -> HarvestPage:
        """GET /api/1/harvest/job/<ident>/items/."""
        response = await self._read_admin(wire.list_job_items_request(job_id, query), permissions)
        return _parsed(response, HARVEST_OPERATION, wire.parse_page)

    async def backends(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        """GET /api/1/harvest/backends/."""
        response = await self._read_admin(wire.backends_request(), permissions)
        return _parsed(response, HARVEST_OPERATION, wire.parse_backends)

    async def create_source(
        self,
        client_input: HarvestSourceInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestMutationResult:
        """POST /api/1/harvest/sources/."""
        return await _mutation_async(
            _CREATE_TARGET,
            mutation_policy,
            "created",
            HARVEST_OPERATION,
            lambda: wire.create_source_request(client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
            success_target=_created_identifier,
        )

    async def update_source(
        self,
        source_id: str,
        client_input: HarvestSourceInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestMutationResult:
        """PUT /api/1/harvest/source/<id>/."""
        return await _mutation_async(
            source_id,
            mutation_policy,
            "updated",
            HARVEST_OPERATION,
            lambda: wire.update_source_request(source_id, client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    async def delete_source(
        self, source_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> HarvestMutationResult:
        """DELETE /api/1/harvest/source/<id>/; requires destructive confirmation."""
        return await _mutation_async(
            source_id,
            mutation_policy,
            "deleted",
            HARVEST_OPERATION,
            lambda: wire.delete_source_request(source_id),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
            destructive=True,
        )

    async def validate_source(
        self,
        source_id: str,
        client_input: HarvestValidationInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestMutationResult:
        """POST /api/1/harvest/source/<id>/validate/."""
        return await _mutation_async(
            source_id,
            mutation_policy,
            "validated",
            HARVEST_OPERATION,
            lambda: wire.validate_source_request(source_id, client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    async def run_source(
        self, source_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> HarvestMutationResult:
        """POST /api/1/harvest/source/<id>/run/; returns a typed job handle."""
        return await _run_mutation_async(
            ASYNC_SETTLEMENT_ERRORS,
            _queued_result(source_id),
            HARVEST_SOURCE_KIND,
            source_id,
            mutation_policy,
            "run",
            HARVEST_OPERATION,
            lambda: wire.run_source_request(source_id),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    async def schedule_source(
        self,
        source_id: str,
        client_input: HarvestScheduleInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestMutationResult:
        """POST /api/1/harvest/source/<id>/schedule/."""
        return await _mutation_async(
            source_id,
            mutation_policy,
            "scheduled",
            HARVEST_OPERATION,
            lambda: wire.schedule_source_request(source_id, client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    async def unschedule_source(
        self, source_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> HarvestMutationResult:
        """DELETE /api/1/harvest/source/<id>/schedule/; requires destructive confirmation."""
        return await _mutation_async(
            source_id,
            mutation_policy,
            "unscheduled",
            HARVEST_OPERATION,
            lambda: wire.unschedule_source_request(source_id),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
            destructive=True,
        )

    async def preview_source_config(
        self,
        client_input: HarvestSourceInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> HarvestPreviewResult:
        """POST /api/1/harvest/source/preview/; a server dry run, never a write."""
        return await _preview_mutation_async(
            _PREVIEW_TARGET,
            mutation_policy,
            "previewed",
            HARVEST_OPERATION,
            lambda: wire.preview_source_config_request(client_input),
            lambda request: self._mutate_admin(request, permissions, mutation_policy),
        )

    def preview_bulk(self, action: str, targets: HarvestBulkTargets) -> HarvestBulkPreview:
        """Return the local non-dispatching preview of one admin bulk plan."""
        return HarvestBulkPreview(
            action=action, targets=targets, destructive=action in HARVEST_DESTRUCTIVE_BULK_ACTIONS
        )

    async def wait_for_job(
        self,
        handle: UDataJobHandle,
        permissions: Permissions,
        *,
        budget: TimeBudget | None = None,
        clock: Clock = monotonic,
        sleeper: AsyncSleep = asyncio.sleep,
    ) -> HarvestJobWaitResult:
        """Wait for one queued job under an explicit finite lease and poll ceiling."""
        lease = _waitable_handle(handle)
        monitor = DeadlineMonitor(budget or lease.budget, clock=clock)

        async def observe(current: UDataJobHandle) -> tuple[UDataJobHandle, MappingRecord]:
            return await self._observe_job(current, permissions, monitor)

        return await _wait_for_job_async(lease, monitor, observe, sleeper)

    async def run_bulk(
        self,
        action: str,
        targets: HarvestBulkTargets,
        permissions: Permissions,
        mutation_policy: Policy = None,
        *,
        execution_policy: BulkExecutionPolicy | None = None,
        checkpoint_sink: CheckpointSink,
        checkpoint: BulkCheckpoint | None = None,
        whole_run_budget: TimeBudget | None = None,
        item_budget: TimeBudget | None = None,
        cancel_event: object | None = None,
        clock: Clock = monotonic,
    ) -> HarvestBulkResult:
        """Run one confirmed, bounded, resumable admin harvest bulk plan."""
        preview = self.preview_bulk(action, targets)
        await self._authorize_plan(action, preview, permissions, mutation_policy)
        plan = bulk_plan(action, targets.source_ids, preview=False)
        recorded: list[BulkCheckpoint] = []

        async def sink(item: BulkCheckpoint) -> None:
            recorded.append(item)
            result = checkpoint_sink(item)
            if isawaitable(result):
                await result

        async def execute(item: CatalogId) -> MutationReceipt:
            return await self._bulk_item(action, item.value, permissions, mutation_policy)

        executor = AsyncBulkExecutor(
            execute,
            policy=execution_policy,
            checkpoint_sink=sink,
            checkpoint=checkpoint,
            platform_max_parallelism=_MAX_BULK_PARALLELISM,
            whole_run_budget=whole_run_budget,
            item_budget=item_budget,
            clock=clock,
            cancel_event=cancel_event,
        )
        entries = [entry async for entry in executor.stream(plan)]
        return _bulk_result(action, plan, tuple(entries), recorded, checkpoint)

    async def _bulk_item(
        self, action: str, source_id: str, permissions: Permissions, policy: Policy
    ) -> MutationReceipt:
        """Settle one bulk item into a per-item receipt without raising."""
        credential = await self._client._resolved_credential_async()
        try:
            result = await _mutation_async(
                source_id,
                _item_policy(action, source_id, policy),
                wire.BULK_MUTATIONS[action],
                HARVEST_OPERATION,
                lambda: _BULK_REQUESTS[action](source_id),
                lambda request: _dataset_mutate(
                    self._client._dataset_call_async,
                    credential,
                    permissions,
                    policy,
                    HARVEST_OPERATION,
                    request,
                    admin=True,
                ),
                destructive=action in HARVEST_DESTRUCTIVE_BULK_ACTIONS,
            )
        except ASYNC_SETTLEMENT_ERRORS as error:
            receipt = getattr(error, "mutation_receipt", None)
            if isinstance(receipt, MutationReceipt):
                return receipt
            return _plan_receipt(policy, action, source_id, "failed", 0)
        return result.receipt

    async def _authorize_plan(
        self, action: str, preview: HarvestBulkPreview, permissions: Permissions, policy: Policy
    ) -> None:
        """Refuse an unauthorized bulk plan before any item is dispatched."""
        try:
            _enforce_mutation_policy(
                HARVEST_OPERATION, preview.confirmation_target, policy, destructive=preview.destructive
            )
            _require_mutation_permission(
                await self._client._resolved_credential_async(), HARVEST_OPERATION, permissions, admin=True
            )
        except ASYNC_SETTLEMENT_ERRORS as error:
            _attach(error, _plan_receipt(policy, action, preview.confirmation_target, "rejected", _error_status(error)))
            raise

    async def _observe_job(
        self, handle: UDataJobHandle, permissions: Permissions, monitor: DeadlineMonitor
    ) -> tuple[UDataJobHandle, MappingRecord]:
        """Take one bounded observation of the queued job this handle names."""
        job_id = handle.job_id
        if job_id is None:
            raise _wait_requirement()
        try:
            monitor.assert_dispatchable(HARVEST_OPERATION, PLATFORM)
        except BudgetExhaustedError:
            return handle.abandoned_lease(exhausted=True), MappingRecord({"status": handle.status})
        response = await self._read_admin(wire.get_job_request(job_id), permissions)
        record = _parsed(response, HARVEST_OPERATION, wire.parse_record)
        return handle.observed(wire.parse_job_status(record.payload, HARVEST_OPERATION)), record

    async def _read_admin(self, request: Request, permissions: Permissions) -> Response:
        """Run one bounded harvest read behind explicit admin permission evidence."""
        method, path, headers, body = request
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=HARVEST_OPERATION,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=_require_mutation_permission(
                await self._client._resolved_credential_async(), HARVEST_OPERATION, permissions, admin=True
            ),
            max_response_bytes=_MAX_HARVEST_READ_BYTES,
        )

    async def _mutate_admin(self, request: Request, permissions: Permissions, policy: Policy) -> Response:
        """Run one harvest write behind explicit admin permission evidence."""
        return await _dataset_mutate(
            self._client._dataset_call_async,
            await self._client._resolved_credential_async(),
            permissions,
            policy,
            HARVEST_OPERATION,
            request,
            admin=True,
        )
