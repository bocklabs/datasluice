"""Both-mode CKAN background-job, task-status, and config-option projections.

Background jobs and task status ride the core ``jobs-and-task-status`` id
while the sysadmin-only config options own their admin ``config-options`` id
with server authorization responses as the runtime evidence (D-09):
forbidden envelopes map to ForbiddenError, never synthesized privilege.
``job_clear`` and ``task_status_delete`` are destructive-family and refuse
pre-dispatch without a confirmed destructive policy through the single 03-03
gate. ``config_option_update`` passes its key-value mapping verbatim so no
server-side option name or value is translated or dropped.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.ckan.clients import (
    _AsyncNativeService,
    _operation_id_from,
    _SyncNativeService,
)
from datasluice.connectors.catalog.ckan.inventory import ActionEntry
from datasluice.connectors.catalog.ckan.mapping import JOB, PLATFORM, TASK
from datasluice.connectors.catalog.ckan.results import CKANMutationResult, require_mutation_tier
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.mutation import build_mutation_receipt

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient

_GROUP = "extensions"
_CONFIG_OPTION = ResourceKind("config-option")


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action == "job_clear":
        return CatalogId(PLATFORM, JOB, "background-jobs")
    if action.startswith("job_"):
        return CatalogId(PLATFORM, JOB, str(params["id"]))
    if action.startswith("config_option"):
        return CatalogId(PLATFORM, _CONFIG_OPTION, "site-config")
    return CatalogId(PLATFORM, TASK, str(params["id"] if "id" in params else params.get("entity_id", "task")))


class SyncExtensionsService(_SyncNativeService):
    """Synchronous extension projection carrying eleven typed actions."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, "extensions")

    def job_list(self, *, queues: list[str] | None = None) -> ResultEnvelope[CKANResultItem]:
        """List registered background jobs as job-kind records."""
        return self._invoke_read("job_list", _drop_unset({"queues": queues}))

    def job_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one background job by its identifier."""
        return self._invoke_read("job_show", {"id": id})

    def job_cancel(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Cancel one queued background job on the standard tier."""
        return self._invoke_mutation("job_cancel", {"id": id}, policy)

    def job_clear(self, *, queues: list[str] | None = None, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Clear queued background jobs on the destructive tier."""
        params: dict[str, object] = {}
        params.update(_drop_unset({"queues": queues}))
        return self._invoke_mutation("job_clear", params, policy)

    def task_status_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one task-status record by its identifier."""
        return self._invoke_read("task_status_show", {"id": id})

    def task_status_update(
        self,
        *,
        entity_id: str,
        task_type: str,
        key: str,
        value: str,
        state: str | None = None,
        error: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one task-status record on the standard tier."""
        params: dict[str, object] = {"entity_id": entity_id, "task_type": task_type, "key": key, "value": value}
        params.update(_drop_unset({"state": state, "error": error}))
        return self._invoke_mutation("task_status_update", params, policy)

    def task_status_update_many(
        self, *, data: list[dict[str, object]], policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update many task-status records from one verbatim batch."""
        return self._invoke_mutation("task_status_update_many", {"data": data}, policy)

    def task_status_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one task-status record on the destructive tier."""
        return self._invoke_mutation("task_status_delete", {"id": id}, policy)

    def config_option_show(self, *, key: str) -> ResultEnvelope[CKANResultItem]:
        """Show one config option's value under the admin id."""
        return self._invoke_read("config_option_show", {"key": key})

    def config_option_list(self) -> ResultEnvelope[CKANResultItem]:
        """List settable config option keys under the admin id."""
        return self._invoke_read("config_option_list", {})

    def config_option_update(
        self, *, values: Mapping[str, object], policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update config options from one verbatim key-value mapping (D-09)."""
        return self._invoke_mutation("config_option_update", dict(values), policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        client: SyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry))

    def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        client: SyncCKANClient = self._client
        owning_id = _operation_id_from(entry.owning_operation_id)
        effective = require_mutation_tier(entry.mutation_class, owning_id, policy)
        assert effective is not None
        operation = CatalogOperationRequest(operation_id=owning_id, payload=params, mutation_policy=effective)
        guard = CatalogOperationGuard(operation_id=owning_id, profile=client._profile)
        envelope = cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry))
        receipt = build_mutation_receipt(
            owning_id, _mutation_target(entry.name, params), effective, "succeeded", {"action": entry.name}
        )
        return CKANMutationResult(result=envelope, receipt=receipt)


class AsyncExtensionsService(_AsyncNativeService):
    """Asynchronous extension projection carrying eleven typed actions."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, "extensions")

    async def job_list(self, *, queues: list[str] | None = None) -> ResultEnvelope[CKANResultItem]:
        """List registered background jobs as job-kind records."""
        return await self._invoke_read("job_list", _drop_unset({"queues": queues}))

    async def job_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one background job by its identifier."""
        return await self._invoke_read("job_show", {"id": id})

    async def job_cancel(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Cancel one queued background job on the standard tier."""
        return await self._invoke_mutation("job_cancel", {"id": id}, policy)

    async def job_clear(
        self, *, queues: list[str] | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Clear queued background jobs on the destructive tier."""
        params: dict[str, object] = {}
        params.update(_drop_unset({"queues": queues}))
        return await self._invoke_mutation("job_clear", params, policy)

    async def task_status_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one task-status record by its identifier."""
        return await self._invoke_read("task_status_show", {"id": id})

    async def task_status_update(
        self,
        *,
        entity_id: str,
        task_type: str,
        key: str,
        value: str,
        state: str | None = None,
        error: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one task-status record on the standard tier."""
        params: dict[str, object] = {"entity_id": entity_id, "task_type": task_type, "key": key, "value": value}
        params.update(_drop_unset({"state": state, "error": error}))
        return await self._invoke_mutation("task_status_update", params, policy)

    async def task_status_update_many(
        self, *, data: list[dict[str, object]], policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update many task-status records from one verbatim batch."""
        return await self._invoke_mutation("task_status_update_many", {"data": data}, policy)

    async def task_status_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one task-status record on the destructive tier."""
        return await self._invoke_mutation("task_status_delete", {"id": id}, policy)

    async def config_option_show(self, *, key: str) -> ResultEnvelope[CKANResultItem]:
        """Show one config option's value under the admin id."""
        return await self._invoke_read("config_option_show", {"key": key})

    async def config_option_list(self) -> ResultEnvelope[CKANResultItem]:
        """List settable config option keys under the admin id."""
        return await self._invoke_read("config_option_list", {})

    async def config_option_update(
        self, *, values: Mapping[str, object], policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update config options from one verbatim key-value mapping (D-09)."""
        return await self._invoke_mutation("config_option_update", dict(values), policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        client: AsyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry))

    async def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        client: AsyncCKANClient = self._client
        owning_id = _operation_id_from(entry.owning_operation_id)
        effective = require_mutation_tier(entry.mutation_class, owning_id, policy)
        assert effective is not None
        operation = CatalogOperationRequest(operation_id=owning_id, payload=params, mutation_policy=effective)
        guard = CatalogOperationGuard(operation_id=owning_id, profile=client._profile)
        envelope = cast(ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry))
        receipt = build_mutation_receipt(
            owning_id, _mutation_target(entry.name, params), effective, "succeeded", {"action": entry.name}
        )
        return CKANMutationResult(result=envelope, receipt=receipt)
