"""Both-mode CKAN datastore projections across the split datastore v2 ids.

Nine query/record-crud actions ride the optional ``query-and-record-crud`` id
while ``datastore_search_sql`` owns its own ``sql-search`` id because the
server-side sqlsearch gate disables it by default (D-02): a not-found wire
envelope on that id classifies DEPLOYMENT_DISABLED, never disabling ordinary
datastore work. ``datastore_delete`` is destructive-tier — it drops the whole
table unless filters engage — and refuses pre-dispatch without a confirmed
destructive policy through the single 03-03 gate; ``datastore_records_delete``
is record-scoped and never engages it. Query parameters (Solr and datastore
dialects alike) flow verbatim per D-04.
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
from datasluice.connectors.catalog.ckan.mapping import PLATFORM
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

_GROUP = "datastore"


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    key = "resource_id" if "resource_id" in params else "name"
    return CatalogId(PLATFORM, ResourceKind.RESOURCE, str(params[key]))


class SyncDatastoreService(_SyncNativeService):
    """Synchronous datastore projection carrying ten typed actions."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, "datastore")

    def datastore_search(
        self,
        *,
        resource_id: str,
        q: str | None = None,
        plain: bool | None = None,
        language: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        fields: list[str] | None = None,
        sort: str | None = None,
        filters: dict[str, object] | None = None,
        distinct: bool | None = None,
        include_total: bool | None = None,
        records_format: str | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Search datastore records with query parameters flowing verbatim."""
        params: dict[str, object] = {"resource_id": resource_id}
        params.update(
            _drop_unset(
                {
                    "q": q,
                    "plain": plain,
                    "language": language,
                    "limit": limit,
                    "offset": offset,
                    "fields": fields,
                    "sort": sort,
                    "filters": filters,
                    "distinct": distinct,
                    "include_total": include_total,
                    "records_format": records_format,
                }
            )
        )
        return self._invoke_read("datastore_search", params)

    def datastore_info(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one resource's datastore schema metadata."""
        return self._invoke_read("datastore_info", {"id": id})

    def datastore_create(
        self,
        *,
        resource_id: str,
        fields: list[dict[str, object]] | None = None,
        records: list[dict[str, object]] | None = None,
        primary_key: list[str] | None = None,
        indexes: list[str] | None = None,
        aliases: list[str] | None = None,
        triggers: list[str] | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Initialize or replace one resource's datastore table."""
        params: dict[str, object] = {"resource_id": resource_id}
        params.update(
            _drop_unset(
                {
                    "fields": fields,
                    "records": records,
                    "primary_key": primary_key,
                    "indexes": indexes,
                    "aliases": aliases,
                    "triggers": triggers,
                }
            )
        )
        return self._invoke_mutation("datastore_create", params, policy)

    def datastore_upsert(
        self,
        *,
        resource_id: str,
        records: list[dict[str, object]],
        method: str,
        dry_run: bool | None = None,
        calculate_record_id: bool | None = None,
        force: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Upsert datastore records with the documented method verb verbatim."""
        params: dict[str, object] = {"resource_id": resource_id, "records": records, "method": method}
        params.update(_drop_unset({"dry_run": dry_run, "calculate_record_id": calculate_record_id, "force": force}))
        return self._invoke_mutation("datastore_upsert", params, policy)

    def datastore_delete(
        self,
        *,
        resource_id: str,
        filters: dict[str, object] | None = None,
        force: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Drop or filter-truncate one datastore table on the destructive tier."""
        params: dict[str, object] = {"resource_id": resource_id}
        params.update(_drop_unset({"filters": filters, "force": force}))
        return self._invoke_mutation("datastore_delete", params, policy)

    def datastore_records_delete(
        self,
        *,
        resource_id: str,
        filters: dict[str, object],
        force: bool | None = None,
        dry_run: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Delete matching datastore records only; never drops the table."""
        params: dict[str, object] = {"resource_id": resource_id, "filters": filters}
        params.update(_drop_unset({"force": force, "dry_run": dry_run}))
        return self._invoke_mutation("datastore_records_delete", params, policy)

    def datastore_function_create(
        self,
        *,
        name: str,
        description: str | None = None,
        language: str | None = None,
        handler: str | None = None,
        source: str | None = None,
        or_replace: bool | None = None,
        return_type: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create one datastore function definition."""
        params: dict[str, object] = {"name": name}
        params.update(
            _drop_unset(
                {
                    "description": description,
                    "language": language,
                    "handler": handler,
                    "source": source,
                    "or_replace": or_replace,
                    "return_type": return_type,
                }
            )
        )
        return self._invoke_mutation("datastore_function_create", params, policy)

    def datastore_function_delete(
        self, *, name: str, force: bool | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete one datastore function definition."""
        params: dict[str, object] = {"name": name}
        params.update(_drop_unset({"force": force}))
        return self._invoke_mutation("datastore_function_delete", params, policy)

    def datastore_run_triggers(self, *, resource_id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Run one resource's datastore triggers explicitly."""
        return self._invoke_mutation("datastore_run_triggers", {"resource_id": resource_id}, policy)

    def datastore_search_sql(self, *, sql: str) -> ResultEnvelope[CKANResultItem]:
        """Execute one SQL query under the deployment-gated sql-search id (D-02)."""
        return self._invoke_read("datastore_search_sql", {"sql": sql})

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


class AsyncDatastoreService(_AsyncNativeService):
    """Asynchronous datastore projection carrying ten typed actions."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, "datastore")

    async def datastore_search(
        self,
        *,
        resource_id: str,
        q: str | None = None,
        plain: bool | None = None,
        language: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        fields: list[str] | None = None,
        sort: str | None = None,
        filters: dict[str, object] | None = None,
        distinct: bool | None = None,
        include_total: bool | None = None,
        records_format: str | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Search datastore records with query parameters flowing verbatim."""
        params: dict[str, object] = {"resource_id": resource_id}
        params.update(
            _drop_unset(
                {
                    "q": q,
                    "plain": plain,
                    "language": language,
                    "limit": limit,
                    "offset": offset,
                    "fields": fields,
                    "sort": sort,
                    "filters": filters,
                    "distinct": distinct,
                    "include_total": include_total,
                    "records_format": records_format,
                }
            )
        )
        return await self._invoke_read("datastore_search", params)

    async def datastore_info(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one resource's datastore schema metadata."""
        return await self._invoke_read("datastore_info", {"id": id})

    async def datastore_create(
        self,
        *,
        resource_id: str,
        fields: list[dict[str, object]] | None = None,
        records: list[dict[str, object]] | None = None,
        primary_key: list[str] | None = None,
        indexes: list[str] | None = None,
        aliases: list[str] | None = None,
        triggers: list[str] | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Initialize or replace one resource's datastore table."""
        params: dict[str, object] = {"resource_id": resource_id}
        params.update(
            _drop_unset(
                {
                    "fields": fields,
                    "records": records,
                    "primary_key": primary_key,
                    "indexes": indexes,
                    "aliases": aliases,
                    "triggers": triggers,
                }
            )
        )
        return await self._invoke_mutation("datastore_create", params, policy)

    async def datastore_upsert(
        self,
        *,
        resource_id: str,
        records: list[dict[str, object]],
        method: str,
        dry_run: bool | None = None,
        calculate_record_id: bool | None = None,
        force: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Upsert datastore records with the documented method verb verbatim."""
        params: dict[str, object] = {"resource_id": resource_id, "records": records, "method": method}
        params.update(_drop_unset({"dry_run": dry_run, "calculate_record_id": calculate_record_id, "force": force}))
        return await self._invoke_mutation("datastore_upsert", params, policy)

    async def datastore_delete(
        self,
        *,
        resource_id: str,
        filters: dict[str, object] | None = None,
        force: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Drop or filter-truncate one datastore table on the destructive tier."""
        params: dict[str, object] = {"resource_id": resource_id}
        params.update(_drop_unset({"filters": filters, "force": force}))
        return await self._invoke_mutation("datastore_delete", params, policy)

    async def datastore_records_delete(
        self,
        *,
        resource_id: str,
        filters: dict[str, object],
        force: bool | None = None,
        dry_run: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Delete matching datastore records only; never drops the table."""
        params: dict[str, object] = {"resource_id": resource_id, "filters": filters}
        params.update(_drop_unset({"force": force, "dry_run": dry_run}))
        return await self._invoke_mutation("datastore_records_delete", params, policy)

    async def datastore_function_create(
        self,
        *,
        name: str,
        description: str | None = None,
        language: str | None = None,
        handler: str | None = None,
        source: str | None = None,
        or_replace: bool | None = None,
        return_type: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create one datastore function definition."""
        params: dict[str, object] = {"name": name}
        params.update(
            _drop_unset(
                {
                    "description": description,
                    "language": language,
                    "handler": handler,
                    "source": source,
                    "or_replace": or_replace,
                    "return_type": return_type,
                }
            )
        )
        return await self._invoke_mutation("datastore_function_create", params, policy)

    async def datastore_function_delete(
        self, *, name: str, force: bool | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete one datastore function definition."""
        params: dict[str, object] = {"name": name}
        params.update(_drop_unset({"force": force}))
        return await self._invoke_mutation("datastore_function_delete", params, policy)

    async def datastore_run_triggers(
        self, *, resource_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Run one resource's datastore triggers explicitly."""
        return await self._invoke_mutation("datastore_run_triggers", {"resource_id": resource_id}, policy)

    async def datastore_search_sql(self, *, sql: str) -> ResultEnvelope[CKANResultItem]:
        """Execute one SQL query under the deployment-gated sql-search id (D-02)."""
        return await self._invoke_read("datastore_search_sql", {"sql": sql})

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
