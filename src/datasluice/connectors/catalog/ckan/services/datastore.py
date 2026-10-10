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

from typing import TYPE_CHECKING

from datasluice.connectors.catalog.ckan.clients import (
    _async_typed_mutation,
    _async_typed_read,
    _AsyncNativeService,
    _sync_typed_mutation,
    _sync_typed_read,
    _SyncNativeService,
)
from datasluice.connectors.catalog.ckan.mapping import PLATFORM
from datasluice.connectors.catalog.ckan.services._shared import WireParams, wire_params
from datasluice.domain.catalog.ids import CatalogId, ResourceKind

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient
    from datasluice.connectors.catalog.ckan.results import CKANMutationResult
    from datasluice.contracts.catalog.native.ckan import CKANResultItem
    from datasluice.domain.catalog.models import ResultEnvelope
    from datasluice.domain.catalog.safety import MutationPolicy

_GROUP = "datastore"


def _mutation_target(_action: str, params: Mapping[str, object]) -> CatalogId:
    key = "resource_id" if "resource_id" in params else "name"
    return CatalogId(PLATFORM, ResourceKind.RESOURCE, str(params[key]))


def _search_params(
    resource_id: str,
    q: str | None,
    plain: bool | None,
    language: str | None,
    limit: int | None,
    offset: int | None,
    fields: list[str] | None,
    sort: str | None,
    filters: dict[str, object] | None,
    distinct: bool | None,
    include_total: bool | None,
    records_format: str | None,
) -> WireParams:
    return wire_params(
        {"resource_id": resource_id},
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
        },
    )


def _create_params(
    resource_id: str,
    fields: list[dict[str, object]] | None,
    records: list[dict[str, object]] | None,
    primary_key: list[str] | None,
    indexes: list[str] | None,
    aliases: list[str] | None,
    triggers: list[str] | None,
) -> WireParams:
    return wire_params(
        {"resource_id": resource_id},
        {
            "fields": fields,
            "records": records,
            "primary_key": primary_key,
            "indexes": indexes,
            "aliases": aliases,
            "triggers": triggers,
        },
    )


def _upsert_params(
    resource_id: str,
    records: list[dict[str, object]],
    method: str,
    dry_run: bool | None,
    calculate_record_id: bool | None,
    force: bool | None,
) -> WireParams:
    return wire_params(
        {"resource_id": resource_id, "records": records, "method": method},
        {"dry_run": dry_run, "calculate_record_id": calculate_record_id, "force": force},
    )


def _function_create_params(
    name: str,
    description: str | None,
    language: str | None,
    handler: str | None,
    source: str | None,
    or_replace: bool | None,
    return_type: str | None,
) -> WireParams:
    return wire_params(
        {"name": name},
        {
            "description": description,
            "language": language,
            "handler": handler,
            "source": source,
            "or_replace": or_replace,
            "return_type": return_type,
        },
    )


class SyncDatastoreService(_SyncNativeService):
    """Synchronous datastore projection carrying ten typed actions."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, _GROUP)

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
        return self._invoke_read(
            "datastore_search",
            _search_params(
                resource_id,
                q,
                plain,
                language,
                limit,
                offset,
                fields,
                sort,
                filters,
                distinct,
                include_total,
                records_format,
            ),
        )

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
        return self._invoke_mutation(
            "datastore_create",
            _create_params(resource_id, fields, records, primary_key, indexes, aliases, triggers),
            policy,
        )

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
        return self._invoke_mutation(
            "datastore_upsert",
            _upsert_params(resource_id, records, method, dry_run, calculate_record_id, force),
            policy,
        )

    def datastore_delete(
        self,
        *,
        resource_id: str,
        filters: dict[str, object] | None = None,
        force: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Drop or filter-truncate one datastore table on the destructive tier."""
        return self._invoke_mutation(
            "datastore_delete", wire_params({"resource_id": resource_id}, {"filters": filters, "force": force}), policy
        )

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
        return self._invoke_mutation(
            "datastore_records_delete",
            wire_params({"resource_id": resource_id, "filters": filters}, {"force": force, "dry_run": dry_run}),
            policy,
        )

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
        return self._invoke_mutation(
            "datastore_function_create",
            _function_create_params(name, description, language, handler, source, or_replace, return_type),
            policy,
        )

    def datastore_function_delete(
        self, *, name: str, force: bool | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete one datastore function definition."""
        return self._invoke_mutation("datastore_function_delete", wire_params({"name": name}, {"force": force}), policy)

    def datastore_run_triggers(self, *, resource_id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Run one resource's datastore triggers explicitly."""
        return self._invoke_mutation("datastore_run_triggers", {"resource_id": resource_id}, policy)

    def datastore_search_sql(self, *, sql: str) -> ResultEnvelope[CKANResultItem]:
        """Execute one SQL query under the deployment-gated sql-search id (D-02)."""
        return self._invoke_read("datastore_search_sql", {"sql": sql})

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        return _sync_typed_read(self._client, _GROUP, action, params)

    def _invoke_mutation(self, action: str, params: WireParams, policy: MutationPolicy | None) -> CKANMutationResult:
        return _sync_typed_mutation(self._client, _GROUP, action, params, policy, _mutation_target)


class AsyncDatastoreService(_AsyncNativeService):
    """Asynchronous datastore projection carrying ten typed actions."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, _GROUP)

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
        return await self._invoke_read(
            "datastore_search",
            _search_params(
                resource_id,
                q,
                plain,
                language,
                limit,
                offset,
                fields,
                sort,
                filters,
                distinct,
                include_total,
                records_format,
            ),
        )

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
        return await self._invoke_mutation(
            "datastore_create",
            _create_params(resource_id, fields, records, primary_key, indexes, aliases, triggers),
            policy,
        )

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
        return await self._invoke_mutation(
            "datastore_upsert",
            _upsert_params(resource_id, records, method, dry_run, calculate_record_id, force),
            policy,
        )

    async def datastore_delete(
        self,
        *,
        resource_id: str,
        filters: dict[str, object] | None = None,
        force: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Drop or filter-truncate one datastore table on the destructive tier."""
        return await self._invoke_mutation(
            "datastore_delete", wire_params({"resource_id": resource_id}, {"filters": filters, "force": force}), policy
        )

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
        return await self._invoke_mutation(
            "datastore_records_delete",
            wire_params({"resource_id": resource_id, "filters": filters}, {"force": force, "dry_run": dry_run}),
            policy,
        )

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
        return await self._invoke_mutation(
            "datastore_function_create",
            _function_create_params(name, description, language, handler, source, or_replace, return_type),
            policy,
        )

    async def datastore_function_delete(
        self, *, name: str, force: bool | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete one datastore function definition."""
        return await self._invoke_mutation(
            "datastore_function_delete", wire_params({"name": name}, {"force": force}), policy
        )

    async def datastore_run_triggers(
        self, *, resource_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Run one resource's datastore triggers explicitly."""
        return await self._invoke_mutation("datastore_run_triggers", {"resource_id": resource_id}, policy)

    async def datastore_search_sql(self, *, sql: str) -> ResultEnvelope[CKANResultItem]:
        """Execute one SQL query under the deployment-gated sql-search id (D-02)."""
        return await self._invoke_read("datastore_search_sql", {"sql": sql})

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        return await _async_typed_read(self._client, _GROUP, action, params)

    async def _invoke_mutation(
        self, action: str, params: WireParams, policy: MutationPolicy | None
    ) -> CKANMutationResult:
        return await _async_typed_mutation(self._client, _GROUP, action, params, policy, _mutation_target)
