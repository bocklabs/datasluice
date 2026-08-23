"""Both-mode CKAN dataset projections: umbrella dispatch plus the exhaustive typed action surface.

Every typed method declares its owning v2 OperationId from the checked-in manifest,
passes documented CKAN 2.11 parameters verbatim (D-04 faithful paging, no translation),
and keeps officially deprecated parameter names unrepresentable (D-01). Mutating
methods accept an optional ``MutationPolicy`` and return a ``CKANMutationResult``
carrying the decoded result plus a redacted receipt through the shared spine gate.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.ckan.clients import (
    _AsyncDatasetService,
    _operation_id_from,
    _SyncDatasetService,
)
from datasluice.connectors.catalog.ckan.inventory import ActionEntry
from datasluice.connectors.catalog.ckan.mapping import PLATFORM
from datasluice.connectors.catalog.ckan.results import CKANMutationResult, require_mutation_tier
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import DatasetRecord, ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.mutation import build_mutation_receipt

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient

_DATASET_GROUP = "datasets"

type FieldSpecList = list[Mapping[str, object]]
type DatasetNameList = list[str]
type ResourceOrder = list[Mapping[str, object]]

_DEPRECATED_PARAMETERS: Mapping[str, frozenset[str]] = {
    "current_package_list_with_resources": frozenset({"page"}),
}

_ORG_TARGET_ACTIONS = frozenset({"bulk_update_private", "bulk_update_public", "bulk_update_delete"})

_UPDATE_FIELDS = (
    "name",
    "title",
    "notes",
    "url",
    "version",
    "license_id",
    "owner_org",
    "private",
    "author",
    "author_email",
    "maintainer",
    "maintainer_email",
    "tags",
    "extras",
    "groups",
    "resources",
)


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def _reject_deprecated(action: str, payload: Mapping[str, object]) -> None:
    banned = _DEPRECATED_PARAMETERS.get(action)
    if not banned:
        return
    clash = sorted(banned.intersection(payload))
    if clash:
        raise CatalogValidationError(
            f"The parameter(s) {clash} are officially deprecated for {action} and are not accepted.",
            operation=f"{PLATFORM.value}/{action}",
            platform=PLATFORM.value,
            safe_action="Use the canonical CKAN 2.11 pagination parameters; offset replaces page.",
        )


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action in _ORG_TARGET_ACTIONS:
        return CatalogId(PLATFORM, ResourceKind.ORGANIZATION, str(params["org_id"]))
    key = "name" if action == "package_create" else "id"
    return CatalogId(PLATFORM, ResourceKind.DATASET, str(params[key]))


class SyncDatasetsService(_SyncDatasetService):
    """Synchronous dataset projection carrying umbrella plus twenty typed actions."""

    __slots__ = ()

    def get(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[DatasetRecord]:
        """Dispatch a normalized get with deprecation discipline enforced."""
        _reject_deprecated(self._backing("get").name, operation.payload)
        return super().get(operation, guard)

    def list(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[DatasetRecord]:
        """Dispatch a normalized list with deprecation discipline enforced."""
        _reject_deprecated(self._backing("list").name, operation.payload)
        return super().list(operation, guard)

    def list_show_search(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, show, or search datasets with deprecation discipline enforced."""
        self._reject_deprecated_in(operation)
        return super().list_show_search(operation, guard)

    def create_update_patch_delete_purge(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Mutate or purge a dataset with deprecation discipline enforced."""
        self._reject_deprecated_in(operation)
        return super().create_update_patch_delete_purge(operation, guard)

    def package_list(self) -> ResultEnvelope[CKANResultItem]:
        """List dataset names verbatim as the deployment returns them."""
        return self._invoke_read("package_list", {})

    def current_package_list_with_resources(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List datasets with resources using native limit/offset paging."""
        return self._invoke_read("current_package_list_with_resources", _drop_unset({"limit": limit, "offset": offset}))

    def package_show(
        self,
        *,
        id: str,
        use_default_schema: bool | None = None,
        include_tracking: bool | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one dataset by id or name."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"use_default_schema": use_default_schema, "include_tracking": include_tracking}))
        return self._invoke_read("package_show", params)

    def package_search(
        self,
        *,
        q: str | None = None,
        fq: str | None = None,
        rows: int | None = None,
        start: int | None = None,
        sort: str | None = None,
        fl: str | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Search datasets with Solr-style rows/start sent verbatim."""
        return self._invoke_read(
            "package_search", _drop_unset({"q": q, "fq": fq, "rows": rows, "start": start, "sort": sort, "fl": fl})
        )

    def package_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete dataset names or titles."""
        return self._invoke_read("package_autocomplete", _drop_unset({"q": q, "limit": limit}))

    def package_create(
        self,
        *,
        name: str,
        title: str | None = None,
        notes: str | None = None,
        url: str | None = None,
        version: str | None = None,
        license_id: str | None = None,
        owner_org: str | None = None,
        private: bool | None = None,
        author: str | None = None,
        author_email: str | None = None,
        maintainer: str | None = None,
        maintainer_email: str | None = None,
        tags: FieldSpecList | None = None,
        extras: FieldSpecList | None = None,
        groups: FieldSpecList | None = None,
        resources: FieldSpecList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create a dataset from documented keyword fields."""
        params: dict[str, object] = {"name": name}
        params.update(
            _drop_unset(
                {
                    "title": title,
                    "notes": notes,
                    "url": url,
                    "version": version,
                    "license_id": license_id,
                    "owner_org": owner_org,
                    "private": private,
                    "author": author,
                    "author_email": author_email,
                    "maintainer": maintainer,
                    "maintainer_email": maintainer_email,
                    "tags": tags,
                    "extras": extras,
                    "groups": groups,
                    "resources": resources,
                }
            )
        )
        return self._invoke_mutation("package_create", params, policy)

    def package_update(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        notes: str | None = None,
        url: str | None = None,
        version: str | None = None,
        license_id: str | None = None,
        owner_org: str | None = None,
        private: bool | None = None,
        author: str | None = None,
        author_email: str | None = None,
        maintainer: str | None = None,
        maintainer_email: str | None = None,
        tags: FieldSpecList | None = None,
        extras: FieldSpecList | None = None,
        groups: FieldSpecList | None = None,
        resources: FieldSpecList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update a dataset from documented keyword fields."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unused_update_fields(locals()))
        return self._invoke_mutation("package_update", params, policy)

    def package_patch(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        notes: str | None = None,
        url: str | None = None,
        version: str | None = None,
        license_id: str | None = None,
        owner_org: str | None = None,
        private: bool | None = None,
        author: str | None = None,
        author_email: str | None = None,
        maintainer: str | None = None,
        maintainer_email: str | None = None,
        tags: FieldSpecList | None = None,
        extras: FieldSpecList | None = None,
        groups: FieldSpecList | None = None,
        resources: FieldSpecList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Patch selected dataset fields without replacing the package."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unused_update_fields(locals()))
        return self._invoke_mutation("package_patch", params, policy)

    def package_revise(
        self,
        *,
        match: Mapping[str, object] | None = None,
        update: Mapping[str, object] | None = None,
        include: bool | None = None,
        filter: Mapping[str, object] | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Revise datasets matched declaratively per the documented revise grammar."""
        params = _drop_unset({"match": match, "update": update, "include": include, "filter": filter})
        return self._invoke_mutation("package_revise", params, policy)

    def package_resource_reorder(
        self, *, id: str, resources: ResourceOrder, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Reorder the resources of one dataset."""
        return self._invoke_mutation("package_resource_reorder", {"id": id, "resources": resources}, policy)

    def package_owner_org_update(
        self, *, id: str, organization_id: str, force: bool | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Move one dataset to another owning organization."""
        params: dict[str, object] = {"id": id, "organization_id": organization_id}
        params.update(_drop_unset({"force": force}))
        return self._invoke_mutation("package_owner_org_update", params, policy)

    def package_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Soft-delete one dataset to state=deleted on the standard tier (D-09)."""
        return self._invoke_mutation("package_delete", {"id": id}, policy)

    def dataset_purge(self, *, id: str, policy: MutationPolicy) -> CKANMutationResult:
        """Purge one dataset irreversibly on the destructive tier (D-09)."""
        return self._invoke_mutation("dataset_purge", {"id": id}, policy)

    def bulk_update_private(
        self, *, datasets: DatasetNameList, org_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Mark the listed datasets private within one organization."""
        return self._invoke_mutation("bulk_update_private", {"datasets": datasets, "org_id": org_id}, policy)

    def bulk_update_public(
        self, *, datasets: DatasetNameList, org_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Mark the listed datasets public within one organization."""
        return self._invoke_mutation("bulk_update_public", {"datasets": datasets, "org_id": org_id}, policy)

    def bulk_update_delete(
        self, *, datasets: DatasetNameList, org_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Bulk soft-delete the listed datasets within one organization."""
        return self._invoke_mutation("bulk_update_delete", {"datasets": datasets, "org_id": org_id}, policy)

    def package_collaborator_create(
        self, *, id: str, user_id: str, capacity: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Grant one user a collaborator capacity on a dataset."""
        params: dict[str, object] = {"id": id, "user_id": user_id, "capacity": capacity}
        return self._invoke_mutation("package_collaborator_create", params, policy)

    def package_collaborator_delete(
        self, *, id: str, user_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one collaborator from a dataset."""
        return self._invoke_mutation("package_collaborator_delete", {"id": id, "user_id": user_id}, policy)

    def package_collaborator_list(self, *, id: str, capacity: str | None = None) -> ResultEnvelope[CKANResultItem]:
        """List collaborators of one dataset."""
        return self._invoke_read("package_collaborator_list", _drop_unset({"id": id, "capacity": capacity}))

    def package_collaborator_list_for_user(
        self, *, user_id: str, capacity: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List dataset collaborations of one user."""
        return self._invoke_read(
            "package_collaborator_list_for_user", _drop_unset({"user_id": user_id, "capacity": capacity})
        )

    def _reject_deprecated_in(self, operation: CatalogOperationRequest) -> None:
        action = operation.payload.get("action")
        if isinstance(action, str):
            _reject_deprecated(action, operation.payload)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _DATASET_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_DATASET_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        _reject_deprecated(action, params)
        client: SyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry))

    def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        _reject_deprecated(action, params)
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


class AsyncDatasetsService(_AsyncDatasetService):
    """Asynchronous dataset projection carrying umbrella plus twenty typed actions."""

    __slots__ = ()

    async def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[DatasetRecord]:
        """Dispatch a normalized get with deprecation discipline enforced."""
        _reject_deprecated(self._backing("get").name, operation.payload)
        return await super().get(operation, guard)

    async def list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[DatasetRecord]:
        """Dispatch a normalized list with deprecation discipline enforced."""
        _reject_deprecated(self._backing("list").name, operation.payload)
        return await super().list(operation, guard)

    async def list_show_search(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, show, or search datasets with deprecation discipline enforced."""
        self._reject_deprecated_in(operation)
        return await super().list_show_search(operation, guard)

    async def create_update_patch_delete_purge(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Mutate or purge a dataset with deprecation discipline enforced."""
        self._reject_deprecated_in(operation)
        return await super().create_update_patch_delete_purge(operation, guard)

    async def package_list(self) -> ResultEnvelope[CKANResultItem]:
        """List dataset names verbatim as the deployment returns them."""
        return await self._invoke_read("package_list", {})

    async def current_package_list_with_resources(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List datasets with resources using native limit/offset paging."""
        return await self._invoke_read(
            "current_package_list_with_resources", _drop_unset({"limit": limit, "offset": offset})
        )

    async def package_show(
        self,
        *,
        id: str,
        use_default_schema: bool | None = None,
        include_tracking: bool | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one dataset by id or name."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"use_default_schema": use_default_schema, "include_tracking": include_tracking}))
        return await self._invoke_read("package_show", params)

    async def package_search(
        self,
        *,
        q: str | None = None,
        fq: str | None = None,
        rows: int | None = None,
        start: int | None = None,
        sort: str | None = None,
        fl: str | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Search datasets with Solr-style rows/start sent verbatim."""
        return await self._invoke_read(
            "package_search", _drop_unset({"q": q, "fq": fq, "rows": rows, "start": start, "sort": sort, "fl": fl})
        )

    async def package_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete dataset names or titles."""
        return await self._invoke_read("package_autocomplete", _drop_unset({"q": q, "limit": limit}))

    async def package_create(
        self,
        *,
        name: str,
        title: str | None = None,
        notes: str | None = None,
        url: str | None = None,
        version: str | None = None,
        license_id: str | None = None,
        owner_org: str | None = None,
        private: bool | None = None,
        author: str | None = None,
        author_email: str | None = None,
        maintainer: str | None = None,
        maintainer_email: str | None = None,
        tags: FieldSpecList | None = None,
        extras: FieldSpecList | None = None,
        groups: FieldSpecList | None = None,
        resources: FieldSpecList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create a dataset from documented keyword fields."""
        params: dict[str, object] = {"name": name}
        params.update(
            _drop_unset(
                {
                    "title": title,
                    "notes": notes,
                    "url": url,
                    "version": version,
                    "license_id": license_id,
                    "owner_org": owner_org,
                    "private": private,
                    "author": author,
                    "author_email": author_email,
                    "maintainer": maintainer,
                    "maintainer_email": maintainer_email,
                    "tags": tags,
                    "extras": extras,
                    "groups": groups,
                    "resources": resources,
                }
            )
        )
        return await self._invoke_mutation("package_create", params, policy)

    async def package_update(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        notes: str | None = None,
        url: str | None = None,
        version: str | None = None,
        license_id: str | None = None,
        owner_org: str | None = None,
        private: bool | None = None,
        author: str | None = None,
        author_email: str | None = None,
        maintainer: str | None = None,
        maintainer_email: str | None = None,
        tags: FieldSpecList | None = None,
        extras: FieldSpecList | None = None,
        groups: FieldSpecList | None = None,
        resources: FieldSpecList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update a dataset from documented keyword fields."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unused_update_fields(locals()))
        return await self._invoke_mutation("package_update", params, policy)

    async def package_patch(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        notes: str | None = None,
        url: str | None = None,
        version: str | None = None,
        license_id: str | None = None,
        owner_org: str | None = None,
        private: bool | None = None,
        author: str | None = None,
        author_email: str | None = None,
        maintainer: str | None = None,
        maintainer_email: str | None = None,
        tags: FieldSpecList | None = None,
        extras: FieldSpecList | None = None,
        groups: FieldSpecList | None = None,
        resources: FieldSpecList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Patch selected dataset fields without replacing the package."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unused_update_fields(locals()))
        return await self._invoke_mutation("package_patch", params, policy)

    async def package_revise(
        self,
        *,
        match: Mapping[str, object] | None = None,
        update: Mapping[str, object] | None = None,
        include: bool | None = None,
        filter: Mapping[str, object] | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Revise datasets matched declaratively per the documented revise grammar."""
        params = _drop_unset({"match": match, "update": update, "include": include, "filter": filter})
        return await self._invoke_mutation("package_revise", params, policy)

    async def package_resource_reorder(
        self, *, id: str, resources: ResourceOrder, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Reorder the resources of one dataset."""
        return await self._invoke_mutation("package_resource_reorder", {"id": id, "resources": resources}, policy)

    async def package_owner_org_update(
        self, *, id: str, organization_id: str, force: bool | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Move one dataset to another owning organization."""
        params: dict[str, object] = {"id": id, "organization_id": organization_id}
        params.update(_drop_unset({"force": force}))
        return await self._invoke_mutation("package_owner_org_update", params, policy)

    async def package_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Soft-delete one dataset to state=deleted on the standard tier (D-09)."""
        return await self._invoke_mutation("package_delete", {"id": id}, policy)

    async def dataset_purge(self, *, id: str, policy: MutationPolicy) -> CKANMutationResult:
        """Purge one dataset irreversibly on the destructive tier (D-09)."""
        return await self._invoke_mutation("dataset_purge", {"id": id}, policy)

    async def bulk_update_private(
        self, *, datasets: DatasetNameList, org_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Mark the listed datasets private within one organization."""
        return await self._invoke_mutation("bulk_update_private", {"datasets": datasets, "org_id": org_id}, policy)

    async def bulk_update_public(
        self, *, datasets: DatasetNameList, org_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Mark the listed datasets public within one organization."""
        return await self._invoke_mutation("bulk_update_public", {"datasets": datasets, "org_id": org_id}, policy)

    async def bulk_update_delete(
        self, *, datasets: DatasetNameList, org_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Bulk soft-delete the listed datasets within one organization."""
        return await self._invoke_mutation("bulk_update_delete", {"datasets": datasets, "org_id": org_id}, policy)

    async def package_collaborator_create(
        self, *, id: str, user_id: str, capacity: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Grant one user a collaborator capacity on a dataset."""
        params: dict[str, object] = {"id": id, "user_id": user_id, "capacity": capacity}
        return await self._invoke_mutation("package_collaborator_create", params, policy)

    async def package_collaborator_delete(
        self, *, id: str, user_id: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one collaborator from a dataset."""
        return await self._invoke_mutation("package_collaborator_delete", {"id": id, "user_id": user_id}, policy)

    async def package_collaborator_list(
        self, *, id: str, capacity: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List collaborators of one dataset."""
        return await self._invoke_read("package_collaborator_list", _drop_unset({"id": id, "capacity": capacity}))

    async def package_collaborator_list_for_user(
        self, *, user_id: str, capacity: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List dataset collaborations of one user."""
        return await self._invoke_read(
            "package_collaborator_list_for_user", _drop_unset({"user_id": user_id, "capacity": capacity})
        )

    def _reject_deprecated_in(self, operation: CatalogOperationRequest) -> None:
        action = operation.payload.get("action")
        if isinstance(action, str):
            _reject_deprecated(action, operation.payload)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _DATASET_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_DATASET_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        _reject_deprecated(action, params)
        client: AsyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry))

    async def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        _reject_deprecated(action, params)
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


def _drop_unused_update_fields(local_vars: Mapping[str, object]) -> dict[str, object]:
    fields = {key: value for key, value in local_vars.items() if key in _UPDATE_FIELDS}
    return _drop_unset(fields)
