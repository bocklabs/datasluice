"""Both-mode CKAN resource-view projections under the optional views v2 id.

Resource views exist only when the deployment registers a view plugin, so the
whole family rides the optional ``resource-views`` id and resolves capability
evidence independently per deployment (the D-06 correction from 03-02). Typed
methods pass documented CKAN 2.11 parameters verbatim (D-04) and view payloads
stay lossless mapping records; mutations return CKANMutationResult through the
shared receipt seam.
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

_GROUP = "views"
_VIEW = ResourceKind("view")


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def _mapping_identity(value: object, *keys: str) -> str:
    if isinstance(value, Mapping):
        for key in keys:
            identity = value.get(key)
            if isinstance(identity, str) and identity:
                return identity
    raise KeyError("the default-view mutation carried no stable object identity")


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action == "package_create_default_resource_views":
        return CatalogId(PLATFORM, ResourceKind.DATASET, _mapping_identity(params["package"], "id", "name"))
    if action == "resource_create_default_resource_views":
        return CatalogId(PLATFORM, ResourceKind.RESOURCE, _mapping_identity(params["resource"], "id"))
    if action in {"resource_view_update", "resource_view_delete"}:
        return CatalogId(PLATFORM, _VIEW, str(params["id"]))
    for key in ("resource_id", "id"):
        if key in params:
            return CatalogId(PLATFORM, ResourceKind.RESOURCE, str(params[key]))
    raise KeyError("the view mutation carried no resource-targeting id")


class SyncViewsService(_SyncNativeService):
    """Synchronous resource-view projection carrying nine typed actions."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, "views")

    def resource_view_create(
        self,
        *,
        resource_id: str,
        view_type: str,
        title: str | None = None,
        description: str | None = None,
        config: dict[str, object] | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create one view configuration on a resource."""
        params: dict[str, object] = {"resource_id": resource_id, "view_type": view_type}
        params.update(_drop_unset({"title": title, "description": description, "config": config}))
        return self._invoke_mutation("resource_view_create", params, policy)

    def resource_view_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one view configuration as a lossless mapping."""
        return self._invoke_read("resource_view_show", {"id": id})

    def resource_view_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List one resource's view configurations as lossless mappings."""
        return self._invoke_read("resource_view_list", {"id": id})

    def resource_view_update(
        self,
        *,
        id: str,
        title: str | None = None,
        description: str | None = None,
        config: dict[str, object] | None = None,
        view_type: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one view configuration on the standard tier."""
        params: dict[str, object] = {"id": id}
        params.update(
            _drop_unset({"title": title, "description": description, "config": config, "view_type": view_type})
        )
        return self._invoke_mutation("resource_view_update", params, policy)

    def resource_view_reorder(
        self, *, id: str, order: list[str], policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Reorder one resource's views by their identifiers."""
        return self._invoke_mutation("resource_view_reorder", {"id": id, "order": order}, policy)

    def resource_view_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one view configuration on the standard tier."""
        return self._invoke_mutation("resource_view_delete", {"id": id}, policy)

    def resource_view_clear(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Clear every view configuration from one resource."""
        return self._invoke_mutation("resource_view_clear", {"id": id}, policy)

    def package_create_default_resource_views(
        self,
        *,
        package: Mapping[str, object],
        create_datastore_views: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create the default views for one package's resources."""
        params = _drop_unset({"package": package, "create_datastore_views": create_datastore_views})
        return self._invoke_mutation("package_create_default_resource_views", params, policy)

    def resource_create_default_resource_views(
        self,
        *,
        resource: Mapping[str, object],
        package: Mapping[str, object] | None = None,
        create_datastore_views: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create the default views for one resource."""
        params = _drop_unset(
            {"resource": resource, "package": package, "create_datastore_views": create_datastore_views}
        )
        return self._invoke_mutation("resource_create_default_resource_views", params, policy)

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


class AsyncViewsService(_AsyncNativeService):
    """Asynchronous resource-view projection carrying nine typed actions."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, "views")

    async def resource_view_create(
        self,
        *,
        resource_id: str,
        view_type: str,
        title: str | None = None,
        description: str | None = None,
        config: dict[str, object] | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create one view configuration on a resource."""
        params: dict[str, object] = {"resource_id": resource_id, "view_type": view_type}
        params.update(_drop_unset({"title": title, "description": description, "config": config}))
        return await self._invoke_mutation("resource_view_create", params, policy)

    async def resource_view_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one view configuration as a lossless mapping."""
        return await self._invoke_read("resource_view_show", {"id": id})

    async def resource_view_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List one resource's view configurations as lossless mappings."""
        return await self._invoke_read("resource_view_list", {"id": id})

    async def resource_view_update(
        self,
        *,
        id: str,
        title: str | None = None,
        description: str | None = None,
        config: dict[str, object] | None = None,
        view_type: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one view configuration on the standard tier."""
        params: dict[str, object] = {"id": id}
        params.update(
            _drop_unset({"title": title, "description": description, "config": config, "view_type": view_type})
        )
        return await self._invoke_mutation("resource_view_update", params, policy)

    async def resource_view_reorder(
        self, *, id: str, order: list[str], policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Reorder one resource's views by their identifiers."""
        return await self._invoke_mutation("resource_view_reorder", {"id": id, "order": order}, policy)

    async def resource_view_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one view configuration on the standard tier."""
        return await self._invoke_mutation("resource_view_delete", {"id": id}, policy)

    async def resource_view_clear(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Clear every view configuration from one resource."""
        return await self._invoke_mutation("resource_view_clear", {"id": id}, policy)

    async def package_create_default_resource_views(
        self,
        *,
        package: Mapping[str, object],
        create_datastore_views: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create the default views for one package's resources."""
        params = _drop_unset({"package": package, "create_datastore_views": create_datastore_views})
        return await self._invoke_mutation("package_create_default_resource_views", params, policy)

    async def resource_create_default_resource_views(
        self,
        *,
        resource: Mapping[str, object],
        package: Mapping[str, object] | None = None,
        create_datastore_views: bool | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create the default views for one resource."""
        params = _drop_unset(
            {"resource": resource, "package": package, "create_datastore_views": create_datastore_views}
        )
        return await self._invoke_mutation("resource_create_default_resource_views", params, policy)

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
