"""Both-mode CKAN organization projections: exhaustive org and membership actions.

Every typed method declares its owning v2 OperationId from the checked-in manifest
and passes documented CKAN 2.11 parameters verbatim (D-04). Admin-tier honesty is
structural, not probed: privileged writes dispatch on the declared profile and the
SERVER's authorization responses are the runtime evidence. ``organization_purge``
refuses pre-dispatch without a confirmed destructive policy through the shared
03-03 gate and returns a ``CKANMutationResult`` with a redacted receipt (D-09).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.ckan.clients import (
    _AsyncOrganizationService,
    _operation_id_from,
    _SyncOrganizationService,
)
from datasluice.connectors.catalog.ckan.inventory import ActionEntry
from datasluice.connectors.catalog.ckan.mapping import MEMBER, PLATFORM
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

_ORGANIZATION_GROUP = "organizations"

type MemberNameList = list[str]
type WireParams = dict[str, object]

_ORG_ID_ACTIONS = frozenset(
    {
        "organization_update",
        "organization_patch",
        "organization_delete",
        "organization_purge",
        "organization_member_create",
        "organization_member_delete",
    }
)


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action == "organization_create":
        return CatalogId(PLATFORM, ResourceKind.ORGANIZATION, str(params["name"]))
    if action in _ORG_ID_ACTIONS:
        return CatalogId(PLATFORM, ResourceKind.ORGANIZATION, str(params["id"]))
    return CatalogId(PLATFORM, MEMBER, str(params["object"]))


class SyncOrganizationsService(_SyncOrganizationService):
    """Synchronous organization projection carrying fifteen typed actions."""

    __slots__ = ()

    def organization_list(
        self,
        *,
        sort: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        capacity: str | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """List public organization records with native sort and offset paging."""
        params = _drop_unset({"sort": sort, "limit": limit, "offset": offset, "capacity": capacity, "all_fields": True})
        return self._invoke_read("organization_list", params)

    def organization_list_for_user(self, *, permission: str | None = None) -> ResultEnvelope[CKANResultItem]:
        """List organizations the authenticated caller may act upon."""
        return self._invoke_read("organization_list_for_user", _drop_unset({"permission": permission}))

    def organization_show(
        self,
        *,
        id: str,
        include_datasets: bool | None = None,
        include_dataset_count: bool | None = None,
        include_users: bool | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one organization by id or name."""
        params: dict[str, object] = {"id": id}
        params.update(
            _drop_unset(
                {
                    "include_datasets": include_datasets,
                    "include_dataset_count": include_dataset_count,
                    "include_users": include_users,
                }
            )
        )
        return self._invoke_read("organization_show", params)

    def organization_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete organization names or titles."""
        return self._invoke_read("organization_autocomplete", _drop_unset({"q": q, "limit": limit}))

    def member_list(
        self, *, id: str, object_type: str | None = None, capacity: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List members of one group-shaped container."""
        return self._invoke_read(
            "member_list", _drop_unset({"id": id, "object_type": object_type, "capacity": capacity})
        )

    def member_roles_list(self) -> ResultEnvelope[CKANResultItem]:
        """List the membership roles this deployment understands."""
        return self._invoke_read("member_roles_list", {})

    def organization_create(
        self,
        *,
        name: str,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        users: MemberNameList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create an organization from documented keyword fields."""
        params: dict[str, object] = {"name": name}
        params.update(_drop_unset({"title": title, "description": description, "image_url": image_url, "users": users}))
        return self._invoke_mutation("organization_create", params, policy)

    def organization_update(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        users: MemberNameList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one organization from documented keyword fields."""
        params: dict[str, object] = {"id": id}
        params.update(
            _drop_unset(
                {"name": name, "title": title, "description": description, "image_url": image_url, "users": users}
            )
        )
        return self._invoke_mutation("organization_update", params, policy)

    def organization_patch(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        users: MemberNameList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Patch selected organization fields without replacing the record."""
        params: dict[str, object] = {"id": id}
        params.update(
            _drop_unset(
                {"name": name, "title": title, "description": description, "image_url": image_url, "users": users}
            )
        )
        return self._invoke_mutation("organization_patch", params, policy)

    def organization_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Soft-delete one organization to state=deleted on the standard tier."""
        return self._invoke_mutation("organization_delete", {"id": id}, policy)

    def organization_purge(self, *, id: str, policy: MutationPolicy) -> CKANMutationResult:
        """Purge one organization irreversibly on the destructive tier (D-09)."""
        return self._invoke_mutation("organization_purge", {"id": id}, policy)

    def organization_member_create(
        self, *, id: str, username: str, role: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Grant one user a role within an organization."""
        params: dict[str, object] = {"id": id, "username": username, "role": role}
        return self._invoke_mutation("organization_member_create", params, policy)

    def organization_member_delete(
        self, *, id: str, username: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one user from an organization."""
        return self._invoke_mutation("organization_member_delete", {"id": id, "username": username}, policy)

    def member_create(
        self, *, id: str, object: str, object_type: str, capacity: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Add one object as a member of a container with a capacity."""
        params: WireParams = {"id": id, "object": object, "object_type": object_type, "capacity": capacity}
        return self._invoke_mutation("member_create", params, policy)

    def member_delete(
        self, *, id: str, object: str, object_type: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one object from a container's membership."""
        params: WireParams = {"id": id, "object": object, "object_type": object_type}
        return self._invoke_mutation("member_delete", params, policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _ORGANIZATION_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_ORGANIZATION_GROUP!r} group.",
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


class AsyncOrganizationsService(_AsyncOrganizationService):
    """Asynchronous organization projection carrying fifteen typed actions."""

    __slots__ = ()

    async def organization_list(
        self,
        *,
        sort: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        capacity: str | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """List public organization records with native sort and offset paging."""
        params = _drop_unset({"sort": sort, "limit": limit, "offset": offset, "capacity": capacity, "all_fields": True})
        return await self._invoke_read("organization_list", params)

    async def organization_list_for_user(self, *, permission: str | None = None) -> ResultEnvelope[CKANResultItem]:
        """List organizations the authenticated caller may act upon."""
        return await self._invoke_read("organization_list_for_user", _drop_unset({"permission": permission}))

    async def organization_show(
        self,
        *,
        id: str,
        include_datasets: bool | None = None,
        include_dataset_count: bool | None = None,
        include_users: bool | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one organization by id or name."""
        params: dict[str, object] = {"id": id}
        params.update(
            _drop_unset(
                {
                    "include_datasets": include_datasets,
                    "include_dataset_count": include_dataset_count,
                    "include_users": include_users,
                }
            )
        )
        return await self._invoke_read("organization_show", params)

    async def organization_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete organization names or titles."""
        return await self._invoke_read("organization_autocomplete", _drop_unset({"q": q, "limit": limit}))

    async def member_list(
        self, *, id: str, object_type: str | None = None, capacity: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List members of one group-shaped container."""
        params = _drop_unset({"id": id, "object_type": object_type, "capacity": capacity})
        return await self._invoke_read("member_list", params)

    async def member_roles_list(self) -> ResultEnvelope[CKANResultItem]:
        """List the membership roles this deployment understands."""
        return await self._invoke_read("member_roles_list", {})

    async def organization_create(
        self,
        *,
        name: str,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        users: MemberNameList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create an organization from documented keyword fields."""
        params: dict[str, object] = {"name": name}
        params.update(_drop_unset({"title": title, "description": description, "image_url": image_url, "users": users}))
        return await self._invoke_mutation("organization_create", params, policy)

    async def organization_update(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        users: MemberNameList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one organization from documented keyword fields."""
        params: dict[str, object] = {"id": id}
        params.update(
            _drop_unset(
                {"name": name, "title": title, "description": description, "image_url": image_url, "users": users}
            )
        )
        return await self._invoke_mutation("organization_update", params, policy)

    async def organization_patch(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        users: MemberNameList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Patch selected organization fields without replacing the record."""
        params: dict[str, object] = {"id": id}
        params.update(
            _drop_unset(
                {"name": name, "title": title, "description": description, "image_url": image_url, "users": users}
            )
        )
        return await self._invoke_mutation("organization_patch", params, policy)

    async def organization_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Soft-delete one organization to state=deleted on the standard tier."""
        return await self._invoke_mutation("organization_delete", {"id": id}, policy)

    async def organization_purge(self, *, id: str, policy: MutationPolicy) -> CKANMutationResult:
        """Purge one organization irreversibly on the destructive tier (D-09)."""
        return await self._invoke_mutation("organization_purge", {"id": id}, policy)

    async def organization_member_create(
        self, *, id: str, username: str, role: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Grant one user a role within an organization."""
        params: dict[str, object] = {"id": id, "username": username, "role": role}
        return await self._invoke_mutation("organization_member_create", params, policy)

    async def organization_member_delete(
        self, *, id: str, username: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one user from an organization."""
        return await self._invoke_mutation("organization_member_delete", {"id": id, "username": username}, policy)

    async def member_create(
        self, *, id: str, object: str, object_type: str, capacity: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Add one object as a member of a container with a capacity."""
        params: WireParams = {"id": id, "object": object, "object_type": object_type, "capacity": capacity}
        return await self._invoke_mutation("member_create", params, policy)

    async def member_delete(
        self, *, id: str, object: str, object_type: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one object from a container's membership."""
        params: WireParams = {"id": id, "object": object, "object_type": object_type}
        return await self._invoke_mutation("member_delete", params, policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _ORGANIZATION_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_ORGANIZATION_GROUP!r} group.",
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
