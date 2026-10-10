"""Both-mode CKAN organization projections: exhaustive org and membership actions.

Every typed method declares its owning v2 OperationId from the checked-in manifest
and passes documented CKAN 2.11 parameters verbatim (D-04). Admin-tier honesty is
structural, not probed: privileged writes dispatch on the declared profile and the
SERVER's authorization responses are the runtime evidence. ``organization_purge``
refuses pre-dispatch without a confirmed destructive policy through the shared
03-03 gate and returns a ``CKANMutationResult`` with a redacted receipt (D-09).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.connectors.catalog.ckan.clients import (
    _async_typed_mutation,
    _async_typed_read,
    _AsyncOrganizationService,
    _sync_typed_mutation,
    _sync_typed_read,
    _SyncOrganizationService,
)
from datasluice.connectors.catalog.ckan.mapping import MEMBER, PLATFORM
from datasluice.connectors.catalog.ckan.services._shared import WireParams, detail_params, drop_unset, wire_params
from datasluice.domain.catalog.ids import CatalogId, ResourceKind

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datasluice.connectors.catalog.ckan.results import CKANMutationResult
    from datasluice.contracts.catalog.native.ckan import CKANResultItem
    from datasluice.domain.catalog.models import ResultEnvelope
    from datasluice.domain.catalog.safety import MutationPolicy

_ORGANIZATION_GROUP = "organizations"

type MemberNameList = list[str]

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


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action == "organization_create":
        return CatalogId(PLATFORM, ResourceKind.ORGANIZATION, str(params["name"]))
    if action in _ORG_ID_ACTIONS:
        return CatalogId(PLATFORM, ResourceKind.ORGANIZATION, str(params["id"]))
    return CatalogId(PLATFORM, MEMBER, str(params["object"]))


def _fields_params(
    id: str,
    name: str | None,
    title: str | None,
    description: str | None,
    image_url: str | None,
    users: MemberNameList | None,
) -> WireParams:
    return wire_params(
        {"id": id},
        {"name": name, "title": title, "description": description, "image_url": image_url, "users": users},
    )


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
        return self._invoke_read(
            "organization_list",
            drop_unset({"sort": sort, "limit": limit, "offset": offset, "capacity": capacity, "all_fields": True}),
        )

    def organization_list_for_user(self, *, permission: str | None = None) -> ResultEnvelope[CKANResultItem]:
        """List organizations the authenticated caller may act upon."""
        return self._invoke_read("organization_list_for_user", drop_unset({"permission": permission}))

    def organization_show(
        self,
        *,
        id: str,
        include_datasets: bool | None = None,
        include_dataset_count: bool | None = None,
        include_users: bool | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one organization by id or name."""
        return self._invoke_read(
            "organization_show", detail_params(id, include_datasets, include_dataset_count, include_users)
        )

    def organization_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete organization names or titles."""
        return self._invoke_read("organization_autocomplete", drop_unset({"q": q, "limit": limit}))

    def member_list(
        self, *, id: str, object_type: str | None = None, capacity: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List members of one group-shaped container."""
        return self._invoke_read(
            "member_list", drop_unset({"id": id, "object_type": object_type, "capacity": capacity})
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
        return self._invoke_mutation(
            "organization_create",
            wire_params(
                {"name": name},
                {"title": title, "description": description, "image_url": image_url, "users": users},
            ),
            policy,
        )

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
        return self._invoke_mutation(
            "organization_update", _fields_params(id, name, title, description, image_url, users), policy
        )

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
        return self._invoke_mutation(
            "organization_patch", _fields_params(id, name, title, description, image_url, users), policy
        )

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
        return self._invoke_mutation(
            "organization_member_create", {"id": id, "username": username, "role": role}, policy
        )

    def organization_member_delete(
        self, *, id: str, username: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one user from an organization."""
        return self._invoke_mutation("organization_member_delete", {"id": id, "username": username}, policy)

    def member_create(
        self, *, id: str, object: str, object_type: str, capacity: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Add one object as a member of a container with a capacity."""
        return self._invoke_mutation(
            "member_create", {"id": id, "object": object, "object_type": object_type, "capacity": capacity}, policy
        )

    def member_delete(
        self, *, id: str, object: str, object_type: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one object from a container's membership."""
        return self._invoke_mutation("member_delete", {"id": id, "object": object, "object_type": object_type}, policy)

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        return _sync_typed_read(self._client, _ORGANIZATION_GROUP, action, params)

    def _invoke_mutation(self, action: str, params: WireParams, policy: MutationPolicy | None) -> CKANMutationResult:
        return _sync_typed_mutation(self._client, _ORGANIZATION_GROUP, action, params, policy, _mutation_target)


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
        return await self._invoke_read(
            "organization_list",
            drop_unset({"sort": sort, "limit": limit, "offset": offset, "capacity": capacity, "all_fields": True}),
        )

    async def organization_list_for_user(self, *, permission: str | None = None) -> ResultEnvelope[CKANResultItem]:
        """List organizations the authenticated caller may act upon."""
        return await self._invoke_read("organization_list_for_user", drop_unset({"permission": permission}))

    async def organization_show(
        self,
        *,
        id: str,
        include_datasets: bool | None = None,
        include_dataset_count: bool | None = None,
        include_users: bool | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one organization by id or name."""
        return await self._invoke_read(
            "organization_show", detail_params(id, include_datasets, include_dataset_count, include_users)
        )

    async def organization_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete organization names or titles."""
        return await self._invoke_read("organization_autocomplete", drop_unset({"q": q, "limit": limit}))

    async def member_list(
        self, *, id: str, object_type: str | None = None, capacity: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List members of one group-shaped container."""
        return await self._invoke_read(
            "member_list", drop_unset({"id": id, "object_type": object_type, "capacity": capacity})
        )

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
        return await self._invoke_mutation(
            "organization_create",
            wire_params(
                {"name": name},
                {"title": title, "description": description, "image_url": image_url, "users": users},
            ),
            policy,
        )

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
        return await self._invoke_mutation(
            "organization_update", _fields_params(id, name, title, description, image_url, users), policy
        )

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
        return await self._invoke_mutation(
            "organization_patch", _fields_params(id, name, title, description, image_url, users), policy
        )

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
        return await self._invoke_mutation(
            "organization_member_create", {"id": id, "username": username, "role": role}, policy
        )

    async def organization_member_delete(
        self, *, id: str, username: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one user from an organization."""
        return await self._invoke_mutation("organization_member_delete", {"id": id, "username": username}, policy)

    async def member_create(
        self, *, id: str, object: str, object_type: str, capacity: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Add one object as a member of a container with a capacity."""
        return await self._invoke_mutation(
            "member_create", {"id": id, "object": object, "object_type": object_type, "capacity": capacity}, policy
        )

    async def member_delete(
        self, *, id: str, object: str, object_type: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one object from a container's membership."""
        return await self._invoke_mutation(
            "member_delete", {"id": id, "object": object, "object_type": object_type}, policy
        )

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        return await _async_typed_read(self._client, _ORGANIZATION_GROUP, action, params)

    async def _invoke_mutation(
        self, action: str, params: WireParams, policy: MutationPolicy | None
    ) -> CKANMutationResult:
        return await _async_typed_mutation(self._client, _ORGANIZATION_GROUP, action, params, policy, _mutation_target)
