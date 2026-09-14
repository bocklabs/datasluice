"""Both-mode CKAN group projections across the split v2 read and write ids.

Every typed method declares its owning v2 OperationId from the checked-in manifest
and passes documented CKAN 2.11 parameters verbatim (D-04). ``group_purge`` refuses
pre-dispatch without a confirmed destructive policy through the shared 03-03 gate;
group member variants decode the connector-declared member kind. Privileged writes
ride the declared profile plus server authorization responses, never synthesized
capability claims.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.ckan.clients import (
    _async_typed_mutation,
    _async_typed_read,
    _AsyncNativeService,
    _sync_typed_mutation,
    _sync_typed_read,
    _SyncNativeService,
)
from datasluice.connectors.catalog.ckan.mapping import GROUP, PLATFORM
from datasluice.connectors.catalog.ckan.results import CKANMutationResult
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.domain.catalog.ids import CatalogId
from datasluice.domain.catalog.models import ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient

_GROUP_GROUP = "groups"


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    key = "name" if action == "group_create" else "id"
    return CatalogId(PLATFORM, GROUP, str(params[key]))


class SyncGroupsService(_SyncNativeService):
    """Synchronous group projection carrying twelve typed actions."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, "groups")

    def group_list(
        self,
        *,
        sort: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        capacity: str | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """List group names with native sort and offset paging."""
        params = _drop_unset({"sort": sort, "limit": limit, "offset": offset, "capacity": capacity})
        return self._invoke_read("group_list", params)

    def group_list_authz(self) -> ResultEnvelope[CKANResultItem]:
        """List groups the authenticated caller is authorized to see."""
        return self._invoke_read("group_list_authz", {})

    def group_show(
        self,
        *,
        id: str,
        include_datasets: bool | None = None,
        include_dataset_count: bool | None = None,
        include_users: bool | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one group by id or name."""
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
        return self._invoke_read("group_show", params)

    def group_package_show(self, *, id: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Show the datasets of one group."""
        return self._invoke_read("group_package_show", _drop_unset({"id": id, "limit": limit}))

    def group_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete group names or titles."""
        return self._invoke_read("group_autocomplete", _drop_unset({"q": q, "limit": limit}))

    def group_create(
        self,
        *,
        name: str,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create a group from documented keyword fields."""
        params: dict[str, object] = {"name": name}
        params.update(_drop_unset({"title": title, "description": description, "image_url": image_url}))
        return self._invoke_mutation("group_create", params, policy)

    def group_update(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one group from documented keyword fields."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"name": name, "title": title, "description": description, "image_url": image_url}))
        return self._invoke_mutation("group_update", params, policy)

    def group_patch(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Patch selected group fields without replacing the record."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"name": name, "title": title, "description": description, "image_url": image_url}))
        return self._invoke_mutation("group_patch", params, policy)

    def group_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Soft-delete one group to state=deleted on the standard tier."""
        return self._invoke_mutation("group_delete", {"id": id}, policy)

    def group_purge(self, *, id: str, policy: MutationPolicy) -> CKANMutationResult:
        """Purge one group irreversibly on the destructive tier (D-09)."""
        return self._invoke_mutation("group_purge", {"id": id}, policy)

    def group_member_create(
        self, *, id: str, username: str, role: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Grant one user a role within a group."""
        params: dict[str, object] = {"id": id, "username": username, "role": role}
        return self._invoke_mutation("group_member_create", params, policy)

    def group_member_delete(
        self, *, id: str, username: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one user from a group."""
        return self._invoke_mutation("group_member_delete", {"id": id, "username": username}, policy)

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        return _sync_typed_read(self._client, _GROUP_GROUP, action, params)

    def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        return _sync_typed_mutation(self._client, _GROUP_GROUP, action, params, policy, _mutation_target)


class AsyncGroupsService(_AsyncNativeService):
    """Asynchronous group projection carrying twelve typed actions."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, "groups")

    async def group_list(
        self,
        *,
        sort: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        capacity: str | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """List group names with native sort and offset paging."""
        params = _drop_unset({"sort": sort, "limit": limit, "offset": offset, "capacity": capacity})
        return await self._invoke_read("group_list", params)

    async def group_list_authz(self) -> ResultEnvelope[CKANResultItem]:
        """List groups the authenticated caller is authorized to see."""
        return await self._invoke_read("group_list_authz", {})

    async def group_show(
        self,
        *,
        id: str,
        include_datasets: bool | None = None,
        include_dataset_count: bool | None = None,
        include_users: bool | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one group by id or name."""
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
        return await self._invoke_read("group_show", params)

    async def group_package_show(self, *, id: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Show the datasets of one group."""
        return await self._invoke_read("group_package_show", _drop_unset({"id": id, "limit": limit}))

    async def group_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete group names or titles."""
        return await self._invoke_read("group_autocomplete", _drop_unset({"q": q, "limit": limit}))

    async def group_create(
        self,
        *,
        name: str,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create a group from documented keyword fields."""
        params: dict[str, object] = {"name": name}
        params.update(_drop_unset({"title": title, "description": description, "image_url": image_url}))
        return await self._invoke_mutation("group_create", params, policy)

    async def group_update(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one group from documented keyword fields."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"name": name, "title": title, "description": description, "image_url": image_url}))
        return await self._invoke_mutation("group_update", params, policy)

    async def group_patch(
        self,
        *,
        id: str,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Patch selected group fields without replacing the record."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"name": name, "title": title, "description": description, "image_url": image_url}))
        return await self._invoke_mutation("group_patch", params, policy)

    async def group_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Soft-delete one group to state=deleted on the standard tier."""
        return await self._invoke_mutation("group_delete", {"id": id}, policy)

    async def group_purge(self, *, id: str, policy: MutationPolicy) -> CKANMutationResult:
        """Purge one group irreversibly on the destructive tier (D-09)."""
        return await self._invoke_mutation("group_purge", {"id": id}, policy)

    async def group_member_create(
        self, *, id: str, username: str, role: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Grant one user a role within a group."""
        params: dict[str, object] = {"id": id, "username": username, "role": role}
        return await self._invoke_mutation("group_member_create", params, policy)

    async def group_member_delete(
        self, *, id: str, username: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Remove one user from a group."""
        return await self._invoke_mutation("group_member_delete", {"id": id, "username": username}, policy)

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        return await _async_typed_read(self._client, _GROUP_GROUP, action, params)

    async def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        return await _async_typed_mutation(self._client, _GROUP_GROUP, action, params, policy, _mutation_target)
