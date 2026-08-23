"""Both-mode CKAN user projections with first-class API-token management (D-05).

Authentication on CKAN is token-only: every privileged call rides the
``Authorization`` header seam, and no cookie/session or legacy-key path exists
anywhere in this module. ``api_token_create`` returns the reveal-only
``CKANTokenResult`` wrapper so issued secrets disclose exclusively through an
explicit accessor; list and revoke keep standard shaping. Privileged writes ride
the declared profile plus the server's authorization responses, never synthesized
capability claims.
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
from datasluice.connectors.catalog.ckan.results import CKANMutationResult, CKANTokenResult, require_mutation_tier
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError
from datasluice.runtime.mutation import build_mutation_receipt

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient

_USER_GROUP = "users"
_TOKEN_OPERATION = "ckan/action-api-v3.user-create-update-delete-token-management"

_ID_TARGET_ACTIONS = frozenset({"user_update", "user_patch", "user_delete"})


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action == "user_create":
        return CatalogId(PLATFORM, ResourceKind.USER, str(params["name"]))
    if action == "user_invite":
        return CatalogId(PLATFORM, ResourceKind.USER, str(params["email"]))
    if action in _ID_TARGET_ACTIONS:
        return CatalogId(PLATFORM, ResourceKind.USER, str(params["id"]))
    if action == "api_token_create":
        return CatalogId(PLATFORM, ResourceKind.USER, str(params["user"]))
    if action == "api_token_revoke":
        return CatalogId(PLATFORM, ResourceKind.USER, str(params["token_id"]))
    return CatalogId(PLATFORM, ResourceKind.USER, "site-user")


class SyncUsersService(_SyncNativeService):
    """Synchronous user projection carrying twelve typed actions incl. the token trio."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, "users")

    def user_list(self, *, q: str | None = None, email: str | None = None) -> ResultEnvelope[CKANResultItem]:
        """List user records matching the documented filters."""
        return self._invoke_read("user_list", _drop_unset({"q": q, "email": email}))

    def user_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one user by id or name."""
        return self._invoke_read("user_show", {"id": id})

    def user_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete user names."""
        return self._invoke_read("user_autocomplete", _drop_unset({"q": q, "limit": limit}))

    def user_create(
        self,
        *,
        name: str,
        email: str,
        fullname: str | None = None,
        about: str | None = None,
        password: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create a user from documented keyword fields."""
        params: dict[str, object] = {"name": name, "email": email}
        params.update(_drop_unset({"fullname": fullname, "about": about, "password": password}))
        return self._invoke_mutation("user_create", params, policy)

    def user_invite(
        self, *, email: str, group_id: str, role: str, name: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Invite one new user into a group or organization with a role."""
        params: dict[str, object] = {"email": email, "group_id": group_id, "role": role}
        params.update(_drop_unset({"name": name}))
        return self._invoke_mutation("user_invite", params, policy)

    def user_update(
        self,
        *,
        id: str,
        email: str | None = None,
        fullname: str | None = None,
        about: str | None = None,
        password: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one user from documented keyword fields."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"email": email, "fullname": fullname, "about": about, "password": password}))
        return self._invoke_mutation("user_update", params, policy)

    def user_patch(
        self,
        *,
        id: str,
        email: str | None = None,
        fullname: str | None = None,
        about: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Patch selected user fields without replacing the record."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"email": email, "fullname": fullname, "about": about}))
        return self._invoke_mutation("user_patch", params, policy)

    def user_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Soft-delete one user to state=deleted on the standard tier."""
        return self._invoke_mutation("user_delete", {"id": id}, policy)

    def get_site_user(self, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Fetch (creating on first use) the deployment's site user record."""
        return self._invoke_mutation("get_site_user", {}, policy)

    def api_token_create(self, *, user: str, name: str, policy: MutationPolicy | None = None) -> CKANTokenResult:
        """Create an API token for one user and return the reveal-only wrapper (D-05)."""
        result = self._invoke_mutation("api_token_create", {"user": user, "name": name}, policy)
        item = result.result.items[0] if result.result.items else None
        if not isinstance(item, CKANTokenResult):
            raise NativeCatalogError(
                "The api_token_create result did not carry a typed token secret.",
                operation=_TOKEN_OPERATION,
                platform=PLATFORM,
            )
        return item

    def api_token_list(self, *, user: str) -> ResultEnvelope[CKANResultItem]:
        """List one user's API tokens decoded losslessly as the platform returns them."""
        return self._invoke_read("api_token_list", {"user": user})

    def api_token_revoke(self, *, token_id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Revoke one API token by its identifier; the server owns revocation semantics."""
        return self._invoke_mutation("api_token_revoke", {"token_id": token_id}, policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _USER_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_USER_GROUP!r} group.",
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


class AsyncUsersService(_AsyncNativeService):
    """Asynchronous user projection carrying twelve typed actions incl. the token trio."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, "users")

    async def user_list(self, *, q: str | None = None, email: str | None = None) -> ResultEnvelope[CKANResultItem]:
        """List user records matching the documented filters."""
        return await self._invoke_read("user_list", _drop_unset({"q": q, "email": email}))

    async def user_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one user by id or name."""
        return await self._invoke_read("user_show", {"id": id})

    async def user_autocomplete(self, *, q: str, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete user names."""
        return await self._invoke_read("user_autocomplete", _drop_unset({"q": q, "limit": limit}))

    async def user_create(
        self,
        *,
        name: str,
        email: str,
        fullname: str | None = None,
        about: str | None = None,
        password: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create a user from documented keyword fields."""
        params: dict[str, object] = {"name": name, "email": email}
        params.update(_drop_unset({"fullname": fullname, "about": about, "password": password}))
        return await self._invoke_mutation("user_create", params, policy)

    async def user_invite(
        self, *, email: str, group_id: str, role: str, name: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Invite one new user into a group or organization with a role."""
        params: dict[str, object] = {"email": email, "group_id": group_id, "role": role}
        params.update(_drop_unset({"name": name}))
        return await self._invoke_mutation("user_invite", params, policy)

    async def user_update(
        self,
        *,
        id: str,
        email: str | None = None,
        fullname: str | None = None,
        about: str | None = None,
        password: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one user from documented keyword fields."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"email": email, "fullname": fullname, "about": about, "password": password}))
        return await self._invoke_mutation("user_update", params, policy)

    async def user_patch(
        self,
        *,
        id: str,
        email: str | None = None,
        fullname: str | None = None,
        about: str | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Patch selected user fields without replacing the record."""
        params: dict[str, object] = {"id": id}
        params.update(_drop_unset({"email": email, "fullname": fullname, "about": about}))
        return await self._invoke_mutation("user_patch", params, policy)

    async def user_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Soft-delete one user to state=deleted on the standard tier."""
        return await self._invoke_mutation("user_delete", {"id": id}, policy)

    async def get_site_user(self, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Fetch (creating on first use) the deployment's site user record."""
        return await self._invoke_mutation("get_site_user", {}, policy)

    async def api_token_create(self, *, user: str, name: str, policy: MutationPolicy | None = None) -> CKANTokenResult:
        """Create an API token for one user and return the reveal-only wrapper (D-05)."""
        result = await self._invoke_mutation("api_token_create", {"user": user, "name": name}, policy)
        item = result.result.items[0] if result.result.items else None
        if not isinstance(item, CKANTokenResult):
            raise NativeCatalogError(
                "The api_token_create result did not carry a typed token secret.",
                operation=_TOKEN_OPERATION,
                platform=PLATFORM,
            )
        return item

    async def api_token_list(self, *, user: str) -> ResultEnvelope[CKANResultItem]:
        """List one user's API tokens decoded losslessly as the platform returns them."""
        return await self._invoke_read("api_token_list", {"user": user})

    async def api_token_revoke(self, *, token_id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Revoke one API token by its identifier; the server owns revocation semantics."""
        return await self._invoke_mutation("api_token_revoke", {"token_id": token_id}, policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _USER_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_USER_GROUP!r} group.",
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
