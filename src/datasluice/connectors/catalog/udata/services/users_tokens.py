"""Typed sync and async users, invitations, and API-token services."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.udata.mapping import UDataPageEnvelope
from datasluice.connectors.catalog.udata.models.users import (
    ApiTokenCreateInput,
    ApiTokenCreationResult,
    ApiTokenMetadata,
    UserAvatarInput,
    UserCreateInput,
    UserDeleteOptions,
    UserListQuery,
    UserMutationResult,
    UserSuggestQuery,
    UserUpdateInput,
)
from datasluice.connectors.catalog.udata.secrets import OneTimeUDataToken
from datasluice.connectors.catalog.udata.wire import users as wire
from datasluice.connectors.catalog.udata.wire.organizations import parse_page
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError
from datasluice.runtime.transport.base import RuntimeResponse

from .datasets import (
    _enforce_mutation_policy,
    _error_status,
    _mutation_outcome,
    _require_mutation_permission,
    _safe_target_value,
)
from .organizations_memberships import _attach, _receipt

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

type Permissions = EffectivePermissions
type Policy = MutationPolicy | None
type Result = UserMutationResult | ApiTokenCreationResult
type ReadParser[T] = Callable[[str, object], T]

_PUBLIC = frozenset({"get_user", "get_user_contact_point", "suggest_users", "user_roles", "list_user_followers"})
_ADMIN = frozenset({"list_users", "create_user", "user_avatar", "update_user", "delete_user", "rotate_user_password"})
_DESTRUCTIVE = frozenset({"delete_me", "revoke_api_token", "delete_user", "unfollow_user"})
_MAPPING_PAGE = {"my_org_topics": "topic", "list_user_followers": "follow", "get_user_contact_point": "contact-point"}
_MAPPING_LIST = {
    "my_reuses": "reuse",
    "my_datasets": "dataset",
    "my_org_datasets": "dataset",
    "my_org_community_resources": "community-resource",
    "my_org_reuses": "reuse",
    "my_org_discussions": "discussion",
    "list_org_invitations": "invitation",
    "suggest_users": "user",
    "user_roles": "role",
}


def _parse_user_read(name: str, payload: object) -> NativeRecord:
    operation = wire.OPERATIONS[name]
    return wire.parse_user(payload, operation=operation)


def _parse_user_page(name: str, payload: object) -> UDataPageEnvelope:
    operation = wire.OPERATIONS[name]
    if name == "list_users":
        return wire.parse_user_page(payload, operation=operation)
    return parse_page(payload, operation=operation, kind=ResourceKind(_MAPPING_PAGE[name]))


def _parse_mapping_read(name: str, payload: object) -> tuple[MappingRecord, ...]:
    return wire.parse_mapping_list(payload, operation=wire.OPERATIONS[name], kind=_MAPPING_LIST[name])


def _parse_token_list(name: str, payload: object) -> tuple[ApiTokenMetadata, ...]:
    return wire.parse_token_list(payload, operation=wire.OPERATIONS[name])


def _parse_metrics(name: str, payload: object) -> MappingRecord:
    if isinstance(payload, Mapping):
        return MappingRecord(payload=payload)
    raise CatalogValidationError(
        "The uData user response has an undocumented shape.",
        operation=wire.OPERATIONS[name],
        platform="udata",
        safe_action="Verify the response against the pinned user schema.",
    )


def _shape_mutation(payload: object, receipt: object, name: str) -> Result:
    if not isinstance(receipt, MutationReceipt):
        raise TypeError("uData mutations require a receipt.")
    operation = wire.OPERATIONS[name]
    if name == "create_api_token":
        if not isinstance(payload, dict) or not isinstance(payload.get("token"), str) or not payload["token"]:
            raise CatalogValidationError(
                "The uData token creation response omitted its one-time token.",
                operation=operation,
                platform="udata",
                safe_action="Verify the response against the pinned token schema.",
            )
        secret = OneTimeUDataToken(payload.pop("token"))
        try:
            return ApiTokenCreationResult(receipt, wire.parse_token(payload, operation=operation), secret)
        except BaseException:
            secret._discard()
            raise
    if name in {"update_me", "create_user", "update_user"}:
        return UserMutationResult(receipt, record=wire.parse_user(payload, operation=operation))
    if isinstance(payload, Mapping):
        return UserMutationResult(receipt, value=MappingRecord(payload=payload))
    if payload is None or payload == "":
        return UserMutationResult(receipt)
    raise CatalogValidationError(
        "The uData user mutation response has an undocumented shape.",
        operation=operation,
        platform="udata",
        safe_action="Verify the response against the pinned user schema.",
    )


def _target(name: str, identifier: str | None, body: object) -> str:
    if identifier is not None:
        return identifier
    if name == "create_user" and isinstance(body, UserCreateInput):
        return _safe_target_value(body.email, opaque=True)
    if name == "create_api_token":
        return "new-api-token"
    return "me"


def _receipt_target(name: str, target: str, payload: object) -> str:
    if name in {"create_user", "create_api_token"} and isinstance(payload, Mapping):
        value = payload.get("id")
        if isinstance(value, str) and value:
            return value
    return target


def _discard_token_plaintext(payload: object, response: RuntimeResponse | None) -> RuntimeResponse | None:
    if isinstance(payload, dict):
        payload.pop("token", None)
    return RuntimeResponse(status_code=response.status_code, headers={}, body=b"") if response is not None else None


def _error_receipt(
    error: BaseException, name: str, target: str, policy: Policy, response: RuntimeResponse | None
) -> None:
    operation = wire.OPERATIONS[name]
    outcome = "ambiguous" if isinstance(error, KeyboardInterrupt) else _mutation_outcome(error, response)
    receipt = _receipt(
        operation,
        target,
        policy,
        outcome,
        _error_status(error, response),
        name,
        kind=ResourceKind("api-token" if "api_token" in name else "user"),
    )
    _attach(error, receipt)


def _close_avatar(upload: UserAvatarInput | None, result: Result | None, primary_error: BaseException | None) -> None:
    if upload is None:
        return
    try:
        upload.close()
    except BaseException as close_error:
        receipt = result.receipt if result is not None else getattr(primary_error, "mutation_receipt", None)
        if isinstance(receipt, MutationReceipt):
            _attach(close_error, receipt)
        if primary_error is not None:
            raise primary_error from close_error
        raise


class SyncUsersTokensService:
    """Named synchronous stock user-family routes."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def _read[T](
        self,
        name: str,
        decoder: ReadParser[T],
        *,
        identifier: str | None = None,
        query: object = None,
        permissions: Permissions | None = None,
    ) -> T:
        operation = wire.OPERATIONS[name]
        try:
            credential = None
            if name not in _PUBLIC:
                credential = _require_mutation_permission(
                    self._client._resolved_credential(), operation, permissions, admin=name in _ADMIN
                )
            method, path, headers, _ = wire.build_request(name, identifier=identifier, query=query)
            _, payload, _ = self._client._dataset_call(
                method=method,
                path=path,
                headers=headers,
                owning_operation=operation,
                permissions=permissions,
                credential=credential,
                emit_success=False,
            )
            result = decoder(name, payload)
        except (Exception, KeyboardInterrupt):
            self._client._emit(operation, "failed")
            raise
        self._client._emit(operation, "succeeded")
        return result

    def _write(
        self,
        name: str,
        *,
        identifier: str | None = None,
        body: object = None,
        query: object = None,
        permissions: Permissions,
        mutation_policy: Policy,
    ) -> Result:
        operation = wire.OPERATIONS[name]
        target = _target(name, identifier, body)
        response: RuntimeResponse | None = None
        upload = body if isinstance(body, UserAvatarInput) else None
        payload: object = None
        result: Result | None = None
        primary_error: BaseException | None = None
        try:
            _enforce_mutation_policy(operation, target, mutation_policy, destructive=name in _DESTRUCTIVE)
            credential = _require_mutation_permission(
                self._client._resolved_credential(), operation, permissions, admin=name in _ADMIN
            )
            if name in {"update_me", "update_user"} and isinstance(body, UserUpdateInput):
                if {"roles", "active"} & set(body.fields):
                    _require_mutation_permission(credential, operation, permissions, admin=True)
            method, path, headers, encoded = wire.build_request(
                name, identifier=identifier, query=query, body=None if upload else body
            )
            status, payload, response = self._client._dataset_call(
                method=method,
                path=path,
                headers=headers,
                owning_operation=operation,
                json_body=encoded,
                permissions=permissions,
                credential=credential,
                idempotency_policy=mutation_policy.idempotency if mutation_policy else None,
                emit_success=False,
                files=(upload.part(),) if upload else (),
            )
            if name == "create_api_token":
                response = RuntimeResponse(status_code=status, headers={}, body=b"")
            receipt = _receipt(
                operation,
                _receipt_target(name, target, payload),
                mutation_policy,
                "succeeded",
                status,
                name,
                kind=ResourceKind("api-token" if "api_token" in name else "user"),
            )
            result = _shape_mutation(payload, receipt, name)
            self._client._emit(operation, "succeeded")
            return result
        except (Exception, KeyboardInterrupt) as error:
            primary_error = error
            if name == "create_api_token":
                response = _discard_token_plaintext(payload, response)
                if isinstance(result, ApiTokenCreationResult):
                    result.secret._discard()
                    result = None
            self._client._emit(operation, "failed")
            _error_receipt(error, name, target, mutation_policy, response)
            raise
        finally:
            _close_avatar(upload, result, primary_error)

    def get_me(self, permissions: Permissions) -> NativeRecord:
        return self._read("get_me", _parse_user_read, permissions=permissions)

    def my_reuses(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return self._read("my_reuses", _parse_mapping_read, permissions=permissions)

    def my_datasets(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return self._read("my_datasets", _parse_mapping_read, permissions=permissions)

    def my_metrics(self, permissions: Permissions) -> MappingRecord:
        return self._read("my_metrics", _parse_metrics, permissions=permissions)

    def my_org_datasets(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return self._read("my_org_datasets", _parse_mapping_read, permissions=permissions, query=q)

    def my_org_community_resources(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return self._read("my_org_community_resources", _parse_mapping_read, permissions=permissions, query=q)

    def my_org_reuses(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return self._read("my_org_reuses", _parse_mapping_read, permissions=permissions, query=q)

    def my_org_discussions(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return self._read("my_org_discussions", _parse_mapping_read, permissions=permissions, query=q)

    def list_api_tokens(self, permissions: Permissions) -> tuple[ApiTokenMetadata, ...]:
        return self._read("list_api_tokens", _parse_token_list, permissions=permissions)

    def list_org_invitations(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return self._read("list_org_invitations", _parse_mapping_read, permissions=permissions)

    def list_users(self, permissions: Permissions, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return self._read("list_users", _parse_user_page, permissions=permissions, query=query or UserListQuery())

    def get_user(self, user_id: str) -> NativeRecord:
        return self._read("get_user", _parse_user_read, identifier=user_id)

    def get_user_contact_point(self, user_id: str, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return self._read(
            "get_user_contact_point", _parse_user_page, identifier=user_id, query=query or UserListQuery()
        )

    def suggest_users(self, query: UserSuggestQuery) -> tuple[MappingRecord, ...]:
        return self._read("suggest_users", _parse_mapping_read, query=query)

    def user_roles(self) -> tuple[MappingRecord, ...]:
        return self._read("user_roles", _parse_mapping_read)

    def my_org_topics(self, permissions: Permissions, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return self._read("my_org_topics", _parse_user_page, permissions=permissions, query=query or UserListQuery())

    def list_user_followers(self, user_id: str, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return self._read("list_user_followers", _parse_user_page, identifier=user_id, query=query or UserListQuery())

    def update_me(
        self, client_input: UserUpdateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write("update_me", body=client_input, permissions=permissions, mutation_policy=mutation_policy),
        )

    def delete_me(self, permissions: Permissions, mutation_policy: Policy = None) -> UserMutationResult:
        return cast(
            UserMutationResult, self._write("delete_me", permissions=permissions, mutation_policy=mutation_policy)
        )

    def my_avatar(
        self, client_input: UserAvatarInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write("my_avatar", body=client_input, permissions=permissions, mutation_policy=mutation_policy),
        )

    def create_api_token(
        self, client_input: ApiTokenCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> ApiTokenCreationResult:
        return cast(
            ApiTokenCreationResult,
            self._write(
                "create_api_token", body=client_input, permissions=permissions, mutation_policy=mutation_policy
            ),
        )

    def revoke_api_token(
        self, token_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                "revoke_api_token", identifier=token_id, permissions=permissions, mutation_policy=mutation_policy
            ),
        )

    def accept_org_invitation(
        self, invitation_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                "accept_org_invitation",
                identifier=invitation_id,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    def refuse_org_invitation(
        self, invitation_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                "refuse_org_invitation",
                identifier=invitation_id,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    def create_user(
        self, client_input: UserCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write("create_user", body=client_input, permissions=permissions, mutation_policy=mutation_policy),
        )

    def user_avatar(
        self, user_id: str, client_input: UserAvatarInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                "user_avatar",
                identifier=user_id,
                body=client_input,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    def update_user(
        self, user_id: str, client_input: UserUpdateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                "update_user",
                identifier=user_id,
                body=client_input,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    def delete_user(
        self,
        user_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
        options: UserDeleteOptions | None = None,
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                "delete_user",
                identifier=user_id,
                query=options,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    def rotate_user_password(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                "rotate_user_password", identifier=user_id, permissions=permissions, mutation_policy=mutation_policy
            ),
        )

    def follow_user(self, user_id: str, permissions: Permissions, mutation_policy: Policy = None) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write("follow_user", identifier=user_id, permissions=permissions, mutation_policy=mutation_policy),
        )

    def unfollow_user(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write("unfollow_user", identifier=user_id, permissions=permissions, mutation_policy=mutation_policy),
        )


class AsyncUsersTokensService:
    """Named asynchronous stock user-family routes."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def _read[T](
        self,
        name: str,
        decoder: ReadParser[T],
        *,
        identifier: str | None = None,
        query: object = None,
        permissions: Permissions | None = None,
    ) -> T:
        operation = wire.OPERATIONS[name]
        try:
            credential = None
            if name not in _PUBLIC:
                credential = _require_mutation_permission(
                    await self._client._resolved_credential_async(), operation, permissions, admin=name in _ADMIN
                )
            method, path, headers, _ = wire.build_request(name, identifier=identifier, query=query)
            _, payload, _ = await self._client._dataset_call_async(
                method=method,
                path=path,
                headers=headers,
                owning_operation=operation,
                permissions=permissions,
                credential=credential,
                emit_success=False,
            )
            result = decoder(name, payload)
        except (Exception, asyncio.CancelledError):
            self._client._emit(operation, "failed")
            raise
        self._client._emit(operation, "succeeded")
        return result

    async def _write(
        self,
        name: str,
        *,
        identifier: str | None = None,
        body: object = None,
        query: object = None,
        permissions: Permissions,
        mutation_policy: Policy,
    ) -> Result:
        operation = wire.OPERATIONS[name]
        target = _target(name, identifier, body)
        response: RuntimeResponse | None = None
        upload = body if isinstance(body, UserAvatarInput) else None
        payload: object = None
        result: Result | None = None
        primary_error: BaseException | None = None
        try:
            _enforce_mutation_policy(operation, target, mutation_policy, destructive=name in _DESTRUCTIVE)
            credential = _require_mutation_permission(
                await self._client._resolved_credential_async(), operation, permissions, admin=name in _ADMIN
            )
            if name in {"update_me", "update_user"} and isinstance(body, UserUpdateInput):
                if {"roles", "active"} & set(body.fields):
                    _require_mutation_permission(credential, operation, permissions, admin=True)
            method, path, headers, encoded = wire.build_request(
                name, identifier=identifier, query=query, body=None if upload else body
            )
            status, payload, response = await self._client._dataset_call_async(
                method=method,
                path=path,
                headers=headers,
                owning_operation=operation,
                json_body=encoded,
                permissions=permissions,
                credential=credential,
                idempotency_policy=mutation_policy.idempotency if mutation_policy else None,
                emit_success=False,
                files=(upload.part(),) if upload else (),
            )
            if name == "create_api_token":
                response = RuntimeResponse(status_code=status, headers={}, body=b"")
            receipt = _receipt(
                operation,
                _receipt_target(name, target, payload),
                mutation_policy,
                "succeeded",
                status,
                name,
                kind=ResourceKind("api-token" if "api_token" in name else "user"),
            )
            result = _shape_mutation(payload, receipt, name)
            self._client._emit(operation, "succeeded")
            return result
        except (Exception, asyncio.CancelledError, KeyboardInterrupt) as error:
            primary_error = error
            if name == "create_api_token":
                response = _discard_token_plaintext(payload, response)
                if isinstance(result, ApiTokenCreationResult):
                    result.secret._discard()
                    result = None
            self._client._emit(operation, "failed")
            _error_receipt(error, name, target, mutation_policy, response)
            raise
        finally:
            _close_avatar(upload, result, primary_error)

    async def get_me(self, permissions: Permissions) -> NativeRecord:
        return await self._read("get_me", _parse_user_read, permissions=permissions)

    async def my_reuses(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return await self._read("my_reuses", _parse_mapping_read, permissions=permissions)

    async def my_datasets(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return await self._read("my_datasets", _parse_mapping_read, permissions=permissions)

    async def my_metrics(self, permissions: Permissions) -> MappingRecord:
        return await self._read("my_metrics", _parse_metrics, permissions=permissions)

    async def my_org_datasets(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return await self._read("my_org_datasets", _parse_mapping_read, permissions=permissions, query=q)

    async def my_org_community_resources(
        self, permissions: Permissions, q: str | None = None
    ) -> tuple[MappingRecord, ...]:
        return await self._read("my_org_community_resources", _parse_mapping_read, permissions=permissions, query=q)

    async def my_org_reuses(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return await self._read("my_org_reuses", _parse_mapping_read, permissions=permissions, query=q)

    async def my_org_discussions(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return await self._read("my_org_discussions", _parse_mapping_read, permissions=permissions, query=q)

    async def list_api_tokens(self, permissions: Permissions) -> tuple[ApiTokenMetadata, ...]:
        return await self._read("list_api_tokens", _parse_token_list, permissions=permissions)

    async def list_org_invitations(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return await self._read("list_org_invitations", _parse_mapping_read, permissions=permissions)

    async def list_users(self, permissions: Permissions, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return await self._read("list_users", _parse_user_page, permissions=permissions, query=query or UserListQuery())

    async def get_user(self, user_id: str) -> NativeRecord:
        return await self._read("get_user", _parse_user_read, identifier=user_id)

    async def get_user_contact_point(self, user_id: str, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return await self._read(
            "get_user_contact_point", _parse_user_page, identifier=user_id, query=query or UserListQuery()
        )

    async def suggest_users(self, query: UserSuggestQuery) -> tuple[MappingRecord, ...]:
        return await self._read("suggest_users", _parse_mapping_read, query=query)

    async def user_roles(self) -> tuple[MappingRecord, ...]:
        return await self._read("user_roles", _parse_mapping_read)

    async def my_org_topics(self, permissions: Permissions, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return await self._read(
            "my_org_topics", _parse_user_page, permissions=permissions, query=query or UserListQuery()
        )

    async def list_user_followers(self, user_id: str, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return await self._read(
            "list_user_followers", _parse_user_page, identifier=user_id, query=query or UserListQuery()
        )

    async def update_me(
        self, client_input: UserUpdateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write("update_me", body=client_input, permissions=permissions, mutation_policy=mutation_policy),
        )

    async def delete_me(self, permissions: Permissions, mutation_policy: Policy = None) -> UserMutationResult:
        return cast(
            UserMutationResult, await self._write("delete_me", permissions=permissions, mutation_policy=mutation_policy)
        )

    async def my_avatar(
        self, client_input: UserAvatarInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write("my_avatar", body=client_input, permissions=permissions, mutation_policy=mutation_policy),
        )

    async def create_api_token(
        self, client_input: ApiTokenCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> ApiTokenCreationResult:
        return cast(
            ApiTokenCreationResult,
            await self._write(
                "create_api_token", body=client_input, permissions=permissions, mutation_policy=mutation_policy
            ),
        )

    async def revoke_api_token(
        self, token_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "revoke_api_token", identifier=token_id, permissions=permissions, mutation_policy=mutation_policy
            ),
        )

    async def accept_org_invitation(
        self, invitation_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "accept_org_invitation",
                identifier=invitation_id,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    async def refuse_org_invitation(
        self, invitation_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "refuse_org_invitation",
                identifier=invitation_id,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    async def create_user(
        self, client_input: UserCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "create_user", body=client_input, permissions=permissions, mutation_policy=mutation_policy
            ),
        )

    async def user_avatar(
        self, user_id: str, client_input: UserAvatarInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "user_avatar",
                identifier=user_id,
                body=client_input,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    async def update_user(
        self, user_id: str, client_input: UserUpdateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "update_user",
                identifier=user_id,
                body=client_input,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    async def delete_user(
        self,
        user_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
        options: UserDeleteOptions | None = None,
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "delete_user",
                identifier=user_id,
                query=options,
                permissions=permissions,
                mutation_policy=mutation_policy,
            ),
        )

    async def rotate_user_password(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "rotate_user_password", identifier=user_id, permissions=permissions, mutation_policy=mutation_policy
            ),
        )

    async def follow_user(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "follow_user", identifier=user_id, permissions=permissions, mutation_policy=mutation_policy
            ),
        )

    async def unfollow_user(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                "unfollow_user", identifier=user_id, permissions=permissions, mutation_policy=mutation_policy
            ),
        )
