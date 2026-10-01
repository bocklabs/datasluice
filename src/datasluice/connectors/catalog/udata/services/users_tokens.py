"""Typed sync and async users, invitations, and API-token services."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
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
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import users as wire
from datasluice.connectors.catalog.udata.wire.organizations import parse_page
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
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
from .organizations_memberships import _attach, _org_receipt
from .taxonomies import Request, Response

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

type Permissions = EffectivePermissions
type Policy = MutationPolicy | None
type Result = UserMutationResult | ApiTokenCreationResult
type Emitter = Callable[[str, str], None]
type Decoder[T] = Callable[[str, object], T]

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


@dataclass(frozen=True, slots=True)
class _ReadRoute:
    name: str
    identifier: str | None = None
    query: object = None
    permissions: Permissions | None = None


@dataclass(frozen=True, slots=True)
class _WriteRoute:
    name: str
    permissions: Permissions
    identifier: str | None = None
    body: object = None
    query: object = None
    mutation_policy: Policy = None


@dataclass(slots=True)
class _WriteState:
    operation: str
    target: str
    upload: UserAvatarInput | None
    response: RuntimeResponse | None = None
    payload: object = None
    result: Result | None = None
    primary_error: BaseException | None = None


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
        except SETTLEMENT_ERRORS:
            secret.discard()
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


def _escalates_privilege(name: str, body: object) -> bool:
    return (
        name in {"update_me", "update_user"}
        and isinstance(body, UserUpdateInput)
        and bool({"roles", "active"} & set(body.fields))
    )


def _discard_token_plaintext(payload: object, response: RuntimeResponse | None) -> RuntimeResponse | None:
    if isinstance(payload, dict):
        payload.pop("token", None)
    return RuntimeResponse(status_code=response.status_code, headers={}, body=b"") if response is not None else None


def _error_receipt(
    error: BaseException, name: str, target: str, policy: Policy, response: RuntimeResponse | None
) -> None:
    operation = wire.OPERATIONS[name]
    outcome = "ambiguous" if isinstance(error, KeyboardInterrupt) else _mutation_outcome(error, response)
    receipt = _org_receipt(
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
    except SETTLEMENT_ERRORS as close_error:
        receipt = result.receipt if result is not None else getattr(primary_error, "mutation_receipt", None)
        if isinstance(receipt, MutationReceipt):
            _attach(close_error, receipt)
        if primary_error is not None:
            raise primary_error from close_error
        raise


def _permission_evidence(
    name: str, operation: str, resolved: object, permissions: Permissions | None
) -> UDataCredential:
    return _require_mutation_permission(resolved, operation, permissions, admin=name in _ADMIN)


def _read_call[R](
    call: Callable[..., R],
    operation: str,
    route: tuple[str, str, dict[str, str]],
    permissions: Permissions | None,
    credential: object | None,
) -> R:
    method, path, headers = route
    return call(
        method=method,
        path=path,
        headers=headers,
        owning_operation=operation,
        permissions=permissions,
        credential=credential,
        emit_success=False,
    )


def _read_settled[T](
    call: Callable[..., Response],
    resolve: Callable[[], object],
    emit: Emitter,
    route: _ReadRoute,
    decoder: Decoder[T],
) -> T:
    operation = wire.OPERATIONS[route.name]
    try:
        credential = None
        if route.name not in _PUBLIC:
            credential = _permission_evidence(route.name, operation, resolve(), route.permissions)
        method, path, headers, _ = wire.build_request(route.name, identifier=route.identifier, query=route.query)
        _, payload, _ = _read_call(call, operation, (method, path, headers), route.permissions, credential)
        result = decoder(route.name, payload)
    except (Exception, KeyboardInterrupt):
        emit(operation, "failed")
        raise
    emit(operation, "succeeded")
    return result


async def _read_settled_async[T](
    call: Callable[..., Awaitable[Response]],
    resolve: Callable[[], Awaitable[object]],
    emit: Emitter,
    route: _ReadRoute,
    decoder: Decoder[T],
) -> T:
    operation = wire.OPERATIONS[route.name]
    try:
        credential = None
        if route.name not in _PUBLIC:
            credential = _permission_evidence(route.name, operation, await resolve(), route.permissions)
        method, path, headers, _ = wire.build_request(route.name, identifier=route.identifier, query=route.query)
        _, payload, _ = await _read_call(call, operation, (method, path, headers), route.permissions, credential)
        result = decoder(route.name, payload)
    except (Exception, asyncio.CancelledError):
        emit(operation, "failed")
        raise
    emit(operation, "succeeded")
    return result


def _write_state(route: _WriteRoute) -> _WriteState:
    return _WriteState(
        operation=wire.OPERATIONS[route.name],
        target=_target(route.name, route.identifier, route.body),
        upload=route.body if isinstance(route.body, UserAvatarInput) else None,
    )


def _write_request(route: _WriteRoute, resolved: object) -> tuple[UDataCredential, Request]:
    operation = wire.OPERATIONS[route.name]
    credential = _permission_evidence(route.name, operation, resolved, route.permissions)
    if _escalates_privilege(route.name, route.body):
        _require_mutation_permission(credential, operation, route.permissions, admin=True)
    method, path, headers, encoded = wire.build_request(
        route.name,
        identifier=route.identifier,
        query=route.query,
        body=None if isinstance(route.body, UserAvatarInput) else route.body,
    )
    return credential, (method, path, headers, encoded)


def _write_call[R](
    call: Callable[..., R],
    credential: UDataCredential,
    route: _WriteRoute,
    request: Request,
    upload: UserAvatarInput | None,
) -> R:
    method, path, headers, encoded = request
    policy = route.mutation_policy
    return call(
        method=method,
        path=path,
        headers=headers,
        owning_operation=wire.OPERATIONS[route.name],
        json_body=encoded,
        permissions=route.permissions,
        credential=credential,
        idempotency_policy=policy.idempotency if policy else None,
        emit_success=False,
        files=(upload.part(),) if upload else (),
    )


def _write_success(state: _WriteState, route: _WriteRoute, status: int, emit: Emitter) -> Result:
    receipt = _org_receipt(
        state.operation,
        _receipt_target(route.name, state.target, state.payload),
        route.mutation_policy,
        "succeeded",
        status,
        route.name,
        kind=ResourceKind("api-token" if "api_token" in route.name else "user"),
    )
    state.result = _shape_mutation(state.payload, receipt, route.name)
    emit(state.operation, "succeeded")
    return state.result


def _write_failure(state: _WriteState, route: _WriteRoute, error: BaseException, emit: Emitter) -> None:
    state.primary_error = error
    if route.name == "create_api_token":
        state.response = _discard_token_plaintext(state.payload, state.response)
        if isinstance(state.result, ApiTokenCreationResult):
            state.result.secret.discard()
            state.result = None
    emit(state.operation, "failed")
    _error_receipt(
        error,
        route.name,
        _receipt_target(route.name, state.target, state.payload),
        route.mutation_policy,
        state.response,
    )


def _write_settled(
    call: Callable[..., Response],
    resolve: Callable[[], object],
    emit: Emitter,
    route: _WriteRoute,
) -> Result:
    state = _write_state(route)
    try:
        _enforce_mutation_policy(
            state.operation, state.target, route.mutation_policy, destructive=route.name in _DESTRUCTIVE
        )
        credential, request = _write_request(route, resolve())
        status, state.payload, state.response = _write_call(call, credential, route, request, state.upload)
        if route.name == "create_api_token":
            state.response = RuntimeResponse(status_code=status, headers={}, body=b"")
        return _write_success(state, route, status, emit)
    except SETTLEMENT_ERRORS as error:
        _write_failure(state, route, error, emit)
        raise
    finally:
        _close_avatar(state.upload, state.result, state.primary_error)


async def _write_settled_async(
    call: Callable[..., Awaitable[Response]],
    resolve: Callable[[], Awaitable[object]],
    emit: Emitter,
    route: _WriteRoute,
) -> Result:
    state = _write_state(route)
    try:
        _enforce_mutation_policy(
            state.operation, state.target, route.mutation_policy, destructive=route.name in _DESTRUCTIVE
        )
        credential, request = _write_request(route, await resolve())
        status, state.payload, state.response = await _write_call(call, credential, route, request, state.upload)
        if route.name == "create_api_token":
            state.response = RuntimeResponse(status_code=status, headers={}, body=b"")
        return _write_success(state, route, status, emit)
    except ASYNC_SETTLEMENT_ERRORS as error:
        _write_failure(state, route, error, emit)
        raise
    finally:
        _close_avatar(state.upload, state.result, state.primary_error)


class SyncUsersTokensService:
    """Named synchronous stock user-family routes."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def get_me(self, permissions: Permissions) -> NativeRecord:
        return self._read(_ReadRoute("get_me", permissions=permissions), _parse_user_read)

    def my_reuses(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return self._read(_ReadRoute("my_reuses", permissions=permissions), _parse_mapping_read)

    def my_datasets(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return self._read(_ReadRoute("my_datasets", permissions=permissions), _parse_mapping_read)

    def my_metrics(self, permissions: Permissions) -> MappingRecord:
        return self._read(_ReadRoute("my_metrics", permissions=permissions), _parse_metrics)

    def my_org_datasets(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return self._read(_ReadRoute("my_org_datasets", permissions=permissions, query=q), _parse_mapping_read)

    def my_org_community_resources(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return self._read(
            _ReadRoute("my_org_community_resources", permissions=permissions, query=q), _parse_mapping_read
        )

    def my_org_reuses(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return self._read(_ReadRoute("my_org_reuses", permissions=permissions, query=q), _parse_mapping_read)

    def my_org_discussions(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return self._read(_ReadRoute("my_org_discussions", permissions=permissions, query=q), _parse_mapping_read)

    def list_api_tokens(self, permissions: Permissions) -> tuple[ApiTokenMetadata, ...]:
        return self._read(_ReadRoute("list_api_tokens", permissions=permissions), _parse_token_list)

    def list_org_invitations(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return self._read(_ReadRoute("list_org_invitations", permissions=permissions), _parse_mapping_read)

    def list_users(self, permissions: Permissions, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return self._read(
            _ReadRoute("list_users", permissions=permissions, query=query or UserListQuery()), _parse_user_page
        )

    def get_user(self, user_id: str) -> NativeRecord:
        return self._read(_ReadRoute("get_user", identifier=user_id), _parse_user_read)

    def get_user_contact_point(self, user_id: str, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return self._read(
            _ReadRoute("get_user_contact_point", identifier=user_id, query=query or UserListQuery()),
            _parse_user_page,
        )

    def suggest_users(self, query: UserSuggestQuery) -> tuple[MappingRecord, ...]:
        return self._read(_ReadRoute("suggest_users", query=query), _parse_mapping_read)

    def user_roles(self) -> tuple[MappingRecord, ...]:
        return self._read(_ReadRoute("user_roles"), _parse_mapping_read)

    def my_org_topics(self, permissions: Permissions, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return self._read(
            _ReadRoute("my_org_topics", permissions=permissions, query=query or UserListQuery()), _parse_user_page
        )

    def list_user_followers(self, user_id: str, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return self._read(
            _ReadRoute("list_user_followers", identifier=user_id, query=query or UserListQuery()),
            _parse_user_page,
        )

    def update_me(
        self, client_input: UserUpdateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(_WriteRoute("update_me", permissions, body=client_input, mutation_policy=mutation_policy)),
        )

    def delete_me(self, permissions: Permissions, mutation_policy: Policy = None) -> UserMutationResult:
        return cast(
            UserMutationResult, self._write(_WriteRoute("delete_me", permissions, mutation_policy=mutation_policy))
        )

    def my_avatar(
        self, client_input: UserAvatarInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(_WriteRoute("my_avatar", permissions, body=client_input, mutation_policy=mutation_policy)),
        )

    def create_api_token(
        self, client_input: ApiTokenCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> ApiTokenCreationResult:
        return cast(
            ApiTokenCreationResult,
            self._write(
                _WriteRoute("create_api_token", permissions, body=client_input, mutation_policy=mutation_policy)
            ),
        )

    def revoke_api_token(
        self, token_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                _WriteRoute("revoke_api_token", permissions, identifier=token_id, mutation_policy=mutation_policy)
            ),
        )

    def accept_org_invitation(
        self, invitation_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                _WriteRoute(
                    "accept_org_invitation",
                    permissions,
                    identifier=invitation_id,
                    mutation_policy=mutation_policy,
                )
            ),
        )

    def refuse_org_invitation(
        self, invitation_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                _WriteRoute(
                    "refuse_org_invitation",
                    permissions,
                    identifier=invitation_id,
                    mutation_policy=mutation_policy,
                )
            ),
        )

    def create_user(
        self, client_input: UserCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(_WriteRoute("create_user", permissions, body=client_input, mutation_policy=mutation_policy)),
        )

    def user_avatar(
        self, user_id: str, client_input: UserAvatarInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                _WriteRoute(
                    "user_avatar",
                    permissions,
                    identifier=user_id,
                    body=client_input,
                    mutation_policy=mutation_policy,
                )
            ),
        )

    def update_user(
        self, user_id: str, client_input: UserUpdateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                _WriteRoute(
                    "update_user",
                    permissions,
                    identifier=user_id,
                    body=client_input,
                    mutation_policy=mutation_policy,
                )
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
                _WriteRoute(
                    "delete_user",
                    permissions,
                    identifier=user_id,
                    query=options,
                    mutation_policy=mutation_policy,
                )
            ),
        )

    def rotate_user_password(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(
                _WriteRoute("rotate_user_password", permissions, identifier=user_id, mutation_policy=mutation_policy)
            ),
        )

    def follow_user(self, user_id: str, permissions: Permissions, mutation_policy: Policy = None) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(_WriteRoute("follow_user", permissions, identifier=user_id, mutation_policy=mutation_policy)),
        )

    def unfollow_user(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            self._write(_WriteRoute("unfollow_user", permissions, identifier=user_id, mutation_policy=mutation_policy)),
        )

    def _read[T](self, route: _ReadRoute, decoder: Decoder[T]) -> T:
        return _read_settled(
            self._client._dataset_call, self._client._resolved_credential, self._client._emit, route, decoder
        )

    def _write(self, route: _WriteRoute) -> Result:
        return _write_settled(self._client._dataset_call, self._client._resolved_credential, self._client._emit, route)


class AsyncUsersTokensService:
    """Named asynchronous stock user-family routes."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def get_me(self, permissions: Permissions) -> NativeRecord:
        return await self._read(_ReadRoute("get_me", permissions=permissions), _parse_user_read)

    async def my_reuses(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return await self._read(_ReadRoute("my_reuses", permissions=permissions), _parse_mapping_read)

    async def my_datasets(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return await self._read(_ReadRoute("my_datasets", permissions=permissions), _parse_mapping_read)

    async def my_metrics(self, permissions: Permissions) -> MappingRecord:
        return await self._read(_ReadRoute("my_metrics", permissions=permissions), _parse_metrics)

    async def my_org_datasets(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return await self._read(_ReadRoute("my_org_datasets", permissions=permissions, query=q), _parse_mapping_read)

    async def my_org_community_resources(
        self, permissions: Permissions, q: str | None = None
    ) -> tuple[MappingRecord, ...]:
        return await self._read(
            _ReadRoute("my_org_community_resources", permissions=permissions, query=q), _parse_mapping_read
        )

    async def my_org_reuses(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return await self._read(_ReadRoute("my_org_reuses", permissions=permissions, query=q), _parse_mapping_read)

    async def my_org_discussions(self, permissions: Permissions, q: str | None = None) -> tuple[MappingRecord, ...]:
        return await self._read(_ReadRoute("my_org_discussions", permissions=permissions, query=q), _parse_mapping_read)

    async def list_api_tokens(self, permissions: Permissions) -> tuple[ApiTokenMetadata, ...]:
        return await self._read(_ReadRoute("list_api_tokens", permissions=permissions), _parse_token_list)

    async def list_org_invitations(self, permissions: Permissions) -> tuple[MappingRecord, ...]:
        return await self._read(_ReadRoute("list_org_invitations", permissions=permissions), _parse_mapping_read)

    async def list_users(self, permissions: Permissions, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return await self._read(
            _ReadRoute("list_users", permissions=permissions, query=query or UserListQuery()), _parse_user_page
        )

    async def get_user(self, user_id: str) -> NativeRecord:
        return await self._read(_ReadRoute("get_user", identifier=user_id), _parse_user_read)

    async def get_user_contact_point(self, user_id: str, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return await self._read(
            _ReadRoute("get_user_contact_point", identifier=user_id, query=query or UserListQuery()),
            _parse_user_page,
        )

    async def suggest_users(self, query: UserSuggestQuery) -> tuple[MappingRecord, ...]:
        return await self._read(_ReadRoute("suggest_users", query=query), _parse_mapping_read)

    async def user_roles(self) -> tuple[MappingRecord, ...]:
        return await self._read(_ReadRoute("user_roles"), _parse_mapping_read)

    async def my_org_topics(self, permissions: Permissions, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return await self._read(
            _ReadRoute("my_org_topics", permissions=permissions, query=query or UserListQuery()), _parse_user_page
        )

    async def list_user_followers(self, user_id: str, query: UserListQuery | None = None) -> UDataPageEnvelope:
        return await self._read(
            _ReadRoute("list_user_followers", identifier=user_id, query=query or UserListQuery()),
            _parse_user_page,
        )

    async def update_me(
        self, client_input: UserUpdateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute("update_me", permissions, body=client_input, mutation_policy=mutation_policy)
            ),
        )

    async def delete_me(self, permissions: Permissions, mutation_policy: Policy = None) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(_WriteRoute("delete_me", permissions, mutation_policy=mutation_policy)),
        )

    async def my_avatar(
        self, client_input: UserAvatarInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute("my_avatar", permissions, body=client_input, mutation_policy=mutation_policy)
            ),
        )

    async def create_api_token(
        self, client_input: ApiTokenCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> ApiTokenCreationResult:
        return cast(
            ApiTokenCreationResult,
            await self._write(
                _WriteRoute("create_api_token", permissions, body=client_input, mutation_policy=mutation_policy)
            ),
        )

    async def revoke_api_token(
        self, token_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute("revoke_api_token", permissions, identifier=token_id, mutation_policy=mutation_policy)
            ),
        )

    async def accept_org_invitation(
        self, invitation_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute(
                    "accept_org_invitation",
                    permissions,
                    identifier=invitation_id,
                    mutation_policy=mutation_policy,
                )
            ),
        )

    async def refuse_org_invitation(
        self, invitation_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute(
                    "refuse_org_invitation",
                    permissions,
                    identifier=invitation_id,
                    mutation_policy=mutation_policy,
                )
            ),
        )

    async def create_user(
        self, client_input: UserCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute("create_user", permissions, body=client_input, mutation_policy=mutation_policy)
            ),
        )

    async def user_avatar(
        self, user_id: str, client_input: UserAvatarInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute(
                    "user_avatar",
                    permissions,
                    identifier=user_id,
                    body=client_input,
                    mutation_policy=mutation_policy,
                )
            ),
        )

    async def update_user(
        self, user_id: str, client_input: UserUpdateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute(
                    "update_user",
                    permissions,
                    identifier=user_id,
                    body=client_input,
                    mutation_policy=mutation_policy,
                )
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
                _WriteRoute(
                    "delete_user",
                    permissions,
                    identifier=user_id,
                    query=options,
                    mutation_policy=mutation_policy,
                )
            ),
        )

    async def rotate_user_password(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute("rotate_user_password", permissions, identifier=user_id, mutation_policy=mutation_policy)
            ),
        )

    async def follow_user(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute("follow_user", permissions, identifier=user_id, mutation_policy=mutation_policy)
            ),
        )

    async def unfollow_user(
        self, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> UserMutationResult:
        return cast(
            UserMutationResult,
            await self._write(
                _WriteRoute("unfollow_user", permissions, identifier=user_id, mutation_policy=mutation_policy)
            ),
        )

    async def _read[T](self, route: _ReadRoute, decoder: Decoder[T]) -> T:
        return await _read_settled_async(
            self._client._dataset_call_async,
            self._client._resolved_credential_async,
            self._client._emit,
            route,
            decoder,
        )

    async def _write(self, route: _WriteRoute) -> Result:
        return await _write_settled_async(
            self._client._dataset_call_async,
            self._client._resolved_credential_async,
            self._client._emit,
            route,
        )
