"""Dual-mode uData activity and discussion service."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.activity_discussions import (
    ActivityQuery,
    CommentInput,
    DiscussionCreateInput,
    DiscussionMutationResult,
    DiscussionSearchQuery,
    DiscussionUpdateInput,
)
from datasluice.connectors.catalog.udata.services.resources import _attach, _receipt
from datasluice.connectors.catalog.udata.wire import activity_discussions as wire
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import NativeCatalogError

from .datasets import _enforce_mutation_policy, _error_status, _mutation_outcome, _require_mutation_permission

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

type Permissions = EffectivePermissions
type Policy = MutationPolicy | None
type Response = tuple[int, object, object]
type Request = tuple[str, str, dict[str, str], object]


def _discussion_receipt(target: str, policy: Policy, outcome: str, status: int, mutation: str, operation: str):
    return _receipt(
        policy,
        target,
        outcome,
        status,
        mutation,
        resource_kind=ResourceKind.RESOURCE,
        operation=operation,
    )


def _reject(error: BaseException, target: str, policy: Policy, mutation: str, operation: str) -> None:
    _attach(error, _discussion_receipt(target, policy, "rejected", _error_status(error), mutation, operation))
    raise error


def _mutation(
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    request: Callable[[], Request],
    dispatch: Callable[[str, str, dict[str, str], object], Response],
) -> DiscussionMutationResult:
    try:
        method, path, headers, body = request()
    except (Exception, KeyboardInterrupt, GeneratorExit) as error:
        _reject(error, target, policy, mutation, operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=mutation == "deleted")
        status, payload, response = dispatch(method, path, headers, body)
        result = DiscussionMutationResult(
            _discussion_receipt(target, policy, "succeeded", status, mutation, operation),
            MappingRecord(payload) if isinstance(payload, Mapping) and payload else None,
        )
    except (Exception, asyncio.CancelledError, KeyboardInterrupt, GeneratorExit) as error:
        outcome = (
            "cancelled"
            if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError))
            else _mutation_outcome(error, response)
        )
        receipt = _discussion_receipt(target, policy, outcome, _error_status(error, response), mutation, operation)
        _attach(error, receipt)
        raise
    return result


async def _mutation_async(
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    request: Callable[[], Request],
    dispatch: Callable[[str, str, dict[str, str], object], Awaitable[Response]],
) -> DiscussionMutationResult:
    try:
        method, path, headers, body = request()
    except (Exception, KeyboardInterrupt, GeneratorExit) as error:
        _reject(error, target, policy, mutation, operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=mutation == "deleted")
        status, payload, response = await dispatch(method, path, headers, body)
        result = DiscussionMutationResult(
            _discussion_receipt(target, policy, "succeeded", status, mutation, operation),
            MappingRecord(payload) if isinstance(payload, Mapping) and payload else None,
        )
    except (Exception, asyncio.CancelledError, KeyboardInterrupt, GeneratorExit) as error:
        outcome = (
            "cancelled"
            if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError))
            else _mutation_outcome(error, response)
        )
        receipt = _discussion_receipt(target, policy, outcome, _error_status(error, response), mutation, operation)
        _attach(error, receipt)
        raise
    return result


class SyncActivityDiscussionsService:
    """Typed synchronous activity and discussion operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def activity(self, query: ActivityQuery) -> MappingRecord:
        return self._read(wire.activity_request(query), wire.ACTIVITY_OPERATION, wire.parse_mapping)

    def list_discussions(self) -> MappingRecord:
        return self._read(wire.list_discussions_request(), wire.LIST_DISCUSSIONS_OPERATION, wire.parse_mapping)

    def get_discussion(self, discussion_id: str) -> MappingRecord:
        return self._read(wire.get_discussion_request(discussion_id), wire.GET_DISCUSSION_OPERATION, wire.parse_mapping)

    def search_discussions(self, query: DiscussionSearchQuery) -> MappingRecord:
        return self._read(wire.search_discussions_request(query), wire.SEARCH_DISCUSSIONS_OPERATION, wire.parse_mapping)

    def create_discussion(
        self,
        client_input: DiscussionCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        subject = str(client_input.subject.get("id", ""))
        return _mutation(
            subject,
            mutation_policy,
            "created",
            wire.CREATE_DISCUSSION_OPERATION,
            lambda: wire.create_discussion_request(client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.CREATE_DISCUSSION_OPERATION
            ),
        )

    def comment_discussion(
        self,
        discussion_id: str,
        client_input: CommentInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return _mutation(
            discussion_id,
            mutation_policy,
            "commented",
            wire.COMMENT_DISCUSSION_OPERATION,
            lambda: wire.comment_discussion_request(discussion_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.COMMENT_DISCUSSION_OPERATION
            ),
        )

    def update_discussion(
        self,
        discussion_id: str,
        client_input: DiscussionUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return _mutation(
            discussion_id,
            mutation_policy,
            "updated",
            wire.UPDATE_DISCUSSION_OPERATION,
            lambda: wire.update_discussion_request(discussion_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UPDATE_DISCUSSION_OPERATION
            ),
        )

    def delete_discussion(
        self, discussion_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DiscussionMutationResult:
        return _mutation(
            discussion_id,
            mutation_policy,
            "deleted",
            wire.DELETE_DISCUSSION_OPERATION,
            lambda: wire.delete_discussion_request(discussion_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_DISCUSSION_OPERATION
            ),
        )

    def edit_discussion_comment(
        self,
        discussion_id: str,
        comment_id: str,
        client_input: CommentInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return _mutation(
            f"{discussion_id}:{comment_id}",
            mutation_policy,
            "edited",
            wire.EDIT_DISCUSSION_COMMENT_OPERATION,
            lambda: wire.edit_discussion_comment_request(discussion_id, comment_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.EDIT_DISCUSSION_COMMENT_OPERATION
            ),
        )

    def delete_discussion_comment(
        self,
        discussion_id: str,
        comment_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return _mutation(
            f"{discussion_id}:{comment_id}",
            mutation_policy,
            "deleted",
            wire.DELETE_DISCUSSION_COMMENT_OPERATION,
            lambda: wire.delete_discussion_comment_request(discussion_id, comment_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_DISCUSSION_COMMENT_OPERATION
            ),
        )

    def _read(
        self,
        request: tuple[str, str, dict[str, str], object],
        operation: str,
        parser: Callable[[object, str], MappingRecord],
    ) -> MappingRecord:
        method, path, headers, body = request
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, headers=headers, json_body=body
        )
        return parser(payload, operation)

    def _mutate(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: object,
        permissions: Permissions,
        policy: Policy,
        operation: str,
    ) -> Response:
        resolved = _require_mutation_permission(self._client._resolved_credential(), operation, permissions)
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
        )


class AsyncActivityDiscussionsService:
    """Typed asynchronous activity and discussion operations."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def activity(self, query: ActivityQuery) -> MappingRecord:
        return await self._read(wire.activity_request(query), wire.ACTIVITY_OPERATION, wire.parse_mapping)

    async def list_discussions(self) -> MappingRecord:
        return await self._read(wire.list_discussions_request(), wire.LIST_DISCUSSIONS_OPERATION, wire.parse_mapping)

    async def get_discussion(self, discussion_id: str) -> MappingRecord:
        return await self._read(
            wire.get_discussion_request(discussion_id), wire.GET_DISCUSSION_OPERATION, wire.parse_mapping
        )

    async def search_discussions(self, query: DiscussionSearchQuery) -> MappingRecord:
        return await self._read(
            wire.search_discussions_request(query), wire.SEARCH_DISCUSSIONS_OPERATION, wire.parse_mapping
        )

    async def create_discussion(
        self,
        client_input: DiscussionCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return await _mutation_async(
            str(client_input.subject.get("id", "")),
            mutation_policy,
            "created",
            wire.CREATE_DISCUSSION_OPERATION,
            lambda: wire.create_discussion_request(client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.CREATE_DISCUSSION_OPERATION
            ),
        )

    async def comment_discussion(
        self,
        discussion_id: str,
        client_input: CommentInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return await _mutation_async(
            discussion_id,
            mutation_policy,
            "commented",
            wire.COMMENT_DISCUSSION_OPERATION,
            lambda: wire.comment_discussion_request(discussion_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.COMMENT_DISCUSSION_OPERATION
            ),
        )

    async def update_discussion(
        self,
        discussion_id: str,
        client_input: DiscussionUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return await _mutation_async(
            discussion_id,
            mutation_policy,
            "updated",
            wire.UPDATE_DISCUSSION_OPERATION,
            lambda: wire.update_discussion_request(discussion_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UPDATE_DISCUSSION_OPERATION
            ),
        )

    async def delete_discussion(
        self, discussion_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DiscussionMutationResult:
        return await _mutation_async(
            discussion_id,
            mutation_policy,
            "deleted",
            wire.DELETE_DISCUSSION_OPERATION,
            lambda: wire.delete_discussion_request(discussion_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_DISCUSSION_OPERATION
            ),
        )

    async def edit_discussion_comment(
        self,
        discussion_id: str,
        comment_id: str,
        client_input: CommentInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return await _mutation_async(
            f"{discussion_id}:{comment_id}",
            mutation_policy,
            "edited",
            wire.EDIT_DISCUSSION_COMMENT_OPERATION,
            lambda: wire.edit_discussion_comment_request(discussion_id, comment_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.EDIT_DISCUSSION_COMMENT_OPERATION
            ),
        )

    async def delete_discussion_comment(
        self,
        discussion_id: str,
        comment_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DiscussionMutationResult:
        return await _mutation_async(
            f"{discussion_id}:{comment_id}",
            mutation_policy,
            "deleted",
            wire.DELETE_DISCUSSION_COMMENT_OPERATION,
            lambda: wire.delete_discussion_comment_request(discussion_id, comment_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_DISCUSSION_COMMENT_OPERATION
            ),
        )

    async def _read(
        self,
        request: tuple[str, str, dict[str, str], object],
        operation: str,
        parser: Callable[[object, str], MappingRecord],
    ) -> MappingRecord:
        method, path, headers, body = request
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, headers=headers, json_body=body
        )
        return parser(payload, operation)

    async def _mutate(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: object,
        permissions: Permissions,
        policy: Policy,
        operation: str,
    ) -> Response:
        resolved = _require_mutation_permission(await self._client._resolved_credential_async(), operation, permissions)
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
        )
