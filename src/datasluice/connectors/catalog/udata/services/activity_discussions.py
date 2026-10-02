"""Dual-mode uData activity and discussion service."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.activity_discussions import (
    ActivityQuery,
    CommentInput,
    DiscussionCreateInput,
    DiscussionMutationResult,
    DiscussionSearchQuery,
    DiscussionUpdateInput,
)
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import activity_discussions as wire
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import NativeCatalogError

from .taxonomies import (
    AsyncCatalogService,
    Permissions,
    Policy,
    SyncCatalogService,
    _run_mutation,
    _run_mutation_async,
)

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    DiscussionMutationResult,
    ResourceKind.RESOURCE,
)
_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    DiscussionMutationResult,
    ResourceKind.RESOURCE,
)

_DELETING_MUTATION = "deleted"


class SyncActivityDiscussionsService(SyncCatalogService):
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
        return _mutation(
            str(client_input.subject.get("id", "")),
            mutation_policy,
            "created",
            wire.CREATE_DISCUSSION_OPERATION,
            lambda: wire.create_discussion_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_DISCUSSION_OPERATION),
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
            lambda request: self._mutate(request, permissions, mutation_policy, wire.COMMENT_DISCUSSION_OPERATION),
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
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_DISCUSSION_OPERATION),
        )

    def delete_discussion(
        self, discussion_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DiscussionMutationResult:
        return _mutation(
            discussion_id,
            mutation_policy,
            _DELETING_MUTATION,
            wire.DELETE_DISCUSSION_OPERATION,
            lambda: wire.delete_discussion_request(discussion_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_DISCUSSION_OPERATION),
            destructive=True,
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
            lambda request: self._mutate(request, permissions, mutation_policy, wire.EDIT_DISCUSSION_COMMENT_OPERATION),
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
            _DELETING_MUTATION,
            wire.DELETE_DISCUSSION_COMMENT_OPERATION,
            lambda: wire.delete_discussion_comment_request(discussion_id, comment_id),
            lambda request: self._mutate(
                request, permissions, mutation_policy, wire.DELETE_DISCUSSION_COMMENT_OPERATION
            ),
            destructive=True,
        )


class AsyncActivityDiscussionsService(AsyncCatalogService):
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
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_DISCUSSION_OPERATION),
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
            lambda request: self._mutate(request, permissions, mutation_policy, wire.COMMENT_DISCUSSION_OPERATION),
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
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_DISCUSSION_OPERATION),
        )

    async def delete_discussion(
        self, discussion_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DiscussionMutationResult:
        return await _mutation_async(
            discussion_id,
            mutation_policy,
            _DELETING_MUTATION,
            wire.DELETE_DISCUSSION_OPERATION,
            lambda: wire.delete_discussion_request(discussion_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_DISCUSSION_OPERATION),
            destructive=True,
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
            lambda request: self._mutate(request, permissions, mutation_policy, wire.EDIT_DISCUSSION_COMMENT_OPERATION),
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
            _DELETING_MUTATION,
            wire.DELETE_DISCUSSION_COMMENT_OPERATION,
            lambda: wire.delete_discussion_comment_request(discussion_id, comment_id),
            lambda request: self._mutate(
                request, permissions, mutation_policy, wire.DELETE_DISCUSSION_COMMENT_OPERATION
            ),
            destructive=True,
        )
