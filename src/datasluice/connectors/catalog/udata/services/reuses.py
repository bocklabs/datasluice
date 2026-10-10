"""Dual-mode uData reuse and reuse-follower service."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.udata.models.reuses import (
    ReuseCreateInput,
    ReuseFollowersQuery,
    ReuseImageInput,
    ReuseListQuery,
    ReuseMutationResult,
    ReuseSearchQuery,
    ReuseSuggestQuery,
    ReuseUpdateInput,
    linked_identifier,
)
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import reuses as wire
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.errors.catalog import NativeCatalogError

from .datasets import _header
from .posts_reports import _record_id
from .taxonomies import (
    AsyncCatalogService,
    Permissions,
    Policy,
    Request,
    Response,
    SupportsUpload,
    SyncCatalogService,
    _dataset_upload,
    _run_mutation,
    _run_mutation_async,
)

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
    from datasluice.domain.catalog.models import MappingRecord

_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    ReuseMutationResult,
    ResourceKind.RESOURCE,
)
_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    ReuseMutationResult,
    ResourceKind.RESOURCE,
)


class SyncReusesService(SyncCatalogService):
    """Typed synchronous reuse and reuse-follower operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def list_reuses(self, query: ReuseListQuery | None = None) -> MappingRecord:
        return self._read(
            wire.list_reuses_request(query or ReuseListQuery()), wire.LIST_REUSES_OPERATION, wire.parse_mapping
        )

    def create_reuse(
        self,
        client_input: ReuseCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return _mutation(
            client_input.title,
            mutation_policy,
            "created",
            wire.CREATE_REUSE_OPERATION,
            lambda: wire.create_reuse_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_REUSE_OPERATION),
            success_target=lambda payload: _record_id(payload, client_input.title),
        )

    def recent_reuses_atom_feed(self, query: ReuseListQuery | None = None) -> MappingRecord:
        operation = wire.RECENT_REUSES_ATOM_FEED_OPERATION
        method, path, _, _ = wire.recent_reuses_atom_feed_request(query or ReuseListQuery())
        _, text, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(
            cast("bytes", text),
            wire._ATOM_MEDIA_TYPE,
            response_media_type=_header(response.headers, "content-type"),
            operation=operation,
        )

    def get_reuse(self, reuse_id: str) -> MappingRecord:
        return self._read(wire.get_reuse_request(reuse_id), wire.GET_REUSE_OPERATION, wire.parse_mapping)

    def update_reuse(
        self,
        reuse_id: str,
        client_input: ReuseUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.UPDATE_REUSE_OPERATION,
            lambda: wire.update_reuse_request(reuse_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_REUSE_OPERATION),
        )

    def delete_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.DELETE_REUSE_OPERATION,
            lambda: wire.delete_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_REUSE_OPERATION),
            destructive=True,
        )

    def reuse_add_dataset(
        self,
        reuse_id: str,
        dataset_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        linked = linked_identifier(dataset_id, wire.REUSE_ADD_DATASET_OPERATION, "uData reuse dataset")
        return _mutation(
            f"{reuse_id}:{linked}",
            mutation_policy,
            "updated",
            wire.REUSE_ADD_DATASET_OPERATION,
            lambda: wire.reuse_add_dataset_request(reuse_id, linked),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.REUSE_ADD_DATASET_OPERATION),
        )

    def reuse_add_dataservice(
        self,
        reuse_id: str,
        dataservice_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        linked = linked_identifier(dataservice_id, wire.REUSE_ADD_DATASERVICE_OPERATION, "uData reuse dataservice")
        return _mutation(
            f"{reuse_id}:{linked}",
            mutation_policy,
            "updated",
            wire.REUSE_ADD_DATASERVICE_OPERATION,
            lambda: wire.reuse_add_dataservice_request(reuse_id, linked),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.REUSE_ADD_DATASERVICE_OPERATION),
        )

    def available_reuse_badges(self) -> MappingRecord:
        return self._read(
            wire.available_reuse_badges_request(), wire.AVAILABLE_REUSE_BADGES_OPERATION, wire.parse_mapping
        )

    def add_reuse_badge(
        self,
        reuse_id: str,
        badge_kind: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.ADD_REUSE_BADGE_OPERATION,
            lambda: wire.add_reuse_badge_request(reuse_id, badge_kind),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.ADD_REUSE_BADGE_OPERATION),
        )

    def delete_reuse_badge(
        self, reuse_id: str, badge_kind: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.DELETE_REUSE_BADGE_OPERATION,
            lambda: wire.delete_reuse_badge_request(reuse_id, badge_kind),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_REUSE_BADGE_OPERATION),
            destructive=True,
        )

    def feature_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.FEATURE_REUSE_OPERATION,
            lambda: wire.feature_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FEATURE_REUSE_OPERATION),
        )

    def unfeature_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.UNFEATURE_REUSE_OPERATION,
            lambda: wire.unfeature_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFEATURE_REUSE_OPERATION),
            destructive=True,
        )

    def suggest_reuses(self, query: ReuseSuggestQuery) -> tuple[MappingRecord, ...]:
        return self._read(
            wire.suggest_reuses_request(query), wire.SUGGEST_REUSES_OPERATION, wire.parse_mapping_sequence
        )

    def reuse_image(
        self,
        reuse_id: str,
        data: bytes,
        content_type: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.REUSE_IMAGE_OPERATION,
            lambda: (*wire.reuse_image_request(reuse_id), None),
            lambda request: self._mutate_upload(
                request,
                ReuseImageInput(data=data, content_type=content_type),
                permissions,
                mutation_policy,
            ),
        )

    def reuse_types(self) -> tuple[MappingRecord, ...]:
        return self._read(wire.reuse_types_request(), wire.REUSE_TYPES_OPERATION, wire.parse_mapping_sequence)

    def reuse_topics(self) -> tuple[MappingRecord, ...]:
        return self._read(wire.reuse_topics_request(), wire.REUSE_TOPICS_OPERATION, wire.parse_mapping_sequence)

    def search_v2(self, query: ReuseSearchQuery | None = None) -> MappingRecord:
        return self._read(
            wire.search_reuses_v2_request(query or ReuseSearchQuery()),
            wire.SEARCH_REUSES_V2_OPERATION,
            wire.parse_mapping,
        )

    def list_v2(self, query: ReuseListQuery | None = None) -> MappingRecord:
        return self._read(
            wire.list_reuses_v2_request(query or ReuseListQuery()), wire.LIST_REUSES_V2_OPERATION, wire.parse_mapping
        )

    def list_reuse_followers(self, reuse_id: str, query: ReuseFollowersQuery | None = None) -> MappingRecord:
        return self._read(
            wire.list_reuse_followers_request(reuse_id, query or ReuseFollowersQuery()),
            wire.LIST_REUSE_FOLLOWERS_OPERATION,
            wire.parse_mapping,
        )

    def follow_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.FOLLOW_REUSE_OPERATION,
            lambda: wire.follow_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FOLLOW_REUSE_OPERATION),
        )

    def unfollow_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.UNFOLLOW_REUSE_OPERATION,
            lambda: wire.unfollow_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFOLLOW_REUSE_OPERATION),
            destructive=True,
        )

    def _mutate_upload(
        self,
        request: Request,
        image: SupportsUpload,
        permissions: Permissions,
        policy: Policy,
    ) -> Response:
        method, path, headers, _ = request
        return _dataset_upload(
            self._client._dataset_call,
            self._client._resolved_credential(),
            permissions,
            policy,
            wire.REUSE_IMAGE_OPERATION,
            (method, path, headers),
            image,
        )


class AsyncReusesService(AsyncCatalogService):
    """Typed asynchronous reuse and reuse-follower operations."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def list_reuses(self, query: ReuseListQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.list_reuses_request(query or ReuseListQuery()), wire.LIST_REUSES_OPERATION, wire.parse_mapping
        )

    async def create_reuse(
        self,
        client_input: ReuseCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return await _mutation_async(
            client_input.title,
            mutation_policy,
            "created",
            wire.CREATE_REUSE_OPERATION,
            lambda: wire.create_reuse_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_REUSE_OPERATION),
            success_target=lambda payload: _record_id(payload, client_input.title),
        )

    async def recent_reuses_atom_feed(self, query: ReuseListQuery | None = None) -> MappingRecord:
        operation = wire.RECENT_REUSES_ATOM_FEED_OPERATION
        method, path, _, _ = wire.recent_reuses_atom_feed_request(query or ReuseListQuery())
        _, text, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(
            cast("bytes", text),
            wire._ATOM_MEDIA_TYPE,
            response_media_type=_header(response.headers, "content-type"),
            operation=operation,
        )

    async def get_reuse(self, reuse_id: str) -> MappingRecord:
        return await self._read(wire.get_reuse_request(reuse_id), wire.GET_REUSE_OPERATION, wire.parse_mapping)

    async def update_reuse(
        self,
        reuse_id: str,
        client_input: ReuseUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.UPDATE_REUSE_OPERATION,
            lambda: wire.update_reuse_request(reuse_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_REUSE_OPERATION),
        )

    async def delete_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.DELETE_REUSE_OPERATION,
            lambda: wire.delete_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_REUSE_OPERATION),
            destructive=True,
        )

    async def reuse_add_dataset(
        self,
        reuse_id: str,
        dataset_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        linked = linked_identifier(dataset_id, wire.REUSE_ADD_DATASET_OPERATION, "uData reuse dataset")
        return await _mutation_async(
            f"{reuse_id}:{linked}",
            mutation_policy,
            "updated",
            wire.REUSE_ADD_DATASET_OPERATION,
            lambda: wire.reuse_add_dataset_request(reuse_id, linked),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.REUSE_ADD_DATASET_OPERATION),
        )

    async def reuse_add_dataservice(
        self,
        reuse_id: str,
        dataservice_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        linked = linked_identifier(dataservice_id, wire.REUSE_ADD_DATASERVICE_OPERATION, "uData reuse dataservice")
        return await _mutation_async(
            f"{reuse_id}:{linked}",
            mutation_policy,
            "updated",
            wire.REUSE_ADD_DATASERVICE_OPERATION,
            lambda: wire.reuse_add_dataservice_request(reuse_id, linked),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.REUSE_ADD_DATASERVICE_OPERATION),
        )

    async def available_reuse_badges(self) -> MappingRecord:
        return await self._read(
            wire.available_reuse_badges_request(), wire.AVAILABLE_REUSE_BADGES_OPERATION, wire.parse_mapping
        )

    async def add_reuse_badge(
        self,
        reuse_id: str,
        badge_kind: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.ADD_REUSE_BADGE_OPERATION,
            lambda: wire.add_reuse_badge_request(reuse_id, badge_kind),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.ADD_REUSE_BADGE_OPERATION),
        )

    async def delete_reuse_badge(
        self, reuse_id: str, badge_kind: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.DELETE_REUSE_BADGE_OPERATION,
            lambda: wire.delete_reuse_badge_request(reuse_id, badge_kind),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_REUSE_BADGE_OPERATION),
            destructive=True,
        )

    async def feature_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.FEATURE_REUSE_OPERATION,
            lambda: wire.feature_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FEATURE_REUSE_OPERATION),
        )

    async def unfeature_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.UNFEATURE_REUSE_OPERATION,
            lambda: wire.unfeature_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFEATURE_REUSE_OPERATION),
            destructive=True,
        )

    async def suggest_reuses(self, query: ReuseSuggestQuery) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.suggest_reuses_request(query), wire.SUGGEST_REUSES_OPERATION, wire.parse_mapping_sequence
        )

    async def reuse_image(
        self,
        reuse_id: str,
        data: bytes,
        content_type: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.REUSE_IMAGE_OPERATION,
            lambda: (*wire.reuse_image_request(reuse_id), None),
            lambda request: self._mutate_upload(
                request,
                ReuseImageInput(data=data, content_type=content_type),
                permissions,
                mutation_policy,
            ),
        )

    async def reuse_types(self) -> tuple[MappingRecord, ...]:
        return await self._read(wire.reuse_types_request(), wire.REUSE_TYPES_OPERATION, wire.parse_mapping_sequence)

    async def reuse_topics(self) -> tuple[MappingRecord, ...]:
        return await self._read(wire.reuse_topics_request(), wire.REUSE_TOPICS_OPERATION, wire.parse_mapping_sequence)

    async def search_v2(self, query: ReuseSearchQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.search_reuses_v2_request(query or ReuseSearchQuery()),
            wire.SEARCH_REUSES_V2_OPERATION,
            wire.parse_mapping,
        )

    async def list_v2(self, query: ReuseListQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.list_reuses_v2_request(query or ReuseListQuery()), wire.LIST_REUSES_V2_OPERATION, wire.parse_mapping
        )

    async def list_reuse_followers(self, reuse_id: str, query: ReuseFollowersQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.list_reuse_followers_request(reuse_id, query or ReuseFollowersQuery()),
            wire.LIST_REUSE_FOLLOWERS_OPERATION,
            wire.parse_mapping,
        )

    async def follow_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.FOLLOW_REUSE_OPERATION,
            lambda: wire.follow_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FOLLOW_REUSE_OPERATION),
        )

    async def unfollow_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.UNFOLLOW_REUSE_OPERATION,
            lambda: wire.unfollow_reuse_request(reuse_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFOLLOW_REUSE_OPERATION),
            destructive=True,
        )

    async def _mutate_upload(
        self,
        request: Request,
        image: SupportsUpload,
        permissions: Permissions,
        policy: Policy,
    ) -> Response:
        method, path, headers, _ = request
        return await _dataset_upload(
            self._client._dataset_call_async,
            await self._client._resolved_credential_async(),
            permissions,
            policy,
            wire.REUSE_IMAGE_OPERATION,
            (method, path, headers),
            image,
        )
