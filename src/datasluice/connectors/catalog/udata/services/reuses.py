"""Dual-mode uData reuse and reuse-follower service."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
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
)
from datasluice.connectors.catalog.udata.services.resources import _attach, _receipt
from datasluice.connectors.catalog.udata.wire import reuses as wire
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


def _reuse_receipt(target: str, policy: Policy, outcome: str, status: int, mutation: str, operation: str):
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
    _attach(error, _reuse_receipt(target, policy, "rejected", _error_status(error), mutation, operation))
    raise error


def _mutation(
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    destructive: bool,
    request: Callable[[], Request],
    dispatch: Callable[[str, str, dict[str, str], object], Response],
) -> ReuseMutationResult:
    try:
        method, path, headers, body = request()
    except (Exception, KeyboardInterrupt, GeneratorExit) as error:
        _reject(error, target, policy, mutation, operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = dispatch(method, path, headers, body)
        result = ReuseMutationResult(
            _reuse_receipt(target, policy, "succeeded", status, mutation, operation),
            MappingRecord(payload) if isinstance(payload, Mapping) and payload else None,
        )
    except (Exception, asyncio.CancelledError, KeyboardInterrupt, GeneratorExit) as error:
        outcome = (
            "cancelled"
            if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError))
            else _mutation_outcome(error, response)
        )
        receipt = _reuse_receipt(target, policy, outcome, _error_status(error, response), mutation, operation)
        _attach(error, receipt)
        raise
    return result


async def _mutation_async(
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    destructive: bool,
    request: Callable[[], Request],
    dispatch: Callable[[str, str, dict[str, str], object], Awaitable[Response]],
) -> ReuseMutationResult:
    try:
        method, path, headers, body = request()
    except (Exception, KeyboardInterrupt, GeneratorExit) as error:
        _reject(error, target, policy, mutation, operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = await dispatch(method, path, headers, body)
        result = ReuseMutationResult(
            _reuse_receipt(target, policy, "succeeded", status, mutation, operation),
            MappingRecord(payload) if isinstance(payload, Mapping) and payload else None,
        )
    except (Exception, asyncio.CancelledError, KeyboardInterrupt, GeneratorExit) as error:
        outcome = (
            "cancelled"
            if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError))
            else _mutation_outcome(error, response)
        )
        receipt = _reuse_receipt(target, policy, outcome, _error_status(error, response), mutation, operation)
        _attach(error, receipt)
        raise
    return result


class SyncReusesService:
    """Typed synchronous reuse and reuse-follower operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def list_reuses(self, query: ReuseListQuery | None = None) -> MappingRecord:
        return self._read(wire.list_reuses_request(query or ReuseListQuery()), wire.LIST_REUSES_OPERATION)

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
            False,
            lambda: wire.create_reuse_request(client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.CREATE_REUSE_OPERATION
            ),
        )

    def recent_reuses_atom_feed(self, query: ReuseListQuery | None = None) -> MappingRecord:
        operation = wire.RECENT_REUSES_ATOM_FEED_OPERATION
        method, path, _, _ = wire.recent_reuses_atom_feed_request(query or ReuseListQuery())
        _, text, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        negotiated = _content_type(response.headers)
        return wire.parse_text_document(
            cast(bytes, text), wire._ATOM_MEDIA_TYPE, response_media_type=negotiated, operation=operation
        )

    def get_reuse(self, reuse_id: str) -> MappingRecord:
        return self._read(wire.get_reuse_request(reuse_id), wire.GET_REUSE_OPERATION)

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
            False,
            lambda: wire.update_reuse_request(reuse_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UPDATE_REUSE_OPERATION
            ),
        )

    def delete_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.DELETE_REUSE_OPERATION,
            True,
            lambda: wire.delete_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_REUSE_OPERATION
            ),
        )

    def reuse_add_dataset(
        self,
        reuse_id: str,
        dataset_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.REUSE_ADD_DATASET_OPERATION,
            False,
            lambda: wire.reuse_add_dataset_request(reuse_id, dataset_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.REUSE_ADD_DATASET_OPERATION
            ),
        )

    def reuse_add_dataservice(
        self,
        reuse_id: str,
        dataservice_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.REUSE_ADD_DATASERVICE_OPERATION,
            False,
            lambda: wire.reuse_add_dataservice_request(reuse_id, dataservice_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.REUSE_ADD_DATASERVICE_OPERATION
            ),
        )

    def available_reuse_badges(self) -> MappingRecord:
        return self._read(wire.available_reuse_badges_request(), wire.AVAILABLE_REUSE_BADGES_OPERATION)

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
            False,
            lambda: wire.add_reuse_badge_request(reuse_id, badge_kind),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.ADD_REUSE_BADGE_OPERATION
            ),
        )

    def delete_reuse_badge(
        self, reuse_id: str, badge_kind: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.DELETE_REUSE_BADGE_OPERATION,
            True,
            lambda: wire.delete_reuse_badge_request(reuse_id, badge_kind),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_REUSE_BADGE_OPERATION
            ),
        )

    def feature_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.FEATURE_REUSE_OPERATION,
            False,
            lambda: wire.feature_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.FEATURE_REUSE_OPERATION
            ),
        )

    def unfeature_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.UNFEATURE_REUSE_OPERATION,
            True,
            lambda: wire.unfeature_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UNFEATURE_REUSE_OPERATION
            ),
        )

    def suggest_reuses(self, query: ReuseSuggestQuery) -> tuple[MappingRecord, ...]:
        return self._read_sequence(wire.suggest_reuses_request(query), wire.SUGGEST_REUSES_OPERATION)

    def reuse_image(
        self,
        reuse_id: str,
        data: bytes,
        content_type: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        image = ReuseImageInput(data=data, content_type=content_type)
        method, path, headers = wire.reuse_image_request(reuse_id)
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.REUSE_IMAGE_OPERATION,
            False,
            lambda: (method, path, headers, None),
            lambda m, p, h, b: self._mutate_upload(m, p, h, image, permissions, mutation_policy),
        )

    def reuse_types(self) -> tuple[MappingRecord, ...]:
        return self._read_sequence(wire.reuse_types_request(), wire.REUSE_TYPES_OPERATION)

    def reuse_topics(self) -> tuple[MappingRecord, ...]:
        return self._read_sequence(wire.reuse_topics_request(), wire.REUSE_TOPICS_OPERATION)

    def search_v2(self, query: ReuseSearchQuery | None = None) -> MappingRecord:
        return self._read(wire.search_reuses_v2_request(query or ReuseSearchQuery()), wire.SEARCH_REUSES_V2_OPERATION)

    def list_v2(self, query: ReuseListQuery | None = None) -> MappingRecord:
        return self._read(wire.list_reuses_v2_request(query or ReuseListQuery()), wire.LIST_REUSES_V2_OPERATION)

    def list_reuse_followers(self, reuse_id: str, query: ReuseFollowersQuery | None = None) -> MappingRecord:
        return self._read(
            wire.list_reuse_followers_request(reuse_id, query or ReuseFollowersQuery()),
            wire.LIST_REUSE_FOLLOWERS_OPERATION,
        )

    def follow_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "updated",
            wire.FOLLOW_REUSE_OPERATION,
            False,
            lambda: wire.follow_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.FOLLOW_REUSE_OPERATION
            ),
        )

    def unfollow_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return _mutation(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.UNFOLLOW_REUSE_OPERATION,
            True,
            lambda: wire.unfollow_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UNFOLLOW_REUSE_OPERATION
            ),
        )

    def _read(
        self,
        request: tuple[str, str, dict[str, str], object],
        operation: str,
    ) -> MappingRecord:
        method, path, headers, body = request
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, headers=headers, json_body=body
        )
        return wire.parse_mapping(payload, operation)

    def _read_sequence(
        self,
        request: tuple[str, str, dict[str, str], object],
        operation: str,
    ) -> tuple[MappingRecord, ...]:
        method, path, headers, body = request
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, headers=headers, json_body=body
        )
        return wire.parse_mapping_sequence(payload, operation)

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

    def _mutate_upload(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        image: ReuseImageInput,
        permissions: Permissions,
        policy: Policy,
    ) -> Response:
        operation = wire.REUSE_IMAGE_OPERATION
        resolved = _require_mutation_permission(self._client._resolved_credential(), operation, permissions)
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
            files=(image.part(),),
        )


class AsyncReusesService:
    """Typed asynchronous reuse and reuse-follower operations."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def list_reuses(self, query: ReuseListQuery | None = None) -> MappingRecord:
        return await self._read(wire.list_reuses_request(query or ReuseListQuery()), wire.LIST_REUSES_OPERATION)

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
            False,
            lambda: wire.create_reuse_request(client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.CREATE_REUSE_OPERATION
            ),
        )

    async def recent_reuses_atom_feed(self, query: ReuseListQuery | None = None) -> MappingRecord:
        operation = wire.RECENT_REUSES_ATOM_FEED_OPERATION
        method, path, _, _ = wire.recent_reuses_atom_feed_request(query or ReuseListQuery())
        _, text, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        negotiated = _content_type(response.headers)
        return wire.parse_text_document(
            cast(bytes, text), wire._ATOM_MEDIA_TYPE, response_media_type=negotiated, operation=operation
        )

    async def get_reuse(self, reuse_id: str) -> MappingRecord:
        return await self._read(wire.get_reuse_request(reuse_id), wire.GET_REUSE_OPERATION)

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
            False,
            lambda: wire.update_reuse_request(reuse_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UPDATE_REUSE_OPERATION
            ),
        )

    async def delete_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.DELETE_REUSE_OPERATION,
            True,
            lambda: wire.delete_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_REUSE_OPERATION
            ),
        )

    async def reuse_add_dataset(
        self,
        reuse_id: str,
        dataset_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.REUSE_ADD_DATASET_OPERATION,
            False,
            lambda: wire.reuse_add_dataset_request(reuse_id, dataset_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.REUSE_ADD_DATASET_OPERATION
            ),
        )

    async def reuse_add_dataservice(
        self,
        reuse_id: str,
        dataservice_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.REUSE_ADD_DATASERVICE_OPERATION,
            False,
            lambda: wire.reuse_add_dataservice_request(reuse_id, dataservice_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.REUSE_ADD_DATASERVICE_OPERATION
            ),
        )

    async def available_reuse_badges(self) -> MappingRecord:
        return await self._read(wire.available_reuse_badges_request(), wire.AVAILABLE_REUSE_BADGES_OPERATION)

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
            False,
            lambda: wire.add_reuse_badge_request(reuse_id, badge_kind),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.ADD_REUSE_BADGE_OPERATION
            ),
        )

    async def delete_reuse_badge(
        self, reuse_id: str, badge_kind: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.DELETE_REUSE_BADGE_OPERATION,
            True,
            lambda: wire.delete_reuse_badge_request(reuse_id, badge_kind),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_REUSE_BADGE_OPERATION
            ),
        )

    async def feature_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.FEATURE_REUSE_OPERATION,
            False,
            lambda: wire.feature_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.FEATURE_REUSE_OPERATION
            ),
        )

    async def unfeature_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.UNFEATURE_REUSE_OPERATION,
            True,
            lambda: wire.unfeature_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UNFEATURE_REUSE_OPERATION
            ),
        )

    async def suggest_reuses(self, query: ReuseSuggestQuery) -> tuple[MappingRecord, ...]:
        return await self._read_sequence(wire.suggest_reuses_request(query), wire.SUGGEST_REUSES_OPERATION)

    async def reuse_image(
        self,
        reuse_id: str,
        data: bytes,
        content_type: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ReuseMutationResult:
        image = ReuseImageInput(data=data, content_type=content_type)
        method, path, headers = wire.reuse_image_request(reuse_id)
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.REUSE_IMAGE_OPERATION,
            False,
            lambda: (method, path, headers, None),
            lambda m, p, h, b: self._mutate_upload(m, p, h, image, permissions, mutation_policy),
        )

    async def reuse_types(self) -> tuple[MappingRecord, ...]:
        return await self._read_sequence(wire.reuse_types_request(), wire.REUSE_TYPES_OPERATION)

    async def reuse_topics(self) -> tuple[MappingRecord, ...]:
        return await self._read_sequence(wire.reuse_topics_request(), wire.REUSE_TOPICS_OPERATION)

    async def search_v2(self, query: ReuseSearchQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.search_reuses_v2_request(query or ReuseSearchQuery()), wire.SEARCH_REUSES_V2_OPERATION
        )

    async def list_v2(self, query: ReuseListQuery | None = None) -> MappingRecord:
        return await self._read(wire.list_reuses_v2_request(query or ReuseListQuery()), wire.LIST_REUSES_V2_OPERATION)

    async def list_reuse_followers(self, reuse_id: str, query: ReuseFollowersQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.list_reuse_followers_request(reuse_id, query or ReuseFollowersQuery()),
            wire.LIST_REUSE_FOLLOWERS_OPERATION,
        )

    async def follow_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "updated",
            wire.FOLLOW_REUSE_OPERATION,
            False,
            lambda: wire.follow_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.FOLLOW_REUSE_OPERATION
            ),
        )

    async def unfollow_reuse(
        self, reuse_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> ReuseMutationResult:
        return await _mutation_async(
            reuse_id,
            mutation_policy,
            "deleted",
            wire.UNFOLLOW_REUSE_OPERATION,
            True,
            lambda: wire.unfollow_reuse_request(reuse_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UNFOLLOW_REUSE_OPERATION
            ),
        )

    async def _read(
        self,
        request: tuple[str, str, dict[str, str], object],
        operation: str,
    ) -> MappingRecord:
        method, path, headers, body = request
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, headers=headers, json_body=body
        )
        return wire.parse_mapping(payload, operation)

    async def _read_sequence(
        self,
        request: tuple[str, str, dict[str, str], object],
        operation: str,
    ) -> tuple[MappingRecord, ...]:
        method, path, headers, body = request
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, headers=headers, json_body=body
        )
        return wire.parse_mapping_sequence(payload, operation)

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

    async def _mutate_upload(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        image: ReuseImageInput,
        permissions: Permissions,
        policy: Policy,
    ) -> Response:
        operation = wire.REUSE_IMAGE_OPERATION
        resolved = _require_mutation_permission(await self._client._resolved_credential_async(), operation, permissions)
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
            files=(image.part(),),
        )


def _content_type(headers: Mapping[str, str]) -> str | None:
    for key, value in headers.items():
        if key.lower() == "content-type":
            return value
    return None
