"""Dual-mode uData post, report, and notification service."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.udata.models.posts_reports import (
    NotificationQuery,
    PostCreateInput,
    PostImageInput,
    PostListQuery,
    PostMutationResult,
    PostSearchQuery,
    PostUpdateInput,
    ReportCreateInput,
    ReportQuery,
    ReportUpdateInput,
)
from datasluice.connectors.catalog.udata.services.resources import _attach, _receipt
from datasluice.connectors.catalog.udata.wire import posts_reports as wire
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

_ADMIN = {
    wire.LIST_REPORTS_OPERATION,
    wire.GET_REPORT_OPERATION,
    wire.UPDATE_REPORT_OPERATION,
    wire.CREATE_POST_OPERATION,
    wire.UPDATE_POST_OPERATION,
    wire.DELETE_POST_OPERATION,
    wire.PUBLISH_POST_OPERATION,
    wire.UNPUBLISH_POST_OPERATION,
    wire.POST_IMAGE_OPERATION,
    wire.RESIZE_POST_IMAGE_OPERATION,
}
_DESTRUCTIVE = {wire.DELETE_POST_OPERATION}
_MUTATION_LABEL = {
    wire.CREATE_REPORT_OPERATION: "created",
    wire.UPDATE_REPORT_OPERATION: "updated",
    wire.CREATE_POST_OPERATION: "created",
    wire.UPDATE_POST_OPERATION: "updated",
    wire.DELETE_POST_OPERATION: "deleted",
    wire.PUBLISH_POST_OPERATION: "updated",
    wire.UNPUBLISH_POST_OPERATION: "updated",
    wire.POST_IMAGE_OPERATION: "updated",
    wire.RESIZE_POST_IMAGE_OPERATION: "updated",
    wire.READ_NOTIFICATION_OPERATION: "updated",
}


def _receipt_for(target: str, policy: Policy, outcome: str, status: int, mutation: str, operation: str):
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
    _attach(error, _receipt_for(target, policy, "rejected", _error_status(error), mutation, operation))
    raise error


def _result(payload: object, target: str, policy: Policy, status: int, mutation: str, operation: str):
    return PostMutationResult(
        _receipt_for(target, policy, "succeeded", status, mutation, operation),
        MappingRecord(payload) if isinstance(payload, Mapping) and payload else None,
    )


def _failure(error: BaseException, target: str, policy: Policy, response: object, mutation: str, operation: str):
    outcome = (
        "cancelled"
        if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError))
        else _mutation_outcome(error, response)
    )
    receipt = _receipt_for(target, policy, outcome, _error_status(error, response), mutation, operation)
    _attach(error, receipt)
    raise error


def _mutation(
    target: str,
    policy: Policy,
    operation: str,
    request: Callable[[], Request],
    dispatch: Callable[[str, str, dict[str, str], object], Response],
    destructive: bool = False,
    success_target: Callable[[object], str] | None = None,
) -> PostMutationResult:
    try:
        method, path, headers, body = request()
    except (Exception, KeyboardInterrupt, GeneratorExit) as error:
        _reject(error, target, policy, _MUTATION_LABEL[operation], operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = dispatch(method, path, headers, body)
        result_target = success_target(payload) if success_target is not None else target
        result = _result(payload, result_target, policy, status, _MUTATION_LABEL[operation], operation)
    except (Exception, asyncio.CancelledError, KeyboardInterrupt, GeneratorExit) as error:
        _failure(error, target, policy, response, _MUTATION_LABEL[operation], operation)
    return result


async def _mutation_async(
    target: str,
    policy: Policy,
    operation: str,
    request: Callable[[], Request],
    dispatch: Callable[[str, str, dict[str, str], object], Awaitable[Response]],
    destructive: bool = False,
    success_target: Callable[[object], str] | None = None,
) -> PostMutationResult:
    try:
        method, path, headers, body = request()
    except (Exception, KeyboardInterrupt, GeneratorExit) as error:
        _reject(error, target, policy, _MUTATION_LABEL[operation], operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = await dispatch(method, path, headers, body)
        result_target = success_target(payload) if success_target is not None else target
        result = _result(payload, result_target, policy, status, _MUTATION_LABEL[operation], operation)
    except (Exception, asyncio.CancelledError, KeyboardInterrupt, GeneratorExit) as error:
        _failure(error, target, policy, response, _MUTATION_LABEL[operation], operation)
    return result


def _record_id(payload: object, fallback: str) -> str:
    if isinstance(payload, Mapping) and isinstance(payload.get("id"), str) and payload["id"]:
        return payload["id"]
    return fallback


class SyncPostsReportsService:
    """Typed synchronous post, report, and notification operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def list_reports(self, query: ReportQuery | None = None) -> MappingRecord:
        return self._read(wire.list_reports_request(query), wire.LIST_REPORTS_OPERATION)

    def create_report(
        self,
        client_input: ReportCreateInput,
        permissions: Permissions | None = None,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            client_input.subject["id"],
            mutation_policy,
            wire.CREATE_REPORT_OPERATION,
            lambda: wire.create_report_request(client_input),
            lambda method, path, headers, body: self._create_report(method, path, headers, body, permissions),
            success_target=lambda payload: _record_id(payload, "report"),
        )

    def _create_report(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: object,
        permissions: Permissions | None,
    ) -> Response:
        resolved = self._client._resolved_credential()
        if permissions is not None:
            resolved = _require_mutation_permission(resolved, wire.CREATE_REPORT_OPERATION, permissions)
            return self._client._dataset_call(
                method=method,
                path=path,
                owning_operation=wire.CREATE_REPORT_OPERATION,
                headers=headers,
                json_body=body,
                permissions=permissions,
                credential=resolved,
            )
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=wire.CREATE_REPORT_OPERATION,
            headers=headers,
            json_body=body,
            credential=resolved,
        )

    def get_report(self, report_id: str) -> MappingRecord:
        return self._read(wire.get_report_request(report_id), wire.GET_REPORT_OPERATION)

    def update_report(
        self,
        report_id: str,
        client_input: ReportUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            report_id,
            mutation_policy,
            wire.UPDATE_REPORT_OPERATION,
            lambda: wire.update_report_request(report_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UPDATE_REPORT_OPERATION
            ),
        )

    def list_reports_reasons(self) -> tuple[MappingRecord, ...]:
        return self._read_sequence(wire.list_reports_reasons_request(), wire.LIST_REPORTS_REASONS_OPERATION)

    def list_posts(self, query: PostListQuery | None = None) -> MappingRecord:
        return self._read(wire.list_posts_request(query), wire.LIST_POSTS_OPERATION)

    def create_post(
        self,
        client_input: PostCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            client_input.name,
            mutation_policy,
            wire.CREATE_POST_OPERATION,
            lambda: wire.create_post_request(client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.CREATE_POST_OPERATION
            ),
            success_target=lambda payload: _record_id(payload, client_input.name),
        )

    def recent_posts_atom_feed(self) -> MappingRecord:
        operation = wire.RECENT_POSTS_ATOM_FEED_OPERATION
        method, path, _, _ = wire.recent_posts_atom_feed_request()
        _, text, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(
            cast(bytes, text), wire._ATOM_MEDIA_TYPE, response_media_type=wire.content_type(response.headers)
        )

    def get_post(self, post_id: str) -> MappingRecord:
        return self._read(wire.get_post_request(post_id), wire.GET_POST_OPERATION)

    def update_post(
        self,
        post_id: str,
        client_input: PostUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            post_id,
            mutation_policy,
            wire.UPDATE_POST_OPERATION,
            lambda: wire.update_post_request(post_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UPDATE_POST_OPERATION
            ),
        )

    def delete_post(
        self,
        post_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            post_id,
            mutation_policy,
            wire.DELETE_POST_OPERATION,
            lambda: wire.delete_post_request(post_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_POST_OPERATION
            ),
            destructive=True,
        )

    def publish_post(
        self,
        post_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            post_id,
            mutation_policy,
            wire.PUBLISH_POST_OPERATION,
            lambda: wire.publish_post_request(post_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.PUBLISH_POST_OPERATION
            ),
        )

    def unpublish_post(
        self,
        post_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            post_id,
            mutation_policy,
            wire.UNPUBLISH_POST_OPERATION,
            lambda: wire.unpublish_post_request(post_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UNPUBLISH_POST_OPERATION
            ),
        )

    def post_image(
        self,
        post_id: str,
        data: bytes,
        content_type: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            post_id,
            mutation_policy,
            wire.POST_IMAGE_OPERATION,
            lambda: self._upload_request(post_id, data, content_type, wire.POST_IMAGE_OPERATION),
            lambda method, path, headers, body: self._mutate_upload(
                method,
                path,
                headers,
                cast(PostImageInput, body),
                permissions,
                mutation_policy,
                wire.POST_IMAGE_OPERATION,
            ),
        )

    def resize_post_image(
        self,
        post_id: str,
        data: bytes,
        content_type: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            post_id,
            mutation_policy,
            wire.RESIZE_POST_IMAGE_OPERATION,
            lambda: self._upload_request(post_id, data, content_type, wire.RESIZE_POST_IMAGE_OPERATION),
            lambda method, path, headers, body: self._mutate_upload(
                method,
                path,
                headers,
                cast(PostImageInput, body),
                permissions,
                mutation_policy,
                wire.RESIZE_POST_IMAGE_OPERATION,
            ),
        )

    def search_posts(self, query: PostSearchQuery | None = None) -> MappingRecord:
        return self._read(wire.search_posts_request(query), wire.SEARCH_POSTS_OPERATION)

    def list_notifications(self, permissions: Permissions, query: NotificationQuery | None = None) -> MappingRecord:
        method, path, headers, body = wire.list_notifications_request(query)
        resolved = _require_mutation_permission(
            self._client._resolved_credential(), wire.LIST_NOTIFICATIONS_OPERATION, permissions
        )
        _, payload, _ = self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=wire.LIST_NOTIFICATIONS_OPERATION,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=resolved,
        )
        return wire.parse_mapping(payload, wire.LIST_NOTIFICATIONS_OPERATION)

    def read_notification(
        self,
        notification_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            notification_id,
            mutation_policy,
            wire.READ_NOTIFICATION_OPERATION,
            lambda: wire.read_notification_request(notification_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.READ_NOTIFICATION_OPERATION
            ),
        )

    def _upload_request(self, post_id: str, data: bytes, content_type: str, operation: str) -> Request:
        image = PostImageInput(data, content_type)
        method, path, headers = (
            wire.post_image_request(post_id)
            if operation == wire.POST_IMAGE_OPERATION
            else wire.resize_post_image_request(post_id)
        )
        return method, path, headers, image

    def _read(self, request: Request, operation: str) -> MappingRecord:
        method, path, headers, body = request
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, headers=headers, json_body=body
        )
        return wire.parse_mapping(payload, operation)

    def _read_sequence(self, request: Request, operation: str) -> tuple[MappingRecord, ...]:
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
        resolved = _require_mutation_permission(
            self._client._resolved_credential(), operation, permissions, admin=operation in _ADMIN
        )
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
        image: PostImageInput,
        permissions: Permissions,
        policy: Policy,
        operation: str,
    ) -> Response:
        resolved = _require_mutation_permission(self._client._resolved_credential(), operation, permissions, admin=True)
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


class AsyncPostsReportsService:
    """Typed asynchronous post, report, and notification operations."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def list_reports(self, query: ReportQuery | None = None) -> MappingRecord:
        return await self._read(wire.list_reports_request(query), wire.LIST_REPORTS_OPERATION)

    async def create_report(
        self,
        client_input: ReportCreateInput,
        permissions: Permissions | None = None,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            client_input.subject["id"],
            mutation_policy,
            wire.CREATE_REPORT_OPERATION,
            lambda: wire.create_report_request(client_input),
            lambda method, path, headers, body: self._create_report(method, path, headers, body, permissions),
            success_target=lambda payload: _record_id(payload, "report"),
        )

    async def _create_report(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: object,
        permissions: Permissions | None,
    ) -> Response:
        resolved = self._client._resolved_credential()
        if permissions is not None:
            resolved = _require_mutation_permission(resolved, wire.CREATE_REPORT_OPERATION, permissions)
            return await self._client._dataset_call_async(
                method=method,
                path=path,
                owning_operation=wire.CREATE_REPORT_OPERATION,
                headers=headers,
                json_body=body,
                permissions=permissions,
                credential=resolved,
            )
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=wire.CREATE_REPORT_OPERATION,
            headers=headers,
            json_body=body,
            credential=resolved,
        )

    async def get_report(self, report_id: str) -> MappingRecord:
        return await self._read(wire.get_report_request(report_id), wire.GET_REPORT_OPERATION)

    async def update_report(
        self,
        report_id: str,
        client_input: ReportUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            report_id,
            mutation_policy,
            wire.UPDATE_REPORT_OPERATION,
            lambda: wire.update_report_request(report_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UPDATE_REPORT_OPERATION
            ),
        )

    async def list_reports_reasons(self) -> tuple[MappingRecord, ...]:
        return await self._read_sequence(wire.list_reports_reasons_request(), wire.LIST_REPORTS_REASONS_OPERATION)

    async def list_posts(self, query: PostListQuery | None = None) -> MappingRecord:
        return await self._read(wire.list_posts_request(query), wire.LIST_POSTS_OPERATION)

    async def create_post(
        self,
        client_input: PostCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            client_input.name,
            mutation_policy,
            wire.CREATE_POST_OPERATION,
            lambda: wire.create_post_request(client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.CREATE_POST_OPERATION
            ),
            success_target=lambda payload: _record_id(payload, client_input.name),
        )

    async def recent_posts_atom_feed(self) -> MappingRecord:
        operation = wire.RECENT_POSTS_ATOM_FEED_OPERATION
        method, path, _, _ = wire.recent_posts_atom_feed_request()
        _, text, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(
            cast(bytes, text), wire._ATOM_MEDIA_TYPE, response_media_type=wire.content_type(response.headers)
        )

    async def get_post(self, post_id: str) -> MappingRecord:
        return await self._read(wire.get_post_request(post_id), wire.GET_POST_OPERATION)

    async def update_post(
        self,
        post_id: str,
        client_input: PostUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            post_id,
            mutation_policy,
            wire.UPDATE_POST_OPERATION,
            lambda: wire.update_post_request(post_id, client_input),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UPDATE_POST_OPERATION
            ),
        )

    async def delete_post(
        self,
        post_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            post_id,
            mutation_policy,
            wire.DELETE_POST_OPERATION,
            lambda: wire.delete_post_request(post_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_POST_OPERATION
            ),
            destructive=True,
        )

    async def publish_post(
        self,
        post_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            post_id,
            mutation_policy,
            wire.PUBLISH_POST_OPERATION,
            lambda: wire.publish_post_request(post_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.PUBLISH_POST_OPERATION
            ),
        )

    async def unpublish_post(
        self,
        post_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            post_id,
            mutation_policy,
            wire.UNPUBLISH_POST_OPERATION,
            lambda: wire.unpublish_post_request(post_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.UNPUBLISH_POST_OPERATION
            ),
        )

    async def post_image(
        self,
        post_id: str,
        data: bytes,
        content_type: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            post_id,
            mutation_policy,
            wire.POST_IMAGE_OPERATION,
            lambda: self._upload_request(post_id, data, content_type, wire.POST_IMAGE_OPERATION),
            lambda method, path, headers, body: self._mutate_upload(
                method,
                path,
                headers,
                cast(PostImageInput, body),
                permissions,
                mutation_policy,
                wire.POST_IMAGE_OPERATION,
            ),
        )

    async def resize_post_image(
        self,
        post_id: str,
        data: bytes,
        content_type: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            post_id,
            mutation_policy,
            wire.RESIZE_POST_IMAGE_OPERATION,
            lambda: self._upload_request(post_id, data, content_type, wire.RESIZE_POST_IMAGE_OPERATION),
            lambda method, path, headers, body: self._mutate_upload(
                method,
                path,
                headers,
                cast(PostImageInput, body),
                permissions,
                mutation_policy,
                wire.RESIZE_POST_IMAGE_OPERATION,
            ),
        )

    async def search_posts(self, query: PostSearchQuery | None = None) -> MappingRecord:
        return await self._read(wire.search_posts_request(query), wire.SEARCH_POSTS_OPERATION)

    async def list_notifications(
        self, permissions: Permissions, query: NotificationQuery | None = None
    ) -> MappingRecord:
        method, path, headers, body = wire.list_notifications_request(query)
        resolved = _require_mutation_permission(
            await self._client._resolved_credential_async(), wire.LIST_NOTIFICATIONS_OPERATION, permissions
        )
        _, payload, _ = await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=wire.LIST_NOTIFICATIONS_OPERATION,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=resolved,
        )
        return wire.parse_mapping(payload, wire.LIST_NOTIFICATIONS_OPERATION)

    async def read_notification(
        self,
        notification_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            notification_id,
            mutation_policy,
            wire.READ_NOTIFICATION_OPERATION,
            lambda: wire.read_notification_request(notification_id),
            lambda method, path, headers, body: self._mutate(
                method, path, headers, body, permissions, mutation_policy, wire.READ_NOTIFICATION_OPERATION
            ),
        )

    def _upload_request(self, post_id: str, data: bytes, content_type: str, operation: str) -> Request:
        image = PostImageInput(data, content_type)
        method, path, headers = (
            wire.post_image_request(post_id)
            if operation == wire.POST_IMAGE_OPERATION
            else wire.resize_post_image_request(post_id)
        )
        return method, path, headers, image

    async def _read(self, request: Request, operation: str) -> MappingRecord:
        method, path, headers, body = request
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, headers=headers, json_body=body
        )
        return wire.parse_mapping(payload, operation)

    async def _read_sequence(self, request: Request, operation: str) -> tuple[MappingRecord, ...]:
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
        resolved = _require_mutation_permission(
            await self._client._resolved_credential_async(), operation, permissions, admin=operation in _ADMIN
        )
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
        image: PostImageInput,
        permissions: Permissions,
        policy: Policy,
        operation: str,
    ) -> Response:
        resolved = _require_mutation_permission(
            await self._client._resolved_credential_async(), operation, permissions, admin=True
        )
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
