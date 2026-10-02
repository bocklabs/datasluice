"""Dual-mode uData post, report, and notification service."""

from __future__ import annotations

from collections.abc import Mapping
from functools import partial
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
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import posts_reports as wire
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import NativeCatalogError

from .taxonomies import (
    AsyncCatalogService,
    Permissions,
    Policy,
    Request,
    Response,
    SupportsUpload,
    SyncCatalogService,
    _dataset_mutate,
    _dataset_upload,
    _parsed,
    _run_mutation,
    _run_mutation_async,
)

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

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

_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    PostMutationResult,
    ResourceKind.RESOURCE,
)
_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    PostMutationResult,
    ResourceKind.RESOURCE,
)


def _upload_request(post_id: str, data: bytes, content_type: str, operation: str) -> Request:
    image = PostImageInput(data, content_type)
    method, path, headers = (
        wire.post_image_request(post_id)
        if operation == wire.POST_IMAGE_OPERATION
        else wire.resize_post_image_request(post_id)
    )
    return method, path, headers, image


def _record_id(payload: object, fallback: str) -> str:
    if isinstance(payload, Mapping) and isinstance(payload.get("id"), str) and payload["id"]:
        return payload["id"]
    return fallback


class SyncPostsReportsService(SyncCatalogService):
    """Typed synchronous post, report, and notification operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def list_reports(self, query: ReportQuery | None = None) -> MappingRecord:
        return self._read(wire.list_reports_request(query), wire.LIST_REPORTS_OPERATION, wire.parse_mapping)

    def create_report(
        self,
        client_input: ReportCreateInput,
        permissions: Permissions | None = None,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            client_input.subject["id"],
            mutation_policy,
            _MUTATION_LABEL[wire.CREATE_REPORT_OPERATION],
            wire.CREATE_REPORT_OPERATION,
            lambda: wire.create_report_request(client_input),
            lambda request: self._create_report(request, permissions),
            success_target=lambda payload: _record_id(payload, "report"),
        )

    def get_report(self, report_id: str) -> MappingRecord:
        return self._read(wire.get_report_request(report_id), wire.GET_REPORT_OPERATION, wire.parse_mapping)

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
            _MUTATION_LABEL[wire.UPDATE_REPORT_OPERATION],
            wire.UPDATE_REPORT_OPERATION,
            lambda: wire.update_report_request(report_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_REPORT_OPERATION),
        )

    def list_reports_reasons(self) -> tuple[MappingRecord, ...]:
        return self._read(
            wire.list_reports_reasons_request(), wire.LIST_REPORTS_REASONS_OPERATION, wire.parse_mapping_sequence
        )

    def list_posts(self, query: PostListQuery | None = None) -> MappingRecord:
        return self._read(wire.list_posts_request(query), wire.LIST_POSTS_OPERATION, wire.parse_mapping)

    def create_post(
        self,
        client_input: PostCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            client_input.name,
            mutation_policy,
            _MUTATION_LABEL[wire.CREATE_POST_OPERATION],
            wire.CREATE_POST_OPERATION,
            lambda: wire.create_post_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_POST_OPERATION),
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
        return self._read(wire.get_post_request(post_id), wire.GET_POST_OPERATION, wire.parse_mapping)

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
            _MUTATION_LABEL[wire.UPDATE_POST_OPERATION],
            wire.UPDATE_POST_OPERATION,
            lambda: wire.update_post_request(post_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_POST_OPERATION),
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
            _MUTATION_LABEL[wire.DELETE_POST_OPERATION],
            wire.DELETE_POST_OPERATION,
            lambda: wire.delete_post_request(post_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_POST_OPERATION),
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
            _MUTATION_LABEL[wire.PUBLISH_POST_OPERATION],
            wire.PUBLISH_POST_OPERATION,
            lambda: wire.publish_post_request(post_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.PUBLISH_POST_OPERATION),
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
            _MUTATION_LABEL[wire.UNPUBLISH_POST_OPERATION],
            wire.UNPUBLISH_POST_OPERATION,
            lambda: wire.unpublish_post_request(post_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNPUBLISH_POST_OPERATION),
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
            _MUTATION_LABEL[wire.POST_IMAGE_OPERATION],
            wire.POST_IMAGE_OPERATION,
            lambda: _upload_request(post_id, data, content_type, wire.POST_IMAGE_OPERATION),
            lambda request: self._mutate_upload(request, permissions, mutation_policy, wire.POST_IMAGE_OPERATION),
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
            _MUTATION_LABEL[wire.RESIZE_POST_IMAGE_OPERATION],
            wire.RESIZE_POST_IMAGE_OPERATION,
            lambda: _upload_request(post_id, data, content_type, wire.RESIZE_POST_IMAGE_OPERATION),
            lambda request: self._mutate_upload(
                request, permissions, mutation_policy, wire.RESIZE_POST_IMAGE_OPERATION
            ),
        )

    def search_posts(self, query: PostSearchQuery | None = None) -> MappingRecord:
        return self._read(wire.search_posts_request(query), wire.SEARCH_POSTS_OPERATION, wire.parse_mapping)

    def list_notifications(self, permissions: Permissions, query: NotificationQuery | None = None) -> MappingRecord:
        operation = wire.LIST_NOTIFICATIONS_OPERATION
        response = self._mutate(wire.list_notifications_request(query), permissions, None, operation)
        return _parsed(response, operation, wire.parse_mapping)

    def read_notification(
        self,
        notification_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return _mutation(
            notification_id,
            mutation_policy,
            _MUTATION_LABEL[wire.READ_NOTIFICATION_OPERATION],
            wire.READ_NOTIFICATION_OPERATION,
            lambda: wire.read_notification_request(notification_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.READ_NOTIFICATION_OPERATION),
        )

    def _create_report(self, request: Request, permissions: Permissions | None) -> Response:
        method, path, headers, body = request
        operation = wire.CREATE_REPORT_OPERATION
        credential = self._client._resolved_credential()
        if permissions is None:
            return self._client._dataset_call(
                method=method,
                path=path,
                owning_operation=operation,
                headers=headers,
                json_body=body,
                credential=credential,
            )
        return _dataset_mutate(self._client._dataset_call, credential, permissions, None, operation, request)

    def _mutate(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        return _dataset_mutate(
            self._client._dataset_call,
            self._client._resolved_credential(),
            permissions,
            policy,
            operation,
            request,
            admin=operation in _ADMIN,
        )

    def _mutate_upload(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        method, path, headers, upload = request
        return _dataset_upload(
            self._client._dataset_call,
            self._client._resolved_credential(),
            permissions,
            policy,
            operation,
            (method, path, headers),
            cast(SupportsUpload, upload),
            admin=True,
        )


class AsyncPostsReportsService(AsyncCatalogService):
    """Typed asynchronous post, report, and notification operations."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def list_reports(self, query: ReportQuery | None = None) -> MappingRecord:
        return await self._read(wire.list_reports_request(query), wire.LIST_REPORTS_OPERATION, wire.parse_mapping)

    async def create_report(
        self,
        client_input: ReportCreateInput,
        permissions: Permissions | None = None,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            client_input.subject["id"],
            mutation_policy,
            _MUTATION_LABEL[wire.CREATE_REPORT_OPERATION],
            wire.CREATE_REPORT_OPERATION,
            lambda: wire.create_report_request(client_input),
            lambda request: self._create_report(request, permissions),
            success_target=lambda payload: _record_id(payload, "report"),
        )

    async def get_report(self, report_id: str) -> MappingRecord:
        return await self._read(wire.get_report_request(report_id), wire.GET_REPORT_OPERATION, wire.parse_mapping)

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
            _MUTATION_LABEL[wire.UPDATE_REPORT_OPERATION],
            wire.UPDATE_REPORT_OPERATION,
            lambda: wire.update_report_request(report_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_REPORT_OPERATION),
        )

    async def list_reports_reasons(self) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.list_reports_reasons_request(),
            wire.LIST_REPORTS_REASONS_OPERATION,
            wire.parse_mapping_sequence,
        )

    async def list_posts(self, query: PostListQuery | None = None) -> MappingRecord:
        return await self._read(wire.list_posts_request(query), wire.LIST_POSTS_OPERATION, wire.parse_mapping)

    async def create_post(
        self,
        client_input: PostCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            client_input.name,
            mutation_policy,
            _MUTATION_LABEL[wire.CREATE_POST_OPERATION],
            wire.CREATE_POST_OPERATION,
            lambda: wire.create_post_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_POST_OPERATION),
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
        return await self._read(wire.get_post_request(post_id), wire.GET_POST_OPERATION, wire.parse_mapping)

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
            _MUTATION_LABEL[wire.UPDATE_POST_OPERATION],
            wire.UPDATE_POST_OPERATION,
            lambda: wire.update_post_request(post_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_POST_OPERATION),
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
            _MUTATION_LABEL[wire.DELETE_POST_OPERATION],
            wire.DELETE_POST_OPERATION,
            lambda: wire.delete_post_request(post_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_POST_OPERATION),
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
            _MUTATION_LABEL[wire.PUBLISH_POST_OPERATION],
            wire.PUBLISH_POST_OPERATION,
            lambda: wire.publish_post_request(post_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.PUBLISH_POST_OPERATION),
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
            _MUTATION_LABEL[wire.UNPUBLISH_POST_OPERATION],
            wire.UNPUBLISH_POST_OPERATION,
            lambda: wire.unpublish_post_request(post_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNPUBLISH_POST_OPERATION),
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
            _MUTATION_LABEL[wire.POST_IMAGE_OPERATION],
            wire.POST_IMAGE_OPERATION,
            lambda: _upload_request(post_id, data, content_type, wire.POST_IMAGE_OPERATION),
            lambda request: self._mutate_upload(request, permissions, mutation_policy, wire.POST_IMAGE_OPERATION),
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
            _MUTATION_LABEL[wire.RESIZE_POST_IMAGE_OPERATION],
            wire.RESIZE_POST_IMAGE_OPERATION,
            lambda: _upload_request(post_id, data, content_type, wire.RESIZE_POST_IMAGE_OPERATION),
            lambda request: self._mutate_upload(
                request, permissions, mutation_policy, wire.RESIZE_POST_IMAGE_OPERATION
            ),
        )

    async def search_posts(self, query: PostSearchQuery | None = None) -> MappingRecord:
        return await self._read(wire.search_posts_request(query), wire.SEARCH_POSTS_OPERATION, wire.parse_mapping)

    async def list_notifications(
        self, permissions: Permissions, query: NotificationQuery | None = None
    ) -> MappingRecord:
        operation = wire.LIST_NOTIFICATIONS_OPERATION
        response = await self._mutate(wire.list_notifications_request(query), permissions, None, operation)
        return _parsed(response, operation, wire.parse_mapping)

    async def read_notification(
        self,
        notification_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> PostMutationResult:
        return await _mutation_async(
            notification_id,
            mutation_policy,
            _MUTATION_LABEL[wire.READ_NOTIFICATION_OPERATION],
            wire.READ_NOTIFICATION_OPERATION,
            lambda: wire.read_notification_request(notification_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.READ_NOTIFICATION_OPERATION),
        )

    async def _create_report(self, request: Request, permissions: Permissions | None) -> Response:
        method, path, headers, body = request
        operation = wire.CREATE_REPORT_OPERATION
        credential = await self._client._resolved_credential_async()
        if permissions is None:
            return await self._client._dataset_call_async(
                method=method,
                path=path,
                owning_operation=operation,
                headers=headers,
                json_body=body,
                credential=credential,
            )
        return await _dataset_mutate(
            self._client._dataset_call_async, credential, permissions, None, operation, request
        )

    async def _mutate(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        return await _dataset_mutate(
            self._client._dataset_call_async,
            await self._client._resolved_credential_async(),
            permissions,
            policy,
            operation,
            request,
            admin=operation in _ADMIN,
        )

    async def _mutate_upload(
        self, request: Request, permissions: Permissions, policy: Policy, operation: str
    ) -> Response:
        method, path, headers, upload = request
        return await _dataset_upload(
            self._client._dataset_call_async,
            await self._client._resolved_credential_async(),
            permissions,
            policy,
            operation,
            (method, path, headers),
            cast(SupportsUpload, upload),
            admin=True,
        )
