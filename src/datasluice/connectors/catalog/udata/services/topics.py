"""Dual-mode uData topic, topic-element, and featured-state service."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.topics import (
    TopicCreateInput,
    TopicElementInput,
    TopicElementsCreateInput,
    TopicElementsQuery,
    TopicListQuery,
    TopicMutationResult,
    TopicSearchQuery,
    TopicUpdateInput,
)
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import topics as wire
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.errors.catalog import NativeCatalogError

from .posts_reports import _record_id
from .taxonomies import (
    AsyncCatalogService,
    Permissions,
    Policy,
    Request,
    Response,
    SyncCatalogService,
    _dataset_mutate,
    _run_mutation,
    _run_mutation_async,
)

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
    from datasluice.domain.catalog.models import MappingRecord

_ADMIN = frozenset({wire.FEATURE_TOPIC_OPERATION, wire.UNFEATURE_TOPIC_OPERATION})

_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    TopicMutationResult,
    ResourceKind.DATASET,
)
_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    TopicMutationResult,
    ResourceKind.DATASET,
)


def _element_target(topic_id: str, element_id: str) -> str:
    return f"{topic_id}:{element_id}"


class SyncTopicsService(SyncCatalogService):
    """Typed synchronous topic, topic-element, and featured-state operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def _mutate(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        """Run one guarded topic mutation, requiring the admin role for the featured routes.

        The pinned ``TopicFeaturedAPI`` guards both transitions with
        ``@apiv2.secure(admin_permission)``, so that role is part of the
        pre-dispatch evidence rather than something the deployment decides later.
        """
        return _dataset_mutate(
            self._client._dataset_call,
            self._client._resolved_credential(),
            permissions,
            policy,
            operation,
            request,
            admin=operation in _ADMIN,
        )

    def search_topics(self, query: TopicSearchQuery | None = None) -> MappingRecord:
        return self._read(
            wire.search_topics_request(query or TopicSearchQuery()),
            wire.SEARCH_TOPICS_OPERATION,
            wire.parse_mapping,
        )

    def list_topics(self, query: TopicListQuery | None = None) -> MappingRecord:
        return self._read(
            wire.list_topics_request(query or TopicListQuery()),
            wire.LIST_TOPICS_OPERATION,
            wire.parse_mapping,
        )

    def create_topic(
        self,
        client_input: TopicCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return _mutation(
            client_input.name,
            mutation_policy,
            "created",
            wire.CREATE_TOPIC_OPERATION,
            lambda: wire.create_topic_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_TOPIC_OPERATION),
            success_target=lambda payload: _record_id(payload, client_input.name),
        )

    def get_topic(self, topic_id: str) -> MappingRecord:
        return self._read(wire.get_topic_request(topic_id), wire.GET_TOPIC_OPERATION, wire.parse_mapping)

    def update_topic(
        self,
        topic_id: str,
        client_input: TopicUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return _mutation(
            topic_id,
            mutation_policy,
            "updated",
            wire.UPDATE_TOPIC_OPERATION,
            lambda: wire.update_topic_request(topic_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_TOPIC_OPERATION),
        )

    def delete_topic(
        self,
        topic_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return _mutation(
            topic_id,
            mutation_policy,
            "deleted",
            wire.DELETE_TOPIC_OPERATION,
            lambda: wire.delete_topic_request(topic_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_TOPIC_OPERATION),
            destructive=True,
        )

    def topic_elements(self, topic_id: str, query: TopicElementsQuery | None = None) -> MappingRecord:
        return self._read(
            wire.topic_elements_request(topic_id, query or TopicElementsQuery()),
            wire.TOPIC_ELEMENTS_OPERATION,
            wire.parse_mapping,
        )

    def topic_elements_create(
        self,
        topic_id: str,
        client_input: TopicElementsCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return _mutation(
            topic_id,
            mutation_policy,
            "updated",
            wire.TOPIC_ELEMENTS_CREATE_OPERATION,
            lambda: wire.topic_elements_create_request(topic_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.TOPIC_ELEMENTS_CREATE_OPERATION),
        )

    def topic_elements_delete(
        self,
        topic_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return _mutation(
            topic_id,
            mutation_policy,
            "deleted",
            wire.TOPIC_ELEMENTS_DELETE_OPERATION,
            lambda: wire.topic_elements_delete_request(topic_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.TOPIC_ELEMENTS_DELETE_OPERATION),
            destructive=True,
        )

    def topic_element_update(
        self,
        topic_id: str,
        element_id: str,
        client_input: TopicElementInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return _mutation(
            _element_target(topic_id, element_id),
            mutation_policy,
            "updated",
            wire.TOPIC_ELEMENT_UPDATE_OPERATION,
            lambda: wire.topic_element_update_request(topic_id, element_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.TOPIC_ELEMENT_UPDATE_OPERATION),
        )

    def topic_element_delete(
        self,
        topic_id: str,
        element_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return _mutation(
            _element_target(topic_id, element_id),
            mutation_policy,
            "deleted",
            wire.TOPIC_ELEMENT_DELETE_OPERATION,
            lambda: wire.topic_element_delete_request(topic_id, element_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.TOPIC_ELEMENT_DELETE_OPERATION),
            destructive=True,
        )

    def feature_topic(
        self, topic_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> TopicMutationResult:
        return _mutation(
            topic_id,
            mutation_policy,
            "updated",
            wire.FEATURE_TOPIC_OPERATION,
            lambda: wire.feature_topic_request(topic_id, featured=True),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FEATURE_TOPIC_OPERATION),
        )

    def unfeature_topic(
        self, topic_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> TopicMutationResult:
        return _mutation(
            topic_id,
            mutation_policy,
            "updated",
            wire.UNFEATURE_TOPIC_OPERATION,
            lambda: wire.feature_topic_request(topic_id, featured=False),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFEATURE_TOPIC_OPERATION),
            destructive=True,
        )


class AsyncTopicsService(AsyncCatalogService):
    """Typed asynchronous topic, topic-element, and featured-state operations."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def _mutate(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        """Run one guarded topic mutation, requiring the admin role for the featured routes."""
        return await _dataset_mutate(
            self._client._dataset_call_async,
            await self._client._resolved_credential_async(),
            permissions,
            policy,
            operation,
            request,
            admin=operation in _ADMIN,
        )

    async def search_topics(self, query: TopicSearchQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.search_topics_request(query or TopicSearchQuery()),
            wire.SEARCH_TOPICS_OPERATION,
            wire.parse_mapping,
        )

    async def list_topics(self, query: TopicListQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.list_topics_request(query or TopicListQuery()),
            wire.LIST_TOPICS_OPERATION,
            wire.parse_mapping,
        )

    async def create_topic(
        self,
        client_input: TopicCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return await _mutation_async(
            client_input.name,
            mutation_policy,
            "created",
            wire.CREATE_TOPIC_OPERATION,
            lambda: wire.create_topic_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_TOPIC_OPERATION),
            success_target=lambda payload: _record_id(payload, client_input.name),
        )

    async def get_topic(self, topic_id: str) -> MappingRecord:
        return await self._read(wire.get_topic_request(topic_id), wire.GET_TOPIC_OPERATION, wire.parse_mapping)

    async def update_topic(
        self,
        topic_id: str,
        client_input: TopicUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return await _mutation_async(
            topic_id,
            mutation_policy,
            "updated",
            wire.UPDATE_TOPIC_OPERATION,
            lambda: wire.update_topic_request(topic_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_TOPIC_OPERATION),
        )

    async def delete_topic(
        self,
        topic_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return await _mutation_async(
            topic_id,
            mutation_policy,
            "deleted",
            wire.DELETE_TOPIC_OPERATION,
            lambda: wire.delete_topic_request(topic_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_TOPIC_OPERATION),
            destructive=True,
        )

    async def topic_elements(self, topic_id: str, query: TopicElementsQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.topic_elements_request(topic_id, query or TopicElementsQuery()),
            wire.TOPIC_ELEMENTS_OPERATION,
            wire.parse_mapping,
        )

    async def topic_elements_create(
        self,
        topic_id: str,
        client_input: TopicElementsCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return await _mutation_async(
            topic_id,
            mutation_policy,
            "updated",
            wire.TOPIC_ELEMENTS_CREATE_OPERATION,
            lambda: wire.topic_elements_create_request(topic_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.TOPIC_ELEMENTS_CREATE_OPERATION),
        )

    async def topic_elements_delete(
        self,
        topic_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return await _mutation_async(
            topic_id,
            mutation_policy,
            "deleted",
            wire.TOPIC_ELEMENTS_DELETE_OPERATION,
            lambda: wire.topic_elements_delete_request(topic_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.TOPIC_ELEMENTS_DELETE_OPERATION),
            destructive=True,
        )

    async def topic_element_update(
        self,
        topic_id: str,
        element_id: str,
        client_input: TopicElementInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return await _mutation_async(
            _element_target(topic_id, element_id),
            mutation_policy,
            "updated",
            wire.TOPIC_ELEMENT_UPDATE_OPERATION,
            lambda: wire.topic_element_update_request(topic_id, element_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.TOPIC_ELEMENT_UPDATE_OPERATION),
        )

    async def topic_element_delete(
        self,
        topic_id: str,
        element_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TopicMutationResult:
        return await _mutation_async(
            _element_target(topic_id, element_id),
            mutation_policy,
            "deleted",
            wire.TOPIC_ELEMENT_DELETE_OPERATION,
            lambda: wire.topic_element_delete_request(topic_id, element_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.TOPIC_ELEMENT_DELETE_OPERATION),
            destructive=True,
        )

    async def feature_topic(
        self, topic_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> TopicMutationResult:
        return await _mutation_async(
            topic_id,
            mutation_policy,
            "updated",
            wire.FEATURE_TOPIC_OPERATION,
            lambda: wire.feature_topic_request(topic_id, featured=True),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FEATURE_TOPIC_OPERATION),
        )

    async def unfeature_topic(
        self, topic_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> TopicMutationResult:
        return await _mutation_async(
            topic_id,
            mutation_policy,
            "updated",
            wire.UNFEATURE_TOPIC_OPERATION,
            lambda: wire.feature_topic_request(topic_id, featured=False),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFEATURE_TOPIC_OPERATION),
            destructive=True,
        )
