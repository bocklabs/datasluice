"""Dual-mode uData contact-point and visualization service."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.udata.models.contact_visualization import (
    ContactPointCreateInput,
    ContactPointUpdateInput,
    ContactVisualizationMutationResult,
    VisualizationCreateInput,
    VisualizationImageInput,
    VisualizationListQuery,
    VisualizationPage,
    VisualizationUpdateInput,
)
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import contact_visualization as wire
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.errors.catalog import NativeCatalogError

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

_VISUALIZATION_KIND = ResourceKind(wire.VISUALIZATION_KIND)
_CONTACT_POINT_KIND = ResourceKind(wire.CONTACT_POINT_KIND)
_CONTACT_POINT_COLLECTION_TARGET = "contacts"

_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    ContactVisualizationMutationResult,
    _VISUALIZATION_KIND,
)
_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    ContactVisualizationMutationResult,
    _VISUALIZATION_KIND,
)
_contact_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    ContactVisualizationMutationResult,
    _CONTACT_POINT_KIND,
)
_contact_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    ContactVisualizationMutationResult,
    _CONTACT_POINT_KIND,
)


def _visualization_target(payload: object, fallback: str) -> str:
    return _record_id(payload, fallback)


def _image_request(visualization_id: str, image: VisualizationImageInput) -> Request:
    method, path, headers = wire.visualization_image_request(visualization_id)
    return method, path, headers, image


class SyncContactVisualizationService(SyncCatalogService):
    """Typed synchronous methods for the assigned contact-point and visualization family."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def list_visualizations(self, query: VisualizationListQuery | None = None) -> VisualizationPage:
        return self._read(
            wire.list_visualizations_request(query or VisualizationListQuery()),
            wire.LIST_VISUALIZATIONS_OPERATION,
            wire.parse_visualization_page,
        )

    def create_visualization(
        self,
        client_input: VisualizationCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return _mutation(
            client_input.title,
            mutation_policy,
            "created",
            wire.CREATE_VISUALIZATION_OPERATION,
            lambda: wire.create_visualization_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_VISUALIZATION_OPERATION),
            success_target=lambda payload: _visualization_target(payload, client_input.title),
        )

    def get_visualization(self, visualization_id: str) -> MappingRecord:
        return self._read(
            wire.get_visualization_request(visualization_id), wire.GET_VISUALIZATION_OPERATION, wire.parse_visualization
        )

    def update_visualization(
        self,
        visualization_id: str,
        client_input: VisualizationUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return _mutation(
            visualization_id,
            mutation_policy,
            "updated",
            wire.UPDATE_VISUALIZATION_OPERATION,
            lambda: wire.update_visualization_request(visualization_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_VISUALIZATION_OPERATION),
        )

    def delete_visualization(
        self,
        visualization_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return _mutation(
            visualization_id,
            mutation_policy,
            "deleted",
            wire.DELETE_VISUALIZATION_OPERATION,
            lambda: wire.delete_visualization_request(visualization_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_VISUALIZATION_OPERATION),
            destructive=True,
        )

    def visualization_image(
        self,
        visualization_id: str,
        image: VisualizationImageInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return _mutation(
            visualization_id,
            mutation_policy,
            "updated",
            wire.VISUALIZATION_IMAGE_OPERATION,
            lambda: _image_request(visualization_id, image),
            lambda request: self._mutate_upload(
                request, permissions, mutation_policy, wire.VISUALIZATION_IMAGE_OPERATION
            ),
        )

    def create_contact_point(
        self,
        client_input: ContactPointCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        """Create one contact point.

        The receipt target is the contact-point collection rather than the
        caller-supplied name, so a contact-point name never enters a retained
        receipt; the server-returned identifier replaces it on success.
        """
        return _contact_mutation(
            _CONTACT_POINT_COLLECTION_TARGET,
            mutation_policy,
            "created",
            wire.CREATE_CONTACT_POINT_OPERATION,
            lambda: wire.create_contact_point_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_CONTACT_POINT_OPERATION),
            success_target=lambda payload: _record_id(payload, _CONTACT_POINT_COLLECTION_TARGET),
        )

    def get_contact_point(self, contact_point_id: str) -> MappingRecord:
        return self._read(
            wire.get_contact_point_request(contact_point_id),
            wire.GET_CONTACT_POINT_OPERATION,
            wire.parse_contact_point,
        )

    def update_contact_point(
        self,
        contact_point_id: str,
        client_input: ContactPointUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return _contact_mutation(
            contact_point_id,
            mutation_policy,
            "updated",
            wire.UPDATE_CONTACT_POINT_OPERATION,
            lambda: wire.update_contact_point_request(contact_point_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_CONTACT_POINT_OPERATION),
        )

    def delete_contact_point(
        self,
        contact_point_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return _contact_mutation(
            contact_point_id,
            mutation_policy,
            "deleted",
            wire.DELETE_CONTACT_POINT_OPERATION,
            lambda: wire.delete_contact_point_request(contact_point_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_CONTACT_POINT_OPERATION),
            destructive=True,
        )

    def contact_point_roles(self) -> tuple[MappingRecord, ...]:
        return self._read(
            wire.contact_point_roles_request(), wire.CONTACT_POINT_ROLES_OPERATION, wire.parse_contact_point_roles
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
            cast("SupportsUpload", upload),
        )


class AsyncContactVisualizationService(AsyncCatalogService):
    """Typed asynchronous methods for the assigned contact-point and visualization family."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def list_visualizations(self, query: VisualizationListQuery | None = None) -> VisualizationPage:
        return await self._read(
            wire.list_visualizations_request(query or VisualizationListQuery()),
            wire.LIST_VISUALIZATIONS_OPERATION,
            wire.parse_visualization_page,
        )

    async def create_visualization(
        self,
        client_input: VisualizationCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return await _mutation_async(
            client_input.title,
            mutation_policy,
            "created",
            wire.CREATE_VISUALIZATION_OPERATION,
            lambda: wire.create_visualization_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_VISUALIZATION_OPERATION),
            success_target=lambda payload: _visualization_target(payload, client_input.title),
        )

    async def get_visualization(self, visualization_id: str) -> MappingRecord:
        return await self._read(
            wire.get_visualization_request(visualization_id),
            wire.GET_VISUALIZATION_OPERATION,
            wire.parse_visualization,
        )

    async def update_visualization(
        self,
        visualization_id: str,
        client_input: VisualizationUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return await _mutation_async(
            visualization_id,
            mutation_policy,
            "updated",
            wire.UPDATE_VISUALIZATION_OPERATION,
            lambda: wire.update_visualization_request(visualization_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_VISUALIZATION_OPERATION),
        )

    async def delete_visualization(
        self,
        visualization_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return await _mutation_async(
            visualization_id,
            mutation_policy,
            "deleted",
            wire.DELETE_VISUALIZATION_OPERATION,
            lambda: wire.delete_visualization_request(visualization_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_VISUALIZATION_OPERATION),
            destructive=True,
        )

    async def visualization_image(
        self,
        visualization_id: str,
        image: VisualizationImageInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return await _mutation_async(
            visualization_id,
            mutation_policy,
            "updated",
            wire.VISUALIZATION_IMAGE_OPERATION,
            lambda: _image_request(visualization_id, image),
            lambda request: self._mutate_upload(
                request, permissions, mutation_policy, wire.VISUALIZATION_IMAGE_OPERATION
            ),
        )

    async def create_contact_point(
        self,
        client_input: ContactPointCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        """Create one contact point.

        The receipt target is the contact-point collection rather than the
        caller-supplied name, so a contact-point name never enters a retained
        receipt; the server-returned identifier replaces it on success.
        """
        return await _contact_mutation_async(
            _CONTACT_POINT_COLLECTION_TARGET,
            mutation_policy,
            "created",
            wire.CREATE_CONTACT_POINT_OPERATION,
            lambda: wire.create_contact_point_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_CONTACT_POINT_OPERATION),
            success_target=lambda payload: _record_id(payload, _CONTACT_POINT_COLLECTION_TARGET),
        )

    async def get_contact_point(self, contact_point_id: str) -> MappingRecord:
        return await self._read(
            wire.get_contact_point_request(contact_point_id),
            wire.GET_CONTACT_POINT_OPERATION,
            wire.parse_contact_point,
        )

    async def update_contact_point(
        self,
        contact_point_id: str,
        client_input: ContactPointUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return await _contact_mutation_async(
            contact_point_id,
            mutation_policy,
            "updated",
            wire.UPDATE_CONTACT_POINT_OPERATION,
            lambda: wire.update_contact_point_request(contact_point_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_CONTACT_POINT_OPERATION),
        )

    async def delete_contact_point(
        self,
        contact_point_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> ContactVisualizationMutationResult:
        return await _contact_mutation_async(
            contact_point_id,
            mutation_policy,
            "deleted",
            wire.DELETE_CONTACT_POINT_OPERATION,
            lambda: wire.delete_contact_point_request(contact_point_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_CONTACT_POINT_OPERATION),
            destructive=True,
        )

    async def contact_point_roles(self) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.contact_point_roles_request(), wire.CONTACT_POINT_ROLES_OPERATION, wire.parse_contact_point_roles
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
            cast("SupportsUpload", upload),
        )
