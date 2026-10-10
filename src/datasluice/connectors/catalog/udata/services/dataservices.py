"""Dual-mode uData dataservice and dataservice-follower service."""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.udata.models.dataservices import (
    DataserviceCreateInput,
    DataserviceDatasetLinkInput,
    DataserviceDeleteOptions,
    DataserviceFollowersQuery,
    DataserviceListQuery,
    DataserviceMutationResult,
    DataserviceSearchQuery,
    DataserviceUpdateInput,
)
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import dataservices as wire
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.errors.catalog import NativeCatalogError

from .datasets import _header, _rdf_redirect_receipt
from .posts_reports import _record_id
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
    from datasluice.domain.catalog.models import MappingRecord
    from datasluice.domain.catalog.receipts import MutationReceipt

_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    DataserviceMutationResult,
    ResourceKind.DATASET,
)
_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    DataserviceMutationResult,
    ResourceKind.DATASET,
)


_DATASERVICE_RDF_PATTERN = r"/api/1/dataservices/([^/]+)/rdf(?:\.([A-Za-z0-9_-]+))?"


def _dataset_target(dataservice_id: str, dataset_id: str) -> str:
    return f"{dataservice_id}:{dataset_id}"


class SyncDataservicesService(SyncCatalogService):
    """Typed synchronous dataservice and dataservice-follower operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def list_dataservices(self, query: DataserviceListQuery | None = None) -> MappingRecord:
        return self._read(
            wire.list_dataservices_request(query or DataserviceListQuery()),
            wire.LIST_DATASERVICES_OPERATION,
            wire.parse_mapping,
        )

    def create_dataservice(
        self,
        client_input: DataserviceCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DataserviceMutationResult:
        return _mutation(
            client_input.title,
            mutation_policy,
            "created",
            wire.CREATE_DATASERVICE_OPERATION,
            lambda: wire.create_dataservice_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_DATASERVICE_OPERATION),
            success_target=lambda payload: _record_id(payload, client_input.title),
        )

    def recent_dataservices_atom_feed(self, query: DataserviceListQuery | None = None) -> MappingRecord:
        operation = wire.RECENT_DATASERVICES_ATOM_FEED_OPERATION
        method, path, _, _ = wire.recent_dataservices_atom_feed_request(query or DataserviceListQuery())
        _, text, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(
            cast("bytes", text),
            wire._ATOM_MEDIA_TYPE,
            response_media_type=_header(response.headers, "content-type"),
            operation=operation,
        )

    def get_dataservice(self, dataservice_id: str) -> MappingRecord:
        return self._read(
            wire.get_dataservice_request(dataservice_id), wire.GET_DATASERVICE_OPERATION, wire.parse_mapping
        )

    def update_dataservice(
        self,
        dataservice_id: str,
        client_input: DataserviceUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DataserviceMutationResult:
        return _mutation(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.UPDATE_DATASERVICE_OPERATION,
            lambda: wire.update_dataservice_request(dataservice_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_DATASERVICE_OPERATION),
        )

    def delete_dataservice(
        self,
        dataservice_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
        options: DataserviceDeleteOptions | None = None,
    ) -> DataserviceMutationResult:
        return _mutation(
            dataservice_id,
            mutation_policy,
            "deleted",
            wire.DELETE_DATASERVICE_OPERATION,
            lambda: wire.delete_dataservice_request(dataservice_id, options or DataserviceDeleteOptions()),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_DATASERVICE_OPERATION),
            destructive=True,
        )

    def feature_dataservice(
        self, dataservice_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DataserviceMutationResult:
        return _mutation(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.FEATURE_DATASERVICE_OPERATION,
            lambda: wire.feature_dataservice_request(dataservice_id, featured=True),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FEATURE_DATASERVICE_OPERATION),
        )

    def unfeature_dataservice(
        self, dataservice_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DataserviceMutationResult:
        return _mutation(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.UNFEATURE_DATASERVICE_OPERATION,
            lambda: wire.feature_dataservice_request(dataservice_id, featured=False),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFEATURE_DATASERVICE_OPERATION),
        )

    def dataservice_datasets_add(
        self,
        dataservice_id: str,
        client_input: DataserviceDatasetLinkInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DataserviceMutationResult:
        return _mutation(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.DATASERVICE_DATASETS_ADD_OPERATION,
            lambda: wire.dataservice_datasets_add_request(dataservice_id, client_input),
            lambda request: self._mutate(
                request, permissions, mutation_policy, wire.DATASERVICE_DATASETS_ADD_OPERATION
            ),
        )

    def dataservice_dataset_remove(
        self,
        dataservice_id: str,
        dataset_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DataserviceMutationResult:
        return _mutation(
            _dataset_target(dataservice_id, dataset_id),
            mutation_policy,
            "deleted",
            wire.DATASERVICE_DATASET_REMOVE_OPERATION,
            lambda: wire.dataservice_dataset_remove_request(dataservice_id, dataset_id),
            lambda request: self._mutate(
                request, permissions, mutation_policy, wire.DATASERVICE_DATASET_REMOVE_OPERATION
            ),
            destructive=True,
        )

    def rdf_dataservice(self, dataservice_id: str) -> MappingRecord | MutationReceipt:
        operation = wire.RDF_DATASERVICE_OPERATION
        method, path, _, _ = wire.rdf_dataservice_request(dataservice_id)
        status, text_or_headers, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True, redirect_mode=True
        )
        if status in {301, 302, 303, 307, 308}:
            return _rdf_redirect_receipt(
                operation,
                dataservice_id,
                cast("dict[str, str]", text_or_headers),
                status,
                self._client._origin,
                _DATASERVICE_RDF_PATTERN,
                "dataservice",
            )
        negotiated = _header(response.headers, "content-type")
        return wire.parse_text_document(
            cast("bytes", text_or_headers),
            wire._RDF_MEDIA_TYPE,
            response_media_type=negotiated,
            operation=operation,
        )

    def rdf_dataservice_format(self, dataservice_id: str, fmt: str) -> MappingRecord:
        operation = wire.RDF_DATASERVICE_FORMAT_OPERATION
        method, path, _, _ = wire.rdf_dataservice_request(dataservice_id, fmt)
        _, body, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        expected = wire.media_type_for_format(fmt)
        return wire.parse_text_document(
            cast("bytes", body),
            expected,
            response_media_type=_header(response.headers, "content-type") or expected,
            operation=operation,
        )

    def search_dataservices(self, query: DataserviceSearchQuery | None = None) -> MappingRecord:
        return self._read(
            wire.search_dataservices_request(query or DataserviceSearchQuery()),
            wire.SEARCH_DATASERVICES_OPERATION,
            wire.parse_mapping,
        )

    def list_dataservice_followers(
        self, dataservice_id: str, query: DataserviceFollowersQuery | None = None
    ) -> MappingRecord:
        return self._read(
            wire.list_dataservice_followers_request(dataservice_id, query or DataserviceFollowersQuery()),
            wire.LIST_DATASERVICE_FOLLOWERS_OPERATION,
            wire.parse_mapping,
        )

    def follow_dataservice(
        self, dataservice_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DataserviceMutationResult:
        return _mutation(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.FOLLOW_DATASERVICE_OPERATION,
            lambda: wire.follow_dataservice_request(dataservice_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FOLLOW_DATASERVICE_OPERATION),
        )

    def unfollow_dataservice(
        self, dataservice_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DataserviceMutationResult:
        return _mutation(
            dataservice_id,
            mutation_policy,
            "deleted",
            wire.UNFOLLOW_DATASERVICE_OPERATION,
            lambda: wire.unfollow_dataservice_request(dataservice_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFOLLOW_DATASERVICE_OPERATION),
            destructive=True,
        )


class AsyncDataservicesService(AsyncCatalogService):
    """Typed asynchronous dataservice and dataservice-follower operations."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def list_dataservices(self, query: DataserviceListQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.list_dataservices_request(query or DataserviceListQuery()),
            wire.LIST_DATASERVICES_OPERATION,
            wire.parse_mapping,
        )

    async def create_dataservice(
        self,
        client_input: DataserviceCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            client_input.title,
            mutation_policy,
            "created",
            wire.CREATE_DATASERVICE_OPERATION,
            lambda: wire.create_dataservice_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.CREATE_DATASERVICE_OPERATION),
            success_target=lambda payload: _record_id(payload, client_input.title),
        )

    async def recent_dataservices_atom_feed(self, query: DataserviceListQuery | None = None) -> MappingRecord:
        operation = wire.RECENT_DATASERVICES_ATOM_FEED_OPERATION
        method, path, _, _ = wire.recent_dataservices_atom_feed_request(query or DataserviceListQuery())
        _, text, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(
            cast("bytes", text),
            wire._ATOM_MEDIA_TYPE,
            response_media_type=_header(response.headers, "content-type"),
            operation=operation,
        )

    async def get_dataservice(self, dataservice_id: str) -> MappingRecord:
        return await self._read(
            wire.get_dataservice_request(dataservice_id), wire.GET_DATASERVICE_OPERATION, wire.parse_mapping
        )

    async def update_dataservice(
        self,
        dataservice_id: str,
        client_input: DataserviceUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.UPDATE_DATASERVICE_OPERATION,
            lambda: wire.update_dataservice_request(dataservice_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UPDATE_DATASERVICE_OPERATION),
        )

    async def delete_dataservice(
        self,
        dataservice_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
        options: DataserviceDeleteOptions | None = None,
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            dataservice_id,
            mutation_policy,
            "deleted",
            wire.DELETE_DATASERVICE_OPERATION,
            lambda: wire.delete_dataservice_request(dataservice_id, options or DataserviceDeleteOptions()),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_DATASERVICE_OPERATION),
            destructive=True,
        )

    async def feature_dataservice(
        self, dataservice_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.FEATURE_DATASERVICE_OPERATION,
            lambda: wire.feature_dataservice_request(dataservice_id, featured=True),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FEATURE_DATASERVICE_OPERATION),
        )

    async def unfeature_dataservice(
        self, dataservice_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.UNFEATURE_DATASERVICE_OPERATION,
            lambda: wire.feature_dataservice_request(dataservice_id, featured=False),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFEATURE_DATASERVICE_OPERATION),
        )

    async def dataservice_datasets_add(
        self,
        dataservice_id: str,
        client_input: DataserviceDatasetLinkInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.DATASERVICE_DATASETS_ADD_OPERATION,
            lambda: wire.dataservice_datasets_add_request(dataservice_id, client_input),
            lambda request: self._mutate(
                request, permissions, mutation_policy, wire.DATASERVICE_DATASETS_ADD_OPERATION
            ),
        )

    async def dataservice_dataset_remove(
        self,
        dataservice_id: str,
        dataset_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            _dataset_target(dataservice_id, dataset_id),
            mutation_policy,
            "deleted",
            wire.DATASERVICE_DATASET_REMOVE_OPERATION,
            lambda: wire.dataservice_dataset_remove_request(dataservice_id, dataset_id),
            lambda request: self._mutate(
                request, permissions, mutation_policy, wire.DATASERVICE_DATASET_REMOVE_OPERATION
            ),
            destructive=True,
        )

    async def rdf_dataservice(self, dataservice_id: str) -> MappingRecord | MutationReceipt:
        operation = wire.RDF_DATASERVICE_OPERATION
        method, path, _, _ = wire.rdf_dataservice_request(dataservice_id)
        status, text_or_headers, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True, redirect_mode=True
        )
        if status in {301, 302, 303, 307, 308}:
            return _rdf_redirect_receipt(
                operation,
                dataservice_id,
                cast("dict[str, str]", text_or_headers),
                status,
                self._client._origin,
                _DATASERVICE_RDF_PATTERN,
                "dataservice",
            )
        negotiated = _header(response.headers, "content-type")
        return wire.parse_text_document(
            cast("bytes", text_or_headers),
            wire._RDF_MEDIA_TYPE,
            response_media_type=negotiated,
            operation=operation,
        )

    async def rdf_dataservice_format(self, dataservice_id: str, fmt: str) -> MappingRecord:
        operation = wire.RDF_DATASERVICE_FORMAT_OPERATION
        method, path, _, _ = wire.rdf_dataservice_request(dataservice_id, fmt)
        _, body, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        expected = wire.media_type_for_format(fmt)
        return wire.parse_text_document(
            cast("bytes", body),
            expected,
            response_media_type=_header(response.headers, "content-type") or expected,
            operation=operation,
        )

    async def search_dataservices(self, query: DataserviceSearchQuery | None = None) -> MappingRecord:
        return await self._read(
            wire.search_dataservices_request(query or DataserviceSearchQuery()),
            wire.SEARCH_DATASERVICES_OPERATION,
            wire.parse_mapping,
        )

    async def list_dataservice_followers(
        self, dataservice_id: str, query: DataserviceFollowersQuery | None = None
    ) -> MappingRecord:
        return await self._read(
            wire.list_dataservice_followers_request(dataservice_id, query or DataserviceFollowersQuery()),
            wire.LIST_DATASERVICE_FOLLOWERS_OPERATION,
            wire.parse_mapping,
        )

    async def follow_dataservice(
        self, dataservice_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            dataservice_id,
            mutation_policy,
            "updated",
            wire.FOLLOW_DATASERVICE_OPERATION,
            lambda: wire.follow_dataservice_request(dataservice_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.FOLLOW_DATASERVICE_OPERATION),
        )

    async def unfollow_dataservice(
        self, dataservice_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> DataserviceMutationResult:
        return await _mutation_async(
            dataservice_id,
            mutation_policy,
            "deleted",
            wire.UNFOLLOW_DATASERVICE_OPERATION,
            lambda: wire.unfollow_dataservice_request(dataservice_id),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.UNFOLLOW_DATASERVICE_OPERATION),
            destructive=True,
        )
