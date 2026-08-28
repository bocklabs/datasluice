"""Complete dual-mode uData dataset service over the shared guarded dispatch."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.mapping import parse_native_page, shape_dataset_page
from datasluice.connectors.catalog.udata.models.datasets import (
    DatasetCreateInput,
    DatasetDeleteOptions,
    DatasetExtrasDelete,
    DatasetExtrasUpdate,
    DatasetListQuery,
    DatasetMutationOutcome,
    DatasetSuggestQuery,
    DatasetUpdateInput,
)
from datasluice.connectors.catalog.udata.wire import datasets as wire
from datasluice.domain.catalog.models import NativeRecord, ResultEnvelope

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient


class SyncDatasetsService:
    """Typed synchronous dataset operations for every assigned coverage row."""

    def __init__(self, client: SyncUDataClient) -> None:
        """Bind the service to one strict sync client."""
        self._client = client

    def list(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/1/datasets/ (row 39)."""
        method, path, _, _ = wire.list_request(query or DatasetListQuery())
        status, payload = self._client._dataset_call(method=method, path=path)
        return shape_dataset_page(parse_native_page(payload))

    def create(self, client_input: DatasetCreateInput) -> NativeRecord:
        """POST /api/1/datasets/ (row 40)."""
        method, path, _, body = wire.create_request(client_input)
        status, payload = self._client._dataset_call(method=method, path=path, json_body=body)
        return wire.parse_dataset_detail(payload)

    def recent_atom(self, query: DatasetListQuery | None = None) -> NativeRecord:
        """GET /api/1/datasets/recent.atom (row 41)."""
        method, path, _, _ = wire.atom_request(query or DatasetListQuery())
        status, text = self._client._dataset_call(method=method, path=path, raw_text=True)
        return wire.parse_text_document(str(text).encode(), "application/atom+xml")

    def get(self, dataset_id: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/ (row 42)."""
        identifier = wire._required_id(dataset_id)
        status, payload = self._client._dataset_call(method="GET", path=f"/api/1/datasets/{identifier}/")
        return wire.parse_dataset_detail(payload)

    def update(self, dataset_id: str, client_input: DatasetUpdateInput) -> NativeRecord:
        """PUT /api/1/datasets/<id>/ (row 43)."""
        method, path, _, body = wire.update_request(dataset_id, client_input)
        status, payload = self._client._dataset_call(method=method, path=path, json_body=body)
        return wire.parse_dataset_detail(payload)

    def delete(self, dataset_id: str, options: DatasetDeleteOptions | None = None) -> DatasetMutationOutcome:
        """DELETE /api/1/datasets/<id>/ (row 44)."""
        method, path, _, body = wire.delete_request(dataset_id, options or DatasetDeleteOptions())
        status, _ = self._client._dataset_call(method=method, path=path, json_body=body)
        return DatasetMutationOutcome(
            operation_id="udata.v1.delete_dataset",
            dataset_id=dataset_id,
            status_code=status,
            outcome="deleted",
        )

    def feature(self, dataset_id: str) -> NativeRecord:
        """POST /api/1/datasets/<id>/featured/ (row 45)."""
        method, path, _, _ = wire.featured_request(dataset_id, True)
        status, payload = self._client._dataset_call(method=method, path=path)
        return wire.parse_dataset_detail(payload)

    def unfeature(self, dataset_id: str) -> NativeRecord:
        """DELETE /api/1/datasets/<id>/featured/ (row 46)."""
        method, path, _, _ = wire.featured_request(dataset_id, False)
        status, payload = self._client._dataset_call(method=method, path=path)
        return wire.parse_dataset_detail(payload)

    def rdf(self, dataset_id: str) -> int:
        """GET /api/1/datasets/<id>/rdf (row 47); returns the redirect status."""
        method, path, _, _ = wire.rdf_request(dataset_id, None)
        status, _ = self._client._dataset_call(
            method=method, path=path, allow_redirect=True, expect_statuses={301, 302}
        )
        return status

    def rdf_format(self, dataset_id: str, fmt: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/rdf.<format> (row 48)."""
        method, path, _, _ = wire.rdf_request(dataset_id, fmt)
        status, text = self._client._dataset_call(method=method, path=path, raw_text=True)
        return wire.parse_text_document(str(text).encode(), "application/rdf+xml")

    def suggest(self, query: DatasetSuggestQuery) -> tuple[NativeRecord, ...]:
        """GET /api/1/datasets/suggest/ (row 67)."""
        method, path, _, _ = wire.suggest_request(query)
        status, payload = self._client._dataset_call(method=method, path=path)
        return wire.parse_suggestions(payload)

    def search_v2(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/search/ (row 75)."""
        method, path, _, _ = wire.v2_search_request(query or DatasetListQuery())
        status, payload = self._client._dataset_call(method=method, path=path)
        return shape_dataset_page(parse_native_page(payload))

    def list_v2(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/ (row 76)."""
        method, path, _, _ = wire.v2_list_request(query or DatasetListQuery())
        status, payload = self._client._dataset_call(method=method, path=path)
        return shape_dataset_page(parse_native_page(payload))

    def get_v2(self, dataset_id: str) -> NativeRecord:
        """GET /api/2/datasets/<id>/ (row 77)."""
        method, path, _, _ = wire.v2_get_request(dataset_id)
        status, payload = self._client._dataset_call(method=method, path=path)
        return wire.parse_dataset_detail(payload)

    def get_extras_v2(self, dataset_id: str) -> dict[str, object]:
        """GET /api/2/datasets/<id>/extras/ (row 78)."""
        method, path, _, _ = wire.v2_extras_get_request(dataset_id)
        status, payload = self._client._dataset_call(method=method, path=path)
        return wire.parse_extras(payload)

    def update_extras_v2(self, dataset_id: str, client_input: DatasetExtrasUpdate) -> dict[str, object]:
        """PUT /api/2/datasets/<id>/extras/ (row 79); null values delete keys."""
        method, path, _, body = wire.v2_extras_put_request(dataset_id, client_input)
        status, payload = self._client._dataset_call(method=method, path=path, json_body=body)
        return wire.parse_extras(payload)

    def delete_extras_v2(self, dataset_id: str, client_input: DatasetExtrasDelete) -> DatasetMutationOutcome:
        """DELETE /api/2/datasets/<id>/extras/ (row 80); returns the 204 receipt."""
        method, path, _, body = wire.v2_extras_delete_request(dataset_id, client_input)
        status, payload = self._client._dataset_call(method=method, path=path, json_body=body)
        return DatasetMutationOutcome(
            operation_id="udata.v2.delete_dataset_extras",
            dataset_id=dataset_id,
            status_code=status,
            outcome="extras_deleted",
        )


class AsyncDatasetsService:
    """Typed asynchronous dataset operations mirroring the sync surface."""

    def __init__(self, client: AsyncUDataClient) -> None:
        """Bind the service to one strict async client."""
        self._client = client

    async def list(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/1/datasets/ (row 39)."""
        method, path, _, _ = wire.list_request(query or DatasetListQuery())
        status, payload = await self._client._dataset_call_async(method=method, path=path)
        return shape_dataset_page(parse_native_page(payload))

    async def create(self, client_input: DatasetCreateInput) -> NativeRecord:
        """POST /api/1/datasets/ (row 40)."""
        method, path, _, body = wire.create_request(client_input)
        status, payload = await self._client._dataset_call_async(method=method, path=path, json_body=body)
        return wire.parse_dataset_detail(payload)

    async def recent_atom(self, query: DatasetListQuery | None = None) -> NativeRecord:
        """GET /api/1/datasets/recent.atom (row 41)."""
        method, path, _, _ = wire.atom_request(query or DatasetListQuery())
        status, text = await self._client._dataset_call_async(method=method, path=path, raw_text=True)
        return wire.parse_text_document(str(text).encode(), "application/atom+xml")

    async def get(self, dataset_id: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/ (row 42)."""
        identifier = wire._required_id(dataset_id)
        status, payload = await self._client._dataset_call_async(method="GET", path=f"/api/1/datasets/{identifier}/")
        return wire.parse_dataset_detail(payload)

    async def update(self, dataset_id: str, client_input: DatasetUpdateInput) -> NativeRecord:
        """PUT /api/1/datasets/<id>/ (row 43)."""
        method, path, _, body = wire.update_request(dataset_id, client_input)
        status, payload = await self._client._dataset_call_async(method=method, path=path, json_body=body)
        return wire.parse_dataset_detail(payload)

    async def delete(self, dataset_id: str, options: DatasetDeleteOptions | None = None) -> DatasetMutationOutcome:
        """DELETE /api/1/datasets/<id>/ (row 44)."""
        method, path, _, body = wire.delete_request(dataset_id, options or DatasetDeleteOptions())
        status, _ = await self._client._dataset_call_async(method=method, path=path, json_body=body)
        return DatasetMutationOutcome(
            operation_id="udata.v1.delete_dataset",
            dataset_id=dataset_id,
            status_code=status,
            outcome="deleted",
        )

    async def feature(self, dataset_id: str) -> NativeRecord:
        """POST /api/1/datasets/<id>/featured/ (row 45)."""
        method, path, _, _ = wire.featured_request(dataset_id, True)
        status, payload = await self._client._dataset_call_async(method=method, path=path)
        return wire.parse_dataset_detail(payload)

    async def unfeature(self, dataset_id: str) -> NativeRecord:
        """DELETE /api/1/datasets/<id>/featured/ (row 46)."""
        method, path, _, _ = wire.featured_request(dataset_id, False)
        status, payload = await self._client._dataset_call_async(method=method, path=path)
        return wire.parse_dataset_detail(payload)

    async def rdf(self, dataset_id: str) -> int:
        """GET /api/1/datasets/<id>/rdf (row 47); returns the redirect status."""
        method, path, _, _ = wire.rdf_request(dataset_id, None)
        status, _ = await self._client._dataset_call_async(
            method=method, path=path, allow_redirect=True, expect_statuses={301, 302}
        )
        return status

    async def rdf_format(self, dataset_id: str, fmt: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/rdf.<format> (row 48)."""
        method, path, _, _ = wire.rdf_request(dataset_id, fmt)
        status, text = await self._client._dataset_call_async(method=method, path=path, raw_text=True)
        return wire.parse_text_document(str(text).encode(), "application/rdf+xml")

    async def suggest(self, query: DatasetSuggestQuery) -> tuple[NativeRecord, ...]:
        """GET /api/1/datasets/suggest/ (row 67)."""
        method, path, _, _ = wire.suggest_request(query)
        status, payload = await self._client._dataset_call_async(method=method, path=path)
        return wire.parse_suggestions(payload)

    async def search_v2(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/search/ (row 75)."""
        method, path, _, _ = wire.v2_search_request(query or DatasetListQuery())
        status, payload = await self._client._dataset_call_async(method=method, path=path)
        return shape_dataset_page(parse_native_page(payload))

    async def list_v2(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/ (row 76)."""
        method, path, _, _ = wire.v2_list_request(query or DatasetListQuery())
        status, payload = await self._client._dataset_call_async(method=method, path=path)
        return shape_dataset_page(parse_native_page(payload))

    async def get_v2(self, dataset_id: str) -> NativeRecord:
        """GET /api/2/datasets/<id>/ (row 77)."""
        method, path, _, _ = wire.v2_get_request(dataset_id)
        status, payload = await self._client._dataset_call_async(method=method, path=path)
        return wire.parse_dataset_detail(payload)

    async def get_extras_v2(self, dataset_id: str) -> dict[str, object]:
        """GET /api/2/datasets/<id>/extras/ (row 78)."""
        method, path, _, _ = wire.v2_extras_get_request(dataset_id)
        status, payload = await self._client._dataset_call_async(method=method, path=path)
        return wire.parse_extras(payload)

    async def update_extras_v2(self, dataset_id: str, client_input: DatasetExtrasUpdate) -> dict[str, object]:
        """PUT /api/2/datasets/<id>/extras/ (row 79); null values delete keys."""
        method, path, _, body = wire.v2_extras_put_request(dataset_id, client_input)
        status, payload = await self._client._dataset_call_async(method=method, path=path, json_body=body)
        return wire.parse_extras(payload)

    async def delete_extras_v2(self, dataset_id: str, client_input: DatasetExtrasDelete) -> DatasetMutationOutcome:
        """DELETE /api/2/datasets/<id>/extras/ (row 80); returns the 204 receipt."""
        method, path, _, body = wire.v2_extras_delete_request(dataset_id, client_input)
        status, payload = await self._client._dataset_call_async(method=method, path=path, json_body=body)
        return DatasetMutationOutcome(
            operation_id="udata.v2.delete_dataset_extras",
            dataset_id=dataset_id,
            status_code=status,
            outcome="extras_deleted",
        )
