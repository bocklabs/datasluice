"""Complete dual-mode uData dataset service over the shared guarded dispatch."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from urllib.parse import unquote, urlparse

from datasluice.connectors.catalog.udata.mapping import parse_native_page, shape_dataset_page
from datasluice.connectors.catalog.udata.models.datasets import (
    DatasetCreateInput,
    DatasetDeleteOptions,
    DatasetExtrasDelete,
    DatasetExtrasUpdate,
    DatasetListQuery,
    DatasetMutationOutcome,
    DatasetSearchQuery,
    DatasetSuggestQuery,
    DatasetUpdateInput,
)
from datasluice.connectors.catalog.udata.wire import datasets as wire
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import NativeRecord, PlatformMetadata, ResultEnvelope
from datasluice.errors.catalog import CatalogError, ForbiddenError, UnauthenticatedError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient


def _receipt(
    operation: str,
    dataset_id: str,
    error: CatalogError,
    *,
    outcome: str,
) -> CatalogError:
    """Attach a bounded redacted mutation receipt to a typed failure and re-raise."""
    status = error.metadata.get("status_code")
    if not isinstance(status, int):
        status = getattr(error, "status_code", None)
    receipt = DatasetMutationOutcome(
        operation_id=operation,
        dataset_id=dataset_id,
        status_code=status if isinstance(status, int) else 0,
        outcome=outcome,
    )
    error.metadata = {**error.metadata, "receipt": receipt.to_dict()}
    raise error


def _require_mutation_permission(
    client: SyncUDataClient | AsyncUDataClient,
    operation: str,
    dataset_id: str,
    permissions: EffectivePermissions | None,
    *,
    admin: bool = False,
) -> EffectivePermissions:
    """Require explicit permission evidence before any dataset mutation dispatch.

    Mutations always need a credential; admin transitions additionally need
    known admin role evidence. Rejections raise typed pre-dispatch errors.
    """
    if client.credentials is None:
        raise UnauthenticatedError(
            "Dataset mutations require explicit uData credentials.",
            operation=operation,
            platform="udata",
            capability_state="unauthorized",
            safe_action="Construct the client with UDataCredential or a credential resolver.",
        )
    if permissions is None:
        raise ForbiddenError(
            "Dataset mutations require explicit effective-permission evidence.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Pass EffectivePermissions derived from the credential identity.",
        )
    try:
        permissions.require(operation, roles={"admin"} if admin else frozenset())
    except CatalogError as error:
        _receipt(operation, dataset_id, error, outcome="rejected")
    return permissions


class SyncDatasetsService:
    """Typed synchronous dataset operations for every assigned coverage row."""

    def __init__(self, client: SyncUDataClient) -> None:
        """Bind the service to one strict sync client."""
        self._client = client

    def list(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/1/datasets/ (row 39)."""
        method, path, _, _ = wire.list_request(query or DatasetListQuery())
        status, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["list"]
        )
        return shape_dataset_page(parse_native_page(payload))

    def create(self, client_input: DatasetCreateInput, permissions: EffectivePermissions) -> NativeRecord:
        """POST /api/1/datasets/ (row 40)."""
        operation = wire.DATASET_OPERATIONS["create"]
        _require_mutation_permission(self._client, operation, client_input.title, permissions)
        method, path, _, body = wire.create_request(client_input)
        try:
            status, payload, _ = self._client._dataset_call(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, client_input.title, error, outcome="failed")
        return wire.parse_dataset_detail(payload)

    def recent_atom(self, query: DatasetListQuery | None = None) -> NativeRecord:
        """GET /api/1/datasets/recent.atom (row 41)."""
        method, path, _, _ = wire.atom_request(query or DatasetListQuery())
        status, text, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["atom"], raw_text=True
        )
        return wire.parse_text_document(str(text).encode(), "application/atom+xml")

    def get(self, dataset_id: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/ (row 42)."""
        identifier = wire._required_id(dataset_id)
        status, payload, _ = self._client._dataset_call(
            method="GET",
            path=f"/api/1/datasets/{identifier}/",
            owning_operation=wire.DATASET_OPERATIONS["get"],
        )
        return wire.parse_dataset_detail(payload)

    def update(
        self, dataset_id: str, client_input: DatasetUpdateInput, permissions: EffectivePermissions
    ) -> NativeRecord:
        """PUT /api/1/datasets/<id>/ (row 43)."""
        operation = wire.DATASET_OPERATIONS["update"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions)
        method, path, _, body = wire.update_request(dataset_id, client_input)
        try:
            status, payload, _ = self._client._dataset_call(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return wire.parse_dataset_detail(payload)

    def delete(
        self,
        dataset_id: str,
        permissions: EffectivePermissions,
        options: DatasetDeleteOptions | None = None,
    ) -> DatasetMutationOutcome:
        """DELETE /api/1/datasets/<id>/ (row 44)."""
        operation = wire.DATASET_OPERATIONS["delete"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions)
        method, path, _, body = wire.delete_request(dataset_id, options or DatasetDeleteOptions())
        try:
            status, _, _ = self._client._dataset_call(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return DatasetMutationOutcome(
            operation_id="udata.v1.delete_dataset",
            dataset_id=dataset_id,
            status_code=status,
            outcome="deleted",
        )

    def feature(self, dataset_id: str, permissions: EffectivePermissions | None = None) -> NativeRecord:
        """POST /api/1/datasets/<id>/featured/ (row 45); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["feature"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions, admin=True)
        method, path, _, _ = wire.featured_request(dataset_id, True)
        try:
            status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return wire.parse_dataset_detail(payload)

    def unfeature(self, dataset_id: str, permissions: EffectivePermissions | None = None) -> NativeRecord:
        """DELETE /api/1/datasets/<id>/featured/ (row 46); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["unfeature"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions, admin=True)
        method, path, _, _ = wire.featured_request(dataset_id, False)
        try:
            status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return wire.parse_dataset_detail(payload)

    def rdf(self, dataset_id: str) -> NativeRecord | DatasetMutationOutcome:
        """GET /api/1/datasets/<id>/rdf (row 47).

        Follow-capable transports return the final RDF document directly;
        non-following transports yield the redirect target as a bounded
        redirect outcome carrying the unquoted target path.
        """
        operation = wire.DATASET_OPERATIONS["rdf"]
        method, path, _, _ = wire.rdf_request(dataset_id, None)
        status, text_or_headers, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True, redirect_mode=True
        )
        if status in {301, 302, 303, 307, 308}:
            headers = cast(dict[str, str], text_or_headers)
            location = str(headers.get("Location", "") or headers.get("location", ""))
            target = urlparse(location).path
            return DatasetMutationOutcome(
                operation_id="udata.v1.rdf_dataset_redirect",
                dataset_id=unquote(target.rstrip("/").rsplit("/", 1)[-1]),
                status_code=status,
                outcome=f"redirect:{target}",
            )
        return wire.parse_text_document(str(text_or_headers).encode(), "application/rdf+xml")

    def rdf_format(self, dataset_id: str, fmt: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/rdf.<format> (row 48)."""
        operation = wire.DATASET_OPERATIONS["rdf_format"]
        method, path, _, _ = wire.rdf_request(dataset_id, fmt)
        status, text, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(str(text).encode(), wire.media_type_for_format(fmt))

    def suggest(self, query: DatasetSuggestQuery) -> tuple[NativeRecord, ...]:
        """GET /api/1/datasets/suggest/ (row 67)."""
        method, path, _, _ = wire.suggest_request(query)
        status, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["suggest"]
        )
        return wire.parse_suggestions(payload)

    def search_v2(self, query: DatasetSearchQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/search/ (row 75); retains facets and native links."""
        method, path, _, _ = wire.v2_search_request(query or DatasetSearchQuery())
        status, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["v2_search"]
        )
        return _shape_v2_page(payload)

    def list_v2(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/ (row 76); retains native pagination links."""
        method, path, _, _ = wire.v2_list_request(query or DatasetListQuery())
        status, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["v2_list"]
        )
        return _shape_v2_page(payload)

    def get_v2(self, dataset_id: str) -> NativeRecord:
        """GET /api/2/datasets/<id>/ (row 77)."""
        identifier = wire._required_id(dataset_id)
        status, payload, _ = self._client._dataset_call(
            method="GET",
            path=f"/api/2/datasets/{identifier}/",
            owning_operation=wire.DATASET_OPERATIONS["v2_get"],
        )
        return wire.parse_dataset_detail(payload)

    def get_extras_v2(self, dataset_id: str) -> dict[str, object]:
        """GET /api/2/datasets/<id>/extras/ (row 78)."""
        identifier = wire._required_id(dataset_id)
        status, payload, _ = self._client._dataset_call(
            method="GET",
            path=f"/api/2/datasets/{identifier}/extras/",
            owning_operation=wire.DATASET_OPERATIONS["v2_get_extras"],
        )
        return wire.parse_extras(payload)

    def update_extras_v2(
        self, dataset_id: str, client_input: DatasetExtrasUpdate, permissions: EffectivePermissions
    ) -> dict[str, object]:
        """PUT /api/2/datasets/<id>/extras/ (row 79); null values delete keys."""
        operation = wire.DATASET_OPERATIONS["v2_update_extras"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions)
        method, path, _, body = wire.v2_extras_put_request(dataset_id, client_input)
        try:
            status, payload, _ = self._client._dataset_call(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return wire.parse_extras(payload)

    def delete_extras_v2(
        self, dataset_id: str, client_input: DatasetExtrasDelete, permissions: EffectivePermissions
    ) -> DatasetMutationOutcome:
        """DELETE /api/2/datasets/<id>/extras/ (row 80); returns the redacted receipt."""
        operation = wire.DATASET_OPERATIONS["v2_delete_extras"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions)
        method, path, _, body = wire.v2_extras_delete_request(dataset_id, client_input)
        try:
            status, _, _ = self._client._dataset_call(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return DatasetMutationOutcome(
            operation_id="udata.v2.delete_dataset_extras",
            dataset_id=dataset_id,
            status_code=status,
            outcome="extras_deleted",
        )


def _shape_v2_page(payload: object) -> ResultEnvelope[NativeRecord]:
    """Decode a v2 page retaining facets and the exact native pagination links."""
    page = parse_native_page(payload)
    envelope = shape_dataset_page(page)
    extensions: dict[str, object] = {
        "udata.nextpage": page.next_page,
        "udata.previouspage": page.previous_page,
    }
    if isinstance(payload, dict) and "facets" in payload:
        extensions["udata.facets"] = payload["facets"]
    metadata = PlatformMetadata(platform=CatalogPlatform.UDATA, extensions=extensions)
    return ResultEnvelope(items=envelope.items, page=envelope.page, platform=metadata)


class AsyncDatasetsService:
    """Typed asynchronous dataset operations mirroring the sync surface."""

    def __init__(self, client: AsyncUDataClient) -> None:
        """Bind the service to one strict async client."""
        self._client = client

    async def list(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/1/datasets/ (row 39)."""
        method, path, _, _ = wire.list_request(query or DatasetListQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["list"]
        )
        return shape_dataset_page(parse_native_page(payload))

    async def create(self, client_input: DatasetCreateInput, permissions: EffectivePermissions) -> NativeRecord:
        """POST /api/1/datasets/ (row 40)."""
        operation = wire.DATASET_OPERATIONS["create"]
        _require_mutation_permission(self._client, operation, client_input.title, permissions)
        method, path, _, body = wire.create_request(client_input)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, client_input.title, error, outcome="failed")
        return wire.parse_dataset_detail(payload)

    async def recent_atom(self, query: DatasetListQuery | None = None) -> NativeRecord:
        """GET /api/1/datasets/recent.atom (row 41)."""
        method, path, _, _ = wire.atom_request(query or DatasetListQuery())
        status, text, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["atom"], raw_text=True
        )
        return wire.parse_text_document(str(text).encode(), "application/atom+xml")

    async def get(self, dataset_id: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/ (row 42)."""
        identifier = wire._required_id(dataset_id)
        status, payload, _ = await self._client._dataset_call_async(
            method="GET",
            path=f"/api/1/datasets/{identifier}/",
            owning_operation=wire.DATASET_OPERATIONS["get"],
        )
        return wire.parse_dataset_detail(payload)

    async def update(
        self, dataset_id: str, client_input: DatasetUpdateInput, permissions: EffectivePermissions
    ) -> NativeRecord:
        """PUT /api/1/datasets/<id>/ (row 43)."""
        operation = wire.DATASET_OPERATIONS["update"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions)
        method, path, _, body = wire.update_request(dataset_id, client_input)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return wire.parse_dataset_detail(payload)

    async def delete(
        self,
        dataset_id: str,
        permissions: EffectivePermissions,
        options: DatasetDeleteOptions | None = None,
    ) -> DatasetMutationOutcome:
        """DELETE /api/1/datasets/<id>/ (row 44)."""
        operation = wire.DATASET_OPERATIONS["delete"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions)
        method, path, _, body = wire.delete_request(dataset_id, options or DatasetDeleteOptions())
        try:
            status, _, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return DatasetMutationOutcome(
            operation_id="udata.v1.delete_dataset",
            dataset_id=dataset_id,
            status_code=status,
            outcome="deleted",
        )

    async def feature(self, dataset_id: str, permissions: EffectivePermissions | None = None) -> NativeRecord:
        """POST /api/1/datasets/<id>/featured/ (row 45); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["feature"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions, admin=True)
        method, path, _, _ = wire.featured_request(dataset_id, True)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return wire.parse_dataset_detail(payload)

    async def unfeature(self, dataset_id: str, permissions: EffectivePermissions | None = None) -> NativeRecord:
        """DELETE /api/1/datasets/<id>/featured/ (row 46); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["unfeature"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions, admin=True)
        method, path, _, _ = wire.featured_request(dataset_id, False)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return wire.parse_dataset_detail(payload)

    async def rdf(self, dataset_id: str) -> NativeRecord | DatasetMutationOutcome:
        """GET /api/1/datasets/<id>/rdf (row 47); see the sync variant."""
        operation = wire.DATASET_OPERATIONS["rdf"]
        method, path, _, _ = wire.rdf_request(dataset_id, None)
        status, text_or_headers, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True, redirect_mode=True
        )
        if status in {301, 302, 303, 307, 308}:
            headers = cast(dict[str, str], text_or_headers)
            location = str(headers.get("Location", "") or headers.get("location", ""))
            target = urlparse(location).path
            return DatasetMutationOutcome(
                operation_id="udata.v1.rdf_dataset_redirect",
                dataset_id=unquote(target.rstrip("/").rsplit("/", 1)[-1]),
                status_code=status,
                outcome=f"redirect:{target}",
            )
        return wire.parse_text_document(str(text_or_headers).encode(), "application/rdf+xml")

    async def rdf_format(self, dataset_id: str, fmt: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/rdf.<format> (row 48)."""
        operation = wire.DATASET_OPERATIONS["rdf_format"]
        method, path, _, _ = wire.rdf_request(dataset_id, fmt)
        status, text, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(str(text).encode(), wire.media_type_for_format(fmt))

    async def suggest(self, query: DatasetSuggestQuery) -> tuple[NativeRecord, ...]:
        """GET /api/1/datasets/suggest/ (row 67)."""
        method, path, _, _ = wire.suggest_request(query)
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["suggest"]
        )
        return wire.parse_suggestions(payload)

    async def search_v2(self, query: DatasetSearchQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/search/ (row 75); retains facets and native links."""
        method, path, _, _ = wire.v2_search_request(query or DatasetSearchQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["v2_search"]
        )
        return _shape_v2_page(payload)

    async def list_v2(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/ (row 76); retains native pagination links."""
        method, path, _, _ = wire.v2_list_request(query or DatasetListQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.DATASET_OPERATIONS["v2_list"]
        )
        return _shape_v2_page(payload)

    async def get_v2(self, dataset_id: str) -> NativeRecord:
        """GET /api/2/datasets/<id>/ (row 77)."""
        identifier = wire._required_id(dataset_id)
        status, payload, _ = await self._client._dataset_call_async(
            method="GET",
            path=f"/api/2/datasets/{identifier}/",
            owning_operation=wire.DATASET_OPERATIONS["v2_get"],
        )
        return wire.parse_dataset_detail(payload)

    async def get_extras_v2(self, dataset_id: str) -> dict[str, object]:
        """GET /api/2/datasets/<id>/extras/ (row 78)."""
        identifier = wire._required_id(dataset_id)
        status, payload, _ = await self._client._dataset_call_async(
            method="GET",
            path=f"/api/2/datasets/{identifier}/extras/",
            owning_operation=wire.DATASET_OPERATIONS["v2_get_extras"],
        )
        return wire.parse_extras(payload)

    async def update_extras_v2(
        self, dataset_id: str, client_input: DatasetExtrasUpdate, permissions: EffectivePermissions
    ) -> dict[str, object]:
        """PUT /api/2/datasets/<id>/extras/ (row 79); null values delete keys."""
        operation = wire.DATASET_OPERATIONS["v2_update_extras"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions)
        method, path, _, body = wire.v2_extras_put_request(dataset_id, client_input)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return wire.parse_extras(payload)

    async def delete_extras_v2(
        self, dataset_id: str, client_input: DatasetExtrasDelete, permissions: EffectivePermissions
    ) -> DatasetMutationOutcome:
        """DELETE /api/2/datasets/<id>/extras/ (row 80); returns the redacted receipt."""
        operation = wire.DATASET_OPERATIONS["v2_delete_extras"]
        _require_mutation_permission(self._client, operation, dataset_id, permissions)
        method, path, _, body = wire.v2_extras_delete_request(dataset_id, client_input)
        try:
            status, _, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation, json_body=body
            )
        except CatalogError as error:
            _receipt(operation, dataset_id, error, outcome="failed")
        return DatasetMutationOutcome(
            operation_id="udata.v2.delete_dataset_extras",
            dataset_id=dataset_id,
            status_code=status,
            outcome="extras_deleted",
        )
