"""Complete dual-mode uData dataset service over the shared guarded dispatch."""

from __future__ import annotations

from collections.abc import Callable
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
    DatasetMutationResult,
    DatasetSearchQuery,
    DatasetSuggestQuery,
    DatasetUpdateInput,
)
from datasluice.connectors.catalog.udata.wire import datasets as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import NativeRecord, PlatformMetadata, ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogError, ForbiddenError, UnauthenticatedError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient


def _require_mutation_permission(
    resolved: object,
    operation: str,
    target_id: str,
    permissions: EffectivePermissions | None,
    *,
    admin: bool = False,
) -> None:
    """Require a resolved uData credential plus permission evidence before dispatch."""

    if not isinstance(resolved, UDataCredential):
        error = UnauthenticatedError(
            "Dataset mutations require an explicitly resolved uData API credential.",
            operation=operation,
            platform="udata",
            capability_state="unauthorized",
            safe_action="Construct the client with UDataCredential or a resolver yielding one.",
        )
        raise _receipt(operation, target_id, error, outcome="rejected")
    if permissions is None:
        error = ForbiddenError(
            "Dataset mutations require explicit effective-permission evidence.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Pass EffectivePermissions derived from the credential identity.",
        )
        raise _receipt(operation, target_id, error, outcome="rejected")
    try:
        permissions.require(operation, roles={"admin"} if admin else frozenset())
    except CatalogError as error:
        raise _receipt(operation, target_id, error, outcome="rejected") from error


def _receipt(operation: str, target_id: str, error: CatalogError, *, outcome: str) -> CatalogError:
    """Attach a bounded redacted mutation receipt to a typed failure and re-raise."""
    status = error.metadata.get("status_code")
    if not isinstance(status, int):
        status = getattr(error, "status_code", None)
    receipt = DatasetMutationOutcome(
        operation_id=operation,
        dataset_id=target_id,
        status_code=status if isinstance(status, int) else 0,
        outcome=outcome,
    )
    raise type(error)(
        str(error),
        operation=error.operation,
        platform=error.platform,
        capability_state=error.capability_state,
        safe_action=error.safe_action,
        metadata={**error.metadata, "receipt": receipt.to_dict()},
    ) from error


def _mutating(
    operation: str,
    target_id: str,
    dispatch: Callable[[], tuple[int, object, object]],
    decode: Callable[[object], object],
    outcome: str,
) -> DatasetMutationResult:
    """Run one mutation with a receipt for every rejected, failed, and successful path."""
    try:
        status, payload, _ = dispatch()
        value = decode(payload)
    except CatalogError as error:
        raise _receipt(operation, target_id, error, outcome="failed") from error
    receipt = DatasetMutationOutcome(
        operation_id=operation,
        dataset_id=target_id,
        status_code=status,
        outcome=outcome,
    )
    return DatasetMutationResult(
        receipt=receipt,
        record=value if isinstance(value, NativeRecord) else None,
        extras=value if isinstance(value, dict) else None,
    )


class SyncDatasetsService:
    """Typed synchronous dataset operations for every assigned coverage row."""

    def __init__(self, client: SyncUDataClient) -> None:
        """Bind the service to one strict sync client."""
        self._client = client

    def list(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/1/datasets/ (row 39)."""
        operation = wire.DATASET_OPERATIONS["list"]
        method, path, _, _ = wire.list_request(query or DatasetListQuery())
        status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        return shape_dataset_page(parse_native_page(payload, operation=operation), operation=operation)

    def create(self, client_input: DatasetCreateInput, permissions: EffectivePermissions) -> DatasetMutationResult:
        """POST /api/1/datasets/ (row 40)."""
        operation = wire.DATASET_OPERATIONS["create"]
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, client_input.title, permissions)
        method, path, _, body = wire.create_request(client_input)
        return _mutating(
            operation,
            client_input.title,
            lambda: self._client._dataset_call(method=method, path=path, owning_operation=operation, json_body=body),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "created",
        )

    def recent_atom(self, query: DatasetListQuery | None = None) -> NativeRecord:
        """GET /api/1/datasets/recent.atom (row 41)."""
        operation = wire.DATASET_OPERATIONS["atom"]
        method, path, _, _ = wire.atom_request(query or DatasetListQuery())
        status, text, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(cast(bytes, text), "application/atom+xml", operation=operation)

    def get(self, dataset_id: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/ (row 42)."""
        operation = wire.DATASET_OPERATIONS["get"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = self._client._dataset_call(
            method="GET", path=f"/api/1/datasets/{wire._path_segment(identifier)}/", owning_operation=operation
        )
        return wire.parse_dataset_detail(payload, operation=operation)

    def update(
        self, dataset_id: str, client_input: DatasetUpdateInput, permissions: EffectivePermissions
    ) -> DatasetMutationResult:
        """PUT /api/1/datasets/<id>/ (row 43)."""
        operation = wire.DATASET_OPERATIONS["update"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, identifier, permissions)
        method, path, _, body = wire.update_request(dataset_id, client_input)
        return _mutating(
            operation,
            identifier,
            lambda: self._client._dataset_call(method=method, path=path, owning_operation=operation, json_body=body),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "updated",
        )

    def delete(
        self,
        dataset_id: str,
        permissions: EffectivePermissions,
        options: DatasetDeleteOptions | None = None,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """DELETE /api/1/datasets/<id>/ (row 44); requires explicit destructive confirmation."""
        operation = wire.DATASET_OPERATIONS["delete"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, identifier, permissions)
        _require_destructive_policy(operation, identifier, mutation_policy)
        method, path, _, body = wire.delete_request(dataset_id, options or DatasetDeleteOptions())
        return _mutating(
            operation,
            identifier,
            lambda: self._client._dataset_call(
                method=method,
                path=path,
                owning_operation=operation,
                json_body=body,
                allow_retry=_mutation_retry_allowed(mutation_policy),
            ),
            lambda payload: payload,
            "deleted",
        )

    def feature(self, dataset_id: str, permissions: EffectivePermissions | None = None) -> DatasetMutationResult:
        """POST /api/1/datasets/<id>/featured/ (row 45); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["feature"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, identifier, permissions, admin=True)
        method, path, _, _ = wire.featured_request(dataset_id, True)
        return _mutating(
            operation,
            identifier,
            lambda: self._client._dataset_call(method=method, path=path, owning_operation=operation),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "featured",
        )

    def unfeature(self, dataset_id: str, permissions: EffectivePermissions | None = None) -> DatasetMutationResult:
        """DELETE /api/1/datasets/<id>/featured/ (row 46); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["unfeature"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, identifier, permissions, admin=True)
        method, path, _, _ = wire.featured_request(dataset_id, False)
        return _mutating(
            operation,
            identifier,
            lambda: self._client._dataset_call(method=method, path=path, owning_operation=operation),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "unfeatured",
        )

    def rdf(self, dataset_id: str) -> NativeRecord | DatasetMutationOutcome:
        """GET /api/1/datasets/<id>/rdf (row 47)."""
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
        return wire.parse_text_document(cast(bytes, text_or_headers), "application/rdf+xml", operation=operation)

    def rdf_format(self, dataset_id: str, fmt: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/rdf.<format> (row 48)."""
        operation = wire.DATASET_OPERATIONS["rdf_format"]
        method, path, _, _ = wire.rdf_request(dataset_id, fmt)
        status, body, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        negotiated = getattr(response, "headers", {}).get("Content-Type") or wire.media_type_for_format(fmt)
        return wire.parse_text_document(cast(bytes, body), negotiated.split(";")[0].strip(), operation=operation)

    def suggest(self, query: DatasetSuggestQuery) -> tuple[NativeRecord, ...]:
        """GET /api/1/datasets/suggest/ (row 67)."""
        operation = wire.DATASET_OPERATIONS["suggest"]
        method, path, _, _ = wire.suggest_request(query)
        status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        return wire.parse_suggestions(payload, operation=operation)

    def search_v2(self, query: DatasetSearchQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/search/ (row 75); retains facets and native links."""
        operation = wire.DATASET_OPERATIONS["v2_search"]
        method, path, _, _ = wire.v2_search_request(query or DatasetSearchQuery())
        status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        return _shape_v2_page(payload, operation=operation)

    def list_v2(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/ (row 76); retains native pagination links."""
        operation = wire.DATASET_OPERATIONS["v2_list"]
        method, path, _, _ = wire.v2_list_request(query or DatasetListQuery())
        status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        return _shape_v2_page(payload, operation=operation)

    def get_v2(self, dataset_id: str) -> NativeRecord:
        """GET /api/2/datasets/<id>/ (row 77)."""
        operation = wire.DATASET_OPERATIONS["v2_get"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = self._client._dataset_call(
            method="GET", path=f"/api/2/datasets/{wire._path_segment(identifier)}/", owning_operation=operation
        )
        return wire.parse_dataset_detail(payload, operation=operation)

    def get_extras_v2(self, dataset_id: str) -> dict[str, object]:
        """GET /api/2/datasets/<id>/extras/ (row 78)."""
        operation = wire.DATASET_OPERATIONS["v2_get_extras"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = self._client._dataset_call(
            method="GET",
            path=f"/api/2/datasets/{wire._path_segment(identifier)}/extras/",
            owning_operation=operation,
        )
        return wire.parse_extras(payload, operation=operation)

    def update_extras_v2(
        self, dataset_id: str, client_input: DatasetExtrasUpdate, permissions: EffectivePermissions
    ) -> DatasetMutationResult:
        """PUT /api/2/datasets/<id>/extras/ (row 79); null values delete keys."""
        operation = wire.DATASET_OPERATIONS["v2_update_extras"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, identifier, permissions)
        method, path, _, body = wire.v2_extras_put_request(dataset_id, client_input)
        return _mutating(
            operation,
            identifier,
            lambda: self._client._dataset_call(method=method, path=path, owning_operation=operation, json_body=body),
            lambda payload: wire.parse_extras(payload, operation=operation),
            "extras_updated",
        )

    def delete_extras_v2(
        self,
        dataset_id: str,
        client_input: DatasetExtrasDelete,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """DELETE /api/2/datasets/<id>/extras/ (row 80); requires destructive confirmation."""
        operation = wire.DATASET_OPERATIONS["v2_delete_extras"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, identifier, permissions)
        _require_destructive_policy(operation, identifier, mutation_policy)
        method, path, _, body = wire.v2_extras_delete_request(dataset_id, client_input)
        return _mutating(
            operation,
            identifier,
            lambda: self._client._dataset_call(
                method=method,
                path=path,
                owning_operation=operation,
                json_body=body,
                allow_retry=_mutation_retry_allowed(mutation_policy),
            ),
            lambda payload: wire.parse_extras(payload, operation=operation) if payload else {},
            "extras_deleted",
        )


def _require_destructive_policy(operation: str, target_id: str, policy: MutationPolicy | None) -> None:
    """Require explicit confirmation and a concurrency instruction for destructive work."""

    def _reject(message: str, safe_action: str) -> CatalogError:
        error = ForbiddenError(
            message,
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action=safe_action,
        )
        return _receipt(operation, target_id, error, outcome="rejected")

    if policy is None:
        raise _reject(
            "Destructive dataset mutations require an explicit mutation policy.",
            "Pass MutationPolicy(confirmation=ConfirmationPolicy(confirmed=True), ...).",
        )
    if policy.confirmation is None or not policy.confirmation.confirmed:
        raise _reject(
            "The destructive dataset mutation is not explicitly confirmed.",
            "Pass MutationPolicy(confirmation=ConfirmationPolicy(confirmed=True)).",
        )
    if policy.concurrency is None or not policy.concurrency.allows_execution():
        raise _reject(
            "The destructive dataset mutation requires a concurrency instruction.",
            "Pass a ConcurrencyPolicy token or explicit overwrite instruction.",
        )


def _mutation_retry_allowed(policy: MutationPolicy | None) -> bool:
    """Return the caller-authorized retry instruction for one mutation."""
    return policy is not None and policy.idempotency.allows_retry()


def _shape_v2_page(payload: object, *, operation: str) -> ResultEnvelope[NativeRecord]:
    """Decode a v2 page retaining facets and the exact native pagination links."""
    page = parse_native_page(payload, operation=operation)
    envelope = shape_dataset_page(page, operation=operation)
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
        operation = wire.DATASET_OPERATIONS["list"]
        method, path, _, _ = wire.list_request(query or DatasetListQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation
        )
        return shape_dataset_page(parse_native_page(payload, operation=operation), operation=operation)

    async def create(
        self, client_input: DatasetCreateInput, permissions: EffectivePermissions
    ) -> DatasetMutationResult:
        """POST /api/1/datasets/ (row 40)."""
        operation = wire.DATASET_OPERATIONS["create"]
        resolved = await self._client._resolved_credential_async()
        _require_mutation_permission(resolved, operation, client_input.title, permissions)
        method, path, _, body = wire.create_request(client_input)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation, json_body=body
            )
            value = wire.parse_dataset_detail(payload, operation=operation)
        except CatalogError as error:
            raise _receipt(operation, client_input.title, error, outcome="failed") from error
        return DatasetMutationResult(
            receipt=DatasetMutationOutcome(
                operation_id=operation, dataset_id=client_input.title, status_code=status, outcome="created"
            ),
            record=value,
        )

    async def recent_atom(self, query: DatasetListQuery | None = None) -> NativeRecord:
        """GET /api/1/datasets/recent.atom (row 41)."""
        operation = wire.DATASET_OPERATIONS["atom"]
        method, path, _, _ = wire.atom_request(query or DatasetListQuery())
        status, body, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        return wire.parse_text_document(cast(bytes, body), "application/atom+xml", operation=operation)

    async def get(self, dataset_id: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/ (row 42)."""
        operation = wire.DATASET_OPERATIONS["get"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = await self._client._dataset_call_async(
            method="GET", path=f"/api/1/datasets/{wire._path_segment(identifier)}/", owning_operation=operation
        )
        return wire.parse_dataset_detail(payload, operation=operation)

    async def update(
        self, dataset_id: str, client_input: DatasetUpdateInput, permissions: EffectivePermissions
    ) -> DatasetMutationResult:
        """PUT /api/1/datasets/<id>/ (row 43)."""
        operation = wire.DATASET_OPERATIONS["update"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = await self._client._resolved_credential_async()
        _require_mutation_permission(resolved, operation, identifier, permissions)
        method, path, _, body = wire.update_request(dataset_id, client_input)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation, json_body=body
            )
            value = wire.parse_dataset_detail(payload, operation=operation)
        except CatalogError as error:
            raise _receipt(operation, identifier, error, outcome="failed") from error
        return DatasetMutationResult(
            receipt=DatasetMutationOutcome(
                operation_id=operation, dataset_id=identifier, status_code=status, outcome="updated"
            ),
            record=value,
        )

    async def delete(
        self,
        dataset_id: str,
        permissions: EffectivePermissions,
        options: DatasetDeleteOptions | None = None,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """DELETE /api/1/datasets/<id>/ (row 44); requires explicit destructive confirmation."""
        operation = wire.DATASET_OPERATIONS["delete"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, identifier, permissions)
        _require_destructive_policy(operation, identifier, mutation_policy)
        method, path, _, body = wire.delete_request(dataset_id, options or DatasetDeleteOptions())
        try:
            status, _, _ = await self._client._dataset_call_async(
                method=method,
                path=path,
                owning_operation=operation,
                json_body=body,
                allow_retry=_mutation_retry_allowed(mutation_policy),
            )
        except CatalogError as error:
            raise _receipt(operation, identifier, error, outcome="failed") from error
        return DatasetMutationResult(
            receipt=DatasetMutationOutcome(
                operation_id=operation, dataset_id=identifier, status_code=status, outcome="deleted"
            )
        )

    async def feature(self, dataset_id: str, permissions: EffectivePermissions | None = None) -> DatasetMutationResult:
        """POST /api/1/datasets/<id>/featured/ (row 45); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["feature"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = await self._client._resolved_credential_async()
        _require_mutation_permission(resolved, operation, identifier, permissions, admin=True)
        method, path, _, _ = wire.featured_request(dataset_id, True)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation
            )
            value = wire.parse_dataset_detail(payload, operation=operation)
        except CatalogError as error:
            raise _receipt(operation, identifier, error, outcome="failed") from error
        return DatasetMutationResult(
            receipt=DatasetMutationOutcome(
                operation_id=operation, dataset_id=identifier, status_code=status, outcome="featured"
            ),
            record=value,
        )

    async def unfeature(
        self, dataset_id: str, permissions: EffectivePermissions | None = None
    ) -> DatasetMutationResult:
        """DELETE /api/1/datasets/<id>/featured/ (row 46); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["unfeature"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = await self._client._resolved_credential_async()
        _require_mutation_permission(resolved, operation, identifier, permissions, admin=True)
        method, path, _, _ = wire.featured_request(dataset_id, False)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation
            )
            value = wire.parse_dataset_detail(payload, operation=operation)
        except CatalogError as error:
            raise _receipt(operation, identifier, error, outcome="failed") from error
        return DatasetMutationResult(
            receipt=DatasetMutationOutcome(
                operation_id=operation, dataset_id=identifier, status_code=status, outcome="unfeatured"
            ),
            record=value,
        )

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
        return wire.parse_text_document(cast(bytes, text_or_headers), "application/rdf+xml", operation=operation)

    async def rdf_format(self, dataset_id: str, fmt: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/rdf.<format> (row 48)."""
        operation = wire.DATASET_OPERATIONS["rdf_format"]
        method, path, _, _ = wire.rdf_request(dataset_id, fmt)
        status, body, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        negotiated = getattr(response, "headers", {}).get("Content-Type") or wire.media_type_for_format(fmt)
        return wire.parse_text_document(cast(bytes, body), negotiated.split(";")[0].strip(), operation=operation)

    async def suggest(self, query: DatasetSuggestQuery) -> tuple[NativeRecord, ...]:
        """GET /api/1/datasets/suggest/ (row 67)."""
        operation = wire.DATASET_OPERATIONS["suggest"]
        method, path, _, _ = wire.suggest_request(query)
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation
        )
        return wire.parse_suggestions(payload, operation=operation)

    async def search_v2(self, query: DatasetSearchQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/search/ (row 75); retains facets and native links."""
        operation = wire.DATASET_OPERATIONS["v2_search"]
        method, path, _, _ = wire.v2_search_request(query or DatasetSearchQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation
        )
        return _shape_v2_page(payload, operation=operation)

    async def list_v2(self, query: DatasetListQuery | None = None) -> ResultEnvelope[NativeRecord]:
        """GET /api/2/datasets/ (row 76); retains native pagination links."""
        operation = wire.DATASET_OPERATIONS["v2_list"]
        method, path, _, _ = wire.v2_list_request(query or DatasetListQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation
        )
        return _shape_v2_page(payload, operation=operation)

    async def get_v2(self, dataset_id: str) -> NativeRecord:
        """GET /api/2/datasets/<id>/ (row 77)."""
        operation = wire.DATASET_OPERATIONS["v2_get"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = await self._client._dataset_call_async(
            method="GET", path=f"/api/2/datasets/{wire._path_segment(identifier)}/", owning_operation=operation
        )
        return wire.parse_dataset_detail(payload, operation=operation)

    async def get_extras_v2(self, dataset_id: str) -> dict[str, object]:
        """GET /api/2/datasets/<id>/extras/ (row 78)."""
        operation = wire.DATASET_OPERATIONS["v2_get_extras"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = await self._client._dataset_call_async(
            method="GET",
            path=f"/api/2/datasets/{wire._path_segment(identifier)}/extras/",
            owning_operation=operation,
        )
        return wire.parse_extras(payload, operation=operation)

    async def update_extras_v2(
        self, dataset_id: str, client_input: DatasetExtrasUpdate, permissions: EffectivePermissions
    ) -> DatasetMutationResult:
        """PUT /api/2/datasets/<id>/extras/ (row 79); null values delete keys."""
        operation = wire.DATASET_OPERATIONS["v2_update_extras"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = await self._client._resolved_credential_async()
        _require_mutation_permission(resolved, operation, identifier, permissions)
        method, path, _, body = wire.v2_extras_put_request(dataset_id, client_input)
        try:
            status, payload, _ = await self._client._dataset_call_async(
                method=method, path=path, owning_operation=operation, json_body=body
            )
            value = wire.parse_extras(payload, operation=operation)
        except CatalogError as error:
            raise _receipt(operation, identifier, error, outcome="failed") from error
        return DatasetMutationResult(
            receipt=DatasetMutationOutcome(
                operation_id=operation, dataset_id=identifier, status_code=status, outcome="extras_updated"
            ),
            extras=value,
        )

    async def delete_extras_v2(
        self,
        dataset_id: str,
        client_input: DatasetExtrasDelete,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """DELETE /api/2/datasets/<id>/extras/ (row 80); requires destructive confirmation."""
        operation = wire.DATASET_OPERATIONS["v2_delete_extras"]
        identifier = wire._required_id(dataset_id, operation=operation)
        resolved = self._client._resolved_credential()
        _require_mutation_permission(resolved, operation, identifier, permissions)
        _require_destructive_policy(operation, identifier, mutation_policy)
        method, path, _, body = wire.v2_extras_delete_request(dataset_id, client_input)
        try:
            status, _, _ = await self._client._dataset_call_async(
                method=method,
                path=path,
                owning_operation=operation,
                json_body=body,
                allow_retry=_mutation_retry_allowed(mutation_policy),
            )
        except CatalogError as error:
            raise _receipt(operation, identifier, error, outcome="failed") from error
        return DatasetMutationResult(
            receipt=DatasetMutationOutcome(
                operation_id=operation, dataset_id=identifier, status_code=status, outcome="extras_deleted"
            )
        )
