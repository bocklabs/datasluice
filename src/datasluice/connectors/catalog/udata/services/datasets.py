"""Complete dual-mode uData dataset service over the shared guarded dispatch."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Never, cast
from urllib.parse import unquote, urlsplit

from datasluice.connectors.catalog.udata.mapping import UDataPageEnvelope, parse_native_page, shape_dataset_page
from datasluice.connectors.catalog.udata.models.datasets import (
    DatasetCreateInput,
    DatasetDeleteOptions,
    DatasetExtrasDelete,
    DatasetExtrasUpdate,
    DatasetListQuery,
    DatasetMutationResult,
    DatasetSearchQuery,
    DatasetSuggestQuery,
    DatasetUpdateInput,
)
from datasluice.connectors.catalog.udata.wire import datasets as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential, credential_scope
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import NativeRecord, PlatformMetadata
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.redaction import contains_credential_content, redact_string
from datasluice.domain.catalog.safety import ConcurrencyPolicy, IdempotencyPolicy, MutationPolicy
from datasluice.errors.catalog import (
    CatalogError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
    attach_catalog_metadata,
)
from datasluice.runtime.mutation import build_mutation_receipt
from datasluice.runtime.transport.base import TransportFailure

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient


def _require_mutation_permission(
    resolved: object,
    operation: str,
    target_id: str,
    permissions: EffectivePermissions | None,
    *,
    admin: bool = False,
) -> UDataCredential:
    """Require a resolved uData credential plus permission evidence before dispatch."""

    if not isinstance(resolved, UDataCredential):
        raise UnauthenticatedError(
            "Dataset mutations require an explicitly resolved uData API credential.",
            operation=operation,
            platform="udata",
            capability_state="unauthorized",
            safe_action="Construct the client with UDataCredential or a resolver yielding one.",
        )
    if permissions is None:
        raise ForbiddenError(
            "Dataset mutations require explicit effective-permission evidence.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Pass EffectivePermissions.for_credential for the resolved credential identity.",
        )
    if permissions.platform != CatalogPlatform.UDATA:
        raise ForbiddenError(
            "Dataset mutation permission evidence must declare the uData platform.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Pass EffectivePermissions for the uData platform.",
        )
    if not permissions.authenticated:
        raise ForbiddenError(
            "Dataset mutation permission evidence must be authenticated.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Pass authenticated EffectivePermissions derived from the resolved credential.",
        )
    expected_scope = credential_scope(resolved)
    if permissions.credential_scope != expected_scope:
        raise ForbiddenError(
            "Dataset mutation permission evidence does not match the resolved credential identity.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Build permission evidence with EffectivePermissions.for_credential using this credential.",
        )
    try:
        permissions.require(operation, roles={"admin"} if admin else frozenset())
    except CatalogError as error:
        raise error
    return resolved


def _operation_id(operation: str) -> OperationId:
    """Convert one profile operation string into its typed identity."""
    platform, _, tail = operation.partition("/")
    service, _, method = tail.partition(".")
    return OperationId(platform=platform, service=service or "native", method=method or tail)


def _safe_target_value(target_id: object, *, opaque: bool = False) -> str:
    """Return a bounded receipt target without retaining credential-shaped input."""
    raw = target_id if isinstance(target_id, str) else type(target_id).__name__
    if opaque or contains_credential_content(raw):
        digest = hashlib.sha256(raw.encode()).hexdigest()[:24]
        return f"request:{digest}"
    scrubbed = redact_string(raw)
    if not scrubbed or len(scrubbed) > 64 or any(not character.isprintable() for character in scrubbed):
        digest = hashlib.sha256(raw.encode()).hexdigest()[:24]
        return f"request:{digest}"
    return scrubbed


def _receipt_target(target_id: object, *, opaque: bool = False) -> CatalogId:
    return CatalogId(
        platform=CatalogPlatform.UDATA,
        resource_kind=ResourceKind.DATASET,
        value=_safe_target_value(target_id, opaque=opaque),
    )


def _receipt_policy(policy: MutationPolicy | None) -> MutationPolicy:
    """Create a receipt-only policy that never retains unsupported tokens."""
    if policy is None:
        return MutationPolicy(concurrency=ConcurrencyPolicy(overwrite=True))
    return MutationPolicy(
        destructive=policy.destructive,
        concurrency=ConcurrencyPolicy(overwrite=True),
        idempotency=IdempotencyPolicy(
            safe=policy.idempotency.safe,
            explicit_retry_opt_in=policy.idempotency.explicit_retry_opt_in,
        ),
        dry_run=policy.dry_run,
    )


def _policy_metadata(policy: MutationPolicy | None) -> dict[str, object]:
    if policy is None:
        return {"provided": False}
    confirmation = policy.confirmation
    concurrency = policy.concurrency
    return {
        "provided": True,
        "destructive": policy.destructive,
        "confirmed": confirmation is not None and confirmation.confirmed,
        "overwrite": concurrency is not None and concurrency.overwrite,
        "concurrency_provided": concurrency is not None,
        "retry_safe": policy.idempotency.safe,
        "retry_opt_in": policy.idempotency.explicit_retry_opt_in,
        "idempotency_provided": policy.idempotency.key is not None,
        "dry_run": policy.dry_run.requested,
    }


def _build_receipt(
    operation: str,
    target_id: object,
    policy: MutationPolicy | None,
    outcome: str,
    *,
    status_code: int = 0,
    mutation: str,
    opaque_target: bool = False,
) -> MutationReceipt:
    return build_mutation_receipt(
        _operation_id(operation),
        _receipt_target(target_id, opaque=opaque_target),
        _receipt_policy(policy),
        outcome,
        {
            "mutation": mutation,
            "status_code": status_code,
            "policy": _policy_metadata(policy),
        },
    )


def _attach_receipt(error: BaseException, receipt: MutationReceipt) -> BaseException:
    """Attach a shared receipt while preserving the original exception type."""
    attach_catalog_metadata(error, {"receipt": receipt.to_dict()})
    error_dict = getattr(error, "__dict__", None)
    if isinstance(error_dict, dict):
        error_dict["mutation_receipt"] = receipt
    return error


def _raise_with_receipt(error: BaseException, receipt: MutationReceipt) -> None:
    attached = _attach_receipt(error, receipt)
    if attached is error:
        raise error
    raise attached from error


def _error_status(error: BaseException, response: object | None = None) -> int:
    status = getattr(error, "status_code", None)
    if isinstance(status, int):
        return status
    metadata = getattr(error, "metadata", None)
    if isinstance(metadata, Mapping) and isinstance(metadata.get("status_code"), int):
        return cast(int, metadata["status_code"])
    response_status = getattr(response, "status_code", None)
    return response_status if isinstance(response_status, int) else 0


def _mutation_outcome(error: BaseException, response: object | None = None) -> str:
    if isinstance(error, BaseException) and error.__class__.__name__ == "CancelledError":
        return "cancelled"
    metadata = getattr(error, "metadata", None)
    if isinstance(metadata, Mapping) and metadata.get("ambiguous") is True:
        return "ambiguous"
    response_status = getattr(response, "status_code", None)
    if isinstance(response_status, int) and 200 <= response_status < 300:
        return "ambiguous"
    if isinstance(error, TransportFailure):
        return "ambiguous"
    error_status = _error_status(error, response)
    if error_status == 0 and isinstance(error, (UnauthenticatedError, ForbiddenError, CatalogValidationError)):
        return "rejected"
    return "failed"


def _header(headers: Mapping[str, str], name: str) -> str | None:
    """Read one response header without depending on its casing."""
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


def _redirect_receipt(
    operation: str,
    requested_id: str,
    headers: Mapping[str, str],
    status_code: int,
    origin: str,
) -> MutationReceipt:
    """Validate a native RDF redirect and retain a shared redacted receipt."""
    location = _header(headers, "location")
    if not location:
        raise NativeCatalogError(
            "The uData RDF redirect response omits its Location header.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    parsed = urlsplit(location)
    configured = urlsplit(origin)
    if parsed.query or parsed.fragment or parsed.scheme not in ("", configured.scheme):
        raise NativeCatalogError(
            "The uData RDF redirect target is not a same-origin URL.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    if parsed.netloc and parsed.netloc != configured.netloc:
        raise NativeCatalogError(
            "The uData RDF redirect points outside the configured deployment origin.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    match = re.fullmatch(r"/api/1/datasets/([^/]+)/rdf(?:\.([A-Za-z0-9_-]+))?", parsed.path)
    if match is None:
        raise NativeCatalogError(
            "The uData RDF redirect target is not a dataset RDF document.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    dataset_id = unquote(match.group(1))
    if dataset_id != requested_id or any(character in dataset_id for character in "/?#"):
        raise NativeCatalogError(
            "The uData RDF redirect target does not identify the requested dataset.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    extension = match.group(2)
    if extension is not None:
        wire.media_type_for_format(extension)
    return _build_receipt(
        operation,
        requested_id,
        None,
        "skipped",
        status_code=status_code,
        mutation="rdf_redirect",
    )


def _sync_create_dispatch(
    client: SyncUDataClient,
    client_input: DatasetCreateInput,
    operation: str,
    permissions: EffectivePermissions,
    mutation_policy: MutationPolicy | None,
) -> tuple[int, object, object]:
    resolved = _require_mutation_permission(client._resolved_credential(), operation, client_input.title, permissions)
    _enforce_mutation_policy(operation, client_input.title, mutation_policy)
    method, path, _, body = wire.create_request(client_input)
    return client._dataset_call(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


def _sync_update_dispatch(
    client: SyncUDataClient,
    dataset_id: str,
    client_input: DatasetUpdateInput,
    operation: str,
    permissions: EffectivePermissions,
    mutation_policy: MutationPolicy | None,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = _require_mutation_permission(client._resolved_credential(), operation, identifier, permissions)
    _enforce_mutation_policy(operation, identifier, mutation_policy)
    method, path, _, body = wire.update_request(identifier, client_input)
    return client._dataset_call(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


def _sync_delete_dispatch(
    client: SyncUDataClient,
    dataset_id: str,
    permissions: EffectivePermissions,
    options: DatasetDeleteOptions,
    mutation_policy: MutationPolicy | None,
    operation: str,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = _require_mutation_permission(client._resolved_credential(), operation, identifier, permissions)
    _enforce_mutation_policy(operation, identifier, mutation_policy, destructive=True)
    method, path, _, body = wire.delete_request(identifier, options)
    return client._dataset_call(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


def _sync_feature_dispatch(
    client: SyncUDataClient,
    dataset_id: str,
    permissions: EffectivePermissions | None,
    mutation_policy: MutationPolicy | None,
    operation: str,
    *,
    featured: bool,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = _require_mutation_permission(
        client._resolved_credential(), operation, identifier, permissions, admin=True
    )
    _enforce_mutation_policy(operation, identifier, mutation_policy, destructive=not featured)
    method, path, _, body = wire.featured_request(identifier, featured)
    return client._dataset_call(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


def _sync_extras_update_dispatch(
    client: SyncUDataClient,
    dataset_id: str,
    client_input: DatasetExtrasUpdate,
    operation: str,
    permissions: EffectivePermissions,
    mutation_policy: MutationPolicy | None,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = _require_mutation_permission(client._resolved_credential(), operation, identifier, permissions)
    _enforce_mutation_policy(operation, identifier, mutation_policy)
    method, path, _, body = wire.v2_extras_put_request(identifier, client_input)
    return client._dataset_call(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


def _sync_extras_delete_dispatch(
    client: SyncUDataClient,
    dataset_id: str,
    client_input: DatasetExtrasDelete,
    operation: str,
    permissions: EffectivePermissions,
    mutation_policy: MutationPolicy | None,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = _require_mutation_permission(client._resolved_credential(), operation, identifier, permissions)
    _enforce_mutation_policy(operation, identifier, mutation_policy, destructive=True)
    method, path, _, body = wire.v2_extras_delete_request(identifier, client_input)
    return client._dataset_call(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


async def _async_create_dispatch(
    client: AsyncUDataClient,
    client_input: DatasetCreateInput,
    operation: str,
    permissions: EffectivePermissions,
    mutation_policy: MutationPolicy | None,
) -> tuple[int, object, object]:
    resolved = await client._resolved_credential_async()
    _require_mutation_permission(resolved, operation, client_input.title, permissions)
    _enforce_mutation_policy(operation, client_input.title, mutation_policy)
    method, path, _, body = wire.create_request(client_input)
    return await client._dataset_call_async(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


async def _async_update_dispatch(
    client: AsyncUDataClient,
    dataset_id: str,
    client_input: DatasetUpdateInput,
    operation: str,
    permissions: EffectivePermissions,
    mutation_policy: MutationPolicy | None,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = await client._resolved_credential_async()
    _require_mutation_permission(resolved, operation, identifier, permissions)
    _enforce_mutation_policy(operation, identifier, mutation_policy)
    method, path, _, body = wire.update_request(identifier, client_input)
    return await client._dataset_call_async(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


async def _async_delete_dispatch(
    client: AsyncUDataClient,
    dataset_id: str,
    permissions: EffectivePermissions,
    options: DatasetDeleteOptions,
    mutation_policy: MutationPolicy | None,
    operation: str,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = await client._resolved_credential_async()
    _require_mutation_permission(resolved, operation, identifier, permissions)
    _enforce_mutation_policy(operation, identifier, mutation_policy, destructive=True)
    method, path, _, body = wire.delete_request(identifier, options)
    return await client._dataset_call_async(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


async def _async_feature_dispatch(
    client: AsyncUDataClient,
    dataset_id: str,
    permissions: EffectivePermissions | None,
    mutation_policy: MutationPolicy | None,
    operation: str,
    *,
    featured: bool,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = await client._resolved_credential_async()
    _require_mutation_permission(resolved, operation, identifier, permissions, admin=True)
    _enforce_mutation_policy(operation, identifier, mutation_policy, destructive=not featured)
    method, path, _, body = wire.featured_request(identifier, featured)
    return await client._dataset_call_async(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


async def _async_extras_update_dispatch(
    client: AsyncUDataClient,
    dataset_id: str,
    client_input: DatasetExtrasUpdate,
    operation: str,
    permissions: EffectivePermissions,
    mutation_policy: MutationPolicy | None,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = await client._resolved_credential_async()
    _require_mutation_permission(resolved, operation, identifier, permissions)
    _enforce_mutation_policy(operation, identifier, mutation_policy)
    method, path, _, body = wire.v2_extras_put_request(identifier, client_input)
    return await client._dataset_call_async(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


async def _async_extras_delete_dispatch(
    client: AsyncUDataClient,
    dataset_id: str,
    client_input: DatasetExtrasDelete,
    operation: str,
    permissions: EffectivePermissions,
    mutation_policy: MutationPolicy | None,
) -> tuple[int, object, object]:
    identifier = wire._required_id(dataset_id, operation=operation)
    resolved = await client._resolved_credential_async()
    _require_mutation_permission(resolved, operation, identifier, permissions)
    _enforce_mutation_policy(operation, identifier, mutation_policy, destructive=True)
    method, path, _, body = wire.v2_extras_delete_request(identifier, client_input)
    return await client._dataset_call_async(
        method=method,
        path=path,
        owning_operation=operation,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=mutation_policy.idempotency if mutation_policy is not None else None,
    )


def _mutating(
    operation: str,
    target_id: object,
    policy: MutationPolicy | None,
    dispatch: Callable[[], tuple[int, object, object]],
    decode: Callable[[object], object],
    mutation: str,
    *,
    opaque_target: bool = False,
    success_target: Callable[[object], object] | None = None,
) -> DatasetMutationResult:
    """Run one mutation with a receipt for every rejected, failed, and successful path."""
    response: object | None = None
    try:
        status, payload, response = dispatch()
        value = decode(payload)
    except BaseException as error:
        receipt = _build_receipt(
            operation,
            target_id,
            policy,
            _mutation_outcome(error, response),
            status_code=_error_status(error, response),
            mutation=mutation,
            opaque_target=opaque_target,
        )
        _raise_with_receipt(error, receipt)
    receipt_target = success_target(value) if success_target is not None else target_id
    receipt = _build_receipt(operation, receipt_target, policy, "succeeded", status_code=status, mutation=mutation)
    return DatasetMutationResult(
        receipt=receipt,
        record=value if isinstance(value, NativeRecord) else None,
        extras=value if isinstance(value, Mapping) else None,
    )


async def _amutating(
    operation: str,
    target_id: object,
    policy: MutationPolicy | None,
    dispatch: Callable[[], object],
    decode: Callable[[object], object],
    mutation: str,
    *,
    opaque_target: bool = False,
    success_target: Callable[[object], object] | None = None,
) -> DatasetMutationResult:
    """Run one async mutation with the same receipt seam as the sync path."""
    response: object | None = None
    try:
        status, payload, response = await cast("Awaitable[tuple[int, object, object]]", dispatch())
        value = decode(payload)
    except BaseException as error:
        receipt = _build_receipt(
            operation,
            target_id,
            policy,
            _mutation_outcome(error, response),
            status_code=_error_status(error, response),
            mutation=mutation,
            opaque_target=opaque_target,
        )
        _raise_with_receipt(error, receipt)
    receipt_target = success_target(value) if success_target is not None else target_id
    receipt = _build_receipt(operation, receipt_target, policy, "succeeded", status_code=status, mutation=mutation)
    return DatasetMutationResult(
        receipt=receipt,
        record=value if isinstance(value, NativeRecord) else None,
        extras=value if isinstance(value, Mapping) else None,
    )


class SyncDatasetsService:
    """Typed synchronous dataset operations for every assigned coverage row."""

    def __init__(self, client: SyncUDataClient) -> None:
        """Bind the service to one strict sync client."""
        self._client = client

    def list(self, query: DatasetListQuery | None = None) -> UDataPageEnvelope:
        """GET /api/1/datasets/ (row 39)."""
        operation = wire.DATASET_OPERATIONS["list"]
        method, path, _, _ = wire.list_request(query or DatasetListQuery())
        status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        return _shape_page(payload, operation=operation)

    def create(
        self,
        client_input: DatasetCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """POST /api/1/datasets/ (row 40)."""
        operation = wire.DATASET_OPERATIONS["create"]
        return _mutating(
            operation,
            getattr(client_input, "title", "<create>"),
            mutation_policy,
            lambda: _sync_create_dispatch(self._client, client_input, operation, permissions, mutation_policy),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "created",
            opaque_target=True,
            success_target=lambda value: value.id.value if isinstance(value, NativeRecord) else "<create>",
        )

    def recent_atom(self, query: DatasetListQuery | None = None) -> NativeRecord:
        """GET /api/1/datasets/recent.atom (row 41)."""
        operation = wire.DATASET_OPERATIONS["atom"]
        method, path, _, _ = wire.atom_request(query or DatasetListQuery())
        status, text, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        negotiated = _header(response.headers, "content-type")
        return wire.parse_text_document(
            cast(bytes, text), "application/atom+xml", response_media_type=negotiated, operation=operation
        )

    def get(self, dataset_id: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/ (row 42)."""
        operation = wire.DATASET_OPERATIONS["get"]
        status, payload, _ = self._client._dataset_call(
            method="GET",
            path=f"/api/1/datasets/{wire._path_segment(wire._required_id(dataset_id, operation=operation))}/",
            owning_operation=operation,
        )
        return wire.parse_dataset_detail(payload, operation=operation)

    def update(
        self,
        dataset_id: str,
        client_input: DatasetUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """PUT /api/1/datasets/<id>/ (row 43)."""
        operation = wire.DATASET_OPERATIONS["update"]
        return _mutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _sync_update_dispatch(
                self._client, dataset_id, client_input, operation, permissions, mutation_policy
            ),
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
        return _mutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _sync_delete_dispatch(
                self._client, dataset_id, permissions, options or DatasetDeleteOptions(), mutation_policy, operation
            ),
            lambda payload: payload,
            "deleted",
        )

    def feature(
        self,
        dataset_id: str,
        permissions: EffectivePermissions | None = None,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """POST /api/1/datasets/<id>/featured/ (row 45); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["feature"]
        return _mutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _sync_feature_dispatch(
                self._client, dataset_id, permissions, mutation_policy, operation, featured=True
            ),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "featured",
        )

    def unfeature(
        self,
        dataset_id: str,
        permissions: EffectivePermissions | None = None,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """DELETE /api/1/datasets/<id>/featured/ (row 46); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["unfeature"]
        return _mutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _sync_feature_dispatch(
                self._client, dataset_id, permissions, mutation_policy, operation, featured=False
            ),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "unfeatured",
        )

    def rdf(self, dataset_id: str) -> NativeRecord | MutationReceipt:
        """GET /api/1/datasets/<id>/rdf (row 47)."""
        operation = wire.DATASET_OPERATIONS["rdf"]
        method, path, _, _ = wire.rdf_request(dataset_id, None)
        status, text_or_headers, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True, redirect_mode=True
        )
        if status in {301, 302, 303, 307, 308}:
            return _redirect_receipt(
                operation,
                dataset_id,
                cast(Mapping[str, str], text_or_headers),
                status,
                self._client._origin,
            )
        negotiated = _header(response.headers, "content-type")
        return wire.parse_text_document(
            cast(bytes, text_or_headers), "application/rdf+xml", response_media_type=negotiated, operation=operation
        )

    def rdf_format(self, dataset_id: str, fmt: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/rdf.<format> (row 48)."""
        operation = wire.DATASET_OPERATIONS["rdf_format"]
        method, path, _, _ = wire.rdf_request(dataset_id, fmt)
        status, body, response = self._client._dataset_call(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        negotiated = _header(response.headers, "content-type") or wire.media_type_for_format(fmt)
        return wire.parse_text_document(
            cast(bytes, body), wire.media_type_for_format(fmt), response_media_type=negotiated, operation=operation
        )

    def suggest(self, query: DatasetSuggestQuery) -> tuple[NativeRecord, ...]:
        """GET /api/1/datasets/suggest/ (row 67)."""
        operation = wire.DATASET_OPERATIONS["suggest"]
        method, path, _, _ = wire.suggest_request(query)
        status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        return wire.parse_suggestions(payload, operation=operation)

    def search_v2(self, query: DatasetSearchQuery | None = None) -> UDataPageEnvelope:
        """GET /api/2/datasets/search/ (row 75); retains facets and native links."""
        operation = wire.DATASET_OPERATIONS["v2_search"]
        method, path, _, _ = wire.v2_search_request(query or DatasetSearchQuery())
        status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        return _shape_page(payload, operation=operation)

    def list_v2(self, query: DatasetListQuery | None = None) -> UDataPageEnvelope:
        """GET /api/2/datasets/ (row 76); retains native pagination links."""
        operation = wire.DATASET_OPERATIONS["v2_list"]
        method, path, _, _ = wire.v2_list_request(query or DatasetListQuery())
        status, payload, _ = self._client._dataset_call(method=method, path=path, owning_operation=operation)
        return _shape_page(payload, operation=operation)

    def get_v2(self, dataset_id: str) -> NativeRecord:
        """GET /api/2/datasets/<id>/ (row 77)."""
        operation = wire.DATASET_OPERATIONS["v2_get"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = self._client._dataset_call(
            method="GET", path=f"/api/2/datasets/{wire._path_segment(identifier)}/", owning_operation=operation
        )
        return wire.parse_dataset_detail(payload, operation=operation)

    def get_extras_v2(self, dataset_id: str) -> Mapping[str, object]:
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
        self,
        dataset_id: str,
        client_input: DatasetExtrasUpdate,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """PUT /api/2/datasets/<id>/extras/ (row 79); null values delete keys."""
        operation = wire.DATASET_OPERATIONS["v2_update_extras"]
        return _mutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _sync_extras_update_dispatch(
                self._client, dataset_id, client_input, operation, permissions, mutation_policy
            ),
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
        return _mutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _sync_extras_delete_dispatch(
                self._client, dataset_id, client_input, operation, permissions, mutation_policy
            ),
            lambda payload: _decode_mutation_extras(payload, operation),
            "extras_deleted",
        )


def _enforce_mutation_policy(
    operation: str,
    target_id: str,
    policy: MutationPolicy | None,
    *,
    destructive: bool = False,
) -> None:
    """Enforce the shared mutation policy contract for any mutating dispatch.

    Every mutation requires an explicit confirmed policy. Destructive
    transitions additionally require the destructive flag, an explicit
    overwrite concurrency instruction, and no unsupported dry-run request
    or concurrency token (the stock uData API supports neither).
    Idempotency keys alone never authorize retries because the stock uData
    API has no idempotency-key header support.
    """

    def _reject(message: str, safe_action: str) -> Never:
        raise ForbiddenError(
            message,
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action=safe_action,
        )

    if not isinstance(policy, MutationPolicy):
        _reject(
            "Dataset mutations require an explicit MutationPolicy.",
            "Pass MutationPolicy(confirmation=ConfirmationPolicy(confirmed=True), ...).",
        )
    confirmation = policy.confirmation
    if confirmation is None or not confirmation.confirmed:
        _reject(
            "The dataset mutation is not explicitly confirmed.",
            "Pass MutationPolicy(confirmation=ConfirmationPolicy(confirmed=True)).",
        )
    if confirmation.operation != operation or confirmation.target != target_id:
        _reject(
            "Mutation confirmation must be bound to this operation and target.",
            "Pass ConfirmationPolicy(confirmed=True, operation=..., target=...).",
        )
    if policy.dry_run.requested:
        _reject(
            "The stock uData API does not support dry-run previews.",
            "Retry without requesting a dry-run preview.",
        )
    if policy.concurrency is None or not policy.concurrency.overwrite:
        _reject(
            "The uData mutation requires explicit overwrite intent because no conditional token is supported.",
            "Pass ConcurrencyPolicy(overwrite=True).",
        )
    if policy.concurrency.token is not None:
        _reject(
            "The stock uData API has no conditional concurrency token support.",
            "Pass ConcurrencyPolicy(overwrite=True) for unconditional writes.",
        )
    if policy.idempotency.key is not None:
        _reject(
            "The stock uData API does not support idempotency-key retry authorization.",
            "Retry without an idempotency key or use an endpoint with documented server-side deduplication.",
        )
    if destructive:
        if not policy.destructive:
            _reject(
                "The destructive dataset mutation must declare MutationPolicy(destructive=True).",
                "Pass MutationPolicy(destructive=True) for delete transitions.",
            )
    elif policy.destructive:
        _reject(
            "A standard dataset mutation cannot carry a destructive policy tier.",
            "Pass MutationPolicy(destructive=False, ...).",
        )


def _mutation_retry_allowed(policy: MutationPolicy | None) -> bool:
    """Return the caller-authorized retry instruction for one mutation."""
    return isinstance(policy, MutationPolicy) and (policy.idempotency.safe or policy.idempotency.explicit_retry_opt_in)


def _decode_mutation_extras(payload: object, operation: str) -> Mapping[str, object]:
    """Decode a documented 204 empty or object extras response identically in both modes."""
    if payload is None:
        return {}
    return wire.parse_extras(payload, operation=operation)


def _shape_page(payload: object, *, operation: str) -> UDataPageEnvelope:
    """Decode a native page retaining links, field presence, and v2 facets."""
    page = parse_native_page(payload, operation=operation)
    envelope = shape_dataset_page(page, operation=operation)
    extensions = dict(envelope.platform.extensions) if envelope.platform is not None else {}
    extensions.update({"udata.nextpage": page.next_page, "udata.previouspage": page.previous_page})
    if isinstance(payload, Mapping) and "facets" in payload:
        extensions["udata.facets"] = payload["facets"]
    metadata = PlatformMetadata(platform=CatalogPlatform.UDATA, extensions=extensions)
    return UDataPageEnvelope(
        items=envelope.items,
        page=envelope.page,
        platform=metadata,
        native_page=envelope.native_page,
    )


class AsyncDatasetsService:
    """Typed asynchronous dataset operations mirroring the sync surface."""

    def __init__(self, client: AsyncUDataClient) -> None:
        """Bind the service to one strict async client."""
        self._client = client

    async def list(self, query: DatasetListQuery | None = None) -> UDataPageEnvelope:
        """GET /api/1/datasets/ (row 39)."""
        operation = wire.DATASET_OPERATIONS["list"]
        method, path, _, _ = wire.list_request(query or DatasetListQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation
        )
        return _shape_page(payload, operation=operation)

    async def create(
        self,
        client_input: DatasetCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """POST /api/1/datasets/ (row 40)."""
        operation = wire.DATASET_OPERATIONS["create"]
        return await _amutating(
            operation,
            getattr(client_input, "title", "<create>"),
            mutation_policy,
            lambda: _async_create_dispatch(self._client, client_input, operation, permissions, mutation_policy),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "created",
            opaque_target=True,
            success_target=lambda value: value.id.value if isinstance(value, NativeRecord) else "<create>",
        )

    async def recent_atom(self, query: DatasetListQuery | None = None) -> NativeRecord:
        """GET /api/1/datasets/recent.atom (row 41)."""
        operation = wire.DATASET_OPERATIONS["atom"]
        method, path, _, _ = wire.atom_request(query or DatasetListQuery())
        status, body, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        negotiated = _header(response.headers, "content-type")
        return wire.parse_text_document(
            cast(bytes, body), "application/atom+xml", response_media_type=negotiated, operation=operation
        )

    async def get(self, dataset_id: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/ (row 42)."""
        operation = wire.DATASET_OPERATIONS["get"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = await self._client._dataset_call_async(
            method="GET", path=f"/api/1/datasets/{wire._path_segment(identifier)}/", owning_operation=operation
        )
        return wire.parse_dataset_detail(payload, operation=operation)

    async def update(
        self,
        dataset_id: str,
        client_input: DatasetUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """PUT /api/1/datasets/<id>/ (row 43)."""
        operation = wire.DATASET_OPERATIONS["update"]
        return await _amutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _async_update_dispatch(
                self._client, dataset_id, client_input, operation, permissions, mutation_policy
            ),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "updated",
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
        return await _amutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _async_delete_dispatch(
                self._client, dataset_id, permissions, options or DatasetDeleteOptions(), mutation_policy, operation
            ),
            lambda payload: payload,
            "deleted",
        )

    async def feature(
        self,
        dataset_id: str,
        permissions: EffectivePermissions | None = None,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """POST /api/1/datasets/<id>/featured/ (row 45); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["feature"]
        return await _amutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _async_feature_dispatch(
                self._client, dataset_id, permissions, mutation_policy, operation, featured=True
            ),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "featured",
        )

    async def unfeature(
        self,
        dataset_id: str,
        permissions: EffectivePermissions | None = None,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """DELETE /api/1/datasets/<id>/featured/ (row 46); requires admin evidence."""
        operation = wire.DATASET_OPERATIONS["unfeature"]
        return await _amutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _async_feature_dispatch(
                self._client, dataset_id, permissions, mutation_policy, operation, featured=False
            ),
            lambda payload: wire.parse_dataset_detail(payload, operation=operation),
            "unfeatured",
        )

    async def rdf(self, dataset_id: str) -> NativeRecord | MutationReceipt:
        """GET /api/1/datasets/<id>/rdf (row 47); see the sync variant."""
        operation = wire.DATASET_OPERATIONS["rdf"]
        method, path, _, _ = wire.rdf_request(dataset_id, None)
        status, text_or_headers, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True, redirect_mode=True
        )
        if status in {301, 302, 303, 307, 308}:
            return _redirect_receipt(
                operation,
                dataset_id,
                cast(Mapping[str, str], text_or_headers),
                status,
                self._client._origin,
            )
        negotiated = _header(response.headers, "content-type")
        return wire.parse_text_document(
            cast(bytes, text_or_headers), "application/rdf+xml", response_media_type=negotiated, operation=operation
        )

    async def rdf_format(self, dataset_id: str, fmt: str) -> NativeRecord:
        """GET /api/1/datasets/<id>/rdf.<format> (row 48)."""
        operation = wire.DATASET_OPERATIONS["rdf_format"]
        method, path, _, _ = wire.rdf_request(dataset_id, fmt)
        status, body, response = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation, raw_text=True
        )
        negotiated = _header(response.headers, "content-type") or wire.media_type_for_format(fmt)
        return wire.parse_text_document(
            cast(bytes, body), negotiated.split(";")[0].strip(), response_media_type=negotiated, operation=operation
        )

    async def suggest(self, query: DatasetSuggestQuery) -> tuple[NativeRecord, ...]:
        """GET /api/1/datasets/suggest/ (row 67)."""
        operation = wire.DATASET_OPERATIONS["suggest"]
        method, path, _, _ = wire.suggest_request(query)
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation
        )
        return wire.parse_suggestions(payload, operation=operation)

    async def search_v2(self, query: DatasetSearchQuery | None = None) -> UDataPageEnvelope:
        """GET /api/2/datasets/search/ (row 75); retains facets and native links."""
        operation = wire.DATASET_OPERATIONS["v2_search"]
        method, path, _, _ = wire.v2_search_request(query or DatasetSearchQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation
        )
        return _shape_page(payload, operation=operation)

    async def list_v2(self, query: DatasetListQuery | None = None) -> UDataPageEnvelope:
        """GET /api/2/datasets/ (row 76); retains native pagination links."""
        operation = wire.DATASET_OPERATIONS["v2_list"]
        method, path, _, _ = wire.v2_list_request(query or DatasetListQuery())
        status, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=operation
        )
        return _shape_page(payload, operation=operation)

    async def get_v2(self, dataset_id: str) -> NativeRecord:
        """GET /api/2/datasets/<id>/ (row 77)."""
        operation = wire.DATASET_OPERATIONS["v2_get"]
        identifier = wire._required_id(dataset_id, operation=operation)
        status, payload, _ = await self._client._dataset_call_async(
            method="GET", path=f"/api/2/datasets/{wire._path_segment(identifier)}/", owning_operation=operation
        )
        return wire.parse_dataset_detail(payload, operation=operation)

    async def get_extras_v2(self, dataset_id: str) -> Mapping[str, object]:
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
        self,
        dataset_id: str,
        client_input: DatasetExtrasUpdate,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DatasetMutationResult:
        """PUT /api/2/datasets/<id>/extras/ (row 79); null values delete keys."""
        operation = wire.DATASET_OPERATIONS["v2_update_extras"]
        return await _amutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _async_extras_update_dispatch(
                self._client, dataset_id, client_input, operation, permissions, mutation_policy
            ),
            lambda payload: wire.parse_extras(payload, operation=operation),
            "extras_updated",
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
        return await _amutating(
            operation,
            dataset_id,
            mutation_policy,
            lambda: _async_extras_delete_dispatch(
                self._client, dataset_id, client_input, operation, permissions, mutation_policy
            ),
            lambda payload: _decode_mutation_extras(payload, operation),
            "extras_deleted",
        )
