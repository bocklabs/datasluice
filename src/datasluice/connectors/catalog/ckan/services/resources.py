"""Both-mode CKAN resource projections with bounded buffered multipart uploads (D-03).

Uploads accept a filesystem path or an open binary handle, are buffered exactly
once through the shared transport multipart channel, and are bounded by the
configured ``max_upload_bytes`` ceiling with a typed pre-dispatch refusal. There
is no streaming mode: the whole source becomes one immutable ``UploadPart``.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, cast

from datasluice.connectors.catalog.ckan.clients import (
    _AsyncResourceService,
    _operation_id_from,
    _SyncResourceService,
)
from datasluice.connectors.catalog.ckan.inventory import ActionEntry
from datasluice.connectors.catalog.ckan.mapping import PLATFORM
from datasluice.connectors.catalog.ckan.results import CKANMutationResult, require_mutation_tier
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import NativeRecord, ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.mutation import build_mutation_receipt
from datasluice.runtime.transport.base import UploadPart

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient

_RESOURCE_GROUP = "resources"
_UPLOAD_FIELD = "upload"
_CHUNK_SIZE = 1 << 20

type FieldValues = Mapping[str, object]
type SearchFilters = Mapping[str, object]

_RESOURCE_FIELDS = (
    "url",
    "name",
    "description",
    "format",
    "mimetype",
    "size",
    "hash",
)


def _buffer_upload(source: str | os.PathLike[str] | BinaryIO, *, max_bytes: int | None) -> bytes:
    """Buffer one upload source fully into bytes under the configured ceiling.

    Args:
        source: A filesystem path, path-like object, or open binary handle.
        max_bytes: The configured ceiling, or ``None`` for an unbounded buffer.

    Returns:
        The full buffered content of the source.

    Raises:
        CatalogValidationError: If the source exceeds the ceiling before any
            transport work begins.
    """
    handle = open(source, "rb") if isinstance(source, str | os.PathLike) else source
    try:
        chunks: list[bytes] = []
        total = 0
        while chunk := handle.read(_CHUNK_SIZE):
            total += len(chunk)
            if max_bytes is not None and total > max_bytes:
                raise _ceiling_error(total, max_bytes)
            chunks.append(chunk)
    finally:
        if isinstance(source, str | os.PathLike):
            handle.close()
    return b"".join(chunks)


def _ceiling_error(observed: int, ceiling: int) -> CatalogValidationError:
    return CatalogValidationError(
        f"The upload source exceeds the configured ceiling of {ceiling} bytes (buffered {observed} bytes).",
        operation=f"{PLATFORM.value}/{_UPLOAD_FIELD}",
        platform=PLATFORM.value,
        safe_action=(f"Reduce the file size below {ceiling} bytes or raise settings.max_upload_bytes before retrying."),
    )


def _source_name(source: str | os.PathLike[str] | BinaryIO) -> str | None:
    if isinstance(source, str | os.PathLike):
        return Path(source).name
    candidate = getattr(source, "name", None)
    if isinstance(candidate, str) and candidate:
        return Path(candidate).name
    return None


def _encode_field(value: object) -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value).encode("utf-8")


def _multipart_parts(params: Mapping[str, object], data: bytes, file_name: str | None) -> tuple[UploadPart, ...]:
    field_parts = tuple(UploadPart(field_name=key, data=_encode_field(params[key])) for key in sorted(params))
    return (*field_parts, UploadPart(field_name=_UPLOAD_FIELD, data=data, file_name=file_name))


def _receipt_target(action: str, params: Mapping[str, object], envelope: ResultEnvelope[CKANResultItem]) -> CatalogId:
    if action == "resource_create":
        record = next((item for item in envelope.items if isinstance(item, NativeRecord)), None)
        if record is not None:
            return record.id
        return CatalogId(PLATFORM, ResourceKind.RESOURCE, str(params.get("package_id", "resource-create")))
    return CatalogId(PLATFORM, ResourceKind.RESOURCE, str(params["id"]))


class SyncResourcesService(_SyncResourceService):
    """Synchronous resource projection carrying six typed actions plus uploads."""

    __slots__ = ()

    def resource_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one resource by id."""
        return self._invoke_read("resource_show", {"id": id})

    def resource_search(
        self,
        *,
        q: str | None = None,
        fields: SearchFilters | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Search resources with native limit/offset paging."""
        return self._invoke_read(
            "resource_search", _drop_unset({"q": q, "fields": fields, "limit": limit, "offset": offset})
        )

    def resource_create(
        self,
        *,
        package_id: str,
        upload: str | os.PathLike[str] | BinaryIO | None = None,
        policy: MutationPolicy | None = None,
        **fields: object,
    ) -> CKANMutationResult:
        """Create one resource, optionally uploading a file from path or handle."""
        unknown = set(fields) - set(_RESOURCE_FIELDS)
        if unknown:
            raise TypeError(f"resource_create received undocumented field(s): {sorted(unknown)}")
        params: dict[str, object] = {"package_id": package_id}
        params.update({key: value for key, value in fields.items() if value is not None})
        return self._invoke_mutation("resource_create", params, policy, upload)

    def resource_update(
        self,
        *,
        id: str,
        upload: str | os.PathLike[str] | BinaryIO | None = None,
        policy: MutationPolicy | None = None,
        **fields: object,
    ) -> CKANMutationResult:
        """Update one resource, replacing its uploaded file when supplied."""
        unknown = set(fields) - set(_RESOURCE_FIELDS) - {"package_id"}
        if unknown:
            raise TypeError(f"resource_update received undocumented field(s): {sorted(unknown)}")
        params: dict[str, object] = {"id": id}
        params.update({key: value for key, value in fields.items() if value is not None})
        return self._invoke_mutation("resource_update", params, policy, upload)

    def resource_patch(
        self,
        *,
        id: str,
        upload: str | os.PathLike[str] | BinaryIO | None = None,
        policy: MutationPolicy | None = None,
        **fields: object,
    ) -> CKANMutationResult:
        """Patch one resource, replacing its uploaded file when supplied."""
        unknown = set(fields) - set(_RESOURCE_FIELDS) - {"package_id"}
        if unknown:
            raise TypeError(f"resource_patch received undocumented field(s): {sorted(unknown)}")
        params: dict[str, object] = {"id": id}
        params.update({key: value for key, value in fields.items() if value is not None})
        return self._invoke_mutation("resource_patch", params, policy, upload)

    def resource_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one resource on the standard tier."""
        return self._invoke_mutation("resource_delete", {"id": id}, policy, None)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _RESOURCE_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_RESOURCE_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        client: SyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry))

    def _invoke_mutation(
        self,
        action: str,
        params: dict[str, object],
        policy: MutationPolicy | None,
        upload: str | os.PathLike[str] | BinaryIO | None,
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        client: SyncCKANClient = self._client
        owning_id = _operation_id_from(entry.owning_operation_id)
        effective = require_mutation_tier(entry.mutation_class, owning_id, policy)
        assert effective is not None
        files = _resolve_files(params, upload, client._max_upload_bytes)
        operation = CatalogOperationRequest(operation_id=owning_id, payload=params, mutation_policy=effective)
        guard = CatalogOperationGuard(operation_id=owning_id, profile=client._profile)
        envelope = cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry, files=files))
        receipt = build_mutation_receipt(
            owning_id, _receipt_target(entry.name, params, envelope), effective, "succeeded", {"action": entry.name}
        )
        return CKANMutationResult(result=envelope, receipt=receipt)


class AsyncResourcesService(_AsyncResourceService):
    """Asynchronous resource projection carrying six typed actions plus uploads."""

    __slots__ = ()

    async def resource_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one resource by id."""
        return await self._invoke_read("resource_show", {"id": id})

    async def resource_search(
        self,
        *,
        q: str | None = None,
        fields: SearchFilters | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Search resources with native limit/offset paging."""
        return await self._invoke_read(
            "resource_search", _drop_unset({"q": q, "fields": fields, "limit": limit, "offset": offset})
        )

    async def resource_create(
        self,
        *,
        package_id: str,
        upload: str | os.PathLike[str] | BinaryIO | None = None,
        policy: MutationPolicy | None = None,
        **fields: object,
    ) -> CKANMutationResult:
        """Create one resource, optionally uploading a file from path or handle."""
        unknown = set(fields) - set(_RESOURCE_FIELDS)
        if unknown:
            raise TypeError(f"resource_create received undocumented field(s): {sorted(unknown)}")
        params: dict[str, object] = {"package_id": package_id}
        params.update({key: value for key, value in fields.items() if value is not None})
        return await self._invoke_mutation("resource_create", params, policy, upload)

    async def resource_update(
        self,
        *,
        id: str,
        upload: str | os.PathLike[str] | BinaryIO | None = None,
        policy: MutationPolicy | None = None,
        **fields: object,
    ) -> CKANMutationResult:
        """Update one resource, replacing its uploaded file when supplied."""
        unknown = set(fields) - set(_RESOURCE_FIELDS) - {"package_id"}
        if unknown:
            raise TypeError(f"resource_update received undocumented field(s): {sorted(unknown)}")
        params: dict[str, object] = {"id": id}
        params.update({key: value for key, value in fields.items() if value is not None})
        return await self._invoke_mutation("resource_update", params, policy, upload)

    async def resource_patch(
        self,
        *,
        id: str,
        upload: str | os.PathLike[str] | BinaryIO | None = None,
        policy: MutationPolicy | None = None,
        **fields: object,
    ) -> CKANMutationResult:
        """Patch one resource, replacing its uploaded file when supplied."""
        unknown = set(fields) - set(_RESOURCE_FIELDS) - {"package_id"}
        if unknown:
            raise TypeError(f"resource_patch received undocumented field(s): {sorted(unknown)}")
        params: dict[str, object] = {"id": id}
        params.update({key: value for key, value in fields.items() if value is not None})
        return await self._invoke_mutation("resource_patch", params, policy, upload)

    async def resource_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one resource on the standard tier."""
        return await self._invoke_mutation("resource_delete", {"id": id}, policy, None)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _RESOURCE_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_RESOURCE_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        client: AsyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry))

    async def _invoke_mutation(
        self,
        action: str,
        params: dict[str, object],
        policy: MutationPolicy | None,
        upload: str | os.PathLike[str] | BinaryIO | None,
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        client: AsyncCKANClient = self._client
        owning_id = _operation_id_from(entry.owning_operation_id)
        effective = require_mutation_tier(entry.mutation_class, owning_id, policy)
        assert effective is not None
        files = _resolve_files(params, upload, client._max_upload_bytes)
        operation = CatalogOperationRequest(operation_id=owning_id, payload=params, mutation_policy=effective)
        guard = CatalogOperationGuard(operation_id=owning_id, profile=client._profile)
        envelope = cast(
            ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry, files=files)
        )
        receipt = build_mutation_receipt(
            owning_id, _receipt_target(entry.name, params, envelope), effective, "succeeded", {"action": entry.name}
        )
        return CKANMutationResult(result=envelope, receipt=receipt)


def _resolve_files(
    params: dict[str, object],
    upload: str | os.PathLike[str] | BinaryIO | None,
    max_bytes: int | None,
) -> tuple[UploadPart, ...]:
    if upload is None:
        return ()
    data = _buffer_upload(upload, max_bytes=max_bytes)
    return _multipart_parts(params, data, _source_name(upload))


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}
