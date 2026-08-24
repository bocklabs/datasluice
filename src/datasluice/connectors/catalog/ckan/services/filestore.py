"""CKAN filestore projections routing upload-and-replacement semantics onto resource paths.

The CKAN FileStore defines zero dedicated Action API endpoints: uploads and
resource file replacement ride the multipart ``upload`` parameter of the
documented resource_create / resource_update / resource_patch actions. This
façade exists so callers can express intent semantically while every wire call
routes through the manifest-owned resource entries — no filestore group action
is or will be registered in the checked-in action manifest.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, BinaryIO, cast

from datasluice.connectors.catalog.ckan.services.resources import (
    AsyncResourcesService,
    SyncResourcesService,
)
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.models import ResourceRecord, ResultEnvelope
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient


_UPLOAD_ACTIONS = frozenset({"resource_create", "resource_update", "resource_patch"})
type UploadSource = str | os.PathLike[str] | BinaryIO


def _upload_call(operation: CatalogOperationRequest) -> tuple[str, UploadSource, dict[str, object]]:
    payload = dict(operation.payload)
    action = payload.pop("action", None)
    upload = payload.pop("upload", None)
    if action not in _UPLOAD_ACTIONS:
        raise CatalogValidationError(
            "The filestore façade accepts resource_create, resource_update, or resource_patch.",
            operation=str(operation.operation_id),
            platform=operation.operation_id.platform,
            safe_action="Choose an upload-capable resource action.",
        )
    if upload is None:
        raise CatalogValidationError(
            "The filestore façade requires an upload source.",
            operation=str(operation.operation_id),
            platform=operation.operation_id.platform,
            safe_action="Provide a file path or open binary handle in payload['upload'].",
        )
    return cast(str, action), cast(UploadSource, upload), payload


class SyncFilestoreService:
    """Synchronous filestore façade delegating to the resource projection."""

    __slots__ = ("_resources",)

    def __init__(self, client: SyncCKANClient) -> None:
        self._resources = SyncResourcesService(client, "resources", ResourceRecord.from_dict)

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native CKAN error type."""
        return NativeCatalogError

    def upload_and_resource_file_replacement(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Route an upload-or-replacement call through its owning resource action."""
        guard.require_allowed()
        action, upload, payload = _upload_call(operation)
        policy = operation.mutation_policy
        if action == "resource_create":
            package_id = payload.pop("package_id", None)
            if not isinstance(package_id, str):
                raise TypeError("Filestore resource_create requires a package_id string.")
            return self._resources.resource_create(
                package_id=package_id, upload=upload, policy=policy, **payload
            ).result
        resource_id = payload.pop("id", None)
        if not isinstance(resource_id, str):
            raise TypeError(f"Filestore {action} requires an id string.")
        if action == "resource_update":
            return self._resources.resource_update(id=resource_id, upload=upload, policy=policy, **payload).result
        return self._resources.resource_patch(id=resource_id, upload=upload, policy=policy, **payload).result


class AsyncFilestoreService:
    """Asynchronous filestore façade delegating to the resource projection."""

    __slots__ = ("_resources",)

    def __init__(self, client: AsyncCKANClient) -> None:
        self._resources = AsyncResourcesService(client, "resources", ResourceRecord.from_dict)

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native CKAN error type."""
        return NativeCatalogError

    async def upload_and_resource_file_replacement(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Route an upload-or-replacement call through its owning resource action."""
        guard.require_allowed()
        action, upload, payload = _upload_call(operation)
        policy = operation.mutation_policy
        if action == "resource_create":
            package_id = payload.pop("package_id", None)
            if not isinstance(package_id, str):
                raise TypeError("Filestore resource_create requires a package_id string.")
            result = await self._resources.resource_create(
                package_id=package_id, upload=upload, policy=policy, **payload
            )
            return result.result
        resource_id = payload.pop("id", None)
        if not isinstance(resource_id, str):
            raise TypeError(f"Filestore {action} requires an id string.")
        if action == "resource_update":
            result = await self._resources.resource_update(id=resource_id, upload=upload, policy=policy, **payload)
        else:
            result = await self._resources.resource_patch(id=resource_id, upload=upload, policy=policy, **payload)
        return result.result
