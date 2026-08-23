"""CKAN filestore projections routing upload-and-replacement semantics onto resource paths.

The CKAN FileStore defines zero dedicated Action API endpoints: uploads and
resource file replacement ride the multipart ``upload`` parameter of the
documented resource_create / resource_update / resource_patch actions. This
façade exists so callers can express intent semantically while every wire call
routes through the manifest-owned resource entries — no filestore group action
is or will be registered in the checked-in action manifest.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.connectors.catalog.ckan.services.resources import (
    AsyncResourcesService,
    SyncResourcesService,
)
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.models import ResourceRecord, ResultEnvelope
from datasluice.errors.catalog import NativeCatalogError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient


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
        return self._resources.list_show_create_update_patch_delete_upload(operation, guard)


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
        return await self._resources.list_show_create_update_patch_delete_upload(operation, guard)
