"""Transport-free CKAN catalog contract façade."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.connectors.catalog._lifecycle import _ConnectorLifecycle

if TYPE_CHECKING:
    from datasluice.contracts.catalog.native.ckan import AsyncCKANServices, SyncCKANServices
    from datasluice.contracts.catalog.protocols import (
        AsyncCatalogClient,
        CatalogConnectorContext,
        SyncCatalogClient,
    )
    from datasluice.domain.catalog.profiles import EffectiveCapabilityProfile


class CKANConnector(_ConnectorLifecycle):
    """Expose injected CKAN service projections without a transport implementation."""

    def __init__(
        self,
        *,
        context: CatalogConnectorContext,
        normalized_sync: SyncCatalogClient,
        normalized_async: AsyncCatalogClient,
        native_sync: SyncCKANServices,
        native_async: AsyncCKANServices,
        effective_profile: EffectiveCapabilityProfile,
    ) -> None:
        super().__init__(context)
        self.normalized_sync = normalized_sync
        self.normalized_async = normalized_async
        self.native_sync = native_sync
        self.native_async = native_async
        self.effective_profile = effective_profile
