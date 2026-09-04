"""Factory for the canonical uData contract façade."""

from __future__ import annotations

from typing import cast

from datasluice.connectors.catalog.udata.connector import UDataConnector
from datasluice.contracts.catalog.native.udata import AsyncUDataServices, SyncUDataServices
from datasluice.contracts.catalog.protocols import (
    AsyncCatalogClient,
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    SyncCatalogClient,
    SyncCatalogOperationExecutor,
)
from datasluice.domain.catalog.profiles import EffectiveCapabilityProfile

_UDATA_PROFILE_VERSION = "17.6.0"
_UDATA_API_VERSION = "uData API v1"
_UDATA_PLATFORM = "udata"


def create_udata_connector(ctx: CatalogConnectorContext) -> UDataConnector:
    """Construct a uData façade from explicit typed service projections."""
    if not isinstance(ctx, CatalogConnectorContext):
        raise TypeError("uData connectors require a CatalogConnectorContext.")
    if not isinstance(ctx.sync_executor, SyncCatalogOperationExecutor):
        raise ValueError("uData connectors require a synchronous catalog executor.")
    if not isinstance(ctx.async_executor, AsyncCatalogOperationExecutor):
        raise ValueError("uData connectors require an asynchronous catalog executor.")
    if type(ctx.manages_sync_executor) is not bool or type(ctx.manages_async_executor) is not bool:
        raise ValueError("uData connector executor ownership must be explicit booleans.")
    if ctx.normalized_sync is None or ctx.normalized_async is None:
        raise ValueError("uData connectors require normalized sync and async service projections.")
    if ctx.native_sync is None or ctx.native_async is None:
        raise ValueError("uData connectors require uData-native sync and async service projections.")
    profile = _require_udata_profile(ctx.effective_profile)
    return UDataConnector(
        context=ctx,
        normalized_sync=cast(SyncCatalogClient, ctx.normalized_sync),
        normalized_async=cast(AsyncCatalogClient, ctx.normalized_async),
        native_sync=cast(SyncUDataServices, ctx.native_sync),
        native_async=cast(AsyncUDataServices, ctx.native_async),
        effective_profile=profile,
    )


def _require_udata_profile(profile: EffectiveCapabilityProfile | None) -> EffectiveCapabilityProfile:
    if profile is None:
        raise ValueError("uData connectors require an effective uData capability profile.")
    declared = profile.declared_profile
    if declared.profile_version != _UDATA_PROFILE_VERSION or declared.platform_api_version != _UDATA_API_VERSION:
        raise ValueError("uData connectors require the pinned uData 17.6.0 API v1 profile.")
    if not declared.operations or any(operation_id.platform != _UDATA_PLATFORM for operation_id in declared.operations):
        raise ValueError("uData connectors require a profile whose operations identify the uData platform.")
    return profile
