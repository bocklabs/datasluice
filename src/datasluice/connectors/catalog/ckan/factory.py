"""Factory for the canonical CKAN contract façade."""

from __future__ import annotations

from typing import cast

from datasluice.connectors.catalog.ckan.adapter import CKANAdapter
from datasluice.contracts.catalog.native.ckan import AsyncCKANServices, SyncCKANServices
from datasluice.contracts.catalog.protocols import (
    AsyncCatalogClient,
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    SyncCatalogClient,
    SyncCatalogOperationExecutor,
)
from datasluice.domain.catalog.profiles import EffectiveCapabilityProfile

_CKAN_PROFILE_VERSION = "2.11.5"
_CKAN_API_VERSION = "Action API v3"
_CKAN_PLATFORM = "ckan"


def create_ckan_connector(ctx: CatalogConnectorContext) -> CKANAdapter:
    """Construct a CKAN façade from explicit typed service projections."""
    if not isinstance(ctx, CatalogConnectorContext):
        raise TypeError("CKAN connectors require a CatalogConnectorContext.")
    if not isinstance(ctx.sync_executor, SyncCatalogOperationExecutor):
        raise ValueError("CKAN connectors require a synchronous catalog executor.")
    if not isinstance(ctx.async_executor, AsyncCatalogOperationExecutor):
        raise ValueError("CKAN connectors require an asynchronous catalog executor.")
    if type(ctx.manages_sync_executor) is not bool or type(ctx.manages_async_executor) is not bool:
        raise ValueError("CKAN connector executor ownership must be explicit booleans.")
    if ctx.normalized_sync is None or ctx.normalized_async is None:
        raise ValueError("CKAN connectors require normalized sync and async service projections.")
    if ctx.native_sync is None or ctx.native_async is None:
        raise ValueError("CKAN connectors require CKAN-native sync and async service projections.")
    profile = _require_ckan_profile(ctx.effective_profile)
    return CKANAdapter(
        context=ctx,
        normalized_sync=cast(SyncCatalogClient, ctx.normalized_sync),
        normalized_async=cast(AsyncCatalogClient, ctx.normalized_async),
        native_sync=cast(SyncCKANServices, ctx.native_sync),
        native_async=cast(AsyncCKANServices, ctx.native_async),
        effective_profile=profile,
    )


def _require_ckan_profile(profile: EffectiveCapabilityProfile | None) -> EffectiveCapabilityProfile:
    if profile is None:
        raise ValueError("CKAN connectors require an effective CKAN capability profile.")
    declared = profile.declared_profile
    if declared.profile_version != _CKAN_PROFILE_VERSION or declared.platform_api_version != _CKAN_API_VERSION:
        raise ValueError("CKAN connectors require the pinned CKAN 2.11.5 Action API v3 profile.")
    if not declared.operations or any(operation_id.platform != _CKAN_PLATFORM for operation_id in declared.operations):
        raise ValueError("CKAN connectors require a profile whose operations identify the CKAN platform.")
    return profile
