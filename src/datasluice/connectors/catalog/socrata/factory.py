"""Factory for the canonical Socrata contract façade."""

from __future__ import annotations

from typing import cast

from datasluice.connectors.catalog.socrata.connector import SocrataConnector
from datasluice.contracts.catalog.native.socrata import AsyncSocrataServices, SyncSocrataServices
from datasluice.contracts.catalog.protocols import (
    AsyncCatalogClient,
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    SyncCatalogClient,
    SyncCatalogOperationExecutor,
)
from datasluice.domain.catalog.profiles import EffectiveCapabilityProfile

_SOCRATA_PROFILE_VERSION = "3.0"
_SOCRATA_API_VERSION = "SODA 3"
_SOCRATA_PLATFORM = "socrata"


def create_socrata_connector(ctx: CatalogConnectorContext) -> SocrataConnector:
    """Construct a Socrata façade from explicit typed service projections."""
    if not isinstance(ctx, CatalogConnectorContext):
        raise TypeError("Socrata connectors require a CatalogConnectorContext.")
    if not isinstance(ctx.sync_executor, SyncCatalogOperationExecutor):
        raise ValueError("Socrata connectors require a synchronous catalog executor.")
    if not isinstance(ctx.async_executor, AsyncCatalogOperationExecutor):
        raise ValueError("Socrata connectors require an asynchronous catalog executor.")
    if type(ctx.manages_sync_executor) is not bool or type(ctx.manages_async_executor) is not bool:
        raise ValueError("Socrata connector executor ownership must be explicit booleans.")
    if ctx.normalized_sync is None or ctx.normalized_async is None:
        raise ValueError("Socrata connectors require normalized sync and async service projections.")
    if ctx.native_sync is None or ctx.native_async is None:
        raise ValueError("Socrata connectors require Socrata-native sync and async service projections.")
    profile = _require_socrata_profile(ctx.effective_profile)
    return SocrataConnector(
        context=ctx,
        normalized_sync=cast(SyncCatalogClient, ctx.normalized_sync),
        normalized_async=cast(AsyncCatalogClient, ctx.normalized_async),
        native_sync=cast(SyncSocrataServices, ctx.native_sync),
        native_async=cast(AsyncSocrataServices, ctx.native_async),
        effective_profile=profile,
    )


def _require_socrata_profile(profile: EffectiveCapabilityProfile | None) -> EffectiveCapabilityProfile:
    if profile is None:
        raise ValueError("Socrata connectors require an effective Socrata capability profile.")
    declared = profile.declared_profile
    if declared.profile_version != _SOCRATA_PROFILE_VERSION or declared.platform_api_version != _SOCRATA_API_VERSION:
        raise ValueError("Socrata connectors require the pinned Socrata SODA 3 profile.")
    if not declared.operations or any(
        operation_id.platform != _SOCRATA_PLATFORM for operation_id in declared.operations
    ):
        raise ValueError("Socrata connectors require a profile whose operations identify the Socrata platform.")
    return profile
