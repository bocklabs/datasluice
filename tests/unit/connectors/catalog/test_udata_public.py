"""Public-contract tests for the canonical uData connector façade."""

from __future__ import annotations

import pytest

from datasluice.contracts.catalog.protocols import CatalogConnectorContext
from datasluice.domain.catalog.operations import AuthClass, OperationTier
from datasluice.domain.catalog.profiles import EffectiveCapabilityProfile
from tests.helpers.catalog_facade import (
    ProfileSpec,
    assert_no_transport_escape_hatch,
    assert_retained_projections,
    context_missing_sync_executor,
    effective_profile,
    facade_context,
)

UDATA_PROFILE_SPEC = ProfileSpec(
    platform="udata",
    service="datasets",
    method="get",
    tier=OperationTier.NORMALIZED,
    request_type="DatasetGetRequest",
    response_type="DatasetRecord",
    auth_class=AuthClass.PUBLIC,
    profile_version="17.6.0",
    platform_api_version="uData API v1",
    official_source_uri="https://udata.readthedocs.io/en/17.6/",
    deployment_url="https://www.data.gouv.fr/api/1/datasets/",
)

CKAN_PROFILE_SPEC = ProfileSpec(
    platform="ckan",
    service="datasets",
    method="get",
    tier=OperationTier.NORMALIZED,
    request_type="DatasetGetRequest",
    response_type="DatasetRecord",
    auth_class=AuthClass.PUBLIC,
    profile_version="2.11.5",
    platform_api_version="Action API v3",
    official_source_uri="https://docs.ckan.org/en/2.11/api/",
    deployment_url="https://demo.ckan.org/api/3/action/package_list",
)

_PROFILE = effective_profile(UDATA_PROFILE_SPEC)
_FOREIGN_PROFILE = effective_profile(CKAN_PROFILE_SPEC)
FORBIDDEN = frozenset(
    {"DataGouvAdapter", "create_datagouv_connector", "transport", "request", "get_json", "raw_request"}
)


def _context(*, profile: EffectiveCapabilityProfile | None = None) -> CatalogConnectorContext:
    return facade_context(profile or _PROFILE)


def test_udata_package_exports_the_canonical_connector_factory_and_live_clients() -> None:
    """The platform package is the sole canonical uData publication point."""
    import datasluice.connectors.catalog.udata as udata

    assert udata.__all__ == [
        "UDataClientSettings",
        "UDataConnector",
        "create_async_client",
        "create_sync_client",
        "create_udata_connector",
    ]


def test_factory_accepts_canonical_context_and_validates_profile_identity() -> None:
    """The factory rejects non-uData profile evidence before exposing services."""
    from datasluice.connectors.catalog.udata import UDataConnector, create_udata_connector

    connector = create_udata_connector(_context())

    assert isinstance(connector, UDataConnector)

    with pytest.raises(ValueError, match="uData"):
        create_udata_connector(_context(profile=_FOREIGN_PROFILE))


def test_factory_requires_both_typed_executor_modes() -> None:
    """A façade cannot silently fall back to an absent executor mode."""
    from datasluice.connectors.catalog.udata import create_udata_connector

    with pytest.raises(ValueError, match="synchronous"):
        create_udata_connector(context_missing_sync_executor(_PROFILE))


def test_connector_exposes_injected_normalized_native_services_and_effective_profile() -> None:
    """The façade retains explicit projections for both execution modes."""
    from datasluice.connectors.catalog.udata import create_udata_connector

    context = _context()
    connector = create_udata_connector(context)

    assert_retained_projections(context, connector)


def test_connector_has_no_legacy_or_raw_request_escape_hatch() -> None:
    """The façade cannot expose legacy compatibility or untyped HTTP helpers."""
    from datasluice.connectors.catalog.udata import UDataConnector

    assert_no_transport_escape_hatch(UDataConnector, FORBIDDEN)
