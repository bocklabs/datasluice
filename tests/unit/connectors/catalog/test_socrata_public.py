"""Public-contract tests for the canonical Socrata connector façade."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from datasluice.domain.catalog.operations import AuthClass, OperationTier
from tests.helpers.catalog_facade import (
    ProfileSpec,
    assert_no_transport_escape_hatch,
    assert_retained_projections,
    context_invalid_async_executor,
    context_missing_sync_executor,
    effective_profile,
    facade_context,
)

if TYPE_CHECKING:
    from datasluice.contracts.catalog.protocols import CatalogConnectorContext
    from datasluice.domain.catalog.profiles import EffectiveCapabilityProfile

SOCRATA_PROFILE_SPEC = ProfileSpec(
    platform="socrata",
    service="soda",
    method="query",
    tier=OperationTier.NATIVE,
    request_type="SocrataQueryRequest",
    response_type="NativeRecord",
    auth_class=AuthClass.AUTHENTICATED,
    profile_version="3.0",
    platform_api_version="SODA 3",
    official_source_uri="https://dev.socrata.com/docs/endpoints",
    deployment_url="https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=1",
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

_PROFILE = effective_profile(SOCRATA_PROFILE_SPEC)
_FOREIGN_PROFILE = effective_profile(CKAN_PROFILE_SPEC)
FORBIDDEN = frozenset({"SODA2Adapter", "transport", "request", "get_json", "raw_request", "raw_response"})


def _context(*, profile: EffectiveCapabilityProfile | None = None) -> CatalogConnectorContext:
    return facade_context(profile or _PROFILE)


def test_socrata_package_exports_only_its_connector_and_factory() -> None:
    """The platform package is the sole canonical Socrata publication point."""
    import datasluice.connectors.catalog.socrata as socrata

    assert socrata.__all__ == ["SocrataConnector", "create_socrata_connector"]


def test_factory_accepts_canonical_context_and_validates_profile_identity() -> None:
    """The factory rejects non-Socrata profile evidence before exposing services."""
    from datasluice.connectors.catalog.socrata import SocrataConnector, create_socrata_connector

    connector = create_socrata_connector(_context())

    assert isinstance(connector, SocrataConnector)

    with pytest.raises(ValueError, match="Socrata"):
        create_socrata_connector(_context(profile=_FOREIGN_PROFILE))


def test_factory_requires_both_typed_executor_modes() -> None:
    """A façade cannot silently fall back to an absent executor mode."""
    from datasluice.connectors.catalog.socrata import create_socrata_connector

    with pytest.raises(ValueError, match="synchronous"):
        create_socrata_connector(context_missing_sync_executor(_PROFILE))


def test_factory_rejects_an_invalid_asynchronous_executor() -> None:
    """A façade cannot silently accept an invalid asynchronous executor."""
    from datasluice.connectors.catalog.socrata import create_socrata_connector

    with pytest.raises(ValueError, match="asynchronous"):
        create_socrata_connector(context_invalid_async_executor(_PROFILE))


def test_connector_exposes_injected_normalized_native_services_and_effective_profile() -> None:
    """The façade retains explicit projections for both execution modes."""
    from datasluice.connectors.catalog.socrata import create_socrata_connector

    context = _context()
    connector = create_socrata_connector(context)

    assert_retained_projections(context, connector)


def test_connector_has_no_soda_two_compatibility_or_raw_request_escape_hatch() -> None:
    """The façade cannot expose SODA 2 compatibility or untyped HTTP helpers."""
    from datasluice.connectors.catalog.socrata import SocrataConnector

    assert_no_transport_escape_hatch(SocrataConnector, FORBIDDEN)
