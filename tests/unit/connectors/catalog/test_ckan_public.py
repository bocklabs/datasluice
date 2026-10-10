"""Public-contract tests for the canonical CKAN connector façade."""

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

UDATA_PROFILE_SPEC = ProfileSpec(
    platform="udata",
    service="datasets",
    method="get",
    tier=OperationTier.NORMALIZED,
    request_type="DatasetGetRequest",
    response_type="DatasetRecord",
    auth_class=AuthClass.PUBLIC,
    profile_version="17.3.0",
    platform_api_version="uData API",
    official_source_uri="https://udata.readthedocs.io/en/stable/",
    deployment_url="https://example.test/api/1/datasets/",
)

_PROFILE = effective_profile(CKAN_PROFILE_SPEC)
_FOREIGN_PROFILE = effective_profile(UDATA_PROFILE_SPEC)
FORBIDDEN = frozenset({"transport", "request", "get_json", "raw_request", "raw_response"})


def _context(*, profile: EffectiveCapabilityProfile | None = None) -> CatalogConnectorContext:
    return facade_context(profile or _PROFILE)


def test_ckan_package_exports_the_canonical_connector_factory_and_live_clients() -> None:
    """The platform package is the sole canonical CKAN publication point."""
    import datasluice.connectors.catalog.ckan as ckan

    assert ckan.__all__ == [
        "CKANClientSettings",
        "CKANConnector",
        "create_async_client",
        "create_ckan_connector",
        "create_sync_client",
    ]


def test_factory_accepts_canonical_context_and_validates_profile_identity() -> None:
    """The factory rejects non-CKAN profile evidence before exposing services."""
    from datasluice.connectors.catalog.ckan import CKANConnector, create_ckan_connector

    connector = create_ckan_connector(_context())

    assert isinstance(connector, CKANConnector)

    with pytest.raises(ValueError, match="CKAN"):
        create_ckan_connector(_context(profile=_FOREIGN_PROFILE))


def test_factory_requires_both_typed_executor_modes() -> None:
    """A façade cannot silently fall back to an absent executor mode."""
    from datasluice.connectors.catalog.ckan import create_ckan_connector

    with pytest.raises(ValueError, match="synchronous"):
        create_ckan_connector(context_missing_sync_executor(_PROFILE))


def test_factory_rejects_an_invalid_asynchronous_executor() -> None:
    """A façade cannot silently accept an invalid asynchronous executor."""
    from datasluice.connectors.catalog.ckan import create_ckan_connector

    with pytest.raises(ValueError, match="asynchronous"):
        create_ckan_connector(context_invalid_async_executor(_PROFILE))


def test_connector_exposes_injected_normalized_native_services_and_effective_profile() -> None:
    """The façade retains explicit projections for both execution modes."""
    from datasluice.connectors.catalog.ckan import create_ckan_connector

    context = _context()
    connector = create_ckan_connector(context)

    assert_retained_projections(context, connector)


def test_connector_has_no_transport_default_or_raw_request_escape_hatch() -> None:
    """The façade cannot bypass the typed injected-executor boundary."""
    from datasluice.connectors.catalog.ckan import CKANConnector

    assert_no_transport_escape_hatch(CKANConnector, FORBIDDEN)
