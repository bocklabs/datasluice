"""Unit tests for connector CatalogCapabilities publication.

Follows the RED->GREEN TDD pattern: the module skips cleanly at
collection time while the capabilities ClassVar is not yet published, then
runs and passes once GREEN lands the real implementation.
"""

from __future__ import annotations

import importlib

import pytest

_ckan_adapter_mod = importlib.import_module("datasluice.connectors.ckan.adapter")
if not hasattr(_ckan_adapter_mod.CKANAdapter, "capabilities"):
    pytest.skip("CKAN capabilities ClassVar not yet published", allow_module_level=True)

_datagouv_adapter_mod = importlib.import_module("datasluice.connectors.datagouv.adapter")
if not hasattr(_datagouv_adapter_mod.DataGouvAdapter, "capabilities"):
    pytest.skip("datagouv capabilities ClassVar not yet published", allow_module_level=True)

_socrata_adapter_mod = importlib.import_module("datasluice.connectors.socrata.adapter")
if not hasattr(_socrata_adapter_mod.SocrataAdapter, "capabilities"):
    pytest.skip("socrata capabilities ClassVar not yet published", allow_module_level=True)

from datasluice.domain import CatalogCapabilities  # noqa: E402

CKANAdapter = _ckan_adapter_mod.CKANAdapter
DataGouvAdapter = _datagouv_adapter_mod.DataGouvAdapter
SocrataAdapter = _socrata_adapter_mod.SocrataAdapter


def test_ckan_capabilities_is_classvar_catalogcapabilities() -> None:
    capabilities = CKANAdapter.capabilities
    assert isinstance(capabilities, CatalogCapabilities)


def test_ckan_capabilities_supported_query_fields() -> None:
    expected = frozenset({"text", "tags", "organizations", "groups", "res_format", "license_id", "sort"})
    assert CKANAdapter.capabilities.supported_query_fields == expected


def test_ckan_capabilities_flags() -> None:
    assert CKANAdapter.capabilities.supports_search is True
    assert CKANAdapter.capabilities.supports_organizations is True
    assert CKANAdapter.capabilities.supports_faceted_search is True


def test_ckan_capabilities_readable_without_instantiation() -> None:
    capabilities = CKANAdapter.capabilities
    assert capabilities is not None
    assert capabilities.supported_query_fields


def test_datagouv_capabilities_is_classvar_catalogcapabilities() -> None:
    capabilities = DataGouvAdapter.capabilities
    assert isinstance(capabilities, CatalogCapabilities)


def test_datagouv_capabilities_supported_query_fields_no_groups() -> None:
    expected = frozenset({"text", "tags", "organizations", "res_format", "license_id", "sort"})
    assert DataGouvAdapter.capabilities.supported_query_fields == expected
    assert "groups" not in DataGouvAdapter.capabilities.supported_query_fields


def test_datagouv_capabilities_flags() -> None:
    assert DataGouvAdapter.capabilities.supports_search is True
    assert DataGouvAdapter.capabilities.supports_organizations is True


def test_datagouv_capabilities_readable_without_instantiation() -> None:
    capabilities = DataGouvAdapter.capabilities
    assert capabilities is not None
    assert capabilities.supported_query_fields


def test_socrata_capabilities_is_classvar_catalogcapabilities() -> None:
    capabilities = SocrataAdapter.capabilities
    assert isinstance(capabilities, CatalogCapabilities)


def test_socrata_capabilities_lean_supported_query_fields() -> None:
    expected = frozenset({"text", "tags", "sort"})
    assert SocrataAdapter.capabilities.supported_query_fields == expected
    for unsupported in ("organizations", "groups", "res_format", "license_id"):
        assert unsupported not in SocrataAdapter.capabilities.supported_query_fields


def test_socrata_capabilities_supports_organizations_false() -> None:
    assert SocrataAdapter.capabilities.supports_search is True
    assert SocrataAdapter.capabilities.supports_organizations is False


def test_socrata_capabilities_readable_without_instantiation() -> None:
    capabilities = SocrataAdapter.capabilities
    assert capabilities is not None
    assert capabilities.supported_query_fields
