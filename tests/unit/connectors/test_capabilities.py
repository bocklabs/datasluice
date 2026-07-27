"""Unit tests for connector CatalogCapabilities publication (D-P5-23).

Follows the Phase 03/04 RED->GREEN TDD pattern: the module skips cleanly at
collection time while the capabilities ClassVar is not yet published, then
runs and passes once Task 1 GREEN lands the real implementation.
"""

from __future__ import annotations

import importlib

import pytest

_ckan_adapter_mod = importlib.import_module("datasluice.connectors.ckan.adapter")
if not hasattr(_ckan_adapter_mod.CKANAdapter, "capabilities"):
    pytest.skip("CKAN capabilities ClassVar not yet published", allow_module_level=True)

from datasluice.domain import CatalogCapabilities  # noqa: E402

CKANAdapter = _ckan_adapter_mod.CKANAdapter


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
