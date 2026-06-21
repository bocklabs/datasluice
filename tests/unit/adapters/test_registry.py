"""Unit tests for the adapter registry and factory."""

from __future__ import annotations

import pytest

from datasluice.adapters import (
    BaseAdapter,
    CKANAdapter,
    CustomAdapter,
    DataGouvAdapter,
    SocrataAdapter,
    registry,
)
from datasluice.exceptions import AdapterNotFoundError


def test_registry_has_builtin_adapters() -> None:
    assert registry.has("ckan")
    assert registry.has("datagouv")
    assert registry.has("socrata")
    assert registry.has("custom")


def test_registry_get_known() -> None:
    assert registry.get("ckan") is CKANAdapter
    assert registry.get("CKAN") is CKANAdapter


def test_registry_get_unknown_raises() -> None:
    with pytest.raises(AdapterNotFoundError):
        registry.get("nonexistent")


def test_registry_known_types() -> None:
    types = registry.known_types
    assert "ckan" in types
    assert "datagouv" in types


def test_registry_register_and_unregister() -> None:
    class Dummy(BaseAdapter):
        portal_type = "dummy"

        def search(self, query=None): ...

        def get_dataset(self, dataset_id): ...

        def list_resources(self, dataset_id): ...

        def get_organization(self, organization_id): ...

    registry.register("dummy", Dummy)
    assert registry.has("dummy")
    registry.unregister("dummy")
    assert not registry.has("dummy")


def test_portal_type_classvar() -> None:
    assert CKANAdapter.portal_type == "ckan"
    assert DataGouvAdapter.portal_type == "datagouv"
    assert SocrataAdapter.portal_type == "socrata"
    assert CustomAdapter.portal_type == "custom"
