"""Unit tests for capability probing — isinstance discrimination across catalog ports."""

from __future__ import annotations

from datasluice.adapters.ckan import CKANAdapter
from datasluice.adapters.socrata import SocrataAdapter
from datasluice.ports import CatalogPort, OrganizationCatalog, SearchableCatalog


def test_ckan_adapter_satisfies_catalog_port() -> None:
    adapter = CKANAdapter("https://data.example.gov")
    assert isinstance(adapter, CatalogPort)


def test_ckan_adapter_satisfies_searchable_catalog() -> None:
    adapter = CKANAdapter("https://data.example.gov")
    assert isinstance(adapter, SearchableCatalog)


def test_ckan_adapter_satisfies_organization_catalog() -> None:
    adapter = CKANAdapter("https://data.example.gov")
    assert isinstance(adapter, OrganizationCatalog)


def test_portal_type_only_fake_satisfies_catalog_port_but_not_searchable() -> None:
    class FakeCatalog:
        portal_type = "fake"

    fake = FakeCatalog()
    assert isinstance(fake, CatalogPort)
    assert not isinstance(fake, SearchableCatalog)


def test_fake_with_search_satisfies_searchable_catalog() -> None:
    class FakeSearchable:
        portal_type = "fake-search"

        def search(self, query: object) -> object:
            return None

    fake = FakeSearchable()
    assert isinstance(fake, CatalogPort)
    assert isinstance(fake, SearchableCatalog)


def test_socrata_adapter_satisfies_organization_catalog() -> None:
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert isinstance(adapter, OrganizationCatalog)


def test_socrata_adapter_satisfies_searchable_catalog() -> None:
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert isinstance(adapter, SearchableCatalog)


def test_socrata_adapter_satisfies_catalog_port() -> None:
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert isinstance(adapter, CatalogPort)
