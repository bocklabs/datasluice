"""Unit tests for capability probing — isinstance discrimination across catalog ports."""

from __future__ import annotations

from datasluice.connectors.ckan import CKANAdapter
from datasluice.connectors.datagouv import DataGouvAdapter
from datasluice.connectors.socrata import SocrataAdapter
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


def test_datagouv_adapter_satisfies_catalog_port() -> None:
    adapter = DataGouvAdapter("https://data.gouv.fr")
    assert isinstance(adapter, CatalogPort)


def test_datagouv_adapter_satisfies_searchable_catalog() -> None:
    adapter = DataGouvAdapter("https://data.gouv.fr")
    assert isinstance(adapter, SearchableCatalog)


def test_datagouv_adapter_satisfies_organization_catalog() -> None:
    adapter = DataGouvAdapter("https://data.gouv.fr")
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


def test_socrata_adapter_does_NOT_satisfy_organization_catalog() -> None:
    """Socrata must NOT satisfy OrganizationCatalog.

    ``get_organization`` is intentionally absent from ``SocrataAdapter`` so
    ``isinstance`` discrimination is genuine (: PEP 544
    python/typing#800 explicit-subclass bypass would make this True if
    ``BaseAdapter`` still declared ``get_organization``, but -01 removed
    that declaration).
    """
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert not isinstance(adapter, OrganizationCatalog)


def test_socrata_adapter_satisfies_searchable_catalog() -> None:
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert isinstance(adapter, SearchableCatalog)


def test_socrata_adapter_satisfies_catalog_port() -> None:
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert isinstance(adapter, CatalogPort)
