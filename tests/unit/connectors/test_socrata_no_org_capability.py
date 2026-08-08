"""Unit tests for Socrata OrganizationCatalog absence (D-P5-19, Pitfall 1).

Asserts that ``SocrataAdapter`` does NOT implement
:class:`~datasluice.ports.catalog.OrganizationCatalog` — the method
``get_organization`` is intentionally ABSENT (not stubbed) so isinstance
discrimination is genuine. This is the honest capability model: Socrata has no
organizations endpoint, so the connector must not advertise one.
"""

from __future__ import annotations

from datasluice.connectors.socrata import SocrataAdapter
from datasluice.ports import CatalogPort, OrganizationCatalog, SearchableCatalog


def test_socrata_has_no_get_organization_method() -> None:
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert not hasattr(adapter, "get_organization")


def test_socrata_not_organization_catalog() -> None:
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert not isinstance(adapter, OrganizationCatalog)


def test_socrata_still_searchable_and_catalog_port() -> None:
    adapter = SocrataAdapter("https://data.socrata.example.gov")
    assert isinstance(adapter, SearchableCatalog)
    assert isinstance(adapter, CatalogPort)


def test_socrata_class_has_no_get_organization_attribute() -> None:
    assert not hasattr(SocrataAdapter, "get_organization")
