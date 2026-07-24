"""Catalog port Protocols for portal catalog connectors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.domain import Organization, Query, SearchResult


@runtime_checkable
class CatalogPort(Protocol):
    """Marker base protocol all catalog connectors share.

    Attributes:
        portal_type: Canonical name for the portal platform (e.g. ``"ckan"``).
    """

    portal_type: str


@runtime_checkable
class SearchableCatalog(CatalogPort, Protocol):
    """Capability protocol for dataset search."""

    def search(self, query: Query) -> SearchResult: ...


@runtime_checkable
class OrganizationCatalog(CatalogPort, Protocol):
    """Capability protocol for organization lookup."""

    def get_organization(self, organization_id: str) -> Organization: ...
