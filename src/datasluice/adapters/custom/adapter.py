"""Template adapter for unsupported portals.

Copy this module, rename the class, and implement the abstract methods to
support a new portal platform.
"""

from __future__ import annotations

from datasluice.adapters.base import BaseAdapter
from datasluice.domain import Dataset, Organization, Query, Resource, SearchResult


class CustomAdapter(BaseAdapter):
    """Skeleton adapter — override each method for your portal."""

    portal_type = "custom"

    def search(self, query: Query | None = None) -> SearchResult:
        raise NotImplementedError

    def get_dataset(self, dataset_id: str) -> Dataset:
        raise NotImplementedError

    def list_resources(self, dataset_id: str) -> list[Resource]:
        raise NotImplementedError

    def get_organization(self, organization_id: str) -> Organization:
        raise NotImplementedError
