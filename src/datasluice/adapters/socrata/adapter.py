"""Socrata adapter implementation.

Communicates with the Socrata Discovery API and SODA2 endpoints.
"""

from __future__ import annotations

from datasluice.adapters.base import BaseAdapter
from datasluice.adapters.socrata.mapper import map_dataset
from datasluice.adapters.socrata.pagination import SocrataPage
from datasluice.domain import Dataset, Organization, Query, Resource, SearchResult


class SocrataAdapter(BaseAdapter):
    """Adapter for Socrata-powered open-data portals.

    Uses the Socrata Discovery API at ``{base_url}/api/catalog/v1``.
    """

    portal_type = "socrata"

    def _catalog(self, **params: object) -> dict:
        """Call the Socrata Discovery API and return parsed JSON."""
        url = f"{self.base_url}/api/catalog/v1"
        return self.transport.get_json(url, params=params)

    def search(self, query: Query | None = None) -> SearchResult:
        """Search datasets via the Discovery API."""
        query = query or Query()
        page = SocrataPage(offset=query.offset, limit=query.limit)
        params: dict[str, object] = {**page.to_params()}
        if query.text:
            params["q"] = query.text
        if query.tags:
            params["tags"] = query.tags[0]
        result = self._catalog(**params)
        datasets = [map_dataset(r) for r in result.get("results", [])]
        total = int(result.get("resultSetSize", len(datasets)))
        return SearchResult(
            datasets=datasets,
            total=total,
            page=(query.offset // query.limit) + 1 if query.limit else 1,
            page_size=query.limit,
            has_next=(query.offset + query.limit) < total,
        )

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Fetch a dataset (view) by its 4x4 identifier."""
        result = self._catalog(ids=dataset_id)
        results = result.get("results", [])
        if not results:
            return Dataset(id=dataset_id)
        return map_dataset(results[0])

    def list_resources(self, dataset_id: str) -> list[Resource]:
        """Return resources for *dataset_id*."""
        return self.get_dataset(dataset_id).resources

    def get_organization(self, organization_id: str) -> Organization:
        """Socrata does not expose a dedicated organizations endpoint.

        Returns a minimal :class:`Organization` with the provided ID.
        """
        return Organization(id=organization_id)
