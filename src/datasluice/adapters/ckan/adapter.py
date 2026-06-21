"""CKAN adapter implementation.

Communicates with the CKAN Action API (``/api/3/action/``).
"""

from __future__ import annotations

from datasluice.adapters.base import BaseAdapter
from datasluice.adapters.ckan.mapper import map_dataset, map_organization
from datasluice.adapters.ckan.pagination import CKANPage
from datasluice.domain import Dataset, Organization, Query, Resource, SearchResult


class CKANAdapter(BaseAdapter):
    """Adapter for CKAN-powered open-data portals.

    Uses the CKAN Action API at ``{base_url}/api/3/action/``.
    """

    portal_type = "ckan"

    def _action(self, action: str, **params: object) -> dict:
        """Call a CKAN Action API endpoint and return the ``result`` dict."""
        url = f"{self.base_url}/api/3/action/{action}"
        response = self.transport.get_json(url, params=params)
        return response.get("result", {})

    def search(self, query: Query | None = None) -> SearchResult:
        """Search datasets via ``package_search``."""
        query = query or Query()
        page = CKANPage(start=query.offset, rows=query.limit)
        params: dict[str, object] = {"q": query.text or "*:*", **page.to_params()}
        if query.sort:
            params["sort"] = query.sort
        result = self._action("package_search", **params)
        datasets = [map_dataset(pkg) for pkg in result.get("results", [])]
        count = int(result.get("count", len(datasets)))
        return SearchResult(
            datasets=datasets,
            total=count,
            page=(query.offset // query.limit) + 1 if query.limit else 1,
            page_size=query.limit,
            has_next=(query.offset + query.limit) < count,
        )

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Fetch a dataset via ``package_show``."""
        result = self._action("package_show", id=dataset_id)
        return map_dataset(result)

    def list_resources(self, dataset_id: str) -> list[Resource]:
        """Return resources for *dataset_id*."""
        return self.get_dataset(dataset_id).resources

    def get_organization(self, organization_id: str) -> Organization:
        """Fetch organization metadata via ``organization_show``."""
        result = self._action("organization_show", id=organization_id)
        org = map_organization(result)
        if org is None:
            return Organization(id=organization_id)
        return org
