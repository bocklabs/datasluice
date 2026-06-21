"""data.gouv.fr (udata) adapter implementation.

Communicates with the udata REST API at ``{base_url}/api/1/``.
"""

from __future__ import annotations

from datasluice.adapters.base import BaseAdapter
from datasluice.adapters.datagouv.mapper import map_dataset, map_organization
from datasluice.adapters.datagouv.pagination import DataGouvPage
from datasluice.domain import Dataset, Organization, Query, Resource, SearchResult


class DataGouvAdapter(BaseAdapter):
    """Adapter for data.gouv.fr and other udata-powered portals.

    Uses the udata REST API at ``{base_url}/api/1/``.
    """

    portal_type = "datagouv"

    def _api(self, path: str, **params: object) -> dict:
        """Call a udata API endpoint and return the parsed JSON."""
        url = f"{self.base_url}/api/1/{path.lstrip('/')}"
        return self.transport.get_json(url, params=params)

    def search(self, query: Query | None = None) -> SearchResult:
        """Search datasets via ``/datasets/``."""
        query = query or Query()
        page = DataGouvPage(
            page=(query.offset // query.limit) + 1 if query.limit else 1,
            page_size=query.limit,
        )
        params: dict[str, object] = {**page.to_params()}
        if query.text:
            params["q"] = query.text
        if query.organizations:
            params["organization"] = query.organizations[0]
        if query.tags:
            params["tag"] = query.tags[0]
        result = self._api("datasets/", **params)
        datasets = [map_dataset(item) for item in result.get("data", [])]
        total = int(result.get("total", len(datasets)))
        return SearchResult(
            datasets=datasets,
            total=total,
            page=page.page,
            page_size=page.page_size,
            has_next=page.page * page.page_size < total,
        )

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Fetch a dataset via ``/datasets/{id}/``."""
        result = self._api(f"datasets/{dataset_id}/")
        return map_dataset(result)

    def list_resources(self, dataset_id: str) -> list[Resource]:
        """Return resources for *dataset_id*."""
        return self.get_dataset(dataset_id).resources

    def get_organization(self, organization_id: str) -> Organization:
        """Fetch organization metadata via ``/organizations/{slug}/``."""
        result = self._api(f"organizations/{organization_id}/")
        org = map_organization(result)
        if org is None:
            return Organization(id=organization_id)
        return org
