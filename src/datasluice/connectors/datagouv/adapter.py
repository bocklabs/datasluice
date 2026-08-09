"""data.gouv.fr (udata) adapter implementation.

Communicates with the udata REST API at ``{base_url}/api/1/``.
"""

from __future__ import annotations

from typing import ClassVar

from datasluice.connectors._reject import _reject_unsupported_fields
from datasluice.connectors.base import BaseAdapter
from datasluice.connectors.datagouv.mapper import map_dataset, map_organization
from datasluice.connectors.datagouv.pagination import DataGouvPage
from datasluice.domain import (
    CatalogCapabilities,
    Dataset,
    Organization,
    Query,
    Resource,
    SearchResult,
)

_DATAGOUV_SUPPORTED_QUERY_FIELDS: frozenset[str] = frozenset(
    {"text", "tags", "organizations", "res_format", "license_id", "sort"}
)


class DataGouvAdapter(BaseAdapter):
    """Adapter for data.gouv.fr and other udata-powered portals.

    Uses the udata REST API at ``{base_url}/api/1/``.

    Attributes:
        capabilities: Published catalog capability contract. udata's
            ``GET /datasets/`` honors ``q``, ``tag[]``, ``organization``,
            ``format``, ``license``, and ``sort`` ( data.gouv
            row). ``groups`` is NOT supported — udata has no ``groups`` param;
            themes use ``topic`` which ``Query`` does not expose.
    """

    portal_type = "datagouv"
    capabilities: ClassVar[CatalogCapabilities] = CatalogCapabilities(
        supports_search=True,
        supports_organizations=True,
        supported_query_fields=_DATAGOUV_SUPPORTED_QUERY_FIELDS,
    )

    def _api(self, path: str, **params: object) -> dict:
        """Call a udata API endpoint and return the parsed JSON."""
        url = f"{self.base_url}/api/1/{path.lstrip('/')}"
        return self.transport.get_json(url, params=params)

    def search(self, query: Query | None = None) -> SearchResult:
        """Search datasets via ``/datasets/``.

        Translates every set supported ``Query`` filter field to its udata-native
        param name: ``res_format`` -> ``format`` (singular!),
        ``license_id`` -> ``license``, ``tags`` -> ``tag`` (array — supports
        multiple values), ``organizations`` -> ``organization``. The reject gate
        guarantees every set field is in ``supported_query_fields``.
        """
        query = query or Query()
        _reject_unsupported_fields(query, self.capabilities.supported_query_fields, "datagouv")
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
            params["tag"] = list(query.tags)
        if query.res_format:
            params["format"] = query.res_format
        if query.license_id:
            params["license"] = query.license_id
        if query.sort:
            params["sort"] = query.sort
        result = self._api("datasets/", **params)
        datasets = [map_dataset(item, base_url=self.base_url) for item in result.get("data", [])]
        total = int(result.get("total", len(datasets)))
        return SearchResult(
            datasets=datasets,
            total=total,
            page=page.page,
            page_size=page.page_size,
            has_next=bool(page.page_size) and page.page * page.page_size < total,
        )

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Fetch a dataset via ``/datasets/{id}/``."""
        result = self._api(f"datasets/{dataset_id}/")
        return map_dataset(result, base_url=self.base_url)

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
