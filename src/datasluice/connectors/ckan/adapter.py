"""CKAN adapter implementation.

Communicates with the CKAN Action API (``/api/3/action/``).
"""

from __future__ import annotations

from typing import ClassVar

from datasluice.connectors._reject import _reject_unsupported_fields
from datasluice.connectors.base import BaseAdapter
from datasluice.connectors.ckan.mapper import map_dataset, map_organization
from datasluice.connectors.ckan.pagination import CKANPage
from datasluice.domain import (
    CatalogCapabilities,
    Dataset,
    Organization,
    Query,
    Resource,
    SearchResult,
)

_CKAN_SUPPORTED_QUERY_FIELDS: frozenset[str] = frozenset(
    {"text", "tags", "organizations", "groups", "res_format", "license_id", "sort"}
)


class CKANAdapter(BaseAdapter):
    """Adapter for CKAN-powered open-data portals.

    Uses the CKAN Action API at ``{base_url}/api/3/action/``.

    Attributes:
        capabilities: Published catalog capability contract (D-P5-23). CKAN's
            ``package_search`` honors all six ``Query`` filter fields via Solr
            ``fq`` clauses (COVERAGE.md CKAN row).
    """

    portal_type = "ckan"
    capabilities: ClassVar[CatalogCapabilities] = CatalogCapabilities(
        supports_search=True,
        supports_organizations=True,
        supports_faceted_search=True,
        supported_query_fields=_CKAN_SUPPORTED_QUERY_FIELDS,
    )

    def _action(self, action: str, **params: object) -> dict:
        """Call a CKAN Action API endpoint and return the ``result`` dict."""
        url = f"{self.base_url}/api/3/action/{action}"
        response = self.transport.get_json(url, params=params)
        return response.get("result", {})

    def search(self, query: Query | None = None) -> SearchResult:
        """Search datasets via ``package_search``.

        Translates every set supported ``Query`` filter field into a CKAN Solr
        ``fq`` clause (CONN-01). The reject gate from Task 1 already guarantees
        every set field is in ``supported_query_fields``, so the translation is
        unconditional. Value quoting is intentionally deferred (D-P5-09): only
        binary field-presence is enforced in v1.
        """
        query = query or Query()
        _reject_unsupported_fields(query, self.capabilities.supported_query_fields, "ckan")
        page = CKANPage(start=query.offset, rows=query.limit)
        params: dict[str, object] = {"q": query.text or "*:*", **page.to_params()}
        if query.sort:
            params["sort"] = query.sort
        fq_clauses: list[str] = []
        for tag in query.tags:
            fq_clauses.append(f"tags:{tag}")
        for org in query.organizations:
            fq_clauses.append(f"organization:{org}")
        for group in query.groups:
            fq_clauses.append(f"groups:{group}")
        if query.res_format:
            fq_clauses.append(f"res_format:{query.res_format}")
        if query.license_id:
            fq_clauses.append(f"license_id:{query.license_id}")
        if fq_clauses:
            params["fq"] = " ".join(fq_clauses)
        result = self._action("package_search", **params)
        datasets = [map_dataset(pkg, base_url=self.base_url) for pkg in result.get("results", [])]
        count = int(result.get("count", len(datasets)))
        return SearchResult(
            datasets=datasets,
            total=count,
            page=(query.offset // query.limit) + 1 if query.limit else 1,
            page_size=query.limit,
            has_next=bool(query.limit) and (query.offset + query.limit) < count,
        )

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Fetch a dataset via ``package_show``."""
        result = self._action("package_show", id=dataset_id)
        return map_dataset(result, base_url=self.base_url)

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
