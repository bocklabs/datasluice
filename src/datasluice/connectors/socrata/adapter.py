"""Socrata adapter implementation.

Communicates with the Socrata Discovery API and SODA2 endpoints.
"""

from __future__ import annotations

from typing import ClassVar

from datasluice.connectors._reject import _reject_unsupported_fields
from datasluice.connectors.base import BaseAdapter
from datasluice.connectors.socrata.mapper import map_dataset
from datasluice.connectors.socrata.pagination import SocrataPage
from datasluice.domain import CatalogCapabilities, Dataset, Query, Resource, SearchResult
from datasluice.exceptions import UnsupportedQueryFieldError

_SOCRATA_SUPPORTED_QUERY_FIELDS: frozenset[str] = frozenset({"text", "tags", "sort"})

_SOCRATA_SORT_FIELD_MAP: dict[str, str] = {
    "name": "name",
    "title": "name",
    "updatedat": "updatedAt",
    "updated_at": "updatedAt",
    "updated": "updatedAt",
    "modified": "updatedAt",
    "metadata_modified": "updatedAt",
    "createdat": "createdAt",
    "created_at": "createdAt",
    "created": "createdAt",
    "issued": "createdAt",
    "metadata_created": "createdAt",
    "relevance": "relevance",
    "score": "relevance",
    "page_views_total": "page_views_total",
    "page_views": "page_views_total",
    "views": "page_views_total",
}


def _translate_sort(sort: str) -> str | None:
    """Translate a ``Query.sort`` spec into a Socrata catalog ``order`` token.

    The Discovery API ``order`` parameter accepts a fixed ascending-only token
    set (``name``, ``updatedAt``, ``createdAt``, ``relevance``,
    ``page_views_total``); any direction suffix — including ``desc`` — makes the
    API return ``'...' is not a supported sort``. Returns ``None`` when the
    sort spec cannot be expressed honestly (unknown field or non-ascending
    direction) so the caller can reject instead of sending a broken param.
    """
    parts = sort.split()
    if not 1 <= len(parts) <= 2:
        return None
    if len(parts) == 2 and parts[1].lower() != "asc":
        return None
    return _SOCRATA_SORT_FIELD_MAP.get(parts[0].lower())


class SocrataAdapter(BaseAdapter):
    """Adapter for Socrata-powered open-data portals.

    Uses the Socrata Discovery API at ``{base_url}/api/catalog/v1``.

    Attributes:
        capabilities: Published catalog capability contract (D-P5-23). Socrata's
            Discovery API supports only ``q``, ``tags``, and ``order`` (BL-02):
            ``Query.sort`` is translated to the ascending-only ``order`` token
            set via :func:`_translate_sort`, and unmappable field/direction
            specs are rejected honestly rather than sent as Socrata's
            nonexistent ``sort`` param (which silently zeroes the result set).
            ``organizations``, ``groups``, ``res_format``, and ``license_id``
            are NOT supported.

    Note:
        This adapter does NOT implement
        :class:`~datasluice.ports.catalog.OrganizationCatalog` (D-P5-19):
        ``get_organization`` is intentionally absent so
        ``isinstance(adapter, OrganizationCatalog)`` is structurally ``False``.
        Socrata has no dedicated organizations endpoint; advertising one via a
        stub-returning placeholder would be a lying capability.
    """

    portal_type = "socrata"
    capabilities: ClassVar[CatalogCapabilities] = CatalogCapabilities(
        supports_search=True,
        supports_organizations=False,
        supported_query_fields=_SOCRATA_SUPPORTED_QUERY_FIELDS,
    )

    def _catalog(self, **params: object) -> dict:
        """Call the Socrata Discovery API and return parsed JSON."""
        url = f"{self.base_url}/api/catalog/v1"
        return self.transport.get_json(url, params=params)

    def search(self, query: Query | None = None) -> SearchResult:
        """Search datasets via the Discovery API."""
        query = query or Query()
        _reject_unsupported_fields(query, self.capabilities.supported_query_fields, "socrata")
        page = SocrataPage(offset=query.offset, limit=query.limit)
        params: dict[str, object] = {**page.to_params()}
        if query.text:
            params["q"] = query.text
        if query.tags:
            params["tags"] = query.tags[0]
        if query.sort:
            order = _translate_sort(query.sort)
            if order is None:
                raise UnsupportedQueryFieldError(
                    field="sort",
                    supported_fields=sorted(self.capabilities.supported_query_fields),
                    portal_name="socrata",
                )
            params["order"] = order
        result = self._catalog(**params)
        datasets = [map_dataset(r, base_url=self.base_url) for r in result.get("results", [])]
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
        return map_dataset(results[0], base_url=self.base_url)

    def list_resources(self, dataset_id: str) -> list[Resource]:
        """Return resources for *dataset_id*."""
        return self.get_dataset(dataset_id).resources
