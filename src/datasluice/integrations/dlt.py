"""dlt (data load tool) integration: use DataSluice as a dlt source.

Requires ``dlt``: install with ``pip install datasluice[dlt]``.
"""

from __future__ import annotations

from typing import Any

from datasluice.logging import get_logger

logger = get_logger("integrations.dlt")


def datasluice_source(
    portal: str,
    query: str | None = None,
    *,
    limit: int = 100,
    **kwargs: Any,
) -> Any:
    """Return a dlt source that yields datasets from *portal*.

    Args:
        portal: Base URL of the open-data portal.
        query: Optional free-text search query.
        limit: Maximum number of datasets to fetch.
        **kwargs: Additional options forwarded to the adapter.

    Returns:
        A dlt ``DltResource`` (requires ``dlt`` to be installed).
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError("dlt integration requires 'dlt'. Install with: pip install datasluice[dlt]") from exc

    from datasluice import DataSluice
    from datasluice.domain import Query

    @dlt.resource(name="datasets", write_disposition="replace")
    def _datasets() -> Any:
        ds = DataSluice(portal, **kwargs)
        result = ds.search(Query(text=query, limit=limit))
        for dataset in result.datasets:
            yield {
                "id": dataset.id,
                "title": dataset.title,
                "name": dataset.name,
                "description": dataset.description,
                "organization": dataset.organization.name if dataset.organization else None,
                "tags": dataset.tags,
                "url": dataset.url,
                "resources": [
                    {"id": r.id, "name": r.name, "url": r.url, "format": r.format} for r in dataset.resources
                ],
            }

    return _datasets
