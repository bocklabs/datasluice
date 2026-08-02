"""dlt (data load tool) integration: use DataSluice as a dlt source.

Requires ``dlt``: install with ``pip install datasluice[dlt]``.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from datasluice.logging import get_logger

logger = get_logger("integrations.dlt")


def _sanitize(resource_id: str) -> str:
    """Return a deterministic destination-safe name for a resource ID."""
    name = re.sub(r"[^A-Za-z0-9_]", "_", resource_id) or "_"
    if name[0].isdigit():
        name = f"_{name}"
    return name[:64]


def datasluice_source(
    portal: str,
    query: str | None = None,
    *,
    state_store: Any | None = None,
    limit: int = 100,
    include_metadata: bool = False,
    **kwargs: Any,
) -> Any:
    """Return a dlt source yielding one Arrow-backed table per resource.

    Args:
        portal: Base URL of the open-data portal.
        query: Optional free-text search query.
        state_store: Optional durable store for per-resource watermarks.
        limit: Maximum number of datasets to fetch.
        include_metadata: Whether to include the dataset catalog as a sibling resource.
        **kwargs: Additional fields forwarded to :class:`~datasluice.domain.Query`.

    Returns:
        A dlt ``DltSource`` containing one resource per supported portal resource.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError("dlt integration requires 'dlt'. Install with: pip install datasluice[dlt]") from exc

    from datasluice import DataSluice
    from datasluice.data.access import DataPlaneResourceReader
    from datasluice.domain import Query
    from datasluice.transport.httpx_transport import HttpxTransport

    @dlt.source(name="datasluice")
    def _source() -> Any:
        data_sluice = DataSluice()
        result = data_sluice.search(portal, Query(text=query, limit=limit, **kwargs))
        datasets = result.datasets
        from datasluice.sync._identity import canonical_identity

        seen_identities: dict[str, str] = {}
        seen_table_names: dict[str, str] = {}

        for dataset in datasets:
            for resource in dataset.resources:
                if resource.access is not None and resource.access.kind in ("query", "stream"):
                    logger.debug(
                        "Skipping unsupported dlt resource %s with access kind %s", resource.id, resource.access.kind
                    )
                    continue

                identity = canonical_identity(resource)
                table_name = _sanitize(resource.id)
                if identity in seen_identities:
                    raise ValueError(
                        f"Resource IDs {seen_identities[identity]!r} and {resource.id!r} collide on canonical "
                        f"identity {identity}"
                    )
                if include_metadata and table_name.casefold() == "datasets":
                    raise ValueError(
                        f"Resource ID {resource.id!r} maps to reserved dlt table name 'datasets' "
                        f"(reserved for the metadata resource emitted when include_metadata=True)"
                    )
                normalized_table_name = table_name.casefold()
                if normalized_table_name in seen_table_names:
                    raise ValueError(
                        f"Resource IDs {seen_table_names[normalized_table_name]!r} and {resource.id!r} "
                        f"collide on sanitized dlt table name {table_name!r}"
                    )
                seen_identities[identity] = resource.id
                seen_table_names[normalized_table_name] = resource.id

                @dlt.resource(name=table_name, table_name=table_name, write_disposition="replace")
                def _resource_body(resource: Any = resource, identity: str = identity) -> Any:
                    from datasluice.domain import SyncState
                    from datasluice.integrations.arrow import to_arrow
                    from datasluice.sync._hashing import logical_sha256

                    transport = HttpxTransport()
                    reader = DataPlaneResourceReader(transport=transport)
                    try:
                        if state_store is not None:
                            prior = state_store.get(identity)
                            watermark = prior.cursor.get(identity) if prior is not None else None
                            dlt.current.resource_state()["datasluice"] = {"watermark": watermark}

                        with reader.open(resource) as stream:
                            table = to_arrow(stream)
                        yield table

                        fresh_watermark = logical_sha256(table)
                        if state_store is not None:
                            state_store.put(
                                identity,
                                SyncState(
                                    cursor={identity: fresh_watermark},
                                    last_synced_at=datetime.now(UTC).isoformat(),
                                ),
                            )
                        dlt.current.resource_state().setdefault("datasluice", {})["watermark"] = fresh_watermark
                    finally:
                        transport.close()

                yield _resource_body

        if include_metadata:

            @dlt.resource(name="datasets", write_disposition="replace")
            def _datasets() -> Any:
                for dataset in datasets:
                    yield {
                        "id": dataset.id,
                        "title": dataset.title,
                        "name": dataset.name,
                        "description": dataset.description,
                        "organization": dataset.organization.name if dataset.organization else None,
                        "tags": dataset.tags,
                        "url": dataset.url,
                        "resources": [
                            {"id": resource.id, "name": resource.name, "url": resource.url, "format": resource.format}
                            for resource in dataset.resources
                        ],
                    }

            yield _datasets

    return _source()
