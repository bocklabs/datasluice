"""dlt (data load tool) integration: use DataSluice as a dlt source.

Requires ``dlt``: install with ``pip install datasluice[dlt]``.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any, cast

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
        state_store: Optional durable store seeded with prior per-resource
            watermarks at the start of each resource (read-only during the run).
            Load-committed watermarks are mirrored back into it after the
            pipeline completes via :func:`mirror_dlt_state`.
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

    allowed_query_fields = {field.name for field in Query.__dataclass_fields__.values()} - {"text", "limit"}
    query_kwargs = {key: value for key, value in kwargs.items() if key in allowed_query_fields}

    @dlt.source(name="datasluice")
    def _source() -> Any:
        with DataSluice() as data_sluice:
            result = cast(Any, data_sluice).search(portal, Query(text=query, limit=limit, **query_kwargs))
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
                    from datasluice.integrations.arrow import to_arrow
                    from datasluice.sync._hashing import logical_sha256

                    transport = HttpxTransport()
                    reader = DataPlaneResourceReader(transport=transport)
                    try:
                        state: dict[str, Any] = {"identity": identity, "watermark": None}
                        if state_store is not None:
                            prior = state_store.get(identity)
                            state["watermark"] = prior.cursor.get(identity) if prior is not None else None
                        dlt.current.resource_state()["datasluice"] = state

                        with reader.open(resource) as stream:
                            table = to_arrow(stream)
                        yield table

                        dlt.current.resource_state()["datasluice"]["watermark"] = logical_sha256(table)
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


def mirror_dlt_state(pipeline: Any, state_store: Any, *, source_name: str = "datasluice") -> None:
    """Mirror dlt's load-committed per-resource watermarks into a DataSluice StateStore.

    dlt persists ``resource_state`` only after a successful load, so the
    watermarks read here reflect rows that actually landed in the destination.
    Call this after ``pipeline.run`` (or ``extract`` + ``normalize`` + ``load``)
    to copy the committed watermarks into *state_store*. Writes are
    compare-and-swap protected when *state_store* satisfies
    :class:`datasluice.ports.state_store.AtomicStateStore`; otherwise plain
    ``put`` is used.

    Resources without a committed ``datasluice`` watermark (e.g. the metadata
    sibling) are skipped. Re-running this after an identical load is a no-op
    against the committed state.

    Args:
        pipeline: A ``dlt.Pipeline`` that has completed a load.
        state_store: The :class:`~datasluice.ports.state_store.StateStore` to mirror into.
        source_name: The dlt source name to read committed state from.
    """
    from datasluice.domain import SyncState
    from datasluice.exceptions import SyncStateConflictError
    from datasluice.ports import AtomicStateStore

    resources = pipeline.state.get("sources", {}).get(source_name, {}).get("resources", {})
    is_atomic = isinstance(state_store, AtomicStateStore)
    for resource_state in resources.values():
        datasluice_state = resource_state.get("datasluice") if isinstance(resource_state, dict) else None
        if not isinstance(datasluice_state, dict):
            continue
        identity = datasluice_state.get("identity")
        watermark = datasluice_state.get("watermark")
        if not identity or not watermark:
            continue
        state = SyncState(cursor={identity: watermark}, last_synced_at=datetime.now(UTC).isoformat())
        if is_atomic:
            try:
                state_store.conditional_put(identity, state, state_store.read_version(identity))
            except SyncStateConflictError:
                logger.warning(
                    "Sync-state conflict mirroring dlt watermark for identity %s; skipping (another writer won)",
                    identity,
                )
        else:
            state_store.put(identity, state)
