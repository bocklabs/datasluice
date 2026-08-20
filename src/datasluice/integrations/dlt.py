"""dlt (data load tool) integration for explicit normalized catalog clients."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import unquote, urlsplit

from datasluice.logging import get_logger
from datasluice.runtime.transport.base import CatalogTransport

if TYPE_CHECKING:
    from datasluice.contracts.catalog.protocols import CatalogOperationRequest, SyncCatalogClient

logger = get_logger("integrations.dlt")


def _sanitize(resource_id: str) -> str:
    """Return a deterministic destination-safe name for a resource ID."""
    name = re.sub(r"[^A-Za-z0-9_]", "_", resource_id) or "_"
    if name[0].isdigit():
        name = f"_{name}"
    return name[:64]


def _resource_from_record(record: Any) -> Any:
    """Convert one normalized resource record into a data-plane resource."""
    from datasluice.domain import HttpDownload, LocalFile, ObjectStorage, Resource

    if record.url is None:
        raise ValueError(f"Normalized resource {record.id.value!r} requires a direct URL for dlt extraction")
    scheme = urlsplit(record.url).scheme
    if scheme in {"http", "https"}:
        access = HttpDownload(url=record.url)
    elif scheme in {"s3", "gs", "gcs", "az", "azure", "abfs"}:
        access = ObjectStorage(uri=record.url)
    elif scheme == "file" or not scheme:
        parts = urlsplit(record.url)
        if parts.netloc not in {"", "localhost"}:
            raise ValueError(f"Normalized resource {record.id.value!r} has an unsupported direct URL host")
        access = LocalFile(path=unquote(parts.path))
    else:
        raise ValueError(f"Normalized resource {record.id.value!r} has an unsupported direct URL scheme")
    return Resource(id=record.id.value, name=record.name, url=record.url, access=access)


def datasluice_source(
    client: SyncCatalogClient,
    query: CatalogOperationRequest,
    *,
    state_store: Any | None = None,
    include_metadata: bool = False,
) -> Any:
    """Return a dlt source from a caller-owned normalized resource query.

    Args:
        client: Explicit normalized synchronous catalog client.
        query: Typed resources.list catalog operation.
        state_store: Optional durable store seeded with prior per-resource watermarks.
        include_metadata: Whether to include normalized resource metadata as a sibling resource.

    Returns:
        A dlt ``DltSource`` containing one resource per normalized catalog resource.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError("dlt integration requires the dlt extra. Install with: uv sync --extra dlt") from exc

    from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest, SyncCatalogClient
    from datasluice.data.access import DataPlaneResourceReader

    if not isinstance(client, SyncCatalogClient):
        raise TypeError("datasluice_source requires a SyncCatalogClient-compatible normalized client")
    if not isinstance(query, CatalogOperationRequest):
        raise TypeError("datasluice_source requires a typed CatalogOperationRequest query")
    if (query.operation_id.service, query.operation_id.method) != ("resources", "list"):
        raise ValueError("datasluice_source requires a resources.list catalog operation")

    records = client.resources.list(query, CatalogOperationGuard(operation_id=query.operation_id)).items

    @dlt.source(name="datasluice")
    def _source() -> Any:
        from datasluice.sync._identity import canonical_identity

        seen_identities: dict[str, str] = {}
        seen_table_names: dict[str, str] = {}

        for record in records:
            resource = _resource_from_record(record)
            identity = canonical_identity(resource)
            table_name = _sanitize(resource.id)
            if identity in seen_identities:
                raise ValueError(
                    f"Resource IDs {seen_identities[identity]!r} and {resource.id!r} "
                    f"collide on canonical identity {identity}"
                )
            if include_metadata and table_name.casefold() == "resources":
                raise ValueError(
                    f"Resource ID {resource.id!r} maps to reserved dlt table name 'resources' "
                    "(reserved for the metadata resource emitted when include_metadata=True)"
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

                transport = cast(CatalogTransport, cast(Any, client)._transport)
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
                    pass

            yield _resource_body

        if include_metadata:

            @dlt.resource(name="resources", write_disposition="replace")
            def _resources() -> Any:
                for record in records:
                    yield {
                        "id": record.id.to_dict(),
                        "dataset_id": record.dataset_id.to_dict(),
                        "name": record.name,
                        "url": record.url,
                    }

            yield _resources

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
