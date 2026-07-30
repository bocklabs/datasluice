"""Checkpointed resource synchronization and per-resource outcomes."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from datasluice.logging import get_logger

if TYPE_CHECKING:
    from datasluice.domain import SyncState

logger = get_logger("sync.sync")


@dataclass(frozen=True)
class SyncOutcome:
    """Describe the result of synchronizing one resource."""

    resource: Any
    action: str
    record: Any | None = None
    state_key: str | None = None


def sync_resources(
    resources: Iterable[Any],
    *,
    state_store: Any,
    reader: Any,
    destination_uri: str,
    transport: Any | None = None,
    cache: Any | None = None,
) -> Iterator[SyncOutcome]:
    """Synchronize resources and emit each outcome after its state checkpoint."""
    from datasluice.sync.materialize import materialize

    for resource in resources:
        kind = resource.access.kind if resource.access is not None else "http_download"
        if kind in ("query", "stream"):
            yield SyncOutcome(resource, action="skipped-unsupported")
            continue

        key = f"{resource.id}"
        state_store.get(key)
        record = materialize(resource, reader=reader, destination_uri=destination_uri)
        checksum = record[3]
        state_store.put(key, _sync_state(resource.id, checksum))
        yield SyncOutcome(resource, action="materialized", record=record, state_key=key)


def _sync_state(resource_id: str, watermark: str) -> SyncState:
    from datasluice.domain import SyncState

    return SyncState(cursor={resource_id: watermark}, last_synced_at=_utcnow_iso())


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()
