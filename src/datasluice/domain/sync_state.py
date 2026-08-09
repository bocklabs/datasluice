"""SyncState model for incremental sync cursors and watermarks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SyncState:
    """Incremental synchronization state for a resource or connector.

    Attributes:
        cursor: Mapping of resource IDs to watermark values.
        partitions: Partition progress metadata for parallel syncs.
        last_synced_at: ISO-8601 timestamp of the last successful sync.
        extra: Connector-native sync fields not captured above.
    """

    cursor: dict[str, str] = field(default_factory=dict)
    partitions: dict[str, Any] = field(default_factory=dict)
    last_synced_at: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
