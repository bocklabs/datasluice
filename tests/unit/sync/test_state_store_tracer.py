"""StateStore round-trip tracer: durability across restart + Protocol conformance."""

from __future__ import annotations

from datasluice.domain import SyncState
from datasluice.ports.state_store import StateStore
from datasluice.sync.state_store import FileStateStore, InMemoryStateStore


def test_put_get_roundtrip(file_store: FileStateStore) -> None:
    state = SyncState(cursor={"resource-1": '"etag-abc"'}, last_synced_at="2026-07-30T00:00:00Z")
    file_store.put("resource-1", state)

    loaded = file_store.get("resource-1")

    assert loaded is not None
    assert loaded.cursor == {"resource-1": '"etag-abc"'}
    assert loaded.last_synced_at == "2026-07-30T00:00:00Z"
    assert loaded.partitions == {}
    assert loaded.extra == {}


def test_crash_restart_durability(tmp_path) -> None:
    store_a = FileStateStore(f"file://{tmp_path}/state")
    state = SyncState(cursor={"resource-1": "a" * 64}, last_synced_at="2026-07-30T00:00:00+00:00")
    store_a.put("resource-1", state)

    store_b = FileStateStore(f"file://{tmp_path}/state")
    loaded = store_b.get("resource-1")

    assert loaded == state


def test_inmemory_ephemeral() -> None:
    store = InMemoryStateStore()
    state = SyncState(cursor={"r1": "etag-abc"})
    store.put("resource-1", state)

    assert store.get("resource-1") == state

    store.delete("resource-1")
    assert store.get("resource-1") is None


def test_protocol_conformance(file_store: FileStateStore) -> None:
    assert isinstance(file_store, StateStore)
    assert isinstance(InMemoryStateStore(), StateStore)
