"""Hardening coverage for StateStore persistence, envelopes, and backends."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from datasluice.domain import SyncState
from datasluice.exceptions import StateStoreError
from datasluice.ports.state_store import StateStore
from datasluice.sync.state_store import FileStateStore, InMemoryStateStore


def test_corrupt_json_raises_statestoreerror(file_store: FileStateStore) -> None:
    key = "resource-1"
    file_store._fs.pipe_file(file_store._state_path(key), b"not-json")

    with pytest.raises(StateStoreError):
        file_store.get(key)


def test_wrong_schema_version_raises(file_store: FileStateStore) -> None:
    key = "resource-1"
    payload = json.dumps({"schema_version": 99, "state": {}}).encode()
    file_store._fs.pipe_file(file_store._state_path(key), payload)

    with pytest.raises(StateStoreError):
        file_store.get(key)


def test_sha256_filename_on_disk(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    store = FileStateStore(f"file://{state_dir}")
    key = "../../signed-url?token=secret"

    store.put(key, SyncState(cursor={"resource-1": '"etag"'}))

    json_files = [path for path in state_dir.iterdir() if path.suffix == ".json"]
    expected_name = f"{hashlib.sha256(key.encode()).hexdigest()}.json"
    assert [path.name for path in json_files] == [expected_name]
    assert key not in json_files[0].name


def test_envelope_roundtrip_all_fields(file_store: FileStateStore) -> None:
    state = SyncState(
        cursor={"r1": "etag", "r2": "lmt"},
        partitions={"p": "v"},
        last_synced_at="2026-07-30T00:00:00Z",
        extra={"k": "v"},
    )

    file_store.put("resource-1", state)

    assert file_store.get("resource-1") == state


def test_delete_then_get_none(file_store: FileStateStore) -> None:
    file_store.put("resource-1", SyncState(cursor={"resource-1": '"etag"'}))

    file_store.delete("resource-1")

    assert file_store.get("resource-1") is None


def test_missing_key_returns_none(file_store: FileStateStore) -> None:
    assert file_store.get("never-written") is None


def test_memory_backend_works(memory_store: FileStateStore) -> None:
    state = SyncState(cursor={"resource-memory": '"etag"'}, extra={"backend": "memory"})

    memory_store.put("resource-memory", state)

    assert memory_store.get("resource-memory") == state


def test_inmemory_protocol_conformance() -> None:
    assert isinstance(InMemoryStateStore(), StateStore)
