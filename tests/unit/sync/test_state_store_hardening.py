"""Hardening coverage for StateStore persistence, envelopes, and backends."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest

from datasluice.domain import SyncState
from datasluice.exceptions import StateStoreError
from datasluice.ports.state_store import StateStore
from datasluice.sync.state_store import FileStateStore, InMemoryStateStore

state_store_module = importlib.import_module("datasluice.sync.state_store")
if not hasattr(state_store_module, "_SECRET_FREE_STATE_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("secret-free durable state implementation pending GREEN phase", allow_module_level=True)


def _sha_watermark(character: str = "a") -> str:
    return character * 64


def _checkpoint(index: int = 2) -> dict[str, Any]:
    return {
        "version": 1,
        "status": "in_progress",
        "next_batch_index": index,
        "position": {
            "kind": "parquet_row_group",
            "row_group_index": index,
        },
    }


def _assert_rejected_before_pipe(
    store: FileStateStore,
    key: str,
    state: SyncState,
    *,
    secret_fragments: tuple[str, ...] = (),
) -> None:
    prior_raw = store.read_raw(key)
    original_pipe = store._fs.pipe_file
    with patch.object(store._fs, "pipe_file", wraps=original_pipe) as pipe_file:
        with pytest.raises(StateStoreError) as exc_info:
            store.put(key, state)
    pipe_file.assert_not_called()
    assert store.read_raw(key) == prior_raw
    message = str(exc_info.value)
    durable = store.read_raw(key) or b""
    for fragment in secret_fragments:
        assert fragment not in message
        assert fragment.encode() not in durable


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

    store.put(key, SyncState(cursor={key: '"etag"'}))

    json_files = [path for path in state_dir.iterdir() if path.suffix == ".json"]
    expected_name = f"{hashlib.sha256(key.encode()).hexdigest()}.json"
    assert [path.name for path in json_files] == [expected_name]
    assert key not in json_files[0].name


def test_serialized_envelope_omits_raw_key_and_signed_url_material(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    store = FileStateStore(f"file://{state_dir}")
    key_fragments = (
        "synthetic-user",
        "synthetic-password",
        "synthetic-token",
        "synthetic-signature",
    )
    key = (
        "https://synthetic-user:synthetic-password@example.test/data?"
        "token=synthetic-token&X-Amz-Signature=synthetic-signature"
    )
    completed = SyncState(
        cursor={key: _sha_watermark()},
        last_synced_at="2026-07-30T12:34:56+00:00",
    )

    store.put(key, completed)

    path = store._state_path(key)
    raw = store._fs.cat_file(path)
    envelope = json.loads(raw)
    assert set(envelope) == {"schema_version", "state"}
    assert envelope["schema_version"] == 1
    assert envelope["state"]["cursor"] == {
        "schema": "datasluice_completed_watermark_v1",
        "watermark": _sha_watermark(),
    }
    assert store.get(key) == completed
    assert path.endswith(f"{hashlib.sha256(key.encode()).hexdigest()}.json")
    for fragment in (*key_fragments, key):
        assert fragment.encode() not in raw

    checkpoint_key = (
        "https://bearer-user:bearer-password@example.test/data?"
        "authorization=bearer-credential&signature=checkpoint-signature&reader_token=legacy-reader-token"
    )
    checkpoint_state = SyncState(extra={"datasluice_checkpoint": _checkpoint()})
    store.put(checkpoint_key, checkpoint_state)
    checkpoint_raw = store._fs.cat_file(store._state_path(checkpoint_key))
    checkpoint_envelope = json.loads(checkpoint_raw)
    assert set(checkpoint_envelope) == {"schema_version", "state"}
    assert checkpoint_envelope["state"]["extra"] == {"datasluice_checkpoint": _checkpoint()}
    assert store.get(checkpoint_key) == checkpoint_state
    for fragment in (
        "bearer-user",
        "bearer-password",
        "bearer-credential",
        "checkpoint-signature",
        "legacy-reader-token",
        checkpoint_key,
    ):
        assert fragment.encode() not in checkpoint_raw


def test_unsafe_checkpoint_and_extra_are_rejected_before_write(file_store: FileStateStore) -> None:
    key = "resource-unsafe"
    prior = SyncState(cursor={key: _sha_watermark("b")})
    file_store.put(key, prior)
    signed_url = "https://example.test/data?X-Amz-Signature=unsafe-signature"
    unsafe_states = (
        SyncState(
            extra={
                "metadata": {
                    "password": "unsafe-password",
                    "authorization": "unsafe-bearer",
                    "signature": "unsafe-signature",
                }
            }
        ),
        SyncState(
            extra={
                "datasluice_checkpoint": {
                    **_checkpoint(),
                    "reader_token": signed_url,
                }
            }
        ),
    )

    for state in unsafe_states:
        _assert_rejected_before_pipe(
            file_store,
            key,
            state,
            secret_fragments=(
                "unsafe-password",
                "unsafe-bearer",
                "unsafe-signature",
                signed_url,
            ),
        )
        assert file_store.get(key) == prior


def test_opaque_secret_under_neutral_extra_field_is_rejected_before_write(file_store: FileStateStore) -> None:
    key = "resource-neutral"
    opaque_secret = "zephyr-lantern-cobalt-7391"
    _assert_rejected_before_pipe(
        file_store,
        key,
        SyncState(extra={"metadata": {"value": opaque_secret}}),
        secret_fragments=(opaque_secret,),
    )
    assert not file_store._fs.exists(file_store._state_path(key))


def test_arbitrary_cursor_mapping_is_rejected_before_write(file_store: FileStateStore) -> None:
    key = "resource-cursor"
    prior = SyncState(cursor={key: _sha_watermark("c")})
    file_store.put(key, prior)
    opaque_value = "neutral-cursor-value-4815"
    invalid_cursors = (
        {key: opaque_value},
        {"different-resource": _sha_watermark("d")},
        {key: _sha_watermark("d"), "different-resource": _sha_watermark("e")},
        {key: {"nested": opaque_value}},
        {key: 42},
    )

    for cursor in invalid_cursors:
        _assert_rejected_before_pipe(
            file_store,
            key,
            SyncState(cursor=cast(Any, cursor)),
            secret_fragments=(opaque_value,),
        )
        assert file_store.get(key) == prior


def test_checkpoint_unknown_nested_fields_are_rejected_before_write(file_store: FileStateStore) -> None:
    key = "resource-checkpoint"
    prior = SyncState(cursor={key: _sha_watermark("f")})
    file_store.put(key, prior)
    opaque_value = "nested-opaque-value-90210"
    checkpoint_extra = {**_checkpoint(), "metadata": opaque_value}
    position_extra = _checkpoint()
    position_extra["position"] = {**position_extra["position"], "metadata": opaque_value}

    for checkpoint in (checkpoint_extra, position_extra):
        _assert_rejected_before_pipe(
            file_store,
            key,
            SyncState(extra={"datasluice_checkpoint": checkpoint}),
            secret_fragments=(opaque_value,),
        )
        assert file_store.get(key) == prior


@pytest.mark.parametrize(
    "state",
    [
        SyncState(partitions={"partition": "value"}),
        SyncState(last_synced_at="2026-07-30T12:34:56"),
        SyncState(last_synced_at="not-a-timestamp"),
        SyncState(last_synced_at="x" * 256),
        SyncState(
            extra={
                "datasluice_checkpoint": {
                    **_checkpoint(),
                    "next_batch_index": -1,
                }
            }
        ),
        SyncState(
            extra={
                "datasluice_checkpoint": {
                    **_checkpoint(),
                    "next_batch_index": True,
                }
            }
        ),
        SyncState(
            extra={
                "datasluice_checkpoint": {
                    **_checkpoint(),
                    "next_batch_index": 3,
                }
            }
        ),
    ],
)
def test_nonempty_partitions_and_invalid_structured_fields_are_rejected_before_write(
    file_store: FileStateStore,
    state: SyncState,
) -> None:
    _assert_rejected_before_pipe(file_store, "resource-invalid", state)


def test_legacy_v1_envelope_with_key_remains_readable(file_store: FileStateStore) -> None:
    key = "legacy-resource"
    legacy_state = SyncState(
        cursor={"legacy-cursor-key": "legacy-watermark"},
        partitions={"legacy-partition": {"offset": 12}},
        last_synced_at="legacy-timestamp",
        extra={"legacy": {"arbitrary": True}},
    )
    payload = json.dumps(
        {
            "schema_version": 1,
            "key": key,
            "state": asdict(legacy_state),
        },
        sort_keys=True,
    ).encode()
    file_store._fs.pipe_file(file_store._state_path(key), payload)

    assert file_store.get(key) == legacy_state


def test_repeated_identical_put_has_same_path_and_bytes(file_store: FileStateStore) -> None:
    key = "resource-deterministic"
    state = SyncState(
        cursor={key: _sha_watermark("1")},
        last_synced_at="2026-07-30T12:34:56+00:00",
    )

    file_store.put(key, state)
    first_path = file_store._state_path(key)
    first_bytes = file_store._fs.cat_file(first_path)
    file_store.put(key, state)
    second_path = file_store._state_path(key)
    second_bytes = file_store._fs.cat_file(second_path)

    assert first_path == second_path
    assert first_bytes == second_bytes
    assert file_store.get(key) == state
    assert json.loads(first_bytes) == json.loads(second_bytes)


def test_concurrent_read_observes_complete_old_or_new_envelope(file_store: FileStateStore) -> None:
    key = "resource-concurrent"
    old_state = SyncState(cursor={key: _sha_watermark("2")})
    new_state = SyncState(cursor={key: _sha_watermark("3")})
    file_store.put(key, old_state)
    final_path = file_store._state_path(key)
    old_bytes = file_store._fs.cat_file(final_path)
    move_reached = threading.Event()
    release_move = threading.Event()
    original_move = file_store._fs.mv
    writer_errors: list[BaseException] = []

    def paused_move(source: str, target: str) -> None:
        assert target == final_path
        move_reached.set()
        if not release_move.wait(timeout=5):
            raise TimeoutError("test did not release state publication")
        original_move(source, target)

    def writer() -> None:
        try:
            file_store.put(key, new_state)
        except BaseException as exc:
            writer_errors.append(exc)

    with patch.object(file_store._fs, "mv", side_effect=paused_move):
        thread = threading.Thread(target=writer)
        thread.start()
        try:
            assert move_reached.wait(timeout=5)
            during_state = file_store.get(key)
            during_bytes = file_store._fs.cat_file(final_path)
        finally:
            release_move.set()
            thread.join(timeout=5)

    assert not thread.is_alive()
    assert writer_errors == []
    after_state = file_store.get(key)
    after_bytes = file_store._fs.cat_file(final_path)
    assert during_state == old_state
    assert during_bytes == old_bytes
    assert after_state == new_state
    assert json.loads(during_bytes)["schema_version"] == 1
    assert json.loads(after_bytes)["schema_version"] == 1


def test_move_failure_preserves_prior_complete_state(file_store: FileStateStore) -> None:
    key = "resource-move-failure"
    old_state = SyncState(cursor={key: _sha_watermark("4")})
    new_state = SyncState(cursor={key: _sha_watermark("5")})
    file_store.put(key, old_state)
    final_path = file_store._state_path(key)
    old_bytes = file_store._fs.cat_file(final_path)
    original_pipe = file_store._fs.pipe_file

    with (
        patch.object(file_store._fs, "pipe_file", wraps=original_pipe) as pipe_file,
        patch.object(file_store._fs, "mv", side_effect=OSError("injected move failure")),
    ):
        with pytest.raises(StateStoreError, match="Failed to publish durable state envelope"):
            file_store.put(key, new_state)

    pipe_file.assert_called_once()
    assert file_store.get(key) == old_state
    assert file_store._fs.cat_file(final_path) == old_bytes
    assert json.loads(old_bytes)["schema_version"] == 1


def test_delete_then_get_none(file_store: FileStateStore) -> None:
    file_store.put("resource-1", SyncState(cursor={"resource-1": '"etag"'}))

    file_store.delete("resource-1")

    assert file_store.get("resource-1") is None


def test_missing_key_returns_none(file_store: FileStateStore) -> None:
    assert file_store.get("never-written") is None


def test_memory_backend_works(memory_store: FileStateStore) -> None:
    state = SyncState(cursor={"resource-memory": '"etag"'})

    memory_store.put("resource-memory", state)

    assert memory_store.get("resource-memory") == state


def test_inmemory_protocol_conformance() -> None:
    assert isinstance(InMemoryStateStore(), StateStore)
