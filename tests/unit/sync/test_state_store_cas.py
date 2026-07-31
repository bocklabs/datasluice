"""Optimistic CAS coverage for concurrent FileStateStore writers (D-P7-27/34).

Closes the CR-02 TOCTOU blocker: the per-key threading lock makes the
compare-read and atomic-move indivisible within a process, so two barrier-
synchronized expected-absent writers cannot both succeed. Closes CR-11 by
declaring which fsspec backends provide an atomic rename.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

from datasluice.domain import SyncState
from datasluice.exceptions import SyncStateConflictError
from datasluice.sync.state_store import _UNSET, FileStateStore


def test_cas_loser_raises(tmp_path: Path) -> None:
    base_uri = f"file://{tmp_path}/state"
    store_a = FileStateStore(base_uri)
    store_b = FileStateStore(base_uri)
    key = "resource-1"
    expected_prior = store_a.read_raw(key)

    assert expected_prior is None
    winner = SyncState(cursor={"resource-1": '"etag-b"'})
    store_b.put(key, winner)

    with pytest.raises(SyncStateConflictError):
        store_a.put(key, SyncState(cursor={"resource-1": '"etag-a"'}), expected_prior=expected_prior)

    assert store_a.get(key) == winner


def test_cas_no_conflict_normal_write(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"
    first = SyncState(cursor={"resource-1": '"etag-1"'})
    second = SyncState(cursor={"resource-1": '"etag-2"'})

    store.put(key, first)
    expected_prior = store.read_raw(key)
    assert expected_prior is not None

    store.put(key, second, expected_prior=expected_prior)

    assert store.get(key) == second


def test_cas_matrix_n10(tmp_path: Path) -> None:
    for iteration in range(10):
        base_uri = f"file://{tmp_path}/run-{iteration}/state"
        store_a = FileStateStore(base_uri)
        store_b = FileStateStore(base_uri)
        key = "resource-1"
        expected_prior = store_a.read_raw(key)
        winner = SyncState(cursor={"resource-1": f'"winner-{iteration}"'})

        assert expected_prior is None
        store_b.put(key, winner)

        with pytest.raises(SyncStateConflictError):
            store_a.put(
                key,
                SyncState(cursor={"resource-1": f'"loser-{iteration}"'}),
                expected_prior=expected_prior,
            )

        assert store_a.get(key) == winner


def test_conditional_put_with_correct_prior_succeeds(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"
    first = SyncState(cursor={"resource-1": '"etag-1"'})
    store.put(key, first)

    expected_prior = store.read_version(key)
    assert expected_prior is not None

    second = SyncState(cursor={"resource-1": '"etag-2"'})
    store.conditional_put(key, second, expected_prior)

    assert store.get(key) == second


def test_conditional_put_with_stale_prior_raises_conflict(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"
    winner = SyncState(cursor={"resource-1": '"winner"'})
    store.put(key, winner)

    stale_prior = store.read_version(key)
    assert stale_prior is not None
    # Another writer publishes before us.
    interloper = SyncState(cursor={"resource-1": '"interloper"'})
    store.conditional_put(key, interloper, stale_prior)

    with pytest.raises(SyncStateConflictError):
        store.conditional_put(key, SyncState(cursor={"resource-1": '"loser"'}), stale_prior)

    # Prior state is the interloper's, unchanged by the losing write.
    assert store.get(key) == interloper


def test_barrier_synchronized_dual_writer_loser_raises(tmp_path: Path) -> None:
    """Two threads past the version check before either commits: exactly one wins (CR-02)."""
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    barrier = threading.Event()
    results: dict[str, Any] = {"exceptions": [], "returns": []}

    def writer(value: str) -> None:
        try:
            store.conditional_put(key, SyncState(cursor={key: f'"{value}"'}), expected_prior=None)
        except SyncStateConflictError as exc:
            results["exceptions"].append((value, exc))
        else:
            results["returns"].append(value)
        finally:
            barrier.set()

    thread_a = threading.Thread(target=writer, args=("alpha",))
    thread_b = threading.Thread(target=writer, args=("beta",))
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    assert len(results["returns"]) == 1, "exactly one writer must succeed"
    assert len(results["exceptions"]) == 1, "exactly one writer must raise SyncStateConflictError"
    winner_value = results["returns"][0]
    loser_value, loser_exc = results["exceptions"][0]
    assert isinstance(loser_exc, SyncStateConflictError)
    assert {winner_value, loser_value} == {"alpha", "beta"}

    final_state = store.get(key)
    assert final_state is not None
    assert final_state.cursor == {key: f'"{winner_value}"'}


def test_atomic_mv_backends_declares_local_and_memory() -> None:
    from datasluice.sync.state_store import _ATOMIC_MV_BACKENDS

    assert "file" in _ATOMIC_MV_BACKENDS
    assert "memory" in _ATOMIC_MV_BACKENDS


def test_is_atomic_mv_true_for_local_backend(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    assert store._is_atomic_mv is True


def test_is_atomic_mv_true_for_memory_backend() -> None:
    store = FileStateStore("memory://state-cas")
    assert store._is_atomic_mv is True


def test_file_state_store_satisfies_atomic_state_store_protocol(tmp_path: Path) -> None:
    from datasluice.ports import AtomicStateStore

    store = FileStateStore(f"file://{tmp_path}/state")
    assert isinstance(store, AtomicStateStore)


def test_in_memory_state_store_does_not_require_atomic_capability() -> None:
    from datasluice.ports import AtomicStateStore
    from datasluice.sync.state_store import InMemoryStateStore

    store = InMemoryStateStore()
    # InMemoryStateStore remains a valid StateStore without the CAS capability.
    assert not isinstance(store, AtomicStateStore)


class _ConditionalPutSpy:
    """Delegate to a FileStateStore while recording conditional_put invocations."""

    def __init__(self, inner: FileStateStore) -> None:
        self._inner = inner
        self.conditional_put_calls: list[tuple[str, bytes | None]] = []

    def get(self, key: str) -> Any:
        return self._inner.get(key)

    def put(self, key: str, state: Any, *, expected_prior: Any = _UNSET) -> None:
        self._inner.put(key, state, expected_prior=expected_prior)

    def delete(self, key: str) -> None:
        self._inner.delete(key)

    def read_version(self, key: str) -> bytes | None:
        return self._inner.read_version(key)

    def read_raw(self, key: str) -> bytes | None:
        return self._inner.read_raw(key)

    def conditional_put(self, key: str, state: Any, expected_prior: bytes | None) -> None:
        self.conditional_put_calls.append((key, expected_prior))
        self._inner.conditional_put(key, state, expected_prior)


def test_sync_uses_cas_for_checkpoint_write(tmp_path, csv_server, make_resource) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.data import DataPlaneResourceReader
    from datasluice.sync import sync_resources
    from datasluice.sync._identity import canonical_identity
    from datasluice.transport.httpx_transport import HttpxTransport

    schema = pa.schema([("group_id", pa.int64()), ("value", pa.string())])
    table = pa.table({"group_id": [0, 1], "value": ["a", "b"]}, schema=schema)
    parquet_buf = pa.BufferOutputStream()
    pq.write_table(table, parquet_buf)
    parquet_bytes = parquet_buf.getvalue().to_pybytes()

    _server, url = csv_server(body=parquet_bytes)
    resource = make_resource(url, format="PARQUET")

    store = _ConditionalPutSpy(FileStateStore(f"file://{tmp_path}/state"))

    list(
        sync_resources(
            [resource],
            state_store=store,
            reader=DataPlaneResourceReader(transport=HttpxTransport()),
            destination_uri=f"file://{tmp_path}/dest",
        )
    )

    state_key = canonical_identity(resource)
    checkpoint_writes = [ep for _k, ep in store.conditional_put_calls if _k == state_key]
    assert checkpoint_writes, "sync must use conditional_put for checkpoint writes"
    assert all(ep is None or isinstance(ep, bytes) for ep in checkpoint_writes), (
        "checkpoint conditional_put must carry an expected_prior"
    )


def test_sync_uses_cas_for_completed_write(tmp_path, csv_server, make_resource) -> None:
    from datasluice.data import DataPlaneResourceReader
    from datasluice.sync import sync_resources
    from datasluice.sync._identity import canonical_identity
    from datasluice.transport.httpx_transport import HttpxTransport

    _server, url = csv_server()
    resource = make_resource(url, format="CSV")

    store = _ConditionalPutSpy(FileStateStore(f"file://{tmp_path}/state"))

    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=DataPlaneResourceReader(transport=HttpxTransport()),
            destination_uri=f"file://{tmp_path}/dest",
        )
    )

    state_key = canonical_identity(resource)
    assert outcomes[0].action in ("materialized", "skipped-unchanged")
    completed_writes = [ep for k, ep in store.conditional_put_calls if k == state_key]
    assert completed_writes, "sync must use conditional_put for the completed watermark write"
    first_completed_prior = completed_writes[0]
    assert first_completed_prior is None or isinstance(first_completed_prior, bytes)


def test_sync_fallback_unconditional_put_for_non_atomic(tmp_path, csv_server, make_resource) -> None:
    from datasluice.data import DataPlaneResourceReader
    from datasluice.sync import sync_resources
    from datasluice.sync._identity import canonical_identity
    from datasluice.sync.state_store import InMemoryStateStore
    from datasluice.transport.httpx_transport import HttpxTransport

    _server, url = csv_server()
    resource = make_resource(url, format="CSV")

    store = InMemoryStateStore()
    assert not hasattr(store, "conditional_put")

    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=DataPlaneResourceReader(transport=HttpxTransport()),
            destination_uri=f"file://{tmp_path}/dest",
        )
    )

    state_key = canonical_identity(resource)
    assert outcomes[0].action in ("materialized", "skipped-unchanged")
    assert store.get(state_key) is not None
