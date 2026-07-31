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
from datasluice.sync.state_store import FileStateStore


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
