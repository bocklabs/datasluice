"""Optimistic CAS coverage for concurrent FileStateStore writers (D-P7-27/34)."""

from __future__ import annotations

from pathlib import Path

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
