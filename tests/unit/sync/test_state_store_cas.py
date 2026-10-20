from __future__ import annotations

import multiprocessing
import threading
from typing import TYPE_CHECKING, Any, cast

import pytest

from datasluice.domain import Artifact, SyncState
from datasluice.exceptions import SyncStateConflictError
from datasluice.sync.state_store import _UNSET, FileStateStore

if TYPE_CHECKING:
    from pathlib import Path


def test_cas_loser_raises(tmp_path: Path) -> None:
    base_uri = f"file://{tmp_path}/state"
    store_a = FileStateStore(base_uri)
    store_b = FileStateStore(base_uri)
    key = "resource-1"
    expected_prior = store_a.read_raw(key)

    assert expected_prior is None
    winner = SyncState(cursor={"resource-1": '"etag-b"'})
    store_b.put(key, winner)

    state = SyncState(cursor={"resource-1": '"etag-a"'})
    with pytest.raises(SyncStateConflictError):
        store_a.put(key, state, expected_prior=expected_prior)

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


def test_conditional_put_with_correct_prior_succeeds(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"
    first = SyncState(cursor={"resource-1": '"etag-1"'})
    store.put(key, first)

    expected_prior = store.read_version(key)
    assert expected_prior is not None

    second = SyncState(cursor={"resource-1": '"etag-2"'})

    committed_version = store.conditional_put(key, second, expected_prior)
    assert committed_version is not None
    assert committed_version == store.read_version(key)

    assert store.get(key) == second


def test_get_with_version_returns_one_read_pair(tmp_path: Path) -> None:
    """get_with_version decodes state and version from a single backend read."""
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"
    state = SyncState(cursor={"resource-1": '"etag-1"'})
    store.put(key, state)

    decoded, version = store.get_with_version(key)
    assert decoded is not None
    assert decoded == state
    assert version == store.read_raw(key)


def test_get_with_version_absent_returns_none_pair(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    assert store.get_with_version("missing") == (None, None)


def test_conditional_put_chain_uses_returned_version(tmp_path: Path) -> None:
    """Chained conditional_put calls pass the returned version as the next expected_prior."""
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    v0 = store.conditional_put(key, SyncState(cursor={key: '"a"'}), None)
    v1 = store.conditional_put(key, SyncState(cursor={key: '"b"'}), v0)
    v2 = store.conditional_put(key, SyncState(cursor={key: '"c"'}), v1)
    assert v0 != v1 != v2

    assert store.read_version(key) == v2


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

    state = SyncState(cursor={"resource-1": '"loser"'})
    with pytest.raises(SyncStateConflictError):
        store.conditional_put(key, state, stale_prior)

    # Prior state is the interloper's, unchanged by the losing write.
    assert store.get(key) == interloper


def test_barrier_synchronized_dual_writer_loser_raises(tmp_path: Path) -> None:
    """Two threads past the version check before either commits: exactly one wins."""
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    barrier = threading.Barrier(2)
    results: dict[str, Any] = {"exceptions": [], "returns": []}

    def writer(value: str) -> None:
        try:
            barrier.wait(timeout=5)
            store.conditional_put(key, SyncState(cursor={key: f'"{value}"'}), expected_prior=None)
        except SyncStateConflictError as exc:
            results["exceptions"].append((value, exc))
        else:
            results["returns"].append(value)

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
        self.get_with_version_calls: list[str] = []

    def key_lock(self, key: str) -> Any:
        return self._inner.key_lock(key)

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

    def get_with_version(self, key: str) -> tuple[Any, bytes | None]:
        self.get_with_version_calls.append(key)
        return self._inner.get_with_version(key)

    def conditional_put(self, key: str, state: Any, expected_prior: bytes | None) -> bytes:
        self.conditional_put_calls.append((key, expected_prior))
        return self._inner.conditional_put(key, state, expected_prior)


def test_sync_uses_cas_for_checkpoint_write(tmp_path) -> None:
    """Checkpointed local Parquet writes use conditional_put for both in-progress and completed state.

    The previous version served an HTTP Parquet body, which the production guard deliberately excludes from checkpointed
    materialization — so its observed conditional write was only the completed-state write, not the intermediate per-
    batch checkpoint writes the test name claims to cover. Use local Parquet with multiple row groups so
    materialize_checkpointed actually runs and emits in-progress checkpoints before the final write.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.data import DataPlaneResourceReader
    from datasluice.domain import LocalFile, Resource
    from datasluice.sync import sync_resources
    from datasluice.sync._identity import canonical_identity

    schema = pa.schema([("group_id", pa.int64()), ("value", pa.string())])
    parquet_path = tmp_path / "multi_rowgroup.parquet"
    with pq.ParquetWriter(parquet_path, schema) as writer:
        writer.write_table(pa.table({"group_id": [0, 1], "value": ["a", "b"]}, schema=schema))
        writer.write_table(pa.table({"group_id": [2, 3], "value": ["c", "d"]}, schema=schema))

    resource = Resource(
        id="multi-rowgroup",
        name="multi-rowgroup",
        format="PARQUET",
        access=LocalFile(path=str(parquet_path)),
    )

    store = _ConditionalPutSpy(FileStateStore(f"file://{tmp_path}/state"))

    list(
        sync_resources(
            [resource],
            state_store=store,
            reader=DataPlaneResourceReader(),
            destination_uri=f"file://{tmp_path}/dest",
        )
    )

    state_key = canonical_identity(resource)
    checkpoint_writes = [ep for _k, ep in store.conditional_put_calls if _k == state_key]
    assert checkpoint_writes, "sync must use conditional_put for checkpoint writes"
    assert all(ep is None or isinstance(ep, bytes) for ep in checkpoint_writes), (
        "checkpoint conditional_put must carry an expected_prior"
    )
    # Multiple row groups mean at least one in-progress checkpoint write
    # precedes the final completed-state write (: the previous test
    # observed only the completed write because HTTP Parquet is not
    # checkpointed).
    assert len(checkpoint_writes) >= 2, "expected at least one in-progress checkpoint write plus one completed write"


def test_sync_uses_cas_for_completed_write(tmp_path, csv_server, make_resource) -> None:
    from datasluice.data import DataPlaneResourceReader
    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
    from datasluice.sync import sync_resources
    from datasluice.sync._identity import canonical_identity

    _server, url = csv_server()
    resource = make_resource(url, format="CSV")

    store = _ConditionalPutSpy(FileStateStore(f"file://{tmp_path}/state"))

    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=DataPlaneResourceReader(transport=HttpxCatalogTransport(), allow_private_http=True),
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
    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
    from datasluice.sync import sync_resources
    from datasluice.sync._identity import canonical_identity
    from datasluice.sync.state_store import InMemoryStateStore

    _server, url = csv_server()
    resource = make_resource(url, format="CSV")

    store = InMemoryStateStore()
    assert not hasattr(store, "conditional_put")

    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=DataPlaneResourceReader(transport=HttpxCatalogTransport(), allow_private_http=True),
            destination_uri=f"file://{tmp_path}/dest",
        )
    )

    state_key = canonical_identity(resource)
    assert outcomes[0].action in ("materialized", "skipped-unchanged")
    assert store.get(state_key) is not None


def test_per_key_lock_released_after_conditional_put(tmp_path) -> None:
    from datasluice.sync import state_store as module

    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"
    store.conditional_put(key, SyncState(cursor={key: '"etag"'}), None)

    scope = store._lock_scope(key)
    with module._GLOBAL_LOCKS_GUARD:
        assert scope not in module._GLOBAL_LOCKS
        assert scope not in module._GLOBAL_LOCKS_USERS


def test_cross_instance_per_key_lock_serializes_writers(tmp_path) -> None:
    """Two FileStateStore instances on the same base URI share one per-key lock."""
    base_uri = f"file://{tmp_path}/state"
    store_a = FileStateStore(base_uri)
    store_b = FileStateStore(base_uri)
    key = "resource-1"
    results: dict[str, Any] = {"exceptions": [], "returns": []}
    barrier = threading.Barrier(2)

    def writer(store: FileStateStore, value: str) -> None:
        try:
            barrier.wait(timeout=5)
            store.conditional_put(key, SyncState(cursor={key: f'"{value}"'}), expected_prior=None)
        except SyncStateConflictError as exc:
            results["exceptions"].append((value, exc))
        else:
            results["returns"].append(value)

    thread_a = threading.Thread(target=writer, args=(store_a, "alpha"))
    thread_b = threading.Thread(target=writer, args=(store_b, "beta"))
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    assert len(results["returns"]) == 1, "exactly one of the two store instances must win"
    assert len(results["exceptions"]) == 1, "the other instance must raise SyncStateConflictError"
    winner_value = results["returns"][0]
    loser_value, loser_exc = results["exceptions"][0]
    assert isinstance(loser_exc, SyncStateConflictError)
    assert {winner_value, loser_value} == {"alpha", "beta"}
    final_state = store_a.get(key)
    assert final_state is not None
    assert final_state.cursor == {key: f'"{winner_value}"'}


def test_concurrent_sync_serializes_artifact_and_state(tmp_path, csv_server, make_resource) -> None:
    """Two concurrent sync_resources calls for the same resource leave a consistent state+artifact."""
    import threading

    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.data import DataPlaneResourceReader
    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
    from datasluice.sync import sync_resources
    from datasluice.sync._identity import canonical_identity

    schema = pa.schema([("group_id", pa.int64()), ("value", pa.string())])
    table = pa.table({"group_id": [0, 1], "value": ["a", "b"]}, schema=schema)
    parquet_buf = pa.BufferOutputStream()
    pq.write_table(table, parquet_buf)
    parquet_bytes = parquet_buf.getvalue().to_pybytes()

    _server, url = csv_server(body=parquet_bytes)
    resource = make_resource(url, format="PARQUET")

    store = FileStateStore(f"file://{tmp_path}/state")
    dest = f"file://{tmp_path}/dest"
    errors: list[BaseException] = []
    outcomes: list[Any] = []
    barrier = threading.Barrier(2)

    def run_sync() -> None:
        try:
            barrier.wait(timeout=5)
            local_outcomes = list(
                sync_resources(
                    [resource],
                    state_store=store,
                    reader=DataPlaneResourceReader(transport=HttpxCatalogTransport(), allow_private_http=True),
                    destination_uri=dest,
                )
            )
            outcomes.extend(local_outcomes)
        except BaseException as exc:
            errors.append(exc)

    # Reconfigure resource access to local file so both writers target the same artifact path deterministically.
    thread_a = threading.Thread(target=run_sync)
    thread_b = threading.Thread(target=run_sync)
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    # At least one writer must have succeeded (the other may have raised SyncStateConflictError).
    successful = [o for o in outcomes if o.action in ("materialized", "skipped-unchanged")]
    assert successful, f"expected at least one successful sync outcome; got outcomes={outcomes}, errors={errors}"

    # The committed state MUST describe the artifact that currently lives at final_uri (no corruption).
    state_key = canonical_identity(resource)
    final_state = store.get(state_key)
    assert final_state is not None, "the winning CAS must have committed a state record"
    artifact = final_state.extra["datasluice_completed_artifact"]
    # destination_health re-reads the artifact and compares checksums — this asserts state matches artifact bytes.
    from datasluice.sync.materialize import destination_health

    assert destination_health(resource, Artifact.from_dict(artifact), destination_uri=dest), (
        "state checksum must match the artifact currently published at final_uri"
    )


def _process_cas(base_uri: str, barrier: Any, results: Any, value: str) -> None:
    try:
        store = FileStateStore(base_uri)
        prior = store.read_version("shared")
        barrier.wait(timeout=10)
        try:
            store.conditional_put("shared", SyncState(cursor={"shared": f'"{value}"'}), prior)
            results.put("committed")
        except SyncStateConflictError:
            results.put("conflict")
    except Exception as exc:
        # surface unexpected child failures instead of a parent-side queue.Empty timeout
        results.put(f"error: {exc!r}")


def test_local_cas_coordinates_separate_processes(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    base_uri = f"file://{tmp_path}/process-state"
    FileStateStore(base_uri)
    processes = [context.Process(target=_process_cas, args=(base_uri, barrier, results, value)) for value in ("a", "b")]
    try:
        for process in processes:
            process.start()
        assert sorted(results.get(timeout=15) for _ in processes) == ["committed", "conflict"]
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        results.close()


def test_atomic_sync_refuses_a_store_without_a_transaction_lock() -> None:
    from datasluice.exceptions import DataSluiceError
    from datasluice.sync.sync import _resource_transaction

    class UnlockedStore:
        pass

    store = UnlockedStore()
    with pytest.raises(DataSluiceError, match="key_lock"):
        _resource_transaction(store, "resource", is_atomic=True)


def test_file_state_store_satisfies_the_key_lock_capability() -> None:
    from datasluice.ports import KeyLockStateStore

    assert isinstance(FileStateStore("file://state"), KeyLockStateStore)


def test_per_key_lock_is_released_between_yielded_outcomes(tmp_path, csv_server, make_resource) -> None:
    from datasluice.data import DataPlaneResourceReader
    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
    from datasluice.sync import state_store as module
    from datasluice.sync import sync_resources
    from datasluice.sync._identity import canonical_identity

    _server, url = csv_server()
    resource = make_resource(url, format="CSV")
    store = FileStateStore(f"file://{tmp_path}/state")
    generator = sync_resources(
        [resource],
        state_store=store,
        reader=DataPlaneResourceReader(transport=HttpxCatalogTransport(), allow_private_http=True),
        destination_uri=f"file://{tmp_path}/dest",
    )
    try:
        next(generator)
        scope = store._lock_scope(canonical_identity(resource))
        with module._GLOBAL_LOCKS_GUARD:
            assert scope not in module._GLOBAL_LOCKS
            assert scope not in module._GLOBAL_LOCKS_USERS
    finally:
        cast("Any", generator).close()


def test_process_cas_reports_unexpected_child_failures(tmp_path) -> None:
    """A child-side failure lands on the queue as an error instead of a parent timeout."""

    class _BrokenBarrier:
        def wait(self, timeout: float | None = None) -> None:
            raise RuntimeError("barrier exploded")

    class _Queue:
        def __init__(self) -> None:
            self.items: list[str] = []

        def put(self, item: str) -> None:
            self.items.append(item)

    results = _Queue()
    _process_cas(f"file://{tmp_path}/state", _BrokenBarrier(), results, "a")

    assert results.items == ["error: RuntimeError('barrier exploded')"]
