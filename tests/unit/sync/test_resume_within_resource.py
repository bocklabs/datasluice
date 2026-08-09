"""Within-resource checkpoint and production Parquet continuation behavior."""

from __future__ import annotations

import hashlib
import importlib
import os
from typing import Any
from unittest.mock import patch

import pytest

from datasluice.data import DataPlaneResourceReader
from datasluice.domain import LocalFile, Resource
from datasluice.exceptions import DataSluiceError
from datasluice.io.filesystem import open_filesystem
from datasluice.sync import sync_resources
from datasluice.sync._identity import canonical_destination_identity, canonical_identity
from datasluice.sync.state_store import InMemoryStateStore
from tests.unit.sync.conftest import FaultInjectingStateStore

batch_stream_module = importlib.import_module("datasluice.data.batch_stream")
parquet_module = importlib.import_module("datasluice.data.readers.parquet")
sync_module = importlib.import_module("datasluice.sync.sync")
if not hasattr(sync_module, "_WITHIN_RESOURCE_RESUME_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("within-resource resume implementation pending GREEN phase", allow_module_level=True)
if not hasattr(sync_module, "_FAILURE_BOUNDARY_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("checkpoint failure-boundary hardening pending GREEN phase", allow_module_level=True)


def _checkpoint(next_batch_index: int) -> dict[str, Any]:
    return {
        "version": 1,
        "status": "in_progress",
        "next_batch_index": next_batch_index,
        "position": {
            "kind": "parquet_row_group",
            "row_group_index": next_batch_index,
        },
    }


def _checkpoint_v2(next_batch_index: int, row_group_index: int, source_version: str | None = None) -> dict[str, Any]:
    return {
        "version": 2,
        "status": "in_progress",
        "next_batch_index": next_batch_index,
        "position": {
            "kind": "parquet_row_group",
            "row_group_index": row_group_index,
        },
        "source_version": source_version,
    }


def _checkpoint_v3(
    next_batch_index: int,
    row_group_index: int,
    source_version: str | None,
    destination_uri: str,
) -> dict[str, Any]:
    return {
        "version": 3,
        "status": "in_progress",
        "next_batch_index": next_batch_index,
        "position": {
            "kind": "parquet_row_group",
            "row_group_index": row_group_index,
        },
        "source_version": source_version,
        "destination_identity": canonical_destination_identity(destination_uri),
    }


_EMPTY_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
_ARTIFACT_HEALTH_READY = hasattr(sync_module, "_ARTIFACT_HEALTH_READY")
_SKIP_ARTIFACT_HEALTH = not _ARTIFACT_HEALTH_READY and os.environ.get("DATASLUICE_TDD_RED") != "1"


def _file_sha256(path: str) -> str:
    with open(path, "rb") as source:
        return hashlib.sha256(source.read()).hexdigest()


def _parquet_resource(tmp_path) -> tuple[Resource, list[int]]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "four-groups.parquet"
    schema = pa.schema([("group_id", pa.int64()), ("value", pa.string())])
    expected = []
    with pq.ParquetWriter(path, schema) as writer:
        for group_id in range(4):
            expected.append(group_id)
            writer.write_table(pa.table({"group_id": [group_id], "value": [f"value-{group_id}"]}, schema=schema))
    return (
        Resource(
            id="four-groups",
            name="four-groups",
            format="PARQUET",
            media_type="application/x-parquet",
            access=LocalFile(path=str(path)),
        ),
        expected,
    )


class _CursorAwareReader:
    def __init__(self, *, fail_at: int | None = 2) -> None:
        import pyarrow as pa

        self._schema = pa.schema([("group_id", pa.int64())])
        self._batches = [pa.record_batch({"group_id": [index]}, schema=self._schema) for index in range(4)]
        self.requested: list[int] = []
        self.fail_at = fail_at
        self.fail_once = True

    def _stream(self, start: int) -> Any:
        batch_stream_type: Any = batch_stream_module.BatchStream

        def batches():
            for index in range(start, len(self._batches)):
                self.requested.append(index)
                if index == self.fail_at and self.fail_once:
                    self.fail_once = False
                    raise RuntimeError("injected batch failure")
                yield self._batches[index]

        return batch_stream_type(batches(), self._schema, start_batch_index=start)

    def open(self, resource: Any, *, batch_size: int = 65536) -> Any:
        return self._stream(0)

    def open_from_cursor(self, resource: Any, cursor: Any, *, batch_size: int = 65536) -> Any:
        return self._stream(cursor.next_batch_index)


def test_interrupt_within_one_resource_resumes_without_refetching_completed_batches(tmp_path) -> None:
    import pyarrow.parquet as pq

    resource = Resource(id="cursor-aware", name="cursor-aware", format="PARQUET", access=LocalFile(path="/dev/null"))
    store = InMemoryStateStore()
    reader = _CursorAwareReader()
    destination = f"file://{tmp_path}/dest"

    with pytest.raises(RuntimeError, match="injected batch failure"):
        list(
            sync_resources(
                [resource],
                state_store=store,
                reader=reader,
                destination_uri=destination,
            )
        )

    interrupted = store.get(canonical_identity(resource))
    assert interrupted is not None
    assert interrupted.cursor == {}
    assert interrupted.extra == {"datasluice_checkpoint": _checkpoint_v3(2, 2, _EMPTY_SHA, destination)}
    assert reader.requested == [0, 1, 2]

    reader.requested.clear()
    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=reader,
            destination_uri=destination,
            resume=True,
        )
    )

    assert reader.requested == [2, 3]
    assert [outcome.action for outcome in outcomes] == ["resumed"]
    record = outcomes[0].record
    assert record is not None
    final = store.get(canonical_identity(resource))
    assert final is not None
    assert "datasluice_checkpoint" not in final.extra
    assert len(final.cursor[canonical_identity(resource)]) == 64
    assert pq.read_table(record.uri).column("group_id").to_pylist() == [0, 1, 2, 3]


def test_dataplane_parquet_resume_does_not_request_completed_row_groups(tmp_path) -> None:
    import pyarrow.parquet as pq

    resource, expected = _parquet_resource(tmp_path)
    store = InMemoryStateStore()
    reader = DataPlaneResourceReader()
    destination = f"file://{tmp_path}/dest"
    requested: list[int] = []
    original = parquet_module.ParquetReader.__dict__["_read_row_group"]
    fail_once = True

    def recording_read(self: Any, parquet_file: Any, row_group_index: int) -> Any:
        nonlocal fail_once
        requested.append(row_group_index)
        batch = original(self, parquet_file, row_group_index)
        if row_group_index == 2 and fail_once:
            fail_once = False
            raise RuntimeError("injected row-group failure")
        return batch

    with patch.object(parquet_module.ParquetReader, "_read_row_group", recording_read):
        with pytest.raises(RuntimeError, match="injected row-group failure"):
            list(
                sync_resources(
                    [resource],
                    state_store=store,
                    reader=reader,
                    destination_uri=destination,
                )
            )

        assert requested == [0, 1, 2]
        interrupted = store.get(canonical_identity(resource))
        assert interrupted is not None
        assert isinstance(resource.access, LocalFile)
        assert interrupted.extra == {
            "datasluice_checkpoint": _checkpoint_v3(2, 2, _file_sha256(resource.access.path), destination)
        }

        requested.clear()
        outcomes = list(
            sync_resources(
                [resource],
                state_store=store,
                reader=reader,
                destination_uri=destination,
                resume=True,
            )
        )

    assert requested == [2, 3]
    assert outcomes[0].action == "resumed"
    record = outcomes[0].record
    assert record is not None
    assert pq.read_table(record.uri).column("group_id").to_pylist() == expected
    completed = store.get(canonical_identity(resource))
    assert completed is not None
    assert "datasluice_checkpoint" not in completed.extra
    assert len(completed.cursor[canonical_identity(resource)]) == 64


def test_resume_reader_without_continuation_fails_before_batch_zero_access(tmp_path) -> None:
    from datasluice.domain import SyncState

    class IncapableReader:
        def __init__(self) -> None:
            self.open_count = 0

        def open(self, resource: Any, *, batch_size: int = 65536) -> Any:
            self.open_count += 1
            raise AssertionError("batch zero must not be accessed")

    resource = Resource(id="incapable", name="incapable", format="PARQUET", access=LocalFile(path="/dev/null"))
    store = InMemoryStateStore()
    store.put(
        canonical_identity(resource),
        SyncState(extra={"datasluice_checkpoint": _checkpoint(2)}),
    )
    reader = IncapableReader()

    with pytest.raises(DataSluiceError, match="continuation.*incapable.*row group 2"):
        list(
            sync_resources(
                [resource],
                state_store=store,
                reader=reader,
                destination_uri=f"file://{tmp_path}/dest",
                resume=True,
            )
        )

    assert reader.open_count == 0


class _NoOverwriteFS:
    def __init__(self, fs: Any) -> None:
        self.fs = fs

    def mv(self, source: str, target: str) -> None:
        if self.fs.exists(target):
            raise FileExistsError(target)
        self.fs.mv(source, target)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.fs, name)


class _FailBeforeSelectedMoveFS(_NoOverwriteFS):
    def __init__(self, fs: Any, target_suffix: str) -> None:
        super().__init__(fs)
        self.target_suffix = target_suffix
        self.failed = False

    def mv(self, source: str, target: str) -> None:
        if target.endswith(self.target_suffix) and not self.failed:
            self.failed = True
            raise OSError("injected pre-shard-move failure")
        super().mv(source, target)


def _partial_uri(destination: str, identity: str) -> str:
    return f"{destination}/.datasluice-partial/{identity}"


def test_failure_before_shard_move_does_not_advance_checkpoint(tmp_path) -> None:
    resource = Resource(id="pre-move", name="pre-move", format="PARQUET", access=LocalFile(path="/dev/null"))
    reader = _CursorAwareReader(fail_at=None)
    store = InMemoryStateStore()
    destination = f"file://{tmp_path}/pre-move"
    fs = _FailBeforeSelectedMoveFS(open_filesystem(destination), "00000000000000000001.parquet")

    with patch("datasluice.io.filesystem.open_filesystem", return_value=fs):
        with pytest.raises(DataSluiceError, match="injected pre-shard-move failure"):
            list(
                sync_resources(
                    [resource],
                    state_store=store,
                    reader=reader,
                    destination_uri=destination,
                )
            )

    state = store.get(canonical_identity(resource))
    assert state is not None
    assert state.extra == {"datasluice_checkpoint": _checkpoint_v3(1, 1, _EMPTY_SHA, destination)}
    partial = _partial_uri(destination, canonical_identity(resource))
    assert fs.exists(f"{partial}/00000000000000000000.parquet")
    assert not fs.exists(f"{partial}/00000000000000000001.parquet")


def test_failure_after_shard_move_before_checkpoint_replaces_stale_shard(tmp_path) -> None:
    resource = Resource(id="post-move", name="post-move", format="PARQUET", access=LocalFile(path="/dev/null"))
    reader = _CursorAwareReader(fail_at=None)
    inner_store = InMemoryStateStore()
    crashing_store = FaultInjectingStateStore(inner_store, raise_on_put=1)
    destination = f"file://{tmp_path}/post-move"
    fs = _NoOverwriteFS(open_filesystem(destination))

    with patch("datasluice.io.filesystem.open_filesystem", return_value=fs):
        with pytest.raises(RuntimeError, match="injected crash"):
            list(
                sync_resources(
                    [resource],
                    state_store=crashing_store,
                    reader=reader,
                    destination_uri=destination,
                )
            )

        partial = _partial_uri(destination, canonical_identity(resource))
        assert inner_store.get(canonical_identity(resource)) is None
        assert fs.exists(f"{partial}/00000000000000000000.parquet")
        reader.requested.clear()
        outcomes = list(
            sync_resources(
                [resource],
                state_store=inner_store,
                reader=reader,
                destination_uri=destination,
            )
        )

    assert reader.requested == [0, 1, 2, 3]
    assert outcomes[0].action == "materialized"
    assert not fs.exists(partial)


def test_failure_after_checkpoint_put_resumes_at_following_batch(tmp_path) -> None:
    resource = Resource(
        id="post-checkpoint",
        name="post-checkpoint",
        format="PARQUET",
        access=LocalFile(path="/dev/null"),
    )
    reader = _CursorAwareReader()
    store = InMemoryStateStore()
    destination = f"file://{tmp_path}/post-checkpoint"

    with pytest.raises(RuntimeError, match="injected batch failure"):
        list(
            sync_resources(
                [resource],
                state_store=store,
                reader=reader,
                destination_uri=destination,
            )
        )

    interrupted = store.get(canonical_identity(resource))
    assert interrupted is not None
    assert interrupted.extra == {"datasluice_checkpoint": _checkpoint_v3(2, 2, _EMPTY_SHA, destination)}
    reader.requested.clear()
    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=reader,
            destination_uri=destination,
            resume=True,
        )
    )

    assert reader.requested == [2, 3]
    assert outcomes[0].action == "resumed"


def test_missing_checkpoint_referenced_shard_fails_as_corrupt_state(tmp_path) -> None:
    import pyarrow as pa

    from datasluice.domain import SyncState
    from datasluice.sync.materialize import _publish_batch_shard

    resource = Resource(id="missing-shard", name="missing-shard", format="PARQUET", access=LocalFile(path="/dev/null"))
    reader = _CursorAwareReader(fail_at=None)
    store = InMemoryStateStore()
    store.put(canonical_identity(resource), SyncState(extra={"datasluice_checkpoint": _checkpoint(2)}))
    destination = f"file://{tmp_path}/missing-shard"
    fs = open_filesystem(destination)
    partial = _partial_uri(destination, canonical_identity(resource))
    fs.makedirs(partial, exist_ok=True)
    _publish_batch_shard(
        fs,
        f"{partial}/00000000000000000000.parquet",
        pa.record_batch({"group_id": [0]}),
    )

    with pytest.raises(DataSluiceError, match="completed shard 1 is missing"):
        list(
            sync_resources(
                [resource],
                state_store=store,
                reader=reader,
                destination_uri=destination,
                resume=True,
            )
        )

    assert reader.requested == []


def _parquet_resource_three_groups(tmp_path) -> tuple[Any, list[int]]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "three-groups.parquet"
    schema = pa.schema([("group_id", pa.int64()), ("value", pa.string())])
    with pq.ParquetWriter(path, schema) as writer:
        writer.write_table(pa.table({"group_id": [0, 1], "value": ["a", "b"]}, schema=schema))
        writer.write_table(pa.table({"group_id": [], "value": []}, schema=schema))
        writer.write_table(pa.table({"group_id": [2, 3], "value": ["c", "d"]}, schema=schema))
    return (
        Resource(
            id="three-groups",
            name="three-groups",
            format="PARQUET",
            media_type="application/x-parquet",
            access=LocalFile(path=str(path)),
        ),
        [0, 1, 2, 3],
    )


def test_empty_row_group_resume_correct(tmp_path) -> None:
    import pyarrow.parquet as pq

    resource, expected = _parquet_resource_three_groups(tmp_path)
    inner_store = InMemoryStateStore()
    reader = DataPlaneResourceReader()
    destination = f"file://{tmp_path}/empty-middle"
    requested: list[int] = []
    fail_once = True

    def recording_read(self: Any, parquet_file: Any, row_group_index: int) -> Any:
        nonlocal fail_once
        import pyarrow as pa

        requested.append(row_group_index)
        if row_group_index == 1 and fail_once:
            fail_once = False
            raise RuntimeError("injected crash")
        table = parquet_file.read_row_group(row_group_index).combine_chunks()
        batches = table.to_batches(max_chunksize=max(table.num_rows, 1))
        if batches:
            batch = batches[0]
        else:
            batch = pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in table.schema],
                schema=table.schema,
            )
        if row_group_index == 1:
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in batch.schema],
                schema=batch.schema,
            )
        return batch

    with patch.object(parquet_module.ParquetReader, "_read_row_group", recording_read):
        with pytest.raises(RuntimeError, match="injected crash"):
            list(
                sync_resources(
                    [resource],
                    state_store=inner_store,
                    reader=reader,
                    destination_uri=destination,
                )
            )

        interrupted = inner_store.get(canonical_identity(resource))
        assert interrupted is not None
        checkpoint = interrupted.extra["datasluice_checkpoint"]
        assert checkpoint["next_batch_index"] == 1
        assert checkpoint["position"]["row_group_index"] == 1

        requested.clear()
        outcomes = list(
            sync_resources(
                [resource],
                state_store=inner_store,
                reader=reader,
                destination_uri=destination,
                resume=True,
            )
        )

    assert requested == [1, 2]
    assert outcomes[0].action == "resumed"
    record = outcomes[0].record
    assert record is not None
    assert pq.read_table(record.uri).column("group_id").to_pylist() == expected


def test_source_replacement_detected_and_restarted(tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.domain import SyncState

    path = tmp_path / "replaceable.parquet"
    schema = pa.schema([("group_id", pa.int64()), ("value", pa.string())])
    with pq.ParquetWriter(path, schema) as writer:
        writer.write_table(pa.table({"group_id": [0, 1], "value": ["a", "b"]}, schema=schema))
        writer.write_table(pa.table({"group_id": [2, 3], "value": ["c", "d"]}, schema=schema))

    resource = Resource(
        id="replaceable",
        name="replaceable",
        format="PARQUET",
        media_type="application/x-parquet",
        access=LocalFile(path=str(path)),
    )
    reader = DataPlaneResourceReader()
    destination = f"file://{tmp_path}/replaced"

    inner_store = InMemoryStateStore()
    crashing_store = FaultInjectingStateStore(inner_store, raise_on_put=2)
    with pytest.raises(RuntimeError, match="injected crash"):
        list(
            sync_resources(
                [resource],
                state_store=crashing_store,
                reader=reader,
                destination_uri=destination,
            )
        )

    interrupted = inner_store.get(canonical_identity(resource))
    assert interrupted is not None
    assert "datasluice_checkpoint" in interrupted.extra

    with pq.ParquetWriter(path, schema) as writer:
        writer.write_table(pa.table({"group_id": [100, 101], "value": ["x", "y"]}, schema=schema))
        writer.write_table(pa.table({"group_id": [102, 103], "value": ["z", "w"]}, schema=schema))

    store = InMemoryStateStore()
    store.put(canonical_identity(resource), SyncState(extra=dict(interrupted.extra)))
    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=reader,
            destination_uri=destination,
            resume=True,
        )
    )

    assert outcomes[0].action == "materialized"
    record = outcomes[0].record
    assert record is not None
    result = pq.read_table(record.uri).column("group_id").to_pylist()
    assert result == [100, 101, 102, 103]
    assert 0 not in result


def test_source_change_during_read_aborts_to_avoid_mixed_artifact(tmp_path) -> None:
    """A source change between initial hash and post-materialize verification aborts."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "shifting.parquet"
    schema = pa.schema([("group_id", pa.int64()), ("value", pa.string())])
    with pq.ParquetWriter(path, schema) as writer:
        writer.write_table(pa.table({"group_id": [0, 1], "value": ["a", "b"]}, schema=schema))
        writer.write_table(pa.table({"group_id": [2, 3], "value": ["c", "d"]}, schema=schema))

    resource = Resource(
        id="shifting",
        name="shifting",
        format="PARQUET",
        media_type="application/x-parquet",
        access=LocalFile(path=str(path)),
    )
    destination = f"file://{tmp_path}/shifted"

    real_compute = sync_module._compute_source_version
    call_count = {"n": 0}

    def shifting_compute(resource):
        call_count["n"] += 1
        result = real_compute(resource)
        # On the post-materialize verification call (the 2nd hash for this
        # resource pass), rewrite the source file so its bytes differ from
        # the pre-read hash. This simulates a source change during the read.
        if call_count["n"] == 2:
            with pq.ParquetWriter(path, schema) as writer:
                writer.write_table(pa.table({"group_id": [100, 101], "value": ["x", "y"]}, schema=schema))
                writer.write_table(pa.table({"group_id": [102, 103], "value": ["z", "w"]}, schema=schema))
            return real_compute(resource)
        return result

    store = InMemoryStateStore()
    with patch.object(sync_module, "_compute_source_version", side_effect=shifting_compute):
        with pytest.raises(DataSluiceError, match="changed during sync"):
            list(
                sync_resources(
                    [resource],
                    state_store=store,
                    reader=DataPlaneResourceReader(),
                    destination_uri=destination,
                )
            )

    # The completed state MUST NOT have been written for the mixed artifact.
    state = store.get(canonical_identity(resource))
    if state is not None and "datasluice_completed_artifact" in state.extra:
        pytest.fail("completed state was written despite the mid-sync source change")


def test_destination_replacement_detected_and_restarted(tmp_path) -> None:
    resource = Resource(
        id="destination-replacement",
        name="destination-replacement",
        format="PARQUET",
        access=LocalFile(path="/dev/null"),
    )
    reader = _CursorAwareReader()
    store = InMemoryStateStore()
    first_destination = f"file://{tmp_path}/first"
    second_destination = f"file://{tmp_path}/second"

    with pytest.raises(RuntimeError, match="injected batch failure"):
        list(
            sync_resources(
                [resource],
                state_store=store,
                reader=reader,
                destination_uri=first_destination,
            )
        )

    checkpoint = store.get(canonical_identity(resource))
    assert checkpoint is not None
    assert checkpoint.extra["datasluice_checkpoint"]["destination_identity"] == canonical_destination_identity(
        first_destination
    )

    reader.requested.clear()
    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=reader,
            destination_uri=second_destination,
            resume=True,
        )
    )

    assert reader.requested == [0, 1, 2, 3]
    assert outcomes[0].action == "materialized"


def test_http_parquet_not_checkpointed(tmp_path, csv_server, make_resource) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.transport.httpx_transport import HttpxTransport

    schema = pa.schema([("group_id", pa.int64())])
    sink = pa.BufferOutputStream()
    pq.write_table(pa.table({"group_id": [1, 2, 3]}, schema=schema), sink)
    parquet_bytes = sink.getvalue().to_pybytes()

    server, url = csv_server(body=parquet_bytes)
    resource = make_resource(url, format="PARQUET", resource_id="http-parquet")
    transport = HttpxTransport()
    store = InMemoryStateStore()
    reader = DataPlaneResourceReader(transport=transport)
    destination = f"file://{tmp_path}/http-parquet"

    with patch("datasluice.sync.materialize.materialize_checkpointed") as mock_mc:
        mock_mc.side_effect = AssertionError("HTTP Parquet must not use checkpointed materialize")
        outcomes = list(
            sync_resources(
                [resource],
                state_store=store,
                reader=reader,
                destination_uri=destination,
                transport=transport,
            )
        )

    assert outcomes[0].action == "materialized"
    state = store.get(canonical_identity(resource))
    assert state is not None
    assert "datasluice_checkpoint" not in state.extra


@pytest.mark.skipif(_SKIP_ARTIFACT_HEALTH, reason="publication ordering implementation pending GREEN phase")
def test_publication_failure_leaves_artifact_and_prior_checkpoint_recoverable(tmp_path) -> None:
    import pyarrow.parquet as pq

    resource = Resource(
        id="publication-failure",
        name="publication-failure",
        format="PARQUET",
        access=LocalFile(path="/dev/null"),
    )
    inner_store = InMemoryStateStore()
    crashing_store = FaultInjectingStateStore(inner_store, raise_on_put=5)
    reader = _CursorAwareReader(fail_at=None)
    destination = f"file://{tmp_path}/publication-failure"
    fs = open_filesystem(destination)
    identity = canonical_identity(resource)
    final_uri = f"{destination}/{identity}.parquet"
    partial_uri = _partial_uri(destination, identity)

    with pytest.raises(RuntimeError, match="injected crash"):
        list(
            sync_resources(
                [resource],
                state_store=crashing_store,
                reader=reader,
                destination_uri=destination,
            )
        )

    checkpoint = inner_store.get(identity)
    assert checkpoint is not None
    assert "datasluice_checkpoint" in checkpoint.extra
    assert fs.exists(final_uri)
    assert fs.exists(f"{partial_uri}/00000000000000000000.parquet")

    outcomes = list(
        sync_resources(
            [resource],
            state_store=inner_store,
            reader=reader,
            destination_uri=destination,
            resume=True,
        )
    )

    assert outcomes[0].action == "resumed"
    record = outcomes[0].record
    assert record is not None
    assert pq.read_table(record.uri).column("group_id").to_pylist() == [0, 1, 2, 3]
    assert not fs.exists(partial_uri)
