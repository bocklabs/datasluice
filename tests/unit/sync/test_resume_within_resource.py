"""Within-resource checkpoint and production Parquet continuation behavior."""

from __future__ import annotations

import importlib
import os
from typing import Any
from unittest.mock import patch

import pytest

from datasluice.data import DataPlaneResourceReader
from datasluice.domain import LocalFile, Resource
from datasluice.exceptions import DataSluiceError
from datasluice.sync import sync_resources
from datasluice.sync.state_store import InMemoryStateStore

batch_stream_module = importlib.import_module("datasluice.data.batch_stream")
parquet_module = importlib.import_module("datasluice.data.readers.parquet")
sync_module = importlib.import_module("datasluice.sync.sync")
if not hasattr(sync_module, "_WITHIN_RESOURCE_RESUME_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("within-resource resume implementation pending GREEN phase", allow_module_level=True)


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
    def __init__(self) -> None:
        import pyarrow as pa

        self._schema = pa.schema([("group_id", pa.int64())])
        self._batches = [pa.record_batch({"group_id": [index]}, schema=self._schema) for index in range(4)]
        self.requested: list[int] = []
        self.fail_once = True

    def _stream(self, start: int) -> Any:
        batch_stream_type: Any = batch_stream_module.BatchStream

        def batches():
            for index in range(start, len(self._batches)):
                self.requested.append(index)
                if index == 2 and self.fail_once:
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

    resource = Resource(id="cursor-aware", name="cursor-aware", format="PARQUET")
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

    interrupted = store.get(resource.id)
    assert interrupted is not None
    assert interrupted.cursor == {}
    assert interrupted.extra == {"datasluice_checkpoint": _checkpoint(2)}
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
    assert outcomes[0].record is not None
    final = store.get(resource.id)
    assert final is not None
    assert "datasluice_checkpoint" not in final.extra
    assert len(final.cursor[resource.id]) == 64
    assert pq.read_table(outcomes[0].record[0]).column("group_id").to_pylist() == [0, 1, 2, 3]


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
        interrupted = store.get(resource.id)
        assert interrupted is not None
        assert interrupted.extra == {"datasluice_checkpoint": _checkpoint(2)}

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
    assert outcomes[0].record is not None
    assert pq.read_table(outcomes[0].record[0]).column("group_id").to_pylist() == expected
    completed = store.get(resource.id)
    assert completed is not None
    assert "datasluice_checkpoint" not in completed.extra
    assert len(completed.cursor[resource.id]) == 64


def test_resume_reader_without_continuation_fails_before_batch_zero_access(tmp_path) -> None:
    from datasluice.domain import SyncState

    class IncapableReader:
        def __init__(self) -> None:
            self.open_count = 0

        def open(self, resource: Any, *, batch_size: int = 65536) -> Any:
            self.open_count += 1
            raise AssertionError("batch zero must not be accessed")

    resource = Resource(id="incapable", name="incapable", format="PARQUET")
    store = InMemoryStateStore()
    store.put(
        resource.id,
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
