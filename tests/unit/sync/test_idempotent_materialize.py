"""Idempotent Parquet and raw materialization behavior."""

from __future__ import annotations

import hashlib
import importlib
import os
from typing import Any
from unittest.mock import patch

import pytest

from datasluice.data import DataPlaneResourceReader
from datasluice.domain import LocalFile, Resource
from datasluice.io.filesystem import open_filesystem
from datasluice.sync import sync_resources
from datasluice.sync.state_store import InMemoryStateStore
from datasluice.transport.httpx_transport import HttpxTransport
from tests.unit.sync.conftest import CSV_BYTES, write_counting_fs

materialize_module = importlib.import_module("datasluice.sync.materialize")
materialize: Any = materialize_module.materialize
if not hasattr(materialize_module, "_IDEMPOTENT_MATERIALIZE_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("idempotent materialize implementation pending GREEN phase", allow_module_level=True)


def _sync(tmp_path, resource, state_store, transport):
    return list(
        sync_resources(
            [resource],
            state_store=state_store,
            reader=DataPlaneResourceReader(transport=transport),
            destination_uri=f"file://{tmp_path}/dest",
            transport=transport,
        )
    )


def test_two_passes_zero_writes_pass2(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    server, url = csv_server()
    resource = make_resource(url)
    transport = HttpxTransport()
    destination = f"file://{tmp_path}/dest"
    counting_fs = write_counting_fs(open_filesystem(destination))

    with patch("datasluice.io.filesystem.open_filesystem", return_value=counting_fs):
        first = _sync(tmp_path, resource, inmemory_state, transport)
        uri, _media_type, _size, checksum = first[0].record
        first_bytes = counting_fs.cat_file(uri)
        writes_after_first = counting_fs.pipe_file_count
        second = _sync(tmp_path, resource, inmemory_state, transport)

    assert first[0].action == "materialized"
    assert second[0].action == "skipped-unchanged"
    assert counting_fs.pipe_file_count == writes_after_first
    assert counting_fs.cat_file(uri) == first_bytes
    assert second[0].record[3] == checksum
    assert server.captured_paths == ["/data.csv", "/data.csv"]


def test_changed_content_rewrites(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    server, url = csv_server()
    resource = make_resource(url)
    transport = HttpxTransport()
    destination = f"file://{tmp_path}/dest"
    counting_fs = write_counting_fs(open_filesystem(destination))

    with patch("datasluice.io.filesystem.open_filesystem", return_value=counting_fs):
        first = _sync(tmp_path, resource, inmemory_state, transport)
        writes_after_first = counting_fs.pipe_file_count
        server.responses["/data.csv"].body = b"id,name\n1,A\n2,changed\n"
        second = _sync(tmp_path, resource, inmemory_state, transport)

    assert first[0].record[3] != second[0].record[3]
    assert second[0].action == "materialized"
    assert counting_fs.pipe_file_count == writes_after_first + 1


def test_two_passes_zero_writes_checkpointed_local_parquet(tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    source_path = tmp_path / "source.parquet"
    pq.write_table(pa.table({"id": [1, 2], "name": ["A", "B"]}), source_path)
    resource = Resource(
        id="checkpointed-local",
        name="checkpointed-local",
        format="PARQUET",
        access=LocalFile(path=str(source_path)),
    )
    destination = f"file://{tmp_path}/dest"
    store = InMemoryStateStore()
    counting_fs = write_counting_fs(open_filesystem(destination))

    with patch("datasluice.io.filesystem.open_filesystem", return_value=counting_fs):
        first = list(
            sync_resources(
                [resource],
                state_store=store,
                reader=DataPlaneResourceReader(),
                destination_uri=destination,
            )
        )
        writes_after_first = counting_fs.pipe_file_count
        second = list(
            sync_resources(
                [resource],
                state_store=store,
                reader=DataPlaneResourceReader(),
                destination_uri=destination,
            )
        )

    assert first[0].action == "materialized"
    assert second[0].action == "skipped-unchanged"
    assert counting_fs.pipe_file_count == writes_after_first
    assert second[0].record == first[0].record


class _RawReader:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read_bytes(self, resource: Any) -> bytes:
        return self.data


def test_raw_passthrough() -> None:
    raw = b"\x00raw\nbytes\xff"
    resource = Resource(id="raw-resource", media_type="application/octet-stream")
    destination = "memory://materialize/raw"
    counting_fs = write_counting_fs(open_filesystem(destination))
    reader = _RawReader(raw)

    with patch("datasluice.io.filesystem.open_filesystem", return_value=counting_fs):
        first = materialize(resource, reader=reader, destination_uri=destination, mode="raw")
        writes_after_first = counting_fs.pipe_file_count
        second = materialize(
            resource,
            reader=reader,
            destination_uri=destination,
            mode="raw",
            stored_checksum=first[3],
        )

    assert counting_fs.cat_file(first[0]) == raw
    assert first[3] == hashlib.sha256(raw).hexdigest()
    assert second == first
    assert counting_fs.pipe_file_count == writes_after_first


def test_destination_uri_is_uri_not_path(tmp_path, csv_server, make_resource) -> None:
    _server, url = csv_server(body=CSV_BYTES)
    resource = make_resource(url)
    transport = HttpxTransport()

    record = materialize(
        resource,
        reader=DataPlaneResourceReader(transport=transport),
        destination_uri=f"file://{tmp_path}/dest",
    )

    assert record[0].startswith("file://")


def test_empty_parquet_resource_syncs_with_schema(tmp_path) -> None:
    """A schema-bearing zero-row Parquet resource still syncs under checkpointed materialization (CR-08)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.sync.state_store import FileStateStore

    schema = pa.schema([("group_id", pa.int64()), ("value", pa.string())])
    empty_table = pa.table({"group_id": pa.array([], type=pa.int64()), "value": pa.array([], type=pa.string())})
    buf = pa.BufferOutputStream()
    pq.write_table(empty_table, buf)
    parquet_path = tmp_path / "empty.parquet"
    parquet_path.write_bytes(buf.getvalue().to_pybytes())

    resource = Resource(
        id="empty-resource",
        name="empty-resource",
        format="PARQUET",
        access=LocalFile(path=str(parquet_path)),
    )
    destination = f"file://{tmp_path}/dest"
    store = FileStateStore(f"file://{tmp_path}/state")

    outcomes = list(
        sync_resources(
            [resource],
            state_store=store,
            reader=DataPlaneResourceReader(),
            destination_uri=destination,
        )
    )

    assert len(outcomes) == 1
    assert outcomes[0].action == "materialized"
    record = outcomes[0].record
    final_uri, _media_type, size, _checksum = record
    # The published zero-row Parquet must retain the source schema.
    with open(final_uri.replace("file://", ""), "rb") as published:
        published_table = pq.read_table(published)
    assert published_table.num_rows == 0
    assert published_table.schema == schema
