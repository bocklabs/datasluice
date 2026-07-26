"""Unit tests for the streaming CSV reader (DATA-03, 04-02 Task 1).

Follows the Phase 03/04-01 RED->GREEN TDD pattern: the module skips cleanly at
collection time while the readers package is not yet importable, then runs and
passes once Task 1 GREEN lands the real implementation.
"""

from __future__ import annotations

import importlib
import io

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa  # noqa: E402

try:
    _readers_mod = importlib.import_module("datasluice.data.readers")
    _base_mod = importlib.import_module("datasluice.data.readers.base")
    _csv_mod = importlib.import_module("datasluice.data.readers.csv")
except ImportError:
    pytest.skip("datasluice.data.readers not importable", allow_module_level=True)

from datasluice.data._byte_source import IterableBytesIO  # noqa: E402

READERS = _readers_mod.READERS
get_reader = _readers_mod.get_reader
BaseFormatReader = _base_mod.BaseFormatReader
CSVReader = _csv_mod.CSVReader


def test_read_batches_yields_record_batch_with_inferred_types() -> None:
    src = io.BytesIO(b"id,name\n1,alice\n2,bob\n3,carol\n")
    reader = CSVReader()
    batches = list(reader.read_batches(src))
    assert len(batches) >= 1
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 3
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()
    assert table.column("name").to_pylist()[0] == "alice"


def test_read_batches_respects_custom_delimiter() -> None:
    src = io.BytesIO(b"a;b\n1;2\n")
    reader = CSVReader(delimiter=";")
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 1
    assert "a" in table.schema.names


def test_read_batches_accepts_batch_size_kwarg() -> None:
    payload = b"id\n" + b"".join(b"%d\n" % i for i in range(50))
    src = io.BytesIO(payload)
    reader = CSVReader()
    batches = list(reader.read_batches(src, batch_size=16))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 50


def test_read_batches_works_on_non_seekable_source() -> None:
    chunks = [b"id,name\n", b"1,a\n", b"2,b\n"]
    src = IterableBytesIO(chunks)
    assert src.seekable() is False
    reader = CSVReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 2


def test_csv_reader_in_registry_and_factory() -> None:
    assert READERS["CSV"] is CSVReader
    reader = get_reader("csv")
    assert isinstance(reader, CSVReader)
    assert isinstance(reader, BaseFormatReader)


def test_get_reader_unknown_raises_key_error() -> None:
    with pytest.raises(KeyError):
        get_reader("UNKNOWN_FMT")
