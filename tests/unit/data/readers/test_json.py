"""Unit tests for the streaming JSON reader.

Follows the -01 RED->GREEN TDD pattern: the module skips cleanly at
collection time while the readers package is not yet importable, then runs and
passes once GREEN lands the real implementation.
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
    _json_mod = importlib.import_module("datasluice.data.readers.json")
except ImportError:
    pytest.skip("datasluice.data.readers not importable", allow_module_level=True)

from datasluice.data._byte_source import IterableBytesIO  # noqa: E402

READERS = _readers_mod.READERS
get_reader = _readers_mod.get_reader
BaseFormatReader = _base_mod.BaseFormatReader
JSONReader = _json_mod.JSONReader


def test_read_batches_json_array() -> None:
    src = io.BytesIO(b'[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]')
    reader = JSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 2
    assert table.schema.field("id").type == pa.int64()
    assert table.column("name").to_pylist() == ["a", "b"]


def test_read_batches_jsonl() -> None:
    src = io.BytesIO(b'{"id": 1, "name": "a"}\n{"id": 2, "name": "b"}\n')
    reader = JSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 2
    assert table.column("name").to_pylist() == ["a", "b"]


def test_read_batches_works_on_non_seekable_source() -> None:
    chunks = [b'{"id": 1, "name": "a"}\n', b'{"id": 2, "name": "b"}\n']
    src = IterableBytesIO(chunks)
    assert src.seekable() is False
    reader = JSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 2


def test_json_reader_in_registry_aliases() -> None:
    assert READERS["JSON"] is JSONReader
    assert READERS["JSONL"] is JSONReader
    assert READERS["NDJSON"] is JSONReader
    reader = get_reader("jsonl")
    assert isinstance(reader, JSONReader)
    assert isinstance(reader, BaseFormatReader)


def test_read_batches_accepts_batch_size_kwarg() -> None:
    lines = b"".join(b'{"i": %d}\n' % i for i in range(20))
    src = io.BytesIO(lines)
    reader = JSONReader()
    batches = list(reader.read_batches(src, batch_size=8))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 20
    assert len(batches) > 1


def test_json_skips_utf8_bom() -> None:
    payload = b"\xef\xbb\xbf" + b'[{"id": 1}, {"id": 2}]'
    src = io.BytesIO(payload)
    reader = JSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 2


def test_json_array_with_non_dict_elements_does_not_crash() -> None:
    payload = b'[{"id": 1}, "not-a-dict", {"id": 2}]'
    src = io.BytesIO(payload)
    reader = JSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows <= 2


def test_jsonl_empty_input_yields_no_batches() -> None:
    src = io.BytesIO(b"")
    reader = JSONReader()
    batches = list(reader.read_batches(src))
    assert batches == []
