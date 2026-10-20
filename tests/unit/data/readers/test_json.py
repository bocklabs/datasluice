from __future__ import annotations

import importlib
import io

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa

try:
    _readers_mod = importlib.import_module("datasluice.data.readers")
    _base_mod = importlib.import_module("datasluice.data.readers.base")
    _json_mod = importlib.import_module("datasluice.data.readers.json")
except ImportError:
    pytest.skip("datasluice.data.readers not importable", allow_module_level=True)

from datasluice.data._byte_source import IterableBytesIO
from datasluice.exceptions import FormatError

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


def test_json_array_rejects_non_object_elements() -> None:
    src = io.BytesIO(b'[{"id": 1}, "not-a-dict", {"id": 2}]')
    with pytest.raises(FormatError, match="elements must be objects"):
        list(JSONReader().read_batches(src))


def test_json_array_has_a_document_size_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_base_mod, "MAX_DOCUMENT_BYTES", 16)
    with pytest.raises(FormatError, match="limited"):
        list(JSONReader().read_batches(io.BytesIO(b'[{"name":"too much data"}]')))


def test_jsonl_yields_before_reading_the_entire_source() -> None:
    class BoundedReads(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            assert size is not None
            assert size > 0
            return super().read(size)

    payload = b'{"id": 1}\n' * 5000000
    source = BoundedReads(payload)
    batches = JSONReader().read_batches(source)
    assert next(batches).num_rows > 0
    assert source.tell() < len(payload)
    batches.close()


def test_jsonl_empty_input_yields_no_batches() -> None:
    src = io.BytesIO(b"")
    reader = JSONReader()
    batches = list(reader.read_batches(src))
    assert batches == []


def test_jsonl_reads_a_record_larger_than_64_kib() -> None:
    payload = b'{"id": 1, "desc": "' + b"x" * 200_000 + b'"}\n'
    batches = list(JSONReader().read_batches(io.BytesIO(payload)))
    assert len(batches) == 1
    assert batches[0].num_rows == 1
    assert set(batches[0].column("desc").to_pylist()) == {"x" * 200_000}
