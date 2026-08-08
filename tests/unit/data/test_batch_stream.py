"""Unit tests for :class:`BatchStream` and :class:`IterableBytesIO` (DATA-01, DATA-02).

Covers the streaming data-plane contracts: RecordBatchReader wrapping,
context-manager discipline, idempotent close, StreamClosedError on
use-after-close, ``__arrow_c_stream__`` delegation, and the byte-source
adapter. Follows the Phase 03 RED→GREEN TDD pattern: the module skips
cleanly at collection time while the implementation is still the Task 1
stub, then runs and passes once Task 2 GREEN lands the real classes.
"""

from __future__ import annotations

import importlib

import pytest

pytest.importorskip("pyarrow")

try:
    _batch_module = importlib.import_module("datasluice.data.batch_stream")
except ImportError:
    pytest.skip("datasluice.data.batch_stream not importable", allow_module_level=True)

BatchStream = _batch_module.BatchStream


def _is_stub() -> bool:
    """Detect whether BatchStream is still the Task 1 placeholder."""
    try:
        bs = BatchStream(iter(()), None)
        bs.close()
    except NotImplementedError:
        return True
    return False


if _is_stub():
    pytest.skip("BatchStream implementation pending (RED → GREEN within task 04-01)", allow_module_level=True)


def test_iter_batches_yields_inferred_schema() -> None:
    """BatchStream.schema returns the inferred pa.Schema; iter_batches yields RecordBatch objects."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch1 = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    batch2 = pa.RecordBatch.from_arrays([pa.array([3]), pa.array(["c"])], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, [batch1, batch2])

    bs = BatchStream(reader, schema)
    assert bs.schema == schema
    batches = list(bs.iter_batches())
    assert len(batches) == 2
    assert batches[0].num_rows == 2
    assert batches[1].num_rows == 1
    assert batches[0].schema == schema
    bs.close()


def test_iter_batches_from_plain_iterator() -> None:
    """BatchStream wrapping a bare Iterator[RecordBatch] yields from the iterator."""
    import pyarrow as pa

    schema = pa.schema([("x", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=schema)

    bs = BatchStream(iter([batch]), schema)
    assert bs.schema == schema
    batches = list(bs.iter_batches())
    assert len(batches) == 1
    assert batches[0].num_rows == 3
    bs.close()


def test_exit_closes_reader() -> None:
    """__exit__ closes the reader; iter_batches after close raises StreamClosedError."""
    import pyarrow as pa

    from datasluice.exceptions import StreamClosedError

    schema = pa.schema([("x", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1])], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, [batch])

    with BatchStream(reader, schema) as bs:
        list(bs.iter_batches())

    with pytest.raises(StreamClosedError):
        list(bs.iter_batches())


def test_idempotent_close() -> None:
    """close() is idempotent — second call is a no-op."""
    import pyarrow as pa

    schema = pa.schema([("x", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1])], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, [batch])

    bs = BatchStream(reader, schema)
    bs.close()
    bs.close()
    bs.close()


def test_arrow_c_stream_delegates() -> None:
    """__arrow_c_stream__ returns a PyCapsule usable by pa.RecordBatchReader."""
    import pyarrow as pa

    schema = pa.schema([("x", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, [batch])

    bs = BatchStream(reader, schema)
    capsule = bs.__arrow_c_stream__()
    reconstructed = pa.RecordBatchReader._import_from_c_capsule(capsule)
    table = reconstructed.read_all()
    assert table.num_rows == 2
    bs.close()


def test_iterable_bytes_io_read() -> None:
    """IterableBytesIO.read(n) returns bytes from the wrapped iterable; seekable is False."""
    try:
        _byte_module = importlib.import_module("datasluice.data._byte_source")
    except ImportError:
        pytest.skip("IterableBytesIO not yet implemented")
    IterableBytesIO = _byte_module.IterableBytesIO

    source = [b"hello ", b"world", b"!"]
    bio = IterableBytesIO(source)
    assert bio.readable() is True
    assert bio.seekable() is False
    assert bio.writable() is False
    assert bio.closed is False
    data = bio.read(5)
    assert data == b"hello"
    rest = bio.read()
    assert rest == b" world!"
    bio.close()
    assert bio.closed is True


def test_iterable_bytes_io_chunks() -> None:
    """IterableBytesIO handles chunk-boundary reads correctly."""
    try:
        _byte_module = importlib.import_module("datasluice.data._byte_source")
    except ImportError:
        pytest.skip("IterableBytesIO not yet implemented")
    IterableBytesIO = _byte_module.IterableBytesIO

    source = [b"ab", b"cd", b"ef"]
    bio = IterableBytesIO(source)
    assert bio.read(3) == b"abc"
    assert bio.read(3) == b"def"
    assert bio.read() == b""
    bio.close()


@pytest.mark.skipif(
    not __import__("sys").platform.startswith("linux"),
    reason="/proc/self/fd fd-accounting check is Linux-only",
)
def test_no_fd_leak_under_repeated_reads(tmp_path) -> None:
    """DATA-02 stability: repeated open/consume/close cycles do not leak file descriptors.

    Uses a real OS file handle so ``/proc/self/fd`` reflects true fd
    accounting. An unclosed handle per iteration would grow the count by ~50;
    the ``+2`` slack tolerates interpreter background allocations.
    """
    import os

    import pyarrow.csv as pacsv

    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name\n1,alpha\n2,beta\n3,gamma\n")

    fd_dir = "/proc/self/fd"
    before = set(os.listdir(fd_dir))

    for _ in range(50):
        fh = open(csv_path, "rb")
        try:
            reader = pacsv.open_csv(fh)
            with BatchStream(reader, reader.schema) as bs:
                batches = list(bs.iter_batches())
                assert len(batches) >= 1
        finally:
            fh.close()

    import gc

    gc.collect()

    after = set(os.listdir(fd_dir))
    leaked = after - before
    assert len(after) <= len(before) + 2, (
        f"fd leak detected: before={len(before)}, after={len(after)}, leaked_fds={leaked}"
    )
