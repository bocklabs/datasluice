"""BatchStream source ownership and data-plane batch-size validation."""

from __future__ import annotations

import importlib
import os
from typing import Any

import pytest

pytest.importorskip("pyarrow")

access_module = importlib.import_module("datasluice.data.access")
batch_stream_module = importlib.import_module("datasluice.data.batch_stream")
if not hasattr(access_module, "_BATCH_LIFECYCLE_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("BatchStream lifecycle implementation pending GREEN phase", allow_module_level=True)

BatchStream = batch_stream_module.BatchStream
DataPlaneResourceReader = access_module.DataPlaneResourceReader


class _CloseSpy:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class _FailingFormatReader:
    def read_batches(self, source: Any, *, batch_size: int = 65536):
        def batches():
            raise RuntimeError("injected prefetch failure")
            yield None

        return batches()


def _local_csv_resource():
    from datasluice.domain import LocalFile, Resource

    return Resource(id="invalid-batch-size", format="CSV", access=LocalFile(path="/tmp/unused.csv"))


def test_source_closed_on_close() -> None:
    import pyarrow as pa

    source = _CloseSpy()
    stream = BatchStream(iter(()), pa.schema([]), closeables=(source,))

    stream.close()
    stream.close()

    assert source.close_calls == 1


def test_source_closed_on_construction_failure(monkeypatch) -> None:
    source = _CloseSpy()
    reader = DataPlaneResourceReader()
    monkeypatch.setattr(access_module, "get_reader", lambda _format: _FailingFormatReader())

    with pytest.raises(RuntimeError, match="prefetch"):
        reader._build_batch_stream(_local_csv_resource(), source, 10)

    assert source.close_calls == 1


def test_batch_size_zero_rejected_before_source_acquisition(monkeypatch) -> None:
    reader = DataPlaneResourceReader()
    acquisitions: list[Any] = []

    def fail_open(access: Any):
        acquisitions.append(access)
        raise AssertionError("source must not be acquired")

    monkeypatch.setattr(reader, "_open_local_file", fail_open)

    from datasluice.exceptions import DataSluiceError

    with pytest.raises(DataSluiceError, match="batch_size"):
        reader.open(_local_csv_resource(), batch_size=0)

    assert acquisitions == []


def test_batch_size_negative_rejected_before_source_acquisition(monkeypatch) -> None:
    reader = DataPlaneResourceReader()
    acquisitions: list[Any] = []

    def fail_open(access: Any):
        acquisitions.append(access)
        raise AssertionError("source must not be acquired")

    monkeypatch.setattr(reader, "_open_local_file", fail_open)

    from datasluice.exceptions import DataSluiceError

    with pytest.raises(DataSluiceError, match="batch_size"):
        reader.open(_local_csv_resource(), batch_size=-1)

    assert acquisitions == []


def test_indexed_iter_batches_with_cursors_respects_closed_stream() -> None:
    """iter_batches_with_cursors on an indexed stream raises StreamClosedError after close."""
    from datasluice.exceptions import StreamClosedError

    indexed_source = iter([(0, "batch-a"), (1, "batch-b")])
    stream = BatchStream(indexed_source, schema=None, indexed=True)
    stream.close()
    with pytest.raises(StreamClosedError):
        list(stream.iter_batches_with_cursors())


def test_close_attempts_every_owned_closeable_even_on_failure() -> None:
    """A failing closeable does not prevent later closeables from being closed."""

    class _FailingCloseable:
        def close(self) -> None:
            raise RuntimeError("injected close failure")

    source = _CloseSpy()
    failing = _FailingCloseable()
    later = _CloseSpy()
    stream = BatchStream(source, schema=None, closeables=(failing, later))
    with pytest.raises(RuntimeError, match="injected close failure"):
        stream.close()
    # The source AND the later closeable must still have been closed even
    # though the failing closeable raised mid-cleanup.
    assert source.close_calls == 1
    assert later.close_calls == 1
