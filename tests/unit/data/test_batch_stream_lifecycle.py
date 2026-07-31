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
