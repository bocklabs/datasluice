"""Opened-resource lifecycle and fluent terminal contracts."""

from __future__ import annotations

import os
from typing import Any

import pytest

from datasluice import DataSluice, OpenedResourceConsumedError, Resource
from datasluice.domain import HttpDownload

if os.environ.get("DATASLUICE_TDD_RED") == "1":
    pytest.skip("opened-resource lifecycle implementation pending GREEN phase", allow_module_level=True)


class _Stream:
    def __init__(
        self, batches: tuple[object, ...] = ("first", "second"), close_error: BaseException | None = None
    ) -> None:
        self._batches = batches
        self._close_error = close_error
        self.close_calls = 0

    def iter_batches(self):
        yield from self._batches

    def close(self) -> None:
        self.close_calls += 1
        if self._close_error is not None:
            raise self._close_error


class _Reader:
    def __init__(self, *streams: _Stream) -> None:
        self._streams = list(streams)
        self.opened: list[Resource] = []

    def open(self, resource: Resource) -> _Stream:
        self.opened.append(resource)
        return self._streams.pop(0)


class _Session:
    _transport = object()
    plugins = object()


class _Pipeline:
    def __init__(self, transformed: _Stream) -> None:
        self._transformed = transformed
        self.runs: list[_Stream] = []

    def run(self, stream: _Stream) -> _Stream:
        self.runs.append(stream)
        return self._transformed


class _FailingPipeline:
    def run(self, stream: _Stream) -> _Stream:
        raise RuntimeError("transform failed")


def _resource() -> Resource:
    url = "https://data.example.test/observations.csv"
    return Resource(id="observations", url=url, format="CSV", access=HttpDownload(url=url))


def _opened(reader: _Reader):
    data_sluice = DataSluice(session=_Session(), reader=reader)
    return data_sluice.open(_resource())


def test_manual_iteration_is_lazy_context_bound_and_single_use() -> None:
    """Manual batch iteration opens once inside a context and is consumed on exit."""
    raw = _Stream()
    reader = _Reader(raw)
    opened = _opened(reader)

    assert reader.opened == []
    with pytest.raises(OpenedResourceConsumedError, match="context"):
        list(opened)
    assert reader.opened == []

    with opened as stream:
        assert next(iter(stream)) == "first"

    assert reader.opened == [_resource()]
    assert raw.close_calls == 1
    with pytest.raises(OpenedResourceConsumedError):
        opened.to_arrow()
    assert reader.opened == [_resource()]


def test_transform_is_lazy_and_closes_transformed_and_raw_streams() -> None:
    """Fluent transform attaches without opening and releases both streams after iteration."""
    raw = _Stream()
    transformed = _Stream(("transformed",))
    reader = _Reader(raw)
    opened = _opened(reader)
    pipeline = _Pipeline(transformed)

    assert opened.transform(pipeline) is opened
    assert reader.opened == []
    assert pipeline.runs == []

    with opened as stream:
        assert list(stream) == ["transformed"]

    assert reader.opened == [_resource()]
    assert pipeline.runs == [raw]
    assert transformed.close_calls == raw.close_calls == 1


def test_transform_failure_closes_the_raw_stream_and_consumes_the_wrapper() -> None:
    """A pipeline error never leaves its opened source reusable or unclosed."""
    raw = _Stream()
    opened = _opened(_Reader(raw)).transform(_FailingPipeline())

    with pytest.raises(RuntimeError, match="transform failed"):
        opened.to_arrow()

    assert raw.close_calls == 1
    with pytest.raises(OpenedResourceConsumedError):
        opened.to_arrow()


def test_terminal_failures_close_the_raw_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each terminal consumes once and closes when its integration fails."""
    raw = _Stream()
    opened = _opened(_Reader(raw))

    def _fail(stream: Any, **kwargs: Any) -> None:
        raise RuntimeError("terminal failed")

    monkeypatch.setattr("datasluice.integrations.arrow.to_arrow", _fail)
    with pytest.raises(RuntimeError, match="terminal failed"):
        opened.to_arrow()

    assert raw.close_calls == 1
    with pytest.raises(OpenedResourceConsumedError):
        opened.to_arrow()


def test_cleanup_attempts_both_streams_and_retains_first_close_failure() -> None:
    """Transformed cleanup failure cannot prevent raw-stream cleanup."""
    raw = _Stream(close_error=RuntimeError("raw close failed"))
    transformed = _Stream(("batch",), close_error=RuntimeError("transformed close failed"))
    opened = _opened(_Reader(raw)).transform(_Pipeline(transformed))

    with pytest.raises(RuntimeError, match="transformed close failed"):
        with opened as stream:
            assert list(stream) == ["batch"]

    assert transformed.close_calls == raw.close_calls == 1


def test_materialize_closes_raw_and_transformed_streams_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Materialization uses the same cleanup path as every terminal."""
    raw = _Stream()
    transformed = _Stream(("batch",))
    opened = _opened(_Reader(raw)).transform(_Pipeline(transformed))

    def _fail(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("materialize failed")

    monkeypatch.setattr("datasluice.application.materialize", _fail)
    with pytest.raises(RuntimeError, match="materialize failed"):
        opened.materialize("memory://artifacts")

    assert transformed.close_calls == raw.close_calls == 1
    with pytest.raises(OpenedResourceConsumedError):
        opened.materialize("memory://artifacts")


def test_all_terminal_methods_close_their_streams(monkeypatch: pytest.MonkeyPatch) -> None:
    """Arrow, pandas, Polars, and DuckDB terminals share finalization behavior."""
    terminals = (
        ("arrow", "to_arrow"),
        ("pandas", "to_pandas"),
        ("polars", "to_polars"),
        ("duckdb", "to_duckdb"),
    )

    for module, terminal in terminals:
        raw = _Stream()
        opened = _opened(_Reader(raw))
        monkeypatch.setattr(f"datasluice.integrations.{module}.to_{module}", lambda stream, **kwargs: object())

        getattr(opened, terminal)()

        assert raw.close_calls == 1
