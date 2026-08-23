"""End-to-end contracts for bounded and streaming ``datasluice open`` output."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest
import typer
from typer.testing import CliRunner

from datasluice.domain import HttpDownload, Resource

if importlib.util.find_spec("datasluice.cli.open") is None:
    if os.environ.get("DATASLUICE_TDD_RED") == "1":
        pytest.fail("streaming open CLI contracts pending GREEN phase", pytrace=False)
    pytest.skip("streaming open CLI contracts pending GREEN phase", allow_module_level=True)

open_command = cast(Any, importlib.import_module("datasluice.cli.open"))
open_app = typer.Typer()
open_app.command()(open_command.open)
runner = CliRunner()


class _Opened:
    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        self._batches = batches
        self.closed = False
        self.yielded_rows = 0

    def __enter__(self) -> _Opened:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def iter_batches(self):
        for batch in self._batches:
            self.yielded_rows += batch.num_rows
            yield batch

    def close(self) -> None:
        self.closed = True


class _Facade:
    def __init__(self, opened: _Opened) -> None:
        self._opened = opened
        self.resolved: list[object] = []
        self.opened: list[Resource] = []

    def __enter__(self) -> _Facade:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def resolve(self, locator: object) -> Resource:
        self.resolved.append(locator)
        url = "https://data.example.test/observations.csv"
        return Resource(id="observations", url=url, format="CSV", access=HttpDownload(url=url))

    def open(self, resource: Resource) -> _Opened:
        self.opened.append(resource)
        return self._opened


def _batches(row_count: int) -> list[pa.RecordBatch]:
    return [pa.RecordBatch.from_pylist([{"id": index, "name": f"item-{index}"}]) for index in range(row_count)]


def _patch_facade(monkeypatch: pytest.MonkeyPatch, facade: _Facade) -> None:
    monkeypatch.setattr(open_command, "open_data_sluice", lambda: facade)


def test_default_preview_reads_and_renders_no_more_than_twenty_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default JSON preview stops after exactly 20 rows and closes the stream."""
    opened = _Opened(_batches(25))
    facade = _Facade(opened)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(open_app, ["https://data.example.test/observations.csv", "--output", "json"])

    assert outcome.exit_code == 0, outcome.output
    result = json.loads(outcome.stdout)
    assert len(result["rows"]) == 20
    assert opened.yielded_rows == 20
    assert opened.closed
    assert "Opening resource" in outcome.stderr


def test_all_rejects_non_jsonl_output_before_resource_acquisition(monkeypatch: pytest.MonkeyPatch) -> None:
    """Whole-resource traversal requires the explicit incremental JSONL mode."""
    opened = _Opened(_batches(1))
    facade = _Facade(opened)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(open_app, ["https://data.example.test/observations.csv", "--all"])

    assert outcome.exit_code == 1
    assert outcome.stdout == ""
    assert "--all requires --output jsonl" in outcome.stderr
    assert facade.resolved == []
    assert facade.opened == []


def test_all_jsonl_emits_each_row_without_collecting(monkeypatch: pytest.MonkeyPatch) -> None:
    """The JSONL path writes every row as batches arrive and preserves diagnostics."""
    opened = _Opened(_batches(25))
    facade = _Facade(opened)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(
        open_app,
        ["https://data.example.test/observations.csv", "--all", "--output", "jsonl"],
    )

    assert outcome.exit_code == 0, outcome.output
    assert [json.loads(line) for line in outcome.stdout.splitlines()] == [
        {"id": index, "name": f"item-{index}"} for index in range(25)
    ]
    assert opened.yielded_rows == 25
    assert opened.closed
    assert "Opening resource" in outcome.stderr
    source = Path(open_command.__file__).read_text()
    assert "to_arrow" not in source
    assert "to_pandas" not in source
    assert "render_jsonl_rows(_iter_rows" in source


_SUBPROCESS_CODE = """
import contextlib
import os
import resource
import sys

import pyarrow as pa

import datasluice.cli.open as open_command
from datasluice.domain import HttpDownload, Resource


class Opened:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def iter_batches(self):
        payload = "x" * 2048
        for start in range(0, 75000, 500):
            yield pa.RecordBatch.from_pylist(
                [{"id": value, "payload": payload} for value in range(start, start + 500)]
            )


class Facade:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def resolve(self, locator):
        url = "https://data.example.test/large.csv"
        return Resource(id="large", url=url, format="CSV", access=HttpDownload(url=url))

    def open(self, resource):
        return Opened()


open_command.open_data_sluice = Facade
with open(os.devnull, "w") as sink:
    with contextlib.redirect_stdout(sink):
        open_command.open("https://data.example.test/large.csv", all_rows=True, output="jsonl")
peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_rss_kb = peak_rss // 1024 if sys.platform == "darwin" else peak_rss
print(f"peak_rss_kb={peak_rss_kb}")
"""


def test_all_jsonl_peak_memory_stays_bounded() -> None:
    """Large streamed JSONL output does not retain every emitted row in process memory."""
    result = subprocess.run(
        [sys.executable, "-c", _SUBPROCESS_CODE],
        capture_output=True,
        text=True,
        check=True,
        timeout=90,
    )
    peak_line = next(line for line in result.stdout.splitlines() if line.startswith("peak_rss_kb="))
    peak_rss_kb = int(peak_line.partition("=")[2])

    assert peak_rss_kb < 400_000
