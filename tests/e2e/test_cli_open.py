from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import typer
from typer.testing import CliRunner

from tests.e2e._facades import Facade as _Facade
from tests.e2e._facades import Opened as _Opened

if TYPE_CHECKING:
    import pytest

_RSS_PROBE = Path(__file__).resolve().parent / "open_rss_probe.py"

open_command = cast("Any", importlib.import_module("datasluice.cli.open"))
open_app = typer.Typer()
open_app.command()(open_command.open)
runner = CliRunner()


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


def test_all_jsonl_renders_rows_lazily_without_collecting(monkeypatch: pytest.MonkeyPatch) -> None:
    """The JSONL path hands render_jsonl_rows a lazy iterator, never a materialized list."""
    opened = _Opened(_batches(25))
    facade = _Facade(opened)
    _patch_facade(monkeypatch, facade)
    received: list[object] = []

    def _capture(rows: object) -> None:
        received.append(rows)

    monkeypatch.setattr(open_command, "render_jsonl_rows", _capture)
    outcome = runner.invoke(open_app, ["https://data.example.test/observations.csv", "--all", "--output", "jsonl"])

    assert outcome.exit_code == 0, outcome.output
    assert len(received) == 1
    assert not isinstance(received[0], (list, tuple)), "JSONL path collected all rows before rendering"


def test_all_jsonl_peak_memory_stays_bounded() -> None:
    """Large streamed JSONL output does not retain every emitted row in process memory."""
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src")}
    result = subprocess.run(
        [sys.executable, str(_RSS_PROBE)],
        capture_output=True,
        text=True,
        check=True,
        timeout=90,
        env=environment,
    )
    peak_line = next(line for line in result.stdout.splitlines() if line.startswith("peak_rss_kb="))
    peak_rss_kb = int(peak_line.partition("=")[2])

    assert peak_rss_kb < 400_000
