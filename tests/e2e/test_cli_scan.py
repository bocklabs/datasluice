"""End-to-end contracts for the bounded ``datasluice scan`` workflow."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest
import typer
from typer.testing import CliRunner

from datasluice.domain import Dataset, HttpDownload, Resource
from datasluice.exceptions import DataSluiceError

if importlib.util.find_spec("datasluice.cli.scan") is None:
    if os.environ.get("DATASLUICE_TDD_RED") == "1":
        pytest.fail("bounded scan CLI contracts pending GREEN phase", pytrace=False)
    pytest.skip("bounded scan CLI contracts pending GREEN phase", allow_module_level=True)

scan_command = cast(Any, importlib.import_module("datasluice.cli.scan"))
parse_locator = cast(Any, importlib.import_module("datasluice.cli._resolver")).parse_locator
scan_app = typer.Typer()
scan_app.command()(scan_command.scan)

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


class _CatalogPortal:
    def __init__(self, dataset: Dataset) -> None:
        self._dataset = dataset
        self.dataset_ids: list[str] = []

    def get_dataset(self, dataset_id: str) -> Dataset:
        self.dataset_ids.append(dataset_id)
        return self._dataset


class _Facade:
    def __init__(self, resource: Resource, opened: _Opened, dataset: Dataset | None = None) -> None:
        self._resource = resource
        self._opened = opened
        self._portal = _CatalogPortal(dataset or Dataset(id="dataset", resources=[resource]))
        self.resolved: list[object] = []
        self.opened: list[Resource] = []

    def __enter__(self) -> _Facade:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def portal(self, _url: str) -> _CatalogPortal:
        return self._portal

    def resolve(self, locator: object) -> Resource:
        self.resolved.append(locator)
        return self._resource

    def open(self, resource: Resource) -> _Opened:
        self.opened.append(resource)
        return self._opened


def _resource(resource_id: str = "observations") -> Resource:
    url = "https://data.example.test/observations.csv"
    return Resource(id=resource_id, url=url, format="CSV", access=HttpDownload(url=url))


def _batches(row_count: int, batch_size: int = 125) -> list[pa.RecordBatch]:
    rows = [{"city": f"city-{index}", "value": index if index % 7 else None} for index in range(row_count)]
    table = pa.Table.from_pylist(rows)
    return table.to_batches(max_chunksize=batch_size)


def _patch_facade(monkeypatch: pytest.MonkeyPatch, facade: _Facade) -> None:
    monkeypatch.setattr(scan_command, "DataSluice", lambda: facade)


def _write_fixture(path: Path, suffix: str, row_count: int) -> None:
    table = pa.Table.from_pylist([{"city": f"city-{index}", "value": index} for index in range(row_count)])
    if suffix == ".csv":
        pacsv.write_csv(table, path)
    else:
        pq.write_table(table, path)


def test_parser_round_trips_direct_and_catalog_locators_to_locked_contract() -> None:
    """The shared parser returns the exact schema-v1 locator envelopes."""
    fixture = json.loads(Path("tests/fixtures/contracts/locator-v1.json").read_text())

    direct = parse_locator(
        "https://data.example.test/files/observations.csv?api_key=secret&page=1",
        portal=None,
        dataset=None,
        resource=None,
    )
    catalog = parse_locator(
        None,
        portal="https://catalog.example.test/api",
        dataset="dataset-42",
        resource="resource-7",
    )

    expected_direct = {**fixture["direct"], "format": "CSV", "extensions": {}}
    expected_catalog = {**fixture["catalog"], "extensions": {}}

    assert direct.to_dict() == expected_direct
    assert catalog.to_dict() == expected_catalog
    assert (
        parse_locator(
            None,
            portal="https://catalog.example.test/api",
            dataset="dataset-42",
            resource="https://data.example.test/observations.csv?token=secret",
        )
        .to_dict()["resource_id"]
        .endswith("token=***")
    )


@pytest.mark.parametrize("suffix", [".csv", ".parquet"])
def test_scan_reads_at_most_default_bound_from_local_inputs(tmp_path: Path, suffix: str) -> None:
    """Local CSV and Parquet inputs return a bounded JSON sample by default."""
    source = tmp_path / f"observations{suffix}"
    _write_fixture(source, suffix, 1_005)

    outcome = runner.invoke(scan_app, [str(source), "--output", "json"])

    assert outcome.exit_code == 0, outcome.output
    result = json.loads(outcome.stdout)
    assert result["rows"] == 1_000
    assert result["sample"][0] == {"city": "city-0", "value": 0}
    assert len(result["sample"]) == 20
    assert result["columns"][0]["name"] == "city"
    assert "Scanning resource" in outcome.stderr


def test_scan_default_bound_does_not_process_more_than_one_thousand_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bounded mode slices the final batch and stops at exactly 1,000 rows."""
    opened = _Opened(_batches(1_250))
    facade = _Facade(_resource(), opened)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(scan_app, ["https://data.example.test/observations.csv", "--output", "json"])

    assert outcome.exit_code == 0, outcome.output
    assert json.loads(outcome.stdout)["rows"] == 1_000
    assert opened.yielded_rows == 1_000
    assert opened.closed


def test_scan_full_computes_exact_statistics(monkeypatch: pytest.MonkeyPatch) -> None:
    """The explicit full mode consumes every row while keeping only the sample."""
    opened = _Opened(_batches(1_250))
    facade = _Facade(_resource(), opened)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(
        scan_app,
        ["https://data.example.test/observations.csv", "--full", "--output", "json"],
    )

    assert outcome.exit_code == 0, outcome.output
    result = json.loads(outcome.stdout)
    assert result["rows"] == 1_250
    assert result["columns"][1]["null_count"] == 179
    assert len(result["sample"]) == 20
    assert opened.yielded_rows == 1_250


def test_ambiguous_catalog_reference_lists_sanitized_selectors_before_opening(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catalog ambiguity is actionable and cannot cause a byte read."""
    secret_resource = _resource("https://data.example.test/a.csv?token=secret")
    selected_resource = _resource("observations")
    facade = _Facade(
        selected_resource,
        _Opened(_batches(1)),
        Dataset(id="weather", resources=[secret_resource, selected_resource]),
    )
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(
        scan_app,
        ["--portal", "https://catalog.example.test", "--dataset", "weather", "--output", "json"],
    )

    assert outcome.exit_code == 1
    assert outcome.stdout == ""
    assert "observations" in outcome.stderr
    assert "secret" not in outcome.stderr
    assert facade.opened == []
    assert facade.resolved == []


def test_machine_result_and_error_diagnostics_use_separate_streams(monkeypatch: pytest.MonkeyPatch) -> None:
    """JSON stdout remains parseable while progress and failures use stderr."""
    opened = _Opened(_batches(2))
    facade = _Facade(_resource(), opened)
    _patch_facade(monkeypatch, facade)

    success = runner.invoke(scan_app, ["https://data.example.test/observations.csv", "--output", "json"])
    assert success.exit_code == 0, success.output
    assert json.loads(success.stdout)["rows"] == 2
    assert "Scanning resource" in success.stderr

    def _fail(_locator: object) -> Resource:
        raise DataSluiceError("resource warning")

    monkeypatch.setattr(facade, "resolve", _fail)
    failed = runner.invoke(scan_app, ["https://data.example.test/observations.csv", "--output", "json"])
    assert failed.exit_code == 1
    assert failed.stdout == ""
    assert "resource warning" in failed.stderr
