"""End-to-end contracts for the one-resource ``datasluice materialize`` workflow."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import os
import re
from pathlib import Path
from typing import Any, cast

import pytest
from typer.testing import CliRunner

from datasluice.domain import Dataset, HttpDownload, Resource

if importlib.util.find_spec("datasluice.cli.materialize") is None:
    if os.environ.get("DATASLUICE_TDD_RED") == "1":
        pytest.fail("materialize CLI contracts pending GREEN phase", pytrace=False)
    pytest.skip("materialize CLI contracts pending GREEN phase", allow_module_level=True)

materialize_command = cast(Any, importlib.import_module("datasluice.cli.materialize"))
app = cast(Any, importlib.import_module("datasluice.cli.app")).app
Artifact = cast(Any, importlib.import_module("datasluice.domain")).Artifact

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    """Strip ANSI escape codes so substring checks survive Rich styling."""
    return _ANSI_RE.sub("", text)


class _Portal:
    def __init__(self, dataset: Dataset) -> None:
        self._dataset = dataset

    def get_dataset(self, _dataset_id: str) -> Dataset:
        return self._dataset


class _Facade:
    def __init__(self, artifact: Any, dataset: Dataset | None = None) -> None:
        self._artifact = artifact
        self._resource = Resource(
            id="observations",
            url="https://data.example.test/observations.csv",
            format="CSV",
            access=HttpDownload(url="https://data.example.test/observations.csv"),
        )
        self._portal = _Portal(dataset or Dataset(id="weather", resources=[self._resource]))
        self.resolved: list[object] = []
        self.materialized: list[tuple[Resource, str, str]] = []

    def __enter__(self) -> _Facade:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def portal(self, _url: str) -> _Portal:
        return self._portal

    def resolve(self, locator: object) -> Resource:
        self.resolved.append(locator)
        return self._resource

    def materialize(self, resource: Resource, destination: str, *, mode: str) -> Any:
        self.materialized.append((resource, destination, mode))
        return self._artifact


def _artifact() -> Any:
    return Artifact.from_dict(json.loads(Path("tests/fixtures/contracts/artifact-v1.json").read_text()))


def _patch_facade(monkeypatch: pytest.MonkeyPatch, facade: _Facade) -> None:
    monkeypatch.setattr(materialize_command, "DataSluice", lambda: facade)


def test_default_materialize_emits_the_canonical_artifact_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default Parquet materialization delegates one resolved resource to the facade."""
    artifact = _artifact()
    facade = _Facade(artifact)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(
        app,
        [
            "materialize",
            "https://data.example.test/observations.csv",
            "--destination",
            "memory://contract-output",
            "--output",
            "json",
        ],
    )

    assert outcome.exit_code == 0, outcome.output
    assert json.loads(outcome.stdout) == artifact.to_dict()
    assert facade.materialized == [(facade._resource, "memory://contract-output", "parquet")]
    assert "Materializing resource" in outcome.stderr


def test_raw_mode_is_explicit_and_uses_the_same_artifact_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raw preservation is available only through the explicit materialization mode."""
    facade = _Facade(_artifact())
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(
        app,
        [
            "materialize",
            "https://data.example.test/observations.csv",
            "--destination",
            "memory://contract-output",
            "--mode",
            "raw",
            "--output",
            "json",
        ],
    )

    assert outcome.exit_code == 0, outcome.output
    assert json.loads(outcome.stdout) == facade._artifact.to_dict()
    assert facade.materialized == [(facade._resource, "memory://contract-output", "raw")]


def test_materialize_rejects_missing_destination_invalid_mode_and_secret_input(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invalid input cannot resolve a resource or create a destination side effect."""
    facade = _Facade(_artifact())
    _patch_facade(monkeypatch, facade)

    missing_destination = runner.invoke(app, ["materialize", "https://data.example.test/observations.csv"])
    invalid_mode = runner.invoke(
        app,
        [
            "materialize",
            "https://data.example.test/observations.csv",
            "--destination",
            "memory://contract-output",
            "--mode",
            "other",
        ],
    )
    secret_input = runner.invoke(
        app,
        [
            "materialize",
            "https://user:password@data.example.test/observations.csv",
            "--destination",
            "memory://contract-output",
            "--output",
            "json",
        ],
    )

    assert missing_destination.exit_code != 0
    assert invalid_mode.exit_code == 1
    assert "--mode must be parquet or raw" in invalid_mode.stderr
    assert secret_input.exit_code == 1
    assert "password" not in secret_input.stderr
    assert facade.resolved == []
    assert facade.materialized == []


def test_ambiguous_catalog_reference_fails_before_materialization(monkeypatch: pytest.MonkeyPatch) -> None:
    """A selector-free multi-resource dataset cannot cause a destination write."""
    facade = _Facade(
        _artifact(),
        Dataset(
            id="weather",
            resources=[
                Resource(id="first", url="https://data.example.test/first.csv", format="CSV"),
                Resource(id="second", url="https://data.example.test/second.csv", format="CSV"),
            ],
        ),
    )
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(
        app,
        [
            "materialize",
            "--portal",
            "https://catalog.example.test",
            "--dataset",
            "weather",
            "--destination",
            "memory://contract-output",
            "--output",
            "json",
        ],
    )

    assert outcome.exit_code == 1
    assert outcome.stdout == ""
    assert "Valid selectors: first, second" in outcome.stderr
    assert facade.resolved == []
    assert facade.materialized == []


def test_help_registers_new_commands_and_uses_annotated_parameters() -> None:
    """The root app exposes each new workflow without B008-style defaults."""
    root_help = runner.invoke(app, ["--help"])
    materialize_help = runner.invoke(app, ["materialize", "--help"])

    assert root_help.exit_code == 0
    root_text = _plain(root_help.stdout)
    assert "scan" in root_text
    assert "open" in root_text
    assert "materialize" in root_text
    assert materialize_help.exit_code == 0
    materialize_text = _plain(materialize_help.stdout)
    assert "--destination" in materialize_text
    assert "--mode" in materialize_text
    assert "--output" in materialize_text
    for command in (materialize_command.materialize,):
        source = inspect.getsource(command)
        assert "Annotated[" in source
        assert "= typer.Option" not in source
        assert "= typer.Argument" not in source
