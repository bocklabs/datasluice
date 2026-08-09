"""Unit tests for the ``datasluice inspect`` command."""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import datasluice.cli.inspect as inspect_cmd
from datasluice.cli.app import app
from datasluice.domain import Dataset, Organization, Resource

runner = CliRunner()


class _FakePortal:
    def __init__(self, dataset: Dataset) -> None:
        self._dataset = dataset
        self.get_dataset_calls: list[str] = []

    def get_dataset(self, dataset_id: str) -> Dataset:
        self.get_dataset_calls.append(dataset_id)
        return self._dataset


class _FakeFacade:
    def __init__(self, dataset: Dataset) -> None:
        self._portal = _FakePortal(dataset)
        self.portal_calls: list[str] = []
        self.closed = False

    def __enter__(self) -> _FakeFacade:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.closed = True

    def portal(self, url: str) -> _FakePortal:
        self.portal_calls.append(url)
        return self._portal


def _dataset() -> Dataset:
    org = Organization(id="org-1", name="city", title="City of Example")
    resource = Resource(id="res-1", name="data.csv", url="https://data.example.test/data.csv", format="CSV")
    return Dataset(
        id="ds-1",
        title="Weather",
        description="Daily weather observations",
        resources=[resource],
        organization=org,
        tags=["weather", "climate"],
    )


def _patch_facade(monkeypatch: pytest.MonkeyPatch, facade: _FakeFacade) -> None:
    monkeypatch.setattr(inspect_cmd, "DataSluice", lambda: facade)


def test_inspect_delegates_to_portal_get_dataset_and_closes_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inspect routes through DataSluice.portal().get_dataset() and closes the facade."""
    facade = _FakeFacade(_dataset())
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["inspect", "--portal", "https://data.example.test", "ds-1"])

    assert outcome.exit_code == 0, outcome.output
    assert facade.portal_calls == ["https://data.example.test"]
    assert facade._portal.get_dataset_calls == ["ds-1"]
    assert facade.closed


def test_inspect_json_output_is_parseable_on_stdout(monkeypatch: pytest.MonkeyPatch) -> None:
    """--output json produces machine-readable catalog metadata on stdout."""
    facade = _FakeFacade(_dataset())
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["inspect", "--portal", "https://data.example.test", "ds-1", "--output", "json"])

    assert outcome.exit_code == 0, outcome.output
    parsed = json.loads(outcome.stdout)
    assert parsed["id"] == "ds-1"
    assert parsed["title"] == "Weather"
    assert len(parsed["resources"]) == 1


def test_inspect_facade_error_exits_nonzero_with_stderr_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    """A DataSluiceError surfaces on stderr and exits 1."""
    from datasluice.exceptions import DataSluiceError

    class _FailingPortal(_FakePortal):
        def get_dataset(self, dataset_id: str) -> Dataset:
            raise DataSluiceError("dataset not found")

    class _FailingFacade(_FakeFacade):
        def __init__(self) -> None:
            super().__init__(_dataset())
            self._portal = _FailingPortal(_dataset())

    facade = _FailingFacade()
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["inspect", "--portal", "https://data.example.test", "missing", "--output", "json"])

    assert outcome.exit_code == 1
    assert outcome.stdout == ""
    assert "dataset not found" in outcome.stderr


def test_cli_uses_annotated_typer_form() -> None:
    """B008 guard: inspect() uses Annotated[...] not = typer.* defaults."""
    source = inspect.getsource(inspect_cmd.inspect)
    assert "Annotated[" in source
    assert "= typer.Option" not in source
    assert "= typer.Argument" not in source


def test_architecture_rejects_private_imports() -> None:
    """Inspect must not import session/connector/transport internals."""
    source = Path(inspect_cmd.__file__).read_text()
    forbidden = [
        "from datasluice.runtime.session",
        "from datasluice.connectors",
        "from datasluice.discovery",
        "from datasluice.transport",
        "from datasluice.io.downloader",
        "DataSluiceSession",
    ]
    for token in forbidden:
        assert token not in source, f"forbidden import found in inspect.py: {token!r}"
