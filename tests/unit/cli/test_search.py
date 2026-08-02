"""Unit tests for the ``datasluice search`` command (facade-only, D-09/APP-08)."""

from __future__ import annotations

import inspect
import json
import os
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import datasluice.cli.search as search_cmd
from datasluice.cli.app import app
from datasluice.domain import Dataset, Organization, Query, Resource, SearchResult

if not hasattr(search_cmd, "DataSluice"):
    if os.environ.get("DATASLUICE_TDD_RED") == "1":
        pytest.fail("search facade refactor pending GREEN phase", pytrace=False)
    pytest.skip("search facade refactor pending GREEN phase", allow_module_level=True)

runner = CliRunner()


class _FakeFacade:
    def __init__(self, result: SearchResult) -> None:
        self._result = result
        self.search_calls: list[tuple[str, Query | None]] = []
        self.closed = False

    def __enter__(self) -> _FakeFacade:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.closed = True

    def search(self, url: str, query: Query | None = None, **kwargs: Any) -> SearchResult:
        self.search_calls.append((url, query))
        return self._result


def _result() -> SearchResult:
    org = Organization(id="org-1", name="city", title="City of Example")
    resource = Resource(id="res-1", name="data.csv", url="https://data.example.test/data.csv", format="CSV")
    dataset = Dataset(id="ds-1", title="Weather", resources=[resource], organization=org)
    return SearchResult(datasets=[dataset], total=1)


def _patch_facade(monkeypatch: pytest.MonkeyPatch, facade: _FakeFacade) -> None:
    monkeypatch.setattr(search_cmd, "DataSluice", lambda: facade)


def test_search_delegates_to_facade_and_closes_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Search constructs DataSluice in a context manager and delegates to ds.search."""
    facade = _FakeFacade(_result())
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["search", "--portal", "https://data.example.test", "weather"])

    assert outcome.exit_code == 0, outcome.output
    assert len(facade.search_calls) == 1
    url, query = facade.search_calls[0]
    assert url == "https://data.example.test"
    assert isinstance(query, Query)
    assert query.text == "weather"
    assert facade.closed


def test_search_json_output_is_parseable_on_stdout(monkeypatch: pytest.MonkeyPatch) -> None:
    """--output json produces machine-readable JSON on stdout with diagnostics on stderr."""
    facade = _FakeFacade(_result())
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["search", "--portal", "https://data.example.test", "weather", "--output", "json"])

    assert outcome.exit_code == 0, outcome.output
    parsed = json.loads(outcome.stdout)
    assert parsed["total"] == 1
    assert parsed["datasets"][0]["id"] == "ds-1"


def test_search_facade_error_exits_nonzero_with_stderr_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    """A DataSluiceError surfaces on stderr and exits 1 without corrupting stdout."""
    from datasluice.exceptions import DataSluiceError

    class _FailingFacade(_FakeFacade):
        def search(self, url: str, query: Query | None = None, **kwargs: Any) -> SearchResult:
            raise DataSluiceError("portal unreachable")

    facade = _FailingFacade(_result())
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["search", "--portal", "https://data.example.test", "--output", "json"])

    assert outcome.exit_code == 1
    assert outcome.stdout == ""
    assert "portal unreachable" in outcome.stderr


def test_cli_uses_annotated_typer_form() -> None:
    """B008 guard: search() uses Annotated[...] not = typer.* defaults."""
    source = inspect.getsource(search_cmd.search)
    assert "Annotated[" in source
    assert "= typer.Option" not in source
    assert "= typer.Argument" not in source


def test_architecture_rejects_private_imports() -> None:
    """P-08-CLI-PRIVATE-BYPASS: search must not import session/connector/transport internals."""
    source = Path(search_cmd.__file__).read_text()
    forbidden = [
        "from datasluice.runtime.session",
        "from datasluice.connectors",
        "from datasluice.discovery",
        "from datasluice.transport",
        "from datasluice.io.downloader",
        "DataSluiceSession",
    ]
    for token in forbidden:
        assert token not in source, f"forbidden import found in search.py: {token!r}"
