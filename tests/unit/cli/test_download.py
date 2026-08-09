"""Unit tests for the ``datasluice download`` command."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import datasluice.cli.download as download_cmd
from datasluice.cli.app import app
from datasluice.domain import Dataset, Resource

runner = CliRunner()


def _make_dataset(formats: list[str | None]) -> Dataset:
    resources = [
        Resource(
            id=f"res-{index}",
            name=f"resource-{index}",
            url=f"https://example.com/files/resource-{index}",
            format=fmt,
        )
        for index, fmt in enumerate(formats)
    ]
    return Dataset(id="dataset-1", title="Test Dataset", resources=resources)


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
        self.download_calls: list[tuple[list[Resource], str]] = []
        self.closed = False

    def __enter__(self) -> _FakeFacade:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.closed = True

    def portal(self, url: str) -> _FakePortal:
        return self._portal

    def download_many(self, resources: list[Resource], destination: str) -> list[dict[str, object]]:
        self.download_calls.append((list(resources), destination))
        return [
            {"resource_id": resource.id, "path": f"{destination}/{resource.id}.bin", "size": 100}
            for resource in resources
        ]


def _patch_facade(monkeypatch: pytest.MonkeyPatch, facade: _FakeFacade) -> None:
    monkeypatch.setattr(download_cmd, "DataSluice", lambda: facade)


@pytest.mark.parametrize(
    ("formats", "fmt", "expected_formats"),
    [
        (["CSV", "JSON", "XLSX"], "CSV", ["CSV"]),
        (["csv", "JSON", "XLSX"], "csv", ["csv"]),
        (["CSV", "csv", "CSV"], "CSV", ["CSV", "csv", "CSV"]),
        (["CSV", "JSON", "XLSX"], None, ["CSV", "JSON", "XLSX"]),
    ],
)
def test_download_format_filtering(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    formats: list[str | None],
    fmt: str | None,
    expected_formats: list[str | None],
) -> None:
    """Format filtering happens in the CLI before raw bulk download through the facade."""
    facade = _FakeFacade(_make_dataset(formats))
    _patch_facade(monkeypatch, facade)

    args = ["download", "--portal", "https://example.com", "dataset-1", "--dest", str(tmp_path)]
    if fmt is not None:
        args.extend(["--format", fmt])
    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    assert facade.closed
    assert len(facade.download_calls) == 1
    received, dest = facade.download_calls[0]
    assert [resource.format for resource in received] == expected_formats
    assert dest == str(tmp_path)


def test_download_no_matching_resources_exits_with_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No matching resources exits 1 without calling the facade download."""
    facade = _FakeFacade(_make_dataset(["JSON", "XLSX"]))
    _patch_facade(monkeypatch, facade)

    result = runner.invoke(
        app,
        ["download", "--portal", "https://example.com", "dataset-1", "--dest", str(tmp_path), "--format", "CSV"],
    )

    assert result.exit_code == 1
    assert "No resources found" in result.output
    assert facade.download_calls == []


def test_download_json_output_is_parseable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """--output json produces machine-readable download results on stdout."""
    facade = _FakeFacade(_make_dataset(["CSV"]))
    _patch_facade(monkeypatch, facade)

    result = runner.invoke(
        app,
        [
            "download",
            "--portal",
            "https://example.com",
            "dataset-1",
            "--dest",
            str(tmp_path),
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    import json

    parsed = json.loads(result.stdout)
    assert parsed["count"] == 1
    assert parsed["downloaded"][0]["resource_id"] == "res-0"


def test_cli_uses_annotated_typer_form() -> None:
    """B008 guard: download() uses Annotated[...] not = typer.* defaults."""
    source = inspect.getsource(download_cmd.download)
    assert "Annotated[" in source
    assert "= typer.Option" not in source
    assert "= typer.Argument" not in source


def test_architecture_rejects_private_imports() -> None:
    """Download must not import session/connector/transport internals."""
    source = Path(download_cmd.__file__).read_text()
    forbidden = [
        "from datasluice.runtime.session",
        "from datasluice.connectors",
        "from datasluice.discovery",
        "from datasluice.transport",
        "from datasluice.io.downloader",
        "DataSluiceSession",
        ".downloader",
    ]
    for token in forbidden:
        assert token not in source, f"forbidden import found in download.py: {token!r}"
