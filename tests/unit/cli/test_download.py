"""Unit tests for the ``datasluice download`` command."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

import datasluice
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


class _RecordingDownloader:
    def __init__(self) -> None:
        self.received: list[Resource] | None = None

    def download_many(self, resources: list[Resource], dest: str | Path) -> list[Path]:
        self.received = list(resources)
        return [Path(dest) / f"{resource.id}.bin" for resource in resources]


def _patch_client(monkeypatch: pytest.MonkeyPatch, dataset: Dataset) -> _RecordingDownloader:
    downloader = _RecordingDownloader()

    class FakeDataSluice:
        def __init__(self, portal: str) -> None:
            self.portal = portal
            self.downloader = downloader

        def get_dataset(self, dataset_id: str) -> Dataset:
            return dataset

        def download_all(self, dataset: Dataset, dest: str | Path) -> list[Path]:
            return self.downloader.download_many(dataset.resources, dest)

    monkeypatch.setattr(datasluice, "DataSluice", FakeDataSluice)
    return downloader


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
    downloader = _patch_client(monkeypatch, _make_dataset(formats))
    args = ["download", "--portal", "https://example.com", "dataset-1", "--dest", str(tmp_path)]
    if fmt is not None:
        args.extend(["--format", fmt])
    result = runner.invoke(app, args)
    assert result.exit_code == 0
    assert downloader.received is not None
    assert [resource.format for resource in downloader.received] == expected_formats


def test_download_no_matching_resources_exits_with_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    downloader = _patch_client(monkeypatch, _make_dataset(["JSON", "XLSX"]))
    result = runner.invoke(
        app,
        ["download", "--portal", "https://example.com", "dataset-1", "--dest", str(tmp_path), "--format", "CSV"],
    )
    assert result.exit_code == 1
    assert "No resources found" in result.output
    assert downloader.received is None
