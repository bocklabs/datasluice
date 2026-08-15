"""Contract tests for direct-resource CLI commands."""

from __future__ import annotations

import inspect

import pytest
from typer.testing import CliRunner

from datasluice.cli._resolver import parse_locator
from datasluice.cli.app import app
from datasluice.cli.materialize import materialize
from datasluice.cli.open import open
from datasluice.cli.scan import scan

runner = CliRunner()


@pytest.mark.parametrize(
    "locator",
    [
        "records.csv",
        "file:///tmp/records.parquet",
        "s3://example-bucket/records.json",
        "https://data.example.test/records.csv",
    ],
)
def test_parse_locator_accepts_direct_resource_locator(locator: str) -> None:
    """Direct file, object-storage, and HTTP locators remain supported."""
    assert parse_locator(locator).uri == locator


@pytest.mark.parametrize(
    ("command", "arguments"),
    [
        ("open", ["https://data.example.test/records.csv", "--portal", "https://portal.example.test"]),
        ("scan", ["https://data.example.test/records.csv", "--dataset", "dataset-1"]),
        (
            "materialize",
            [
                "https://data.example.test/records.csv",
                "--resource",
                "resource-1",
                "--destination",
                "file:///tmp/output.parquet",
            ],
        ),
    ],
)
def test_resource_commands_reject_retired_catalog_selectors(command: str, arguments: list[str]) -> None:
    """Retired catalog selectors fail during CLI option parsing."""
    result = runner.invoke(app, [command, *arguments])

    assert result.exit_code == 2
    assert "No such option" in result.output


def test_resource_commands_have_no_catalog_resolution_surface() -> None:
    """The data plane accepts only direct locators without catalog selectors."""
    for command in (open, scan, materialize):
        assert not {"portal", "dataset", "resource"}.intersection(inspect.signature(command).parameters)
