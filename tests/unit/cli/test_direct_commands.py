"""Contract tests for direct-resource CLI commands and the retained command inventory."""

from __future__ import annotations

import inspect
import re

import pytest
from typer.testing import CliRunner

from datasluice.cli._resolver import parse_locator
from datasluice.cli.app import app
from datasluice.cli.materialize import materialize
from datasluice.cli.open import open
from datasluice.cli.scan import scan

runner = CliRunner()

_RETIRED_COMMANDS = ("search", "inspect", "download", "detect")


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


def test_app_registers_exactly_the_retained_direct_commands() -> None:
    """The Typer app registers only scan, open, and materialize."""
    registered = {command.name for command in app.registered_commands}
    assert registered == {"scan", "open", "materialize"}


def test_help_lists_retained_commands_and_makes_removals_visible() -> None:
    """Root help advertises exactly the retained commands with no retired names."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in ("scan", "open", "materialize"):
        assert command in result.output
    for retired in _RETIRED_COMMANDS:
        assert retired not in result.output


@pytest.mark.parametrize("retired", _RETIRED_COMMANDS)
def test_retired_commands_are_not_invokable(retired: str) -> None:
    """Retired portal-era commands fail resolution instead of redirecting."""
    result = runner.invoke(app, [retired, "https://data.example.test/records.csv"])

    assert result.exit_code == 2
    assert re.search(r"No such command", result.output, re.IGNORECASE)


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
