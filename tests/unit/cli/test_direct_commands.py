"""Contract tests for direct-resource CLI commands and the retained command inventory."""

from __future__ import annotations

import inspect
import re

import pytest
from typer.testing import CliRunner

from datasluice.cli._resolver import parse_locator
from datasluice.cli.app import app
from datasluice.cli.capabilities import show_profile
from datasluice.cli.credentials import validate
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


def test_app_registers_direct_commands_and_runtime_command_groups() -> None:
    """The Typer app registers direct commands and the runtime inspection groups."""
    registered = {command.name for command in app.registered_commands}
    assert registered == {"scan", "open", "materialize"}
    assert {group.name for group in app.registered_groups} == {"capabilities", "credentials"}


def test_help_lists_retained_commands_and_makes_removals_visible() -> None:
    """Root help advertises exactly the retained commands with no retired names."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in ("scan", "open", "materialize", "capabilities", "credentials"):
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


def test_capabilities_list_and_show_declared_states_without_network() -> None:
    """Canonical packaged profiles render their baseline state through the CLI."""
    listing = runner.invoke(app, ["capabilities", "list", "--output", "json"])
    showing = runner.invoke(app, ["capabilities", "show", "ckan", "--output", "json"])

    assert listing.exit_code == 0, listing.output
    assert showing.exit_code == 0, showing.output
    for platform in ("ckan", "udata", "socrata"):
        assert platform in listing.output
    assert '"state_source":"declared-profile-fallback"' in showing.output
    assert '"probe_status":"not-probed"' in showing.output


def test_credentials_availability_and_validation_redact_explicit_secrets() -> None:
    """Credential output advertises extras while rejecting secret disclosure."""
    availability = runner.invoke(app, ["credentials", "availability", "--output", "json"])
    validation = runner.invoke(
        app,
        [
            "credentials",
            "validate",
            "--platform",
            "ckan",
            "--credential-json",
            '{"api_token":"cli-secret-value"}',
            "--output",
            "json",
        ],
    )

    assert availability.exit_code == 0, availability.output
    for extra in ("keychain", "secrets-aws", "secrets-vault"):
        assert extra in availability.output
    assert validation.exit_code == 0, validation.output
    assert "cli-secret-value" not in validation.output
    assert "***" in validation.output


def test_new_command_options_use_annotated_parameters() -> None:
    """Grouped commands use Annotated metadata instead of call defaults."""
    for command in (show_profile, validate):
        source = inspect.getsource(command)
        assert "Annotated[" in source
        assert "= typer.Option" not in source
