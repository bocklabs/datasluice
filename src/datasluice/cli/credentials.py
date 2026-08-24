"""Credential discovery inspection and safe explicit credential validation."""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, cast

import typer

from datasluice.cli._output import render_json, result_console
from datasluice.cli._platforms import CATALOG_PLATFORMS
from datasluice.domain.catalog.auth import CatalogCredential, CKANCredential, SocrataCredential, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import credential_from_fields
from datasluice.runtime.redaction import redact_for_output

app = typer.Typer(help="Inspect opt-in credential sources and safely validate explicit credentials.")

_SOURCES = (
    ("environment", None, None),
    ("keychain", "keyring", "keychain"),
    ("secrets-aws", "boto3", "secrets-aws"),
    ("secrets-vault", "hvac", "secrets-vault"),
)


@app.command("availability")
def availability(
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Show which opt-in credential resolution sources are installed."""
    _validate_output(output)
    sources = [_source_status(name, module, extra) for name, module, extra in _SOURCES]
    if output == "json":
        render_json({"sources": sources})
        return
    for source in sources:
        status = "available" if source["available"] else "not installed"
        hint = source["install_hint"]
        result_console.print(f"{source['source']}: {status}{f' — {hint}' if hint else ''}")


@app.command("validate")
def validate(
    platform: Annotated[str, typer.Option("--platform", help="Canonical platform: ckan, udata, or socrata")],
    credential_json: Annotated[
        str | None, typer.Option("--credential-json", help="Explicit credential JSON object")
    ] = None,
    credential_file: Annotated[
        Path | None, typer.Option("--credential-file", help="Path to explicit credential JSON")
    ] = None,
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Validate caller-supplied explicit credentials without revealing secret values."""
    _validate_output(output)
    if platform not in CATALOG_PLATFORMS:
        raise typer.BadParameter("--platform must be one of: ckan, udata, socrata")
    if (credential_json is None) == (credential_file is None):
        raise typer.BadParameter("Supply exactly one of --credential-json or --credential-file")
    try:
        if credential_json is not None:
            raw_credential = credential_json
        elif credential_file is not None:
            raw_credential = credential_file.read_text(encoding="utf-8")
        else:
            raise ValueError("Credential input is required.")
        fields = json.loads(raw_credential)
        if not isinstance(fields, Mapping):
            raise ValueError("Credential input must be a JSON object.")
        credential = credential_from_fields(CatalogPlatform(platform), fields)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise typer.BadParameter("Credential input is invalid; secret values were not rendered.") from exc
    result = {"platform": platform, "valid": True, "credential": _credential_output(credential)}
    if output == "json":
        render_json(result)
        return
    result_console.print(f"{platform}: explicit credential is valid")
    result_console.print(result["credential"])


def _source_status(name: str, module: str | None, extra: str | None) -> dict[str, object]:
    """Describe one resolution source without importing its optional dependency."""
    available = module is None or importlib.util.find_spec(module) is not None
    return {
        "source": name,
        "available": available,
        "install_hint": None if available or extra is None else f"uv sync --extra {extra}",
    }


def _credential_output(credential: CatalogCredential) -> Mapping[str, object]:
    """Render one validated credential through the central redaction gate."""
    if isinstance(credential, CKANCredential):
        value: Mapping[str, object] = {"type": "ckan", "api_token": credential.api_token}
    elif isinstance(credential, UDataCredential):
        value = {"type": "udata", "api_key": credential.api_key}
    elif isinstance(credential, SocrataCredential):
        value = {
            "type": "socrata",
            "app_token": credential.app_token,
            "username": credential.username,
            "password": credential.password,
        }
    else:
        raise TypeError("Unsupported explicit credential type.")
    return cast(Mapping[str, object], redact_for_output(value))


def _validate_output(output: str) -> None:
    """Reject unsupported machine-output formats before credential processing."""
    if output not in {"human", "json"}:
        raise typer.BadParameter("--output must be human or json")
