"""Capability-inspection commands backed by packaged connector profiles."""

from __future__ import annotations

import json
from datetime import date
from importlib.resources import files
from typing import Annotated, Any, cast

import typer

from datasluice.cli._output import render_json, result_console
from datasluice.cli._platforms import CATALOG_PLATFORMS
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile
from datasluice.runtime.capability import EffectiveCapabilityCache

app = typer.Typer(help="Inspect declared and effective catalog operation capabilities.")

_AUTH_CLASS_LABELS = {
    **{item.value: item for item in AuthClass},
    "application-token-or-authenticated": AuthClass.AUTHENTICATED,
}


@app.command("list")
def list_profiles(
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """List the canonical connector profiles installed with DataSluice."""
    _validate_output(output)
    profiles = [_profile_summary(platform) for platform in CATALOG_PLATFORMS]
    if output == "json":
        render_json({"profiles": profiles})
        return
    for profile in profiles:
        result_console.print(f"{profile['platform']}: {profile['profile_version']} ({profile['platform_api_version']})")


@app.command("show")
def show_profile(
    platform: Annotated[str, typer.Argument(help="Canonical platform: ckan, udata, or socrata")],
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Show declared fallback states for every operation of one platform."""
    _validate_output(output)
    profile, entries = _load_profile(platform)
    cache = EffectiveCapabilityCache(profile)
    operations = [
        {
            "operation_id": entry["id"],
            "declared_state": operation.capability_class.value,
            "effective_state": cache.peek(operation_id).for_operation(operation_id).state.value,
            "state_source": "declared-profile-fallback",
        }
        for entry, (operation_id, operation) in zip(entries, profile.operations.items(), strict=True)
    ]
    result = {
        "platform": platform,
        "profile_version": profile.profile_version,
        "probe_status": "not-probed",
        "operations": operations,
    }
    if output == "json":
        render_json(result)
        return
    result_console.print(f"{platform} {profile.profile_version} — declared-profile fallback (not probed)")
    for operation in operations:
        result_console.print(f"{operation['operation_id']}: {operation['effective_state']}")


def _profile_summary(platform: str) -> dict[str, str]:
    """Return public metadata for one packaged profile."""
    profile = _declared_profile(platform)
    return {
        "platform": platform,
        "profile_version": profile.profile_version,
        "platform_api_version": profile.platform_api_version,
    }


def _declared_profile(platform: str) -> DeclaredCapabilityProfile:
    """Load one immutable declared profile without probing a deployment."""
    return _load_profile(platform)[0]


def _load_profile(platform: str) -> tuple[DeclaredCapabilityProfile, list[dict[str, str]]]:
    """Parse the packaged profile document once and build the runtime profile."""
    if platform not in CATALOG_PLATFORMS:
        raise typer.BadParameter("platform must be one of: ckan, udata, socrata")
    try:
        document = _profile_document(platform)
        entries = _profile_entries(document)
        specs = [_operation_spec(entry) for entry in entries]
        operations = {spec.id: spec for spec in specs}
        if len(operations) != len(specs):
            raise ValueError("duplicate operation identifiers")
        profile = DeclaredCapabilityProfile(
            profile_version=document["profile_version"],
            schema_version=document["schema_version"],
            platform_api_version=document["platform_api_version"],
            official_source_uri=document["official_source_uri"],
            source_accessed_at=date.fromisoformat(document["source_accessed_at"]),
            fixture_fingerprint=document["fixture_fingerprint"],
            operations=operations,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise typer.BadParameter(f"Packaged capability profile for {platform!r} is invalid: {exc}") from exc
    return profile, entries


def _operation_id(value: str) -> OperationId:
    """Decode a packaged operation identifier into the runtime value object."""
    platform, qualified_name = value.split("/", 1)
    if "." in qualified_name:
        service, method = qualified_name.split(".", 1)
    else:
        service, method = "profile", qualified_name
    return OperationId(platform=platform, service=service, method=method)


def _profile_document(platform: str) -> dict[str, Any]:
    """Load raw packaged profile data without contacting a deployment."""
    profile_files = files("datasluice.contracts.catalog.profiles")
    matches = sorted(
        (path for path in profile_files.iterdir() if path.name.startswith(f"{platform}-")), key=lambda path: path.name
    )
    if len(matches) != 1:
        raise ValueError(f"expected exactly one packaged profile, found {len(matches)}")
    document = json.loads(matches[0].read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("Packaged capability profiles must be JSON objects.")
    return cast(dict[str, Any], document)


def _profile_entries(document: dict[str, Any]) -> list[dict[str, str]]:
    """Return the typed operation entries from one parsed profile document."""
    entries = document.get("operations")
    if not isinstance(entries, list) or not all(isinstance(entry, dict) for entry in entries):
        raise ValueError("Packaged capability profiles must declare operations.")
    return [cast(dict[str, str], entry) for entry in entries]


def _operation_spec(entry: dict[str, str]) -> OperationSpec:
    """Build the runtime operation shape required by the probe engine."""
    try:
        auth_class = _AUTH_CLASS_LABELS[entry["authentication"]]
        mutation_class = MutationClass(entry["mutation"])
        capability_class = CapabilityClass(entry["capability"])
    except (KeyError, ValueError) as exc:
        raise ValueError(
            f"Packaged operation {entry.get('id', '<unknown>')!r} declares unsupported value for {exc.args[0]!r}."
        ) from exc
    return OperationSpec(
        id=_operation_id(entry["id"]),
        tier=OperationTier.NATIVE,
        request_type="catalog-request",
        response_type="catalog-response",
        auth_class=auth_class,
        mutation_class=mutation_class,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.NONE,
        capability_class=capability_class,
    )


def _validate_output(output: str) -> None:
    """Reject unsupported machine-output formats before profile work."""
    if output not in {"human", "json"}:
        raise typer.BadParameter("--output must be human or json")
