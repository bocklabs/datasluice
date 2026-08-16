"""Strict checked-in fixture loading for catalog reference clients."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

_PROFILES = Path(__file__).parents[1] / "profiles"
_FIXTURES = Path(__file__).parent


@dataclass(frozen=True, slots=True)
class ReferenceCase:
    """One declared deterministic capability outcome."""

    operation_id: ReferenceOperationId
    outcome: str
    credential_class: str | None = None


@dataclass(frozen=True, slots=True)
class ReferenceFixtureSet:
    """A profile-validated, immutable reference fixture collection."""

    platform: str
    profile_version: str
    fingerprint: str
    cases: tuple[ReferenceCase, ...]
    declared_operations: frozenset[ReferenceOperationId]
    evidence: Mapping[str, object]

    @property
    def success_cases(self) -> tuple[ReferenceCase, ...]:
        """Return cases that deterministically reach the reference executor."""
        return tuple(
            case
            for case in self.cases
            if case.outcome in {"core", "optional", "authenticated-success", "async-pending"}
        )


@dataclass(frozen=True, slots=True)
class ReferenceOperationId:
    """A profile identity that preserves the checked-in operation spelling."""

    platform: str
    value: str

    def __str__(self) -> str:
        """Return the exact declared operation identity."""
        return self.value


def load_reference_fixture_set(platform: str, *, cases_path: Path | None = None) -> ReferenceFixtureSet:
    """Load one profile-bound fixture set without network access or ambient state."""
    if platform not in {"ckan", "udata", "socrata"}:
        raise ValueError("Reference fixtures require a declared platform.")
    path = cases_path or _FIXTURES / platform / "cases.json"
    evidence_path = path.with_name("evidence.json")
    profile_path = _matching_profile(platform)
    profile = _object(_read_json(profile_path), "profile")
    cases_document = _object(_read_json(path), "cases")
    evidence = _object(_read_json(evidence_path), "evidence")
    fingerprint = hashlib.sha256(path.read_bytes()).hexdigest()
    if profile.get("schema_version") != "1.0" or cases_document.get("schema_version") != "1.0":
        raise ValueError("Reference fixtures require schema version 1.0.")
    if profile.get("platform") != platform or cases_document.get("platform") != platform:
        raise ValueError("Reference fixture platform does not match its profile.")
    if profile.get("profile_version") != cases_document.get("profile_version"):
        raise ValueError("Reference fixture profile version does not match its cases.")
    if profile.get("fixture_fingerprint") != fingerprint:
        raise ValueError("Reference fixture fingerprint does not match the checked-in cases.")
    declared_operations = frozenset(
        _operation_id(entry) for entry in _list(profile.get("operations"), "profile.operations")
    )
    cases = tuple(
        _case(entry, platform, declared_operations) for entry in _list(cases_document.get("cases"), "cases.cases")
    )
    if not cases or not isinstance(evidence.get("platform_version"), str):
        raise ValueError("Reference fixtures require evidence and at least one declared case.")
    return ReferenceFixtureSet(
        platform=platform,
        profile_version=str(profile["profile_version"]),
        fingerprint=fingerprint,
        cases=cases,
        declared_operations=declared_operations,
        evidence=MappingProxyType(dict(evidence)),
    )


def _matching_profile(platform: str) -> Path:
    """Return the single checked-in profile belonging to one platform."""
    paths = [
        path for path in _PROFILES.glob("*.json") if _object(_read_json(path), "profile").get("platform") == platform
    ]
    if len(paths) != 1:
        raise ValueError("Reference fixture platform must have exactly one pinned profile.")
    return paths[0]


def _case(value: object, platform: str, declared: frozenset[ReferenceOperationId]) -> ReferenceCase:
    """Decode one case and reject undeclared operations or states."""
    data = _object(value, "case")
    allowed_keys = {"operation", "outcome", "credential_class", "receipt_metadata"}
    if (
        not set(data).issubset(allowed_keys)
        or not isinstance(data.get("operation"), str)
        or not isinstance(data.get("outcome"), str)
    ):
        raise ValueError("Reference cases require declared operation IDs and outcomes.")
    operation_id = _operation_id(data)
    if operation_id.platform != platform or operation_id not in declared:
        raise ValueError("Reference case references an undeclared operation ID.")
    outcomes = {
        "core",
        "optional",
        "authenticated-success",
        "missing-credentials",
        "invalid-credentials",
        "forbidden",
        "deployment-disabled",
        "unavailable",
        "async-pending",
        "rate-limited",
    }
    outcome = data["outcome"]
    if not isinstance(outcome, str) or outcome not in outcomes:
        raise ValueError("Reference case has an undeclared outcome.")
    credential_class = data.get("credential_class")
    if credential_class is not None and not isinstance(credential_class, str):
        raise ValueError("Reference case credential classes must be strings.")
    return ReferenceCase(operation_id=operation_id, outcome=outcome, credential_class=credential_class)


def _operation_id(value: object) -> ReferenceOperationId:
    """Parse one portable operation identity from a checked-in object."""
    operation = (
        value.get("id")
        if isinstance(value, Mapping) and "id" in value
        else value.get("operation")
        if isinstance(value, Mapping)
        else None
    )
    if not isinstance(operation, str) or "/" not in operation:
        raise ValueError("Reference fixture operation IDs must be portable identifiers.")
    platform, name = operation.split("/", 1)
    if not platform or not name:
        raise ValueError("Reference fixture operation IDs must be portable identifiers.")
    return ReferenceOperationId(platform=platform, value=operation)


def _read_json(path: Path) -> object:
    """Read one packaged JSON object with a useful strict-loader failure."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"Reference fixture {path.name} is missing from the installed package ({path}); "
            "the datasluice distribution may be corrupted."
        ) from error


def _object(value: object, name: str) -> dict[str, object]:
    """Require a JSON object with string keys."""
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"Reference {name} must be a JSON object.")
    return value


def _list(value: object, name: str) -> list[object]:
    """Require a JSON list."""
    if not isinstance(value, list):
        raise ValueError(f"Reference {name} must be a JSON list.")
    return value
