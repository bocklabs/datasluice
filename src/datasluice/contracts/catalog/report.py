"""Versioned JSON-safe report envelopes for catalog contract execution."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import ClassVar, Literal, cast

_METADATA_FIELDS = frozenset({"platform", "fixture", "environment", "profile_version"})
_EVIDENCE_FIELDS = frozenset({"fixture_fingerprint", "evidence_fingerprint", "classification"})
_OUTCOME_STATES = frozenset({"passed", "failed", "blocked"})
_SECRET_PATTERN = re.compile(
    r"(?i)(authorization|api[-_ ]?key|token|secret|password|cookie)\s*[:=]\s*(?:bearer\s+)?[^\s,;]+"
)


def _report_error(path: str) -> ValueError:
    return ValueError(f"Invalid catalog contract report at {path}")


def _sanitize_text(value: str, path: str) -> str:
    if len(value) > 256:
        raise _report_error(path)
    return _SECRET_PATTERN.sub(lambda match: f"{match.group(1)}: [REDACTED]", value)


def _freeze_json(value: object, path: str) -> object:
    if value is None or isinstance(value, bool) or type(value) is int:
        return value
    if isinstance(value, str):
        return _sanitize_text(value, path)
    if isinstance(value, Mapping):
        if len(value) > 32:
            raise _report_error(path)
        frozen: dict[str, object] = {}
        for key, nested in value.items():
            if not isinstance(key, str) or len(key) > 64:
                raise _report_error(path)
            frozen[key] = _freeze_json(nested, f"{path}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (tuple, list)):
        if len(value) > 32:
            raise _report_error(path)
        return tuple(_freeze_json(nested, path) for nested in value)
    raise _report_error(path)


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(nested) for nested in value]
    return value


def _sanitize_mapping(value: Mapping[str, object], allowed: frozenset[str], path: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _report_error(path)
    return MappingProxyType(
        {key: _freeze_json(nested, f"{path}.{key}") for key, nested in value.items() if key in allowed}
    )


def _identity(value: str | None, path: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > 128:
        raise _report_error(path)
    return _sanitize_text(value, path)


@dataclass(frozen=True, slots=True)
class CaseOutcome:
    """Immutable evidence for one catalog contract case execution."""

    operation_id: str
    mode: Literal["sync", "async"]
    capability: Literal["available", "unavailable"]
    state: Literal["passed", "failed", "blocked"]
    tier: str = "core"
    warnings: tuple[str, ...] = ()
    evidence: Mapping[str, object] = field(default_factory=dict)
    platform_metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.operation_id, str) or not self.operation_id or len(self.operation_id) > 256:
            raise _report_error("outcome.operation_id")
        if self.mode not in {"sync", "async"} or self.capability not in {"available", "unavailable"}:
            raise _report_error("outcome")
        if self.state not in _OUTCOME_STATES or not isinstance(self.tier, str) or not self.tier or len(self.tier) > 64:
            raise _report_error("outcome")
        if (
            not isinstance(self.warnings, tuple)
            or len(self.warnings) > 32
            or not all(isinstance(warning, str) for warning in self.warnings)
        ):
            raise _report_error("outcome.warnings")
        object.__setattr__(
            self, "warnings", tuple(_sanitize_text(warning, "outcome.warnings") for warning in self.warnings)
        )
        object.__setattr__(self, "evidence", _sanitize_mapping(self.evidence, _EVIDENCE_FIELDS, "outcome.evidence"))
        object.__setattr__(
            self,
            "platform_metadata",
            _sanitize_mapping(self.platform_metadata, _METADATA_FIELDS, "outcome.platform_metadata"),
        )

    @property
    def case_id(self) -> str:
        """Return the stable runner-owned case identity."""
        return f"{self.operation_id}[{self.tier}][{self.mode}]"

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe case outcome envelope."""
        return {
            "operation_id": self.operation_id,
            "mode": self.mode,
            "capability": self.capability,
            "state": self.state,
            "tier": self.tier,
            "warnings": list(self.warnings),
            "evidence": _thaw_json(self.evidence),
            "platform_metadata": _thaw_json(self.platform_metadata),
        }

    @classmethod
    def from_dict(cls, value: object) -> CaseOutcome:
        """Decode one strict JSON-safe case outcome envelope."""
        fields = {"operation_id", "mode", "capability", "state", "tier", "warnings", "evidence", "platform_metadata"}
        if not isinstance(value, dict) or set(value) != fields:
            raise _report_error("outcome")
        warnings = value["warnings"]
        evidence = value["evidence"]
        metadata = value["platform_metadata"]
        if (
            not isinstance(value["operation_id"], str)
            or value["mode"] not in {"sync", "async"}
            or value["capability"] not in {"available", "unavailable"}
            or value["state"] not in _OUTCOME_STATES
            or not isinstance(value["tier"], str)
            or not isinstance(warnings, list)
            or not all(isinstance(warning, str) for warning in warnings)
            or not isinstance(evidence, dict)
            or not isinstance(metadata, dict)
        ):
            raise _report_error("outcome")
        return cls(
            operation_id=value["operation_id"],
            mode=cast(Literal["sync", "async"], value["mode"]),
            capability=cast(Literal["available", "unavailable"], value["capability"]),
            state=cast(Literal["passed", "failed", "blocked"], value["state"]),
            tier=value["tier"],
            warnings=tuple(warnings),
            evidence=evidence,
            platform_metadata=metadata,
        )


@dataclass(frozen=True, slots=True)
class ComplianceReport:
    """Schema-versioned aggregate evidence from catalog contract execution."""

    SCHEMA_VERSION: ClassVar[int] = 1

    outcomes: tuple[CaseOutcome, ...]
    connector_id: str | None = None
    manifest_version: str | None = None
    profile_version: str | None = None
    fixture_fingerprint: str | None = None
    contract_schema_version: str | None = None
    generated_at: str | None = None
    expected_case_ids: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    platform_metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.outcomes, tuple)
            or not self.outcomes
            or len(self.outcomes) > 4096
            or not all(isinstance(outcome, CaseOutcome) for outcome in self.outcomes)
            or len({outcome.case_id for outcome in self.outcomes}) != len(self.outcomes)
        ):
            raise _report_error("report.outcomes")
        for name in (
            "connector_id",
            "manifest_version",
            "profile_version",
            "fixture_fingerprint",
            "contract_schema_version",
            "generated_at",
        ):
            object.__setattr__(self, name, _identity(getattr(self, name), f"report.{name}"))
        expected = self.expected_case_ids or tuple(outcome.case_id for outcome in self.outcomes)
        if (
            not isinstance(expected, tuple)
            or len(expected) > 4096
            or not all(isinstance(case_id, str) and case_id and len(case_id) <= 384 for case_id in expected)
        ):
            raise _report_error("report.expected_case_ids")
        object.__setattr__(self, "expected_case_ids", tuple(sorted(set(expected))))
        if (
            not isinstance(self.warnings, tuple)
            or len(self.warnings) > 32
            or not all(isinstance(warning, str) for warning in self.warnings)
        ):
            raise _report_error("report.warnings")
        object.__setattr__(
            self, "warnings", tuple(_sanitize_text(warning, "report.warnings") for warning in self.warnings)
        )
        object.__setattr__(
            self,
            "platform_metadata",
            _sanitize_mapping(self.platform_metadata, _METADATA_FIELDS, "report.platform_metadata"),
        )

    @property
    def gaps(self) -> tuple[str, ...]:
        """Return explicit missing or non-passing required case evidence."""
        outcomes = {outcome.case_id: outcome for outcome in self.outcomes}
        return tuple(
            f"{case_id}: missing" if case_id not in outcomes else f"{case_id}: {outcomes[case_id].state}"
            for case_id in self.expected_case_ids
            if case_id not in outcomes or outcomes[case_id].state != "passed"
        )

    @property
    def is_compliant(self) -> bool:
        """Return whether all runner-owned required evidence passed."""
        return not self.gaps

    @property
    def coverage_by_mode(self) -> dict[str, int]:
        """Return deterministic outcome coverage counts by execution mode."""
        return self._coverage("mode")

    @property
    def coverage_by_state(self) -> dict[str, int]:
        """Return deterministic outcome coverage counts by execution state."""
        return self._coverage("state")

    @property
    def coverage_by_tier(self) -> dict[str, int]:
        """Return deterministic outcome coverage counts by declared contract tier."""
        return self._coverage("tier")

    def _coverage(self, field_name: Literal["mode", "state", "tier"]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for outcome in self.outcomes:
            value = getattr(outcome, field_name)
            counts[value] = counts.get(value, 0) + 1
        return dict(sorted(counts.items()))

    def to_dict(self) -> dict[str, object]:
        """Return a strict, deterministic, JSON-safe compliance report envelope."""
        return {
            "schema_version": self.SCHEMA_VERSION,
            "connector_id": self.connector_id,
            "manifest_version": self.manifest_version,
            "profile_version": self.profile_version,
            "fixture_fingerprint": self.fixture_fingerprint,
            "contract_schema_version": self.contract_schema_version,
            "generated_at": self.generated_at,
            "expected_case_ids": list(self.expected_case_ids),
            "outcomes": [outcome.to_dict() for outcome in self.outcomes],
            "warnings": list(self.warnings),
            "platform_metadata": _thaw_json(self.platform_metadata),
        }

    def write_json(self, path: Path | str) -> None:
        """Write this report only to the caller-selected local path."""
        Path(path).write_text(json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")), encoding="utf-8")

    @classmethod
    def from_dict(cls, value: object) -> ComplianceReport:
        """Decode one strict JSON-safe compliance report envelope."""
        fields = {
            "schema_version",
            "connector_id",
            "manifest_version",
            "profile_version",
            "fixture_fingerprint",
            "contract_schema_version",
            "generated_at",
            "expected_case_ids",
            "outcomes",
            "warnings",
            "platform_metadata",
        }
        if not isinstance(value, dict) or set(value) != fields or value["schema_version"] != cls.SCHEMA_VERSION:
            raise _report_error("report")
        identities = (
            "connector_id",
            "manifest_version",
            "profile_version",
            "fixture_fingerprint",
            "contract_schema_version",
            "generated_at",
        )
        if (
            not all(value[name] is None or isinstance(value[name], str) for name in identities)
            or not isinstance(value["expected_case_ids"], list)
            or not all(isinstance(case_id, str) for case_id in value["expected_case_ids"])
            or not isinstance(value["outcomes"], list)
            or not isinstance(value["warnings"], list)
            or not all(isinstance(warning, str) for warning in value["warnings"])
            or not isinstance(value["platform_metadata"], dict)
        ):
            raise _report_error("report")
        return cls(
            outcomes=tuple(CaseOutcome.from_dict(outcome) for outcome in value["outcomes"]),
            connector_id=cast(str | None, value["connector_id"]),
            manifest_version=cast(str | None, value["manifest_version"]),
            profile_version=cast(str | None, value["profile_version"]),
            fixture_fingerprint=cast(str | None, value["fixture_fingerprint"]),
            contract_schema_version=cast(str | None, value["contract_schema_version"]),
            generated_at=cast(str | None, value["generated_at"]),
            expected_case_ids=tuple(value["expected_case_ids"]),
            warnings=tuple(value["warnings"]),
            platform_metadata=value["platform_metadata"],
        )
