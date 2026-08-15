"""Versioned JSON-safe report envelopes for catalog contract execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import ClassVar, Literal, cast

_METADATA_FIELDS = frozenset({"platform", "fixture", "environment", "profile_version"})
_OUTCOME_STATES = frozenset({"passed", "failed", "blocked"})


def _report_error(path: str) -> ValueError:
    return ValueError(f"Invalid catalog contract report at {path}")


def _freeze_json(value: object, path: str) -> object:
    if value is None or isinstance(value, bool) or type(value) is int:
        return value
    if isinstance(value, str):
        if len(value) > 256:
            raise _report_error(path)
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, nested in value.items():
            if not isinstance(key, str):
                raise _report_error(path)
            frozen[key] = _freeze_json(nested, f"{path}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (tuple, list)):
        return tuple(_freeze_json(nested, path) for nested in value)
    raise _report_error(path)


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(nested) for nested in value]
    return value


def _sanitize_metadata(value: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType(
        {
            key: _freeze_json(nested, f"platform_metadata.{key}")
            for key, nested in value.items()
            if key in _METADATA_FIELDS
        }
    )


@dataclass(frozen=True)
class CaseOutcome:
    """Immutable evidence for one catalog contract case execution."""

    operation_id: str
    mode: Literal["sync", "async"]
    capability: Literal["available", "unavailable"]
    state: Literal["passed", "failed", "blocked"]
    warnings: tuple[str, ...] = ()
    platform_metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.operation_id or self.mode not in {"sync", "async"}:
            raise _report_error("outcome")
        if self.capability not in {"available", "unavailable"} or self.state not in _OUTCOME_STATES:
            raise _report_error("outcome")
        if not isinstance(self.warnings, tuple) or not all(
            isinstance(warning, str) and len(warning) <= 256 for warning in self.warnings
        ):
            raise _report_error("outcome.warnings")
        object.__setattr__(self, "platform_metadata", _sanitize_metadata(self.platform_metadata))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe case outcome envelope."""
        return {
            "operation_id": self.operation_id,
            "mode": self.mode,
            "capability": self.capability,
            "state": self.state,
            "warnings": list(self.warnings),
            "platform_metadata": _thaw_json(self.platform_metadata),
        }

    @classmethod
    def from_dict(cls, value: object) -> CaseOutcome:
        """Decode one strict JSON-safe case outcome envelope."""
        if not isinstance(value, dict) or set(value) != {
            "operation_id",
            "mode",
            "capability",
            "state",
            "warnings",
            "platform_metadata",
        }:
            raise _report_error("outcome")
        warnings = value["warnings"]
        metadata = value["platform_metadata"]
        if (
            not isinstance(value["operation_id"], str)
            or value["mode"] not in {"sync", "async"}
            or value["capability"] not in {"available", "unavailable"}
            or value["state"] not in _OUTCOME_STATES
            or not isinstance(warnings, list)
            or not all(isinstance(warning, str) for warning in warnings)
            or not isinstance(metadata, dict)
        ):
            raise _report_error("outcome")
        return cls(
            operation_id=value["operation_id"],
            mode=cast(Literal["sync", "async"], value["mode"]),
            capability=cast(Literal["available", "unavailable"], value["capability"]),
            state=cast(Literal["passed", "failed", "blocked"], value["state"]),
            warnings=tuple(warnings),
            platform_metadata=metadata,
        )


@dataclass(frozen=True)
class ComplianceReport:
    """Schema-versioned aggregate evidence from catalog contract execution."""

    SCHEMA_VERSION: ClassVar[int] = 1

    outcomes: tuple[CaseOutcome, ...]
    warnings: tuple[str, ...] = ()
    platform_metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.outcomes, tuple)
            or not self.outcomes
            or not all(isinstance(outcome, CaseOutcome) for outcome in self.outcomes)
        ):
            raise _report_error("report.outcomes")
        if not isinstance(self.warnings, tuple) or not all(
            isinstance(warning, str) and len(warning) <= 256 for warning in self.warnings
        ):
            raise _report_error("report.warnings")
        object.__setattr__(self, "platform_metadata", _sanitize_metadata(self.platform_metadata))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe compliance report envelope."""
        return {
            "schema_version": self.SCHEMA_VERSION,
            "outcomes": [outcome.to_dict() for outcome in self.outcomes],
            "warnings": list(self.warnings),
            "platform_metadata": _thaw_json(self.platform_metadata),
        }

    @classmethod
    def from_dict(cls, value: object) -> ComplianceReport:
        """Decode one strict JSON-safe compliance report envelope."""
        if not isinstance(value, dict) or set(value) != {"schema_version", "outcomes", "warnings", "platform_metadata"}:
            raise _report_error("report")
        outcomes = value["outcomes"]
        warnings = value["warnings"]
        metadata = value["platform_metadata"]
        if (
            value["schema_version"] != cls.SCHEMA_VERSION
            or not isinstance(outcomes, list)
            or not isinstance(warnings, list)
            or not all(isinstance(warning, str) for warning in warnings)
            or not isinstance(metadata, dict)
        ):
            raise _report_error("report")
        return cls(
            outcomes=tuple(CaseOutcome.from_dict(outcome) for outcome in outcomes),
            warnings=tuple(warnings),
            platform_metadata=metadata,
        )
