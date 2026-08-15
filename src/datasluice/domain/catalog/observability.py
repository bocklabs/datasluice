"""Secure diagnostic, event, telemetry, TLS, and audit contracts."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

_REDACTED = "***"
_MAX_METADATA_ENTRIES = 32
_SENSITIVE_PARTS = frozenset(
    {"authorization", "credential", "token", "secret", "password", "cookie", "api_key", "body", "header"}
)


class FrozenMetadata(Mapping[str, object]):
    """An immutable redacted mapping that remains safe in dataclass serialization."""

    __slots__ = ("_values",)
    _values: Mapping[str, object]

    def __init__(self, values: Mapping[str, object] | None = None) -> None:
        self._values = MappingProxyType(dict(values or {}))

    def __getitem__(self, key: str) -> object:
        """Return one redacted metadata value."""
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate metadata keys."""
        return iter(self._values)

    def __len__(self) -> int:
        """Return the bounded metadata entry count."""
        return len(self._values)

    def __deepcopy__(self, memo: dict[int, object]) -> FrozenMetadata:
        """Return this immutable mapping during serialization copies."""
        return self


def _redact_metadata(values: Mapping[str, object]) -> FrozenMetadata:
    if len(values) > _MAX_METADATA_ENTRIES:
        raise ValueError("Event metadata exceeds the entry limit.")
    redacted: dict[str, object] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not key:
            raise ValueError("Event metadata keys must be non-empty strings.")
        normalized = key.lower().replace("-", "_")
        if any(part in normalized for part in _SENSITIVE_PARTS):
            redacted[key] = _REDACTED
        elif isinstance(value, Mapping):
            redacted[key] = _redact_metadata(value)
        elif isinstance(value, tuple | list):
            if len(value) > _MAX_METADATA_ENTRIES:
                raise ValueError("Event metadata sequence exceeds the entry limit.")
            redacted[key] = tuple(value)
        elif isinstance(value, str):
            redacted[key] = value[:256]
        else:
            redacted[key] = value
    return FrozenMetadata(redacted)


@dataclass(frozen=True, slots=True)
class TLSPolicy:
    """Verify TLS by default and scope insecure exceptions explicitly."""

    verify: bool = True
    override_scope: str | None = None

    def __post_init__(self) -> None:
        if type(self.verify) is not bool:
            raise ValueError("TLS verification must be a boolean.")
        if self.override_scope is not None and self.override_scope not in {"development", "private-pki"}:
            raise ValueError("TLS override scopes must be development or private-pki.")
        if not self.verify and self.override_scope is None:
            raise ValueError("Disabled TLS verification requires an explicit narrow override scope.")
        if self.verify and self.override_scope is not None:
            raise ValueError("TLS override scopes are only valid when verification is disabled.")


@dataclass(frozen=True, slots=True)
class DiagnosticPolicy:
    """Keep diagnostics redacted and raw response bodies explicitly bounded."""

    include_raw_body: bool = False
    raw_body_max_bytes: int | None = None

    def __post_init__(self) -> None:
        if type(self.include_raw_body) is not bool:
            raise ValueError("Raw diagnostic inclusion must be a boolean.")
        if self.include_raw_body:
            if type(self.raw_body_max_bytes) is not int or not 1 <= self.raw_body_max_bytes <= 65_536:
                raise ValueError("Raw diagnostics require an explicit byte bound no greater than 65536.")
        elif self.raw_body_max_bytes is not None:
            raise ValueError("Raw diagnostic byte bounds require raw diagnostics to be enabled.")

    def bound_raw_body(self, body: bytes) -> bytes:
        """Return a caller-requested raw body bounded by the configured limit."""
        if not self.include_raw_body or self.raw_body_max_bytes is None:
            raise ValueError("Raw diagnostics are not enabled.")
        if not isinstance(body, bytes):
            raise ValueError("Raw diagnostic bodies must be bytes.")
        return body[: self.raw_body_max_bytes]


@dataclass(frozen=True, slots=True)
class StructuredEvent:
    """A bounded local event whose metadata excludes credential-shaped values."""

    name: str
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("Structured events require non-empty names.")
        if not isinstance(self.metadata, Mapping):
            raise ValueError("Structured event metadata must be a mapping.")
        object.__setattr__(self, "metadata", _redact_metadata(self.metadata))


class EventHook(Protocol):
    """Receive one in-process structured event."""

    def __call__(self, event: StructuredEvent) -> None:
        """Handle a local structured event."""


@dataclass(frozen=True, slots=True)
class TelemetryPolicy:
    """Keep telemetry disabled unless a caller supplies a local exporter hook."""

    enabled: bool = False
    hook: EventHook | None = None

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise ValueError("Telemetry enabled state must be a boolean.")
        if self.enabled and self.hook is None:
            raise ValueError("Enabled telemetry requires a caller-supplied hook.")


class AuditSink(Protocol):
    """Persist caller-owned audit events only when explicitly supplied."""

    def record(self, event: StructuredEvent) -> None:
        """Persist one redacted structured event."""
