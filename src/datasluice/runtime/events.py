"""Versioned, redacted runtime events and caller-owned in-process sinks."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, cast

from datasluice.domain.catalog.models import _freeze_json, _object_dict, _thaw_json
from datasluice.runtime.redaction import redact_event_metadata

_EVENT_KEYS = frozenset(
    {"schema_version", "kind", "operation_id", "platform", "outcome", "correlation_ids", "metadata"}
)


def _required_text(value: object, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Runtime event {path} must be a non-empty string.")
    return value


def _frozen_mapping(value: object, path: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Runtime event {path} must be a mapping.")
    frozen = _freeze_json(value, f"runtime_event.{path}")
    if not isinstance(frozen, Mapping):
        raise ValueError(f"Runtime event {path} must be a mapping.")
    return frozen


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """One strict schema-v1 redacted runtime event."""

    operation_id: str
    platform: str
    outcome: str
    correlation_ids: Mapping[str, object] = field(default_factory=dict)
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _required_text(self.operation_id, "operation_id")
        _required_text(self.platform, "platform")
        _required_text(self.outcome, "outcome")
        object.__setattr__(self, "correlation_ids", _frozen_mapping(self.correlation_ids, "correlation_ids"))
        object.__setattr__(self, "metadata", _frozen_mapping(self.metadata, "metadata"))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe schema-v1 event envelope."""
        return {
            "schema_version": 1,
            "kind": "runtime_event",
            "operation_id": self.operation_id,
            "platform": self.platform,
            "outcome": self.outcome,
            "correlation_ids": _thaw_json(self.correlation_ids),
            "metadata": _thaw_json(self.metadata),
        }

    @classmethod
    def from_dict(cls, value: object) -> EventEnvelope:
        """Decode one strict schema-v1 event envelope."""
        data = _object_dict(value, "runtime_event")
        if set(data) != _EVENT_KEYS or data["schema_version"] != 1 or type(data["schema_version"]) is not int:
            raise ValueError("Invalid schema-v1 runtime event envelope.")
        if data["kind"] != "runtime_event":
            raise ValueError("Invalid schema-v1 runtime event envelope.")
        return cls(
            operation_id=_required_text(data["operation_id"], "operation_id"),
            platform=_required_text(data["platform"], "platform"),
            outcome=_required_text(data["outcome"], "outcome"),
            correlation_ids=_object_dict(data["correlation_ids"], "runtime_event.correlation_ids"),
            metadata=_object_dict(data["metadata"], "runtime_event.metadata"),
        )


class EventSink(Protocol):
    """Receive one already-redacted event envelope."""

    def record(self, event: EventEnvelope) -> None:
        """Record an event envelope."""


class ListSink:
    """Keep a bounded in-process sequence of redacted event envelopes."""

    def __init__(self, *, max_events: int = 1000) -> None:
        if type(max_events) is not int or max_events < 1:
            raise ValueError("List sinks require a positive event bound.")
        self._events: deque[EventEnvelope] = deque(maxlen=max_events)

    @property
    def events(self) -> tuple[EventEnvelope, ...]:
        """Return a stable oldest-first snapshot of retained events."""
        return tuple(self._events)

    def record(self, event: EventEnvelope) -> None:
        """Keep one redacted event envelope."""
        self._events.append(event)


class LoggingSink:
    """Log only serialized redacted event envelopes at INFO."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("datasluice.runtime.events")

    def record(self, event: EventEnvelope) -> None:
        """Log one redacted event envelope."""
        self._logger.info("%s", event.to_dict())


class EventEmitter:
    """Create gate-redacted envelopes and fan them out in registration order."""

    def __init__(self, *, sinks: tuple[EventSink | Callable[[EventEnvelope], None], ...] = ()) -> None:
        self._sinks = sinks

    def record(
        self,
        *,
        operation_id: str,
        platform: str,
        outcome: str,
        metadata: Mapping[str, object] | None = None,
        correlation_ids: Mapping[str, object] | None = None,
    ) -> EventEnvelope:
        """Redact metadata once, then send the resulting envelope to every sink."""
        envelope = EventEnvelope(
            operation_id=operation_id,
            platform=platform,
            outcome=outcome,
            correlation_ids=MappingProxyType(dict(correlation_ids or {})),
            metadata=redact_event_metadata(metadata or {}),
        )
        for sink in self._sinks:
            if callable(sink):
                hook = cast(Callable[[EventEnvelope], None], sink)
                hook(envelope)
            else:
                sink.record(envelope)
        return envelope
