"""Versioned, redacted runtime events and caller-owned in-process sinks."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, cast

from datasluice.domain.catalog.models import _freeze_json, _object_dict, _thaw_json
from datasluice.exceptions import DataSluiceError
from datasluice.runtime.redaction import redact_event_metadata

_LOGGER = logging.getLogger(__name__)

_MAX_BOUNDED_RETRY_COUNT = 16

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
        try:
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
        except DataSluiceError as exc:
            raise ValueError("Invalid schema-v1 runtime event envelope.") from exc


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
        """Log one redacted event envelope when INFO logging is enabled."""
        if self._logger.isEnabledFor(logging.INFO):
            self._logger.info("%s", event.to_dict())


class EventEmitter:
    """Create gate-redacted envelopes and fan them out in registration order."""

    def __init__(
        self,
        *,
        sinks: tuple[EventSink | Callable[[EventEnvelope], None], ...] = (),
        correlation_id_provider: Callable[[], Mapping[str, object]] | None = None,
    ) -> None:
        self._sinks = sinks
        self._correlation_id_provider = correlation_id_provider

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
        active_correlation_ids: Mapping[str, object] = {}
        if self._correlation_id_provider is not None:
            try:
                active_correlation_ids = self._correlation_id_provider()
            except Exception:
                _LOGGER.exception("Runtime correlation id provider failed; continuing without correlation ids.")
        merged_correlation_ids = {**active_correlation_ids, **(correlation_ids or {})}
        envelope = EventEnvelope(
            operation_id=operation_id,
            platform=platform,
            outcome=outcome,
            correlation_ids=MappingProxyType(dict(redact_event_metadata(merged_correlation_ids))),
            metadata=redact_event_metadata(metadata or {}),
        )
        for sink in self._sinks:
            try:
                if callable(sink):
                    hook = cast(Callable[[EventEnvelope], None], sink)
                    hook(envelope)
                else:
                    sink.record(envelope)
            except Exception:
                _LOGGER.exception("Runtime event sink %r failed; continuing event dispatch.", sink)
        return envelope


class OtelBridge:
    """Adapt redacted envelopes to caller-configured OpenTelemetry APIs."""

    def __init__(self) -> None:
        try:
            from opentelemetry import metrics, trace
        except ImportError as exc:
            raise ImportError("OtelBridge requires the telemetry extra. Install with: datasluice[telemetry]") from exc
        self._trace = trace
        self._tracer = trace.get_tracer("datasluice.runtime")
        meter = metrics.get_meter("datasluice.runtime")
        self._retry_counter = meter.create_counter("datasluice.retry.count")
        self._breaker_counter = meter.create_counter("datasluice.breaker.state_changes")
        self._budget_histogram = meter.create_histogram("datasluice.budget.usage")

    def correlation_ids(self) -> Mapping[str, object]:
        """Return the active trace identifiers when a caller SDK has created one."""
        span_context = self._trace.get_current_span().get_span_context()
        if not span_context.is_valid:
            return {}
        return {"trace_id": f"{span_context.trace_id:032x}", "span_id": f"{span_context.span_id:016x}"}

    def emitter(self, *, sinks: tuple[EventSink | Callable[[EventEnvelope], None], ...] = ()) -> EventEmitter:
        """Create an emitter that correlates envelopes and exports to this bridge."""
        return EventEmitter(sinks=(*sinks, self), correlation_id_provider=self.correlation_ids)

    def record(self, event: EventEnvelope) -> None:
        """Record one redacted envelope as a short-lived span and optional metrics."""
        span_type = event.metadata.get("span_type", "request")
        name = f"catalog.{span_type}" if isinstance(span_type, str) else "catalog.request"
        with self._tracer.start_as_current_span(name, attributes=self._span_attributes(event)):
            pass
        self._record_metrics(event)

    def _span_attributes(self, event: EventEnvelope) -> dict[str, str | int | float | bool]:
        attributes: dict[str, str | int | float | bool] = {
            "datasluice.operation_id": event.operation_id,
            "datasluice.platform": event.platform,
            "datasluice.outcome": event.outcome,
        }
        for key, value in event.correlation_ids.items():
            if isinstance(value, str | int | float | bool):
                attributes[f"datasluice.correlation.{key}"] = value
        return attributes

    def _metric_attributes(self, event: EventEnvelope) -> dict[str, str | int]:
        attributes: dict[str, str | int] = {
            "datasluice.operation_id": event.operation_id,
            "datasluice.platform": event.platform,
            "datasluice.outcome": event.outcome,
        }
        retry_count = event.metadata.get("retry_count")
        if type(retry_count) is int and 0 < retry_count <= _MAX_BOUNDED_RETRY_COUNT:
            attributes["datasluice.retry_count"] = retry_count
        return attributes

    def _record_metrics(self, event: EventEnvelope) -> None:
        attributes = self._metric_attributes(event)
        retry_count = event.metadata.get("retry_count")
        if type(retry_count) is int and retry_count > 0:
            self._retry_counter.add(retry_count, attributes=attributes)
        if event.outcome == "breaker_state_change":
            self._breaker_counter.add(1, attributes=attributes)
        budget_usage = event.metadata.get("budget_usage")
        if (type(budget_usage) is int or type(budget_usage) is float) and budget_usage >= 0:
            self._budget_histogram.record(float(budget_usage), attributes=attributes)
