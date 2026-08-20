"""Tests for versioned runtime event envelopes and in-process sinks."""

from __future__ import annotations

import pytest

from datasluice.runtime.events import EventEmitter, EventEnvelope, ListSink


def test_event_envelope_round_trips_with_exact_schema() -> None:
    """A versioned event envelope remains JSON-safe and strict."""
    envelope = EventEnvelope(
        operation_id="reference/datasets/get",
        platform="reference",
        outcome="succeeded",
        correlation_ids={"request_id": "request-123"},
        metadata={"status_code": 200, "attempts": 1},
    )

    encoded = envelope.to_dict()

    assert set(encoded) == {
        "schema_version",
        "kind",
        "operation_id",
        "platform",
        "outcome",
        "correlation_ids",
        "metadata",
    }
    assert EventEnvelope.from_dict(encoded) == envelope


@pytest.mark.parametrize(
    "encoded",
    (
        {
            "schema_version": 1,
            "kind": "runtime_event",
            "operation_id": "reference/datasets/get",
            "platform": "reference",
            "outcome": "succeeded",
            "correlation_ids": {},
            "metadata": {},
            "unexpected": True,
        },
        {
            "kind": "runtime_event",
            "operation_id": "reference/datasets/get",
            "platform": "reference",
            "outcome": "succeeded",
            "correlation_ids": {},
            "metadata": {},
        },
    ),
)
def test_event_envelope_rejects_non_exact_schema(encoded: dict[str, object]) -> None:
    """Unknown and missing envelope schema keys fail closed."""
    with pytest.raises(ValueError):
        EventEnvelope.from_dict(encoded)


def test_emitter_redacts_before_fanning_out_to_sinks() -> None:
    """Every sink observes a redacted envelope exactly once in registration order."""
    first = ListSink()
    second = ListSink()
    emitter = EventEmitter(sinks=(first, second))

    envelope = emitter.record(
        operation_id="reference/datasets/get",
        platform="reference",
        outcome="succeeded",
        metadata={"detail": "authorization: Bearer aBcDeFgH1234"},
        correlation_ids={"request_id": "request-123"},
    )

    assert first.events == (envelope,)
    assert second.events == (envelope,)
    serialized = str(envelope.to_dict())
    assert "aBcDeFgH1234" not in serialized
    assert "Bearer ***" in serialized
