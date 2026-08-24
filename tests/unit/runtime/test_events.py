"""Tests for versioned runtime event envelopes and in-process sinks."""

from __future__ import annotations

import asyncio
import logging

import pytest

from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.clients import AsyncCatalogClient, SyncCatalogClient
from datasluice.runtime.events import EventEmitter, EventEnvelope, ListSink, LoggingSink
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.unit.runtime._fixtures import _envelope, _guard, _profile, _request, _Transport


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


@pytest.mark.parametrize(
    ("schema_version", "kind"),
    [
        (2, "runtime_event"),
        ("1", "runtime_event"),
        (1.0, "runtime_event"),
        (True, "runtime_event"),
        (1, "mutation_receipt"),
        (None, None),
    ],
)
def test_event_envelope_rejects_invalid_schema_version_or_kind(schema_version: object, kind: object) -> None:
    """Wrong schema_version values or kinds fail closed before decoding fields."""
    encoded = {
        "schema_version": schema_version,
        "kind": kind,
        "operation_id": "reference/datasets/get",
        "platform": "reference",
        "outcome": "succeeded",
        "correlation_ids": {},
        "metadata": {},
    }

    with pytest.raises(ValueError):
        EventEnvelope.from_dict(encoded)


@pytest.mark.parametrize("encoded", (None, [], {"correlation_ids": [], "metadata": {}}))
def test_event_envelope_normalizes_non_mapping_inputs_to_value_error(encoded: object) -> None:
    with pytest.raises(ValueError, match="schema-v1 runtime event"):
        EventEnvelope.from_dict(encoded)


def test_emitter_redacts_before_fanning_out_to_sinks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every sink observes a redacted envelope exactly once in registration order."""
    monkeypatch.delenv("DATASLUICE_NO_REDACT", raising=False)
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
    assert envelope.metadata == {"detail": "authorization: Bearer ***"}
    serialized = str(envelope.to_dict())
    assert "aBcDeFgH1234" not in serialized
    assert "Bearer ***" in serialized


def test_emitter_redacts_merged_correlation_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Caller and provider correlation ids pass the same redaction gate as metadata."""
    monkeypatch.delenv("DATASLUICE_NO_REDACT", raising=False)

    def provider() -> dict[str, object]:
        return {"trace_id": "a" * 32}

    sink = ListSink()
    emitter = EventEmitter(sinks=(sink,), correlation_id_provider=provider)

    envelope = emitter.record(
        operation_id="reference/datasets/get",
        platform="reference",
        outcome="succeeded",
        correlation_ids={
            "authorization": "Bearer aBcDeFgH1234",
            "request_id": "request-123",
            "trace_id": "caller-trace",
        },
    )

    assert envelope.correlation_ids["authorization"] == "***"
    assert envelope.correlation_ids["request_id"] == "request-123"
    assert envelope.correlation_ids["trace_id"] == "caller-trace"


def test_emitter_contains_a_failing_correlation_provider(caplog: pytest.LogCaptureFixture) -> None:
    def failing_provider() -> dict[str, object]:
        raise RuntimeError("provider exploded")

    emitter = EventEmitter(correlation_id_provider=failing_provider)
    with caplog.at_level(logging.ERROR, logger="datasluice.runtime.events"):
        envelope = emitter.record(operation_id="op", platform="reference", outcome="succeeded")

    assert envelope.correlation_ids == {}
    assert "provider exploded" in caplog.text


def test_emitter_contains_failing_sinks_and_continues_dispatch(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """One bad sink never breaks dispatch or masks the emitted envelope."""

    def failing_sink(event: EventEnvelope) -> None:
        raise RuntimeError("sink exploded")

    good = ListSink()
    emitter = EventEmitter(sinks=(failing_sink, good))
    with caplog.at_level(logging.ERROR, logger="datasluice.runtime.events"):
        envelope = emitter.record(operation_id="op", platform="reference", outcome="succeeded")

    assert good.events == (envelope,)
    assert "sink exploded" in caplog.text


def test_logging_sink_guards_eager_serialization(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serialization is deferred until INFO logging is enabled."""
    logger = logging.getLogger("datasluice.runtime.events.guard-test")
    logger.setLevel(logging.WARNING)
    sink = LoggingSink(logger)
    calls: list[EventEnvelope] = []
    original = EventEnvelope.to_dict

    def counting_to_dict(self: EventEnvelope) -> dict[str, object]:
        calls.append(self)
        return original(self)

    monkeypatch.setattr(EventEnvelope, "to_dict", counting_to_dict)
    envelope = EventEnvelope(operation_id="reference/datasets/get", platform="reference", outcome="succeeded")

    sink.record(envelope)
    assert calls == []

    logger.setLevel(logging.INFO)
    sink.record(envelope)
    assert calls == [envelope]


def test_sync_client_emits_one_outcome_envelope() -> None:
    """Client dispatch emits an outcome through a caller-provided emitter."""
    sink = ListSink()
    client = SyncCatalogClient(
        _Transport(RuntimeResponse(200, {}, _envelope())),
        _profile(),
        emitter=EventEmitter(sinks=(sink,)),
    )

    client.get(_request(), _guard())

    assert len(sink.events) == 1
    assert sink.events[0].operation_id == "reference/datasets.get"
    assert sink.events[0].outcome == "succeeded"


def test_sync_client_emits_failed_outcome_exactly_once_on_error() -> None:
    """The error path emits one failed outcome instead of suppressing or duplicating it."""
    sink = ListSink()
    client = SyncCatalogClient(
        _Transport(RuntimeResponse(400, {}, b"bad request")),
        _profile(),
        emitter=EventEmitter(sinks=(sink,)),
    )

    with pytest.raises(CatalogValidationError):
        client.get(_request(), _guard())

    assert [event.outcome for event in sink.events] == ["failed"]


class _AsyncTransport:
    def __init__(self, response: RuntimeResponse) -> None:
        self.response = response

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self.response

    async def aclose(self) -> None:
        await asyncio.sleep(0)


def test_async_client_emits_failed_outcome_exactly_once_on_error() -> None:
    """The async error path mirrors the sync single failed emission contract."""

    async def exercise() -> tuple[str, ...]:
        sink = ListSink()
        client = AsyncCatalogClient(
            _AsyncTransport(RuntimeResponse(400, {}, b"bad request")),
            _profile(),
            emitter=EventEmitter(sinks=(sink,)),
        )
        with pytest.raises(CatalogValidationError):
            await client.get(_request(), _guard())
        return tuple(event.outcome for event in sink.events)

    assert asyncio.run(exercise()) == ("failed",)
