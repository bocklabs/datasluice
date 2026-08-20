"""Tests for the optional OpenTelemetry bridge."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from datasluice.runtime.events import OtelBridge

Telemetry = tuple[OtelBridge, InMemorySpanExporter, InMemoryMetricReader]


@pytest.fixture
def telemetry() -> Telemetry:
    """Configure in-memory caller-owned exporters for one bridge test."""
    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    trace.set_tracer_provider(tracer_provider)
    metric_reader = InMemoryMetricReader()
    from opentelemetry import metrics

    metrics.set_meter_provider(MeterProvider(metric_readers=(metric_reader,)))
    return OtelBridge(), span_exporter, metric_reader


def test_bridge_is_noop_without_a_caller_sdk() -> None:
    """API defaults produce no exported spans when no SDK is configured."""
    bridge = OtelBridge()

    bridge.emitter().record(
        operation_id="reference/datasets/get",
        platform="reference",
        outcome="succeeded",
    )


def test_bridge_derives_spans_metrics_and_trace_context(telemetry: Telemetry) -> None:
    """Bridge exports only redacted envelope attributes and active correlation."""
    bridge, span_exporter, metric_reader = telemetry
    emitter = bridge.emitter()
    tracer = trace.get_tracer("test")

    with tracer.start_as_current_span("parent") as parent:
        envelope = emitter.record(
            operation_id="reference/datasets/get",
            platform="reference",
            outcome="breaker_state_change",
            metadata={"retry_count": 2, "budget_usage": 3.5},
        )
        assert envelope.correlation_ids["trace_id"] == f"{parent.get_span_context().trace_id:032x}"

    spans = span_exporter.get_finished_spans()
    runtime_span = next(span for span in spans if span.name == "catalog.request")
    assert runtime_span.attributes is not None
    assert runtime_span.attributes["datasluice.operation_id"] == "reference/datasets/get"
    assert runtime_span.attributes["datasluice.platform"] == "reference"
    metrics_data = metric_reader.get_metrics_data()
    assert metrics_data is not None
    metric_names = {
        metric.name
        for resource in metrics_data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    assert {"datasluice.retry.count", "datasluice.breaker.state_changes", "datasluice.budget.usage"} <= metric_names


def test_bridge_missing_extra_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit bridge construction names the telemetry extra when API import fails."""
    import builtins

    original_import = builtins.__import__

    def blocked_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = (),
        level: int = 0,
    ) -> object:
        if name == "opentelemetry":
            raise ImportError("missing")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    with pytest.raises(ImportError, match=r"datasluice\[telemetry\]"):
        OtelBridge()
