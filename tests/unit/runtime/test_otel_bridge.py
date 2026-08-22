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
def telemetry(monkeypatch: pytest.MonkeyPatch) -> Telemetry:
    """Snapshot-and-restore caller-owned OTel providers for one bridge test."""
    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=(metric_reader,))
    monkeypatch.setattr("opentelemetry.trace._TRACER_PROVIDER", tracer_provider)
    monkeypatch.setattr("opentelemetry.metrics._internal._METER_PROVIDER", meter_provider)
    return OtelBridge(), span_exporter, metric_reader


def test_bridge_is_noop_without_a_caller_sdk() -> None:
    """API defaults produce no exported spans when no SDK is configured."""
    bridge = OtelBridge()

    envelope = bridge.emitter().record(
        operation_id="reference/datasets/get",
        platform="reference",
        outcome="succeeded",
    )

    assert envelope.correlation_ids == {}


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
    assert set(runtime_span.attributes) == {
        "datasluice.operation_id",
        "datasluice.platform",
        "datasluice.outcome",
        "datasluice.correlation.trace_id",
        "datasluice.correlation.span_id",
    }
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
    metric_attributes = [
        {key: value for key, value in point.attributes.items()}
        for resource in metrics_data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        for point in metric.data.data_points
        if point.attributes is not None
    ]
    assert metric_attributes
    for attributes in metric_attributes:
        assert set(attributes) <= {
            "datasluice.operation_id",
            "datasluice.platform",
            "datasluice.outcome",
            "datasluice.retry_count",
        }
        assert attributes["datasluice.operation_id"] == "reference/datasets/get"


def test_bridge_metric_attributes_stay_bounded_for_high_retry_counts(telemetry: Telemetry) -> None:
    """Retry counts beyond the bounded attribute set stay out of metric series."""
    bridge, _, metric_reader = telemetry
    emitter = bridge.emitter()

    emitter.record(
        operation_id="reference/datasets/get",
        platform="reference",
        outcome="failed",
        metadata={"retry_count": 999},
    )

    metrics_data = metric_reader.get_metrics_data()
    assert metrics_data is not None
    points = [
        {key: value for key, value in point.attributes.items()}
        for resource in metrics_data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.data is not None
        for point in metric.data.data_points
        if point.attributes is not None
    ]
    retry_points = [point for point in points if "datasluice.retry_count" in point]
    assert retry_points == []


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
