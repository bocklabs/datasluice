"""Tests for retry and circuit-breaker runtime holders."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from datasluice.domain.catalog.resilience import CircuitKey, TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import BudgetExhaustedError, CatalogUnavailableError
from datasluice.runtime.clients import SyncCatalogClient
from datasluice.runtime.resilience import BreakerRegistry, DeadlineMonitor, RetryLoop
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure
from tests.unit.runtime.test_clients_sync import _envelope, _profile, _request


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


def _key() -> CircuitKey:
    return CircuitKey(origin="https://catalog.example", credential_scope="reference")


def _registry(clock: _Clock | None = None) -> BreakerRegistry:
    return BreakerRegistry(failure_threshold=5, cooldown=30.0, clock=clock or _Clock())


def _budget() -> TimeBudget:
    return TimeBudget(connect=1.0, read=1.0, write=1.0, total=10.0)


def test_breaker_opens_after_five_5xx_but_not_after_five_404s() -> None:
    failed = _registry()
    healthy = _registry()

    for _ in range(5):
        failed.record_response(_key(), 503)
        healthy.record_response(_key(), 404)

    assert failed.inspect(_key()).open
    assert healthy.inspect(_key()).failure_count == 0
    assert not healthy.inspect(_key()).open


def test_success_resets_an_occasional_transport_failure() -> None:
    registry = _registry()
    registry.record_transport_failure(_key())
    registry.record_success(_key())

    assert registry.inspect(_key()).failure_count == 0
    assert not registry.inspect(_key()).open


def test_half_open_admits_one_trial_then_closes_or_reopens() -> None:
    clock = _Clock()
    registry = _registry(clock)
    for _ in range(5):
        registry.record_response(_key(), 503)
    assert not registry.admit(_key())

    clock.value = 30.0
    assert registry.admit(_key())
    assert not registry.admit(_key())
    assert not registry.record_success(_key()).open

    for _ in range(5):
        registry.record_response(_key(), 503)
    clock.value = 60.0
    assert registry.admit(_key())
    assert registry.record_transport_failure(_key()).open
    assert not registry.admit(_key())


def test_concurrent_failure_records_are_consistent() -> None:
    registry = _registry()
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _: registry.record_transport_failure(_key()), range(8)))

    state = registry.inspect(_key())
    assert state.open
    assert state.failure_count == 8


def test_retry_loop_retries_rate_limit_wait_but_not_bad_request() -> None:
    clock = _Clock()
    deadline = DeadlineMonitor(_budget(), clock=clock)
    deadline.assert_dispatchable("reference/datasets.get", "reference")
    sleeps: list[float] = []
    responses = iter((RuntimeResponse(429, {}, b"", retry_after=2.0), RuntimeResponse(200, {}, b"ok")))
    loop = RetryLoop(
        budget=_budget(),
        idempotency=IdempotencyPolicy(safe=True),
        deadline=deadline,
        sleep=sleeps.append,
    )

    assert loop.run(lambda: next(responses)).status_code == 200
    assert sleeps == [2.0]
    assert loop.run(lambda: RuntimeResponse(400, {}, b"")).status_code == 400


def test_retry_loop_surfaces_terminal_transport_failure() -> None:
    deadline = DeadlineMonitor(_budget())
    deadline.assert_dispatchable("reference/datasets.get", "reference")
    loop = RetryLoop(
        budget=_budget(),
        idempotency=IdempotencyPolicy(safe=True),
        deadline=deadline,
        max_attempts=1,
        sleep=lambda _: None,
    )

    with pytest.raises(TransportFailure):
        loop.run(lambda: (_ for _ in ()).throw(TransportFailure("offline")))


class _SequenceTransport:
    def __init__(self, responses: list[RuntimeResponse], clock: _Clock | None = None) -> None:
        self.responses = responses
        self.clock = clock
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if self.clock is not None:
            self.clock.value += 1.0
        return self.responses.pop(0)

    def close(self) -> None:
        return None


def test_client_pipeline_retries_503_then_returns_successful_envelope() -> None:
    transport = _SequenceTransport([RuntimeResponse(503, {}, b""), RuntimeResponse(200, {}, _envelope())])
    client = SyncCatalogClient(transport, _profile(), retry_sleep=lambda _: None)

    result = client.get(_request())

    assert result.items[0].name == "Fixture dataset"
    assert len(transport.requests) == 2


def test_client_pipeline_exhausts_tiny_total_budget_between_attempts() -> None:
    clock = _Clock()
    transport = _SequenceTransport([RuntimeResponse(503, {}, b"")] * 3, clock)
    client = SyncCatalogClient(
        transport,
        _profile(),
        budget=TimeBudget(connect=1.0, read=1.0, write=1.0, total=3.0),
        clock=clock,
        retry_sleep=lambda _: None,
        max_attempts=5,
    )

    with pytest.raises(BudgetExhaustedError):
        client.get(_request())


def test_client_rejects_open_breaker_until_explicit_reset() -> None:
    registry = BreakerRegistry(failure_threshold=1, cooldown=30.0)
    transport = _SequenceTransport([RuntimeResponse(503, {}, b""), RuntimeResponse(200, {}, _envelope())])
    client = SyncCatalogClient(
        transport,
        _profile(),
        breakers=registry,
        max_attempts=1,
        retry_sleep=lambda _: None,
    )

    with pytest.raises(CatalogUnavailableError):
        client.get(_request())
    key = CircuitKey(origin="http://127.0.0.1:8000", credential_scope="anonymous")
    assert registry.inspect(key).failure_count == 1
    with pytest.raises(CatalogUnavailableError) as raised:
        client.get(_request())
    assert "cool-down" in raised.value.safe_action

    registry.reset(key)
    assert client.get(_request()).items[0].name == "Fixture dataset"
