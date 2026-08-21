"""Capability probing engine and cache tests."""

from __future__ import annotations

import asyncio
import threading
from datetime import date

import pytest

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    DeclaredCapabilityProfile,
    EffectiveCapabilityState,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.errors.catalog import (
    CatalogError,
    CatalogValidationError,
    ForbiddenError,
    UnauthenticatedError,
    UnsupportedCapabilityError,
)
from datasluice.runtime.capability import (
    EffectiveCapabilityCache,
    build_catalog_operation_guard,
)
from datasluice.runtime.clients import AsyncCatalogClient, SyncCatalogClient
from datasluice.runtime.constants import DEFAULT_CAPABILITY_CACHE_TTL_SECONDS
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse


class _Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


def _operation(method: str = "get") -> OperationSpec:
    operation = OperationId("reference", "datasets", method)
    return OperationSpec(
        id=operation,
        tier=OperationTier.NORMALIZED,
        request_type="DatasetRequest",
        response_type="DatasetRecord",
        auth_class=AuthClass.PUBLIC,
        mutation_class=MutationClass.READ,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.NONE,
        capability_class=CapabilityClass.CORE,
    )


def _profile(*operations: OperationSpec) -> DeclaredCapabilityProfile:
    return DeclaredCapabilityProfile(
        profile_version="v1",
        schema_version="v1",
        platform_api_version="v1",
        official_source_uri="https://example.test/source",
        source_accessed_at=date(2026, 8, 20),
        fixture_fingerprint="fixture-v1",
        operations={operation.id: operation for operation in operations},
    )


def _evidence(
    operation: OperationSpec, response_class: ProbeResponseClass = ProbeResponseClass.SUCCESS
) -> ProbeEvidence:
    return ProbeEvidence(
        operation_id=operation.id,
        deployment_url="https://catalog.example.test/api",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=response_class,
    )


class _CountingRunner:
    def __init__(self, evidence: ProbeEvidence | dict[OperationId, ProbeEvidence]) -> None:
        self.evidence = evidence
        self.calls = 0
        self._lock = threading.Lock()
        self.entered = threading.Event()
        self.release = threading.Event()

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        with self._lock:
            self.calls += 1
            self.entered.set()
        self.release.wait()
        if isinstance(self.evidence, dict):
            return self.evidence[operation_id]
        return self.evidence


class _AsyncCountingRunner:
    def __init__(self, evidence: ProbeEvidence) -> None:
        self.evidence = evidence
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return self.evidence


def test_first_resolve_probes_once_and_caches_the_effective_state() -> None:
    operation = _operation()
    runner = _CountingRunner(_evidence(operation))
    runner.release.set()
    cache = EffectiveCapabilityCache(_profile(operation), runner)

    first = cache.resolve(operation.id)
    second = cache.resolve(operation.id)

    assert first.for_operation(operation.id).state is EffectiveCapabilityState.CORE
    assert second.for_operation(operation.id).state is EffectiveCapabilityState.CORE
    assert runner.calls == 1


def test_ttl_expiry_reprobes_using_an_injected_clock() -> None:
    operation = _operation()
    runner = _CountingRunner(_evidence(operation))
    runner.release.set()
    clock = _Clock()
    cache = EffectiveCapabilityCache(_profile(operation), runner, ttl_seconds=5.0, clock=clock)

    cache.resolve(operation.id)
    clock.value = 105.1
    cache.resolve(operation.id)

    assert runner.calls == 2


def test_invalidate_drops_all_or_one_operation_entry() -> None:
    first_operation = _operation("get")
    second_operation = _operation("list")
    runner = _CountingRunner(
        {
            first_operation.id: _evidence(first_operation),
            second_operation.id: _evidence(second_operation),
        }
    )
    runner.release.set()
    cache = EffectiveCapabilityCache(_profile(first_operation, second_operation), runner)

    cache.resolve(first_operation.id)
    cache.resolve(second_operation.id)
    cache.invalidate(first_operation.id)
    cache.resolve(first_operation.id)
    cache.resolve(second_operation.id)
    cache.invalidate()
    cache.resolve(first_operation.id)
    cache.resolve(second_operation.id)

    assert runner.calls == 5


def test_threads_share_one_in_flight_probe_and_result() -> None:
    operation = _operation()
    runner = _CountingRunner(_evidence(operation))
    cache = EffectiveCapabilityCache(_profile(operation), runner)
    results: list[EffectiveCapabilityState] = []

    def resolve() -> None:
        results.append(cache.resolve(operation.id).for_operation(operation.id).state)

    threads = [threading.Thread(target=resolve) for _ in range(8)]
    for thread in threads:
        thread.start()
    assert runner.entered.wait(timeout=1.0)
    runner.release.set()
    for thread in threads:
        thread.join(timeout=1.0)

    assert runner.calls == 1
    assert results == [EffectiveCapabilityState.CORE] * 8
    assert all(not thread.is_alive() for thread in threads)


def test_async_tasks_share_one_in_flight_probe_and_result() -> None:
    async def exercise() -> None:
        operation = _operation()
        runner = _AsyncCountingRunner(_evidence(operation))
        cache = EffectiveCapabilityCache(_profile(operation), async_probe_runner=runner)
        tasks = [asyncio.create_task(cache.resolve_async(operation.id)) for _ in range(8)]
        await runner.entered.wait()
        runner.release.set()
        profiles = await asyncio.gather(*tasks)

        assert runner.calls == 1
        assert [profile.for_operation(operation.id).state for profile in profiles] == [
            EffectiveCapabilityState.CORE
        ] * 8

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("response_class", "expected_state", "expected_error"),
    [
        (ProbeResponseClass.UNSUPPORTED, EffectiveCapabilityState.UNSUPPORTED, UnsupportedCapabilityError),
        (ProbeResponseClass.UNAUTHORIZED, EffectiveCapabilityState.UNAUTHORIZED, UnauthenticatedError),
        (ProbeResponseClass.FORBIDDEN, EffectiveCapabilityState.FORBIDDEN, ForbiddenError),
        (ProbeResponseClass.UNAVAILABLE, EffectiveCapabilityState.UNAVAILABLE, UnsupportedCapabilityError),
        (
            ProbeResponseClass.DEPLOYMENT_DISABLED,
            EffectiveCapabilityState.DEPLOYMENT_DISABLED,
            UnsupportedCapabilityError,
        ),
    ],
)
def test_denied_probe_states_create_typed_guard_rejections(
    response_class: ProbeResponseClass,
    expected_state: EffectiveCapabilityState,
    expected_error: type[CatalogError],
) -> None:
    operation = _operation()
    runner = _CountingRunner(_evidence(operation, response_class))
    runner.release.set()
    cache = EffectiveCapabilityCache(_profile(operation), runner)
    guard = build_catalog_operation_guard(operation.id, cache.resolve(operation.id))

    with pytest.raises(expected_error) as raised:
        guard.require_allowed()

    assert raised.value.capability_state == expected_state.value
    assert raised.value.operation == str(operation.id)
    assert raised.value.platform == operation.id.platform
    assert raised.value.safe_action


def test_probe_evidence_with_an_unsanitized_url_is_rejected_as_a_typed_error() -> None:
    operation = _operation()
    evidence = object.__new__(ProbeEvidence)
    object.__setattr__(evidence, "operation_id", operation.id)
    object.__setattr__(evidence, "deployment_url", "https://catalog.example.test/api?api_key=secret")
    object.__setattr__(evidence, "credential_classification", CredentialClassification.ANONYMOUS)
    object.__setattr__(evidence, "role_classification", RoleClassification.ANONYMOUS)
    object.__setattr__(evidence, "observed_response_class", ProbeResponseClass.SUCCESS)

    class _UnsafeRunner:
        def probe(self, operation_id: OperationId) -> ProbeEvidence:
            return evidence

    with pytest.raises(CatalogValidationError) as raised:
        EffectiveCapabilityCache(_profile(operation), _UnsafeRunner()).resolve(operation.id)

    assert raised.value.operation == str(operation.id)
    assert raised.value.capability_state == "invalid-probe-evidence"
    assert raised.value.safe_action


def test_default_ttl_is_named_and_positive() -> None:
    assert DEFAULT_CAPABILITY_CACHE_TTL_SECONDS == 300.0
    assert DEFAULT_CAPABILITY_CACHE_TTL_SECONDS > 0


def test_guard_factory_returns_a_catalog_operation_guard() -> None:
    operation = _operation()
    runner = _CountingRunner(_evidence(operation))
    runner.release.set()
    cache = EffectiveCapabilityCache(_profile(operation), runner)

    guard = build_catalog_operation_guard(operation.id, cache.resolve(operation.id))

    assert isinstance(guard, CatalogOperationGuard)


def _request(operation: OperationId) -> CatalogOperationRequest:
    return CatalogOperationRequest(operation, {"url": "http://127.0.0.1:8000/datasets/fixture"})


def _guard(operation: OperationId) -> CatalogOperationGuard:
    return CatalogOperationGuard(operation_id=operation)


def _envelope() -> bytes:
    return (
        b'{"schema_version":1,"kind":"result_envelope","items":[{"schema_version":1,"kind":"dataset",'
        b'"id":{"schema_version":1,"kind":"catalog_id","platform":"reference","resource_kind":"dataset",'
        b'"value":"fixture"},"name":"Fixture dataset","description":null,"extensions":{}}],"page":null,'
        b'"warnings":[],"platform":null}'
    )


class _SyncTransport:
    def __init__(self, responses: list[RuntimeResponse]) -> None:
        self.responses = responses
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return self.responses.pop(0)

    def close(self) -> None:
        return None


class _AsyncTransport:
    def __init__(self, responses: list[RuntimeResponse]) -> None:
        self.responses = responses
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return self.responses.pop(0)

    async def aclose(self) -> None:
        return None


class _SequenceRunner:
    def __init__(self, operation: OperationSpec, response_classes: list[ProbeResponseClass]) -> None:
        self.operation = operation
        self.response_classes = response_classes
        self.calls: list[OperationId] = []

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.calls.append(operation_id)
        response_class = self.response_classes[min(len(self.calls) - 1, len(self.response_classes) - 1)]
        return _evidence(self.operation, response_class)


class _AsyncSequenceRunner:
    def __init__(self, operation: OperationSpec) -> None:
        self.operation = operation
        self.calls: list[OperationId] = []

    async def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.calls.append(operation_id)
        return _evidence(self.operation)


def test_sync_client_probes_once_per_operation_and_capability_does_not_send() -> None:
    operation = _operation()
    runner = _SequenceRunner(operation, [ProbeResponseClass.SUCCESS])
    transport = _SyncTransport([RuntimeResponse(200, {}, _envelope()), RuntimeResponse(200, {}, _envelope())])
    client = SyncCatalogClient(transport, _profile(operation), probe_runner=runner)

    client.get(_request(operation.id), _guard(operation.id))
    client.get(_request(operation.id), _guard(operation.id))

    assert runner.calls == [operation.id]
    assert len(transport.requests) == 2
    assert client.capability(str(operation.id)) == "available"
    assert len(transport.requests) == 2


@pytest.mark.parametrize(
    ("status_code", "expected_error", "response_class"),
    [
        (401, UnauthenticatedError, ProbeResponseClass.UNAUTHORIZED),
        (403, ForbiddenError, ProbeResponseClass.FORBIDDEN),
    ],
)
def test_sync_client_maps_auth_response_and_rejects_second_dispatch(
    status_code: int,
    expected_error: type[CatalogError],
    response_class: ProbeResponseClass,
) -> None:
    operation = _operation()
    runner = _SequenceRunner(operation, [ProbeResponseClass.SUCCESS])
    transport = _SyncTransport([RuntimeResponse(status_code, {}, b"")])
    client = SyncCatalogClient(transport, _profile(operation), probe_runner=runner)

    with pytest.raises(expected_error) as first:
        client.get(_request(operation.id), _guard(operation.id))
    with pytest.raises(expected_error) as second:
        client.get(_request(operation.id), _guard(operation.id))

    assert first.value.capability_state == response_class.value
    assert second.value.capability_state == response_class.value
    assert runner.calls == [operation.id]
    assert len(transport.requests) == 1


def test_sync_client_retains_post_dispatch_forbidden_state_without_probe_runner() -> None:
    operation = _operation()
    transport = _SyncTransport([RuntimeResponse(403, {}, b"")])
    client = SyncCatalogClient(transport, _profile(operation))

    with pytest.raises(ForbiddenError):
        client.get(_request(operation.id), _guard(operation.id))
    with pytest.raises(ForbiddenError):
        client.get(_request(operation.id), _guard(operation.id))

    assert len(transport.requests) == 1


def test_sync_client_capability_reports_deployment_disabled_without_transport_send() -> None:
    operation = _operation()
    runner = _SequenceRunner(operation, [ProbeResponseClass.DEPLOYMENT_DISABLED])
    transport = _SyncTransport([])
    client = SyncCatalogClient(transport, _profile(operation), probe_runner=runner)

    assert client.capability(str(operation.id)) == "deployment-disabled"
    assert transport.requests == []


def test_sync_client_invalidate_causes_a_new_probe() -> None:
    operation = _operation()
    runner = _SequenceRunner(operation, [ProbeResponseClass.SUCCESS, ProbeResponseClass.SUCCESS])
    transport = _SyncTransport([RuntimeResponse(200, {}, _envelope()), RuntimeResponse(200, {}, _envelope())])
    client = SyncCatalogClient(transport, _profile(operation), probe_runner=runner)

    client.get(_request(operation.id), _guard(operation.id))
    client.invalidate(str(operation.id))
    client.get(_request(operation.id), _guard(operation.id))

    assert runner.calls == [operation.id, operation.id]
    assert len(transport.requests) == 2


def test_async_client_uses_async_probe_runner_and_invalidate() -> None:
    async def exercise() -> None:
        operation = _operation()
        runner = _AsyncSequenceRunner(operation)
        transport = _AsyncTransport([RuntimeResponse(200, {}, _envelope()), RuntimeResponse(200, {}, _envelope())])
        client = AsyncCatalogClient(transport, _profile(operation), probe_runner=runner)

        await client.get(_request(operation.id))
        client.invalidate(str(operation.id))
        await client.get(_request(operation.id))

        assert runner.calls == [operation.id, operation.id]
        assert len(transport.requests) == 2

    asyncio.run(exercise())
