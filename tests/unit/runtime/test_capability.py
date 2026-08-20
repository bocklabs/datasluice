"""Capability probing engine and cache tests."""

from __future__ import annotations

import asyncio
import threading
from datetime import date

import pytest

from datasluice.contracts.catalog.protocols import CatalogOperationGuard
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
from datasluice.errors.catalog import CatalogValidationError, UnsupportedCapabilityError
from datasluice.runtime.capability import (
    EffectiveCapabilityCache,
    build_catalog_operation_guard,
)
from datasluice.runtime.constants import DEFAULT_CAPABILITY_CACHE_TTL_SECONDS


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
    ("response_class", "expected_state"),
    [
        (ProbeResponseClass.UNSUPPORTED, EffectiveCapabilityState.UNSUPPORTED),
        (ProbeResponseClass.UNAUTHORIZED, EffectiveCapabilityState.UNAUTHORIZED),
        (ProbeResponseClass.FORBIDDEN, EffectiveCapabilityState.FORBIDDEN),
        (ProbeResponseClass.UNAVAILABLE, EffectiveCapabilityState.UNAVAILABLE),
        (ProbeResponseClass.DEPLOYMENT_DISABLED, EffectiveCapabilityState.DEPLOYMENT_DISABLED),
    ],
)
def test_denied_probe_states_create_typed_guard_rejections(
    response_class: ProbeResponseClass, expected_state: EffectiveCapabilityState
) -> None:
    operation = _operation()
    runner = _CountingRunner(_evidence(operation, response_class))
    runner.release.set()
    cache = EffectiveCapabilityCache(_profile(operation), runner)
    guard = build_catalog_operation_guard(operation.id, cache.resolve(operation.id))

    with pytest.raises(UnsupportedCapabilityError) as raised:
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
