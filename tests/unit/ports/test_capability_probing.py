"""Per-operation capability guard probing for the canonical catalog contract."""

from __future__ import annotations

from datetime import date
from typing import cast

import pytest

from datasluice.contracts.catalog.protocols import (
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    CatalogOperationGuard,
    CatalogOperationRequest,
    SyncCatalogOperationExecutor,
    SyncManagedExecutor,
)
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import CatalogPlatform
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
    EffectiveCapabilityProfile,
    EffectiveCapabilityState,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.errors.catalog import ForbiddenError, UnauthenticatedError, UnsupportedCapabilityError


class _RecordingSyncExecutor:
    """Structural sync executor double that records dispatched operations."""

    def __init__(self) -> None:
        self.dispatched: list[str] = []

    def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> object:
        self.dispatched.append(str(operation.operation_id))
        return object()

    def close(self) -> None:
        return None


class _RecordingAsyncExecutor:
    """Structural async executor double that never dispatches in these tests."""

    async def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> object:
        return object()

    async def aclose(self) -> None:
        return None


def _operation(
    *,
    method: str = "get_dataset",
    capability_class: CapabilityClass = CapabilityClass.CORE,
    mutation_class: MutationClass = MutationClass.READ,
) -> OperationSpec:
    return OperationSpec(
        id=OperationId(platform="ckan", service="datasets", method=method),
        tier=OperationTier.NORMALIZED,
        request_type="DatasetRequest",
        response_type="DatasetResponse",
        auth_class=AuthClass.PUBLIC if capability_class is not CapabilityClass.ADMIN else AuthClass.PRIVILEGED,
        mutation_class=mutation_class,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.SINGLE_RESOURCE,
        capability_class=capability_class,
    )


def _declared_profile(*operations: OperationSpec) -> DeclaredCapabilityProfile:
    return DeclaredCapabilityProfile(
        profile_version="1.0",
        schema_version="1.0",
        platform_api_version="3.0",
        official_source_uri="https://docs.ckan.org/en/latest/api/",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint="sha256:fixture",
        operations={operation.id: operation for operation in operations},
    )


def _evidence(operation: OperationSpec, response_class: ProbeResponseClass) -> ProbeEvidence:
    return ProbeEvidence(
        operation_id=operation.id,
        deployment_url="https://catalog.example.test/api",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=response_class,
    )


def _effective(
    operation: OperationSpec,
    profile: DeclaredCapabilityProfile,
    response_class: ProbeResponseClass,
) -> EffectiveCapabilityProfile:
    return EffectiveCapabilityProfile.derive(profile, [_evidence(operation, response_class)])


@pytest.mark.parametrize(
    ("response_class", "expected_state", "expected_remedy"),
    [
        (
            ProbeResponseClass.UNAVAILABLE,
            EffectiveCapabilityState.UNAVAILABLE,
            "Retry after the target deployment is available.",
        ),
        (
            ProbeResponseClass.UNAUTHORIZED,
            EffectiveCapabilityState.UNAUTHORIZED,
            "Provide credentials with access to the operation.",
        ),
        (
            ProbeResponseClass.FORBIDDEN,
            EffectiveCapabilityState.FORBIDDEN,
            "Use credentials with the required role for the operation.",
        ),
        (
            ProbeResponseClass.DEPLOYMENT_DISABLED,
            EffectiveCapabilityState.DEPLOYMENT_DISABLED,
            "Enable the capability on the target deployment.",
        ),
    ],
)
def test_controlled_operation_states_fail_the_guard_with_exact_remedies(
    response_class: ProbeResponseClass,
    expected_state: EffectiveCapabilityState,
    expected_remedy: str,
) -> None:
    """Unavailable, unauthorized, forbidden, and unverified-controlled states deny dispatch."""
    operation = _operation()
    profile = _declared_profile(operation)
    effective = _effective(operation, profile, response_class)

    decision = effective.guard(operation.id)

    assert decision.operation_id is operation.id
    assert decision.state is expected_state
    assert decision.allowed is False
    assert decision.remedy == expected_remedy


def test_guard_decisions_are_per_operation_not_global_booleans() -> None:
    """One profile can allow one OperationId while denying another in the same deployment."""
    read_operation = _operation(method="get_dataset")
    write_operation = _operation(
        method="create_dataset",
        capability_class=CapabilityClass.AUTHENTICATED,
        mutation_class=MutationClass.CREATE,
    )
    profile = _declared_profile(read_operation, write_operation)
    effective = EffectiveCapabilityProfile.derive(
        profile,
        [
            _evidence(read_operation, ProbeResponseClass.SUCCESS),
            _evidence(write_operation, ProbeResponseClass.FORBIDDEN),
        ],
    )

    read_decision = effective.guard(read_operation.id)
    write_decision = effective.guard(write_operation.id)

    assert read_decision.state is EffectiveCapabilityState.CORE
    assert read_decision.allowed is True
    assert read_decision.remedy is None
    assert write_decision.state is EffectiveCapabilityState.FORBIDDEN
    assert write_decision.allowed is False


@pytest.mark.parametrize(
    "capability_class",
    [CapabilityClass.CORE, CapabilityClass.OPTIONAL, CapabilityClass.AUTHENTICATED, CapabilityClass.ADMIN],
)
def test_evidence_backed_states_dispatch_with_their_declared_classes(
    capability_class: CapabilityClass,
) -> None:
    """Successful probe evidence maps each OperationId to its declared capability class."""
    operation = _operation(capability_class=capability_class)
    profile = _declared_profile(operation)
    effective = _effective(operation, profile, ProbeResponseClass.SUCCESS)

    decision = effective.guard(operation.id)

    expected_state = EffectiveCapabilityState(capability_class.value)
    assert decision.state is expected_state
    assert decision.allowed is True
    assert decision.remedy is None


def test_undeclared_operation_id_is_unsupported_with_a_profile_remedy() -> None:
    """Guarding an OperationId missing from the profile is denied as unsupported."""
    operation = _operation()
    profile = _declared_profile(operation)
    effective = _effective(operation, profile, ProbeResponseClass.SUCCESS)
    undeclared = OperationId(platform="ckan", service="datasets", method="purge_dataset")

    decision = effective.guard(undeclared)

    assert decision.state is EffectiveCapabilityState.UNSUPPORTED
    assert decision.allowed is False
    assert decision.remedy == "Use a connector profile that declares the operation."


def test_missing_probe_evidence_denies_the_operation_as_unavailable() -> None:
    """A declared OperationId without probe evidence never dispatches as a silent success."""
    operation = _operation()
    profile = _declared_profile(operation)

    effective = EffectiveCapabilityProfile.derive(profile, [])

    assert effective.for_operation(operation.id).state is EffectiveCapabilityState.UNAVAILABLE
    assert not effective.guard(operation.id).allowed


@pytest.mark.parametrize(
    ("response_class", "expected_state"),
    [
        (ProbeResponseClass.UNAVAILABLE, EffectiveCapabilityState.UNAVAILABLE),
        (ProbeResponseClass.UNAUTHORIZED, EffectiveCapabilityState.UNAUTHORIZED),
        (ProbeResponseClass.FORBIDDEN, EffectiveCapabilityState.FORBIDDEN),
        (ProbeResponseClass.DEPLOYMENT_DISABLED, EffectiveCapabilityState.DEPLOYMENT_DISABLED),
    ],
)
def test_denied_states_raise_typed_errors_before_executor_dispatch(
    response_class: ProbeResponseClass,
    expected_state: EffectiveCapabilityState,
) -> None:
    """require_allowed raises a typed catalog error with a remedy before the executor runs."""
    operation = _operation()
    profile = _declared_profile(operation)
    effective = _effective(operation, profile, response_class)
    executor = _RecordingSyncExecutor()
    context = CatalogConnectorContext(
        sync_executor=cast(SyncCatalogOperationExecutor, executor),
        async_executor=cast(AsyncCatalogOperationExecutor, _RecordingAsyncExecutor()),
    )
    request = CatalogOperationRequest(operation_id=operation.id)
    guard = CatalogOperationGuard(operation_id=operation.id, profile=effective)

    with pytest.raises(UnsupportedCapabilityError) as excinfo:
        SyncManagedExecutor(context).execute(request, guard)

    error = excinfo.value
    assert error.operation == "ckan/datasets.get_dataset"
    assert error.platform == "ckan"
    assert error.capability_state == expected_state.value
    assert error.safe_action is not None
    assert error.safe_action == effective.guard(operation.id).remedy
    assert executor.dispatched == []


def test_allowed_guard_dispatches_through_the_managed_executor() -> None:
    """An allowed per-operation guard reaches the injected executor exactly once."""
    operation = _operation()
    profile = _declared_profile(operation)
    effective = _effective(operation, profile, ProbeResponseClass.SUCCESS)
    executor = _RecordingSyncExecutor()
    context = CatalogConnectorContext(
        sync_executor=cast(SyncCatalogOperationExecutor, executor),
        async_executor=cast(AsyncCatalogOperationExecutor, _RecordingAsyncExecutor()),
    )
    request = CatalogOperationRequest(operation_id=operation.id)
    guard = CatalogOperationGuard(operation_id=operation.id, profile=effective)

    SyncManagedExecutor(context).execute(request, guard)

    assert executor.dispatched == ["ckan/datasets.get_dataset"]


def test_permission_guard_raises_typed_unauthenticated_failure() -> None:
    """Anonymous effective permissions reject controlled work before dispatch."""
    operation = _operation()
    permissions = EffectivePermissions(platform=CatalogPlatform.CKAN)
    guard = CatalogOperationGuard(operation_id=operation.id, permissions=permissions)

    with pytest.raises(UnauthenticatedError) as excinfo:
        guard.require_allowed()

    error = excinfo.value
    assert error.capability_state == "unauthorized"
    assert error.safe_action == "Provide valid credentials and retry the operation."
    assert error.operation == "ckan/datasets.get_dataset"


def test_permission_guard_raises_typed_forbidden_failure_for_missing_scope() -> None:
    """Authenticated credentials without the required scope fail as forbidden."""
    operation = _operation(method="create_dataset", mutation_class=MutationClass.CREATE)
    permissions = EffectivePermissions(
        platform=CatalogPlatform.CKAN,
        scopes=frozenset({"datasets:read"}),
        authenticated=True,
        operation_scopes={"ckan/datasets.create_dataset": frozenset({"datasets:write"})},
    )
    guard = CatalogOperationGuard(operation_id=operation.id, permissions=permissions)

    with pytest.raises(ForbiddenError) as excinfo:
        guard.require_allowed()

    error = excinfo.value
    assert error.capability_state == "forbidden"
    assert error.safe_action == "Use credentials with the required scope or role."


def test_permission_guard_allows_satisfied_operation_scopes() -> None:
    """Effective permissions holding the required scope permit the operation."""
    operation = _operation()
    permissions = EffectivePermissions(
        platform=CatalogPlatform.CKAN,
        scopes=frozenset({"datasets:read"}),
        authenticated=True,
        operation_scopes={"ckan/datasets.get_dataset": frozenset({"datasets:read"})},
    )
    guard = CatalogOperationGuard(operation_id=operation.id, permissions=permissions)

    guard.require_allowed()
