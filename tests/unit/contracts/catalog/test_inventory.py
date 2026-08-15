"""Contract tests for catalog operation and capability profile values."""

from datetime import date

import pytest

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


def _operation(
    *,
    method: str = "get_dataset",
    capability_class: CapabilityClass = CapabilityClass.CORE,
    mutation_class: MutationClass = MutationClass.READ,
) -> OperationSpec:
    operation_id = OperationId(platform="ckan", service="datasets", method=method)
    return OperationSpec(
        id=operation_id,
        tier=OperationTier.NATIVE,
        request_type="DatasetRequest",
        response_type="DatasetResponse",
        auth_class=AuthClass.PUBLIC,
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


def test_operation_spec_has_complete_typed_operation_taxonomy() -> None:
    operation = _operation()

    assert operation.id == OperationId(platform="ckan", service="datasets", method="get_dataset")
    assert operation.tier is OperationTier.NATIVE
    assert operation.request_type == "DatasetRequest"
    assert operation.response_type == "DatasetResponse"
    assert operation.auth_class is AuthClass.PUBLIC
    assert operation.mutation_class is MutationClass.READ
    assert operation.idempotency is Idempotency.SAFE
    assert operation.concurrency is ConcurrencyRequirement.NONE
    assert operation.atomicity is Atomicity.SINGLE_RESOURCE
    assert operation.capability_class is CapabilityClass.CORE


def test_declared_profile_rejects_duplicate_or_missing_operation_ids() -> None:
    operation = _operation()

    with pytest.raises(ValueError, match="missing operation IDs"):
        DeclaredCapabilityProfile(
            profile_version="1.0",
            schema_version="1.0",
            platform_api_version="3.0",
            official_source_uri="https://docs.ckan.org/en/latest/api/",
            source_accessed_at=date(2026, 8, 15),
            fixture_fingerprint="sha256:fixture",
            operations={},
        )

    with pytest.raises(ValueError, match="duplicate operation ID"):
        DeclaredCapabilityProfile(
            profile_version="1.0",
            schema_version="1.0",
            platform_api_version="3.0",
            official_source_uri="https://docs.ckan.org/en/latest/api/",
            source_accessed_at=date(2026, 8, 15),
            fixture_fingerprint="sha256:fixture",
            operations={OperationId(platform="ckan", service="datasets", method="alias"): operation},
        )


@pytest.mark.parametrize(
    ("capability_class", "response_class", "expected_state"),
    [
        (CapabilityClass.CORE, ProbeResponseClass.SUCCESS, EffectiveCapabilityState.CORE),
        (CapabilityClass.OPTIONAL, ProbeResponseClass.SUCCESS, EffectiveCapabilityState.OPTIONAL),
        (CapabilityClass.AUTHENTICATED, ProbeResponseClass.SUCCESS, EffectiveCapabilityState.AUTHENTICATED),
        (CapabilityClass.CORE, ProbeResponseClass.UNAUTHORIZED, EffectiveCapabilityState.UNAUTHORIZED),
        (CapabilityClass.CORE, ProbeResponseClass.FORBIDDEN, EffectiveCapabilityState.FORBIDDEN),
        (CapabilityClass.CORE, ProbeResponseClass.UNAVAILABLE, EffectiveCapabilityState.UNAVAILABLE),
        (
            CapabilityClass.CORE,
            ProbeResponseClass.DEPLOYMENT_DISABLED,
            EffectiveCapabilityState.DEPLOYMENT_DISABLED,
        ),
    ],
)
def test_effective_profile_preserves_operation_specific_probe_evidence(
    capability_class: CapabilityClass,
    response_class: ProbeResponseClass,
    expected_state: EffectiveCapabilityState,
) -> None:
    operation = _operation(capability_class=capability_class)
    profile = _declared_profile(operation)
    evidence = ProbeEvidence(
        operation_id=operation.id,
        deployment_url="https://catalog.example.test/api?token=discard-me",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=response_class,
    )

    effective = EffectiveCapabilityProfile.derive(profile, [evidence])
    capability = effective.for_operation(operation.id)

    assert capability.state is expected_state
    assert capability.evidence == evidence
    assert "token" not in capability.evidence.deployment_url


@pytest.mark.parametrize(
    ("response_class", "expected_remedy"),
    [
        (ProbeResponseClass.UNSUPPORTED, "Use a connector profile that declares the operation."),
        (ProbeResponseClass.DEPLOYMENT_DISABLED, "Enable the capability on the target deployment."),
        (ProbeResponseClass.UNAUTHORIZED, "Provide credentials with access to the operation."),
    ],
)
def test_unavailable_states_produce_safe_pre_dispatch_guard_decisions(
    response_class: ProbeResponseClass,
    expected_remedy: str,
) -> None:
    operation = _operation()
    profile = _declared_profile(operation)
    effective = EffectiveCapabilityProfile.derive(
        profile,
        [
            ProbeEvidence(
                operation_id=operation.id,
                deployment_url="https://catalog.example.test/api",
                credential_classification=CredentialClassification.ANONYMOUS,
                role_classification=RoleClassification.ANONYMOUS,
                observed_response_class=response_class,
            )
        ],
    )

    decision = effective.guard(operation.id)

    assert not decision.allowed
    assert decision.remedy == expected_remedy


def test_public_read_evidence_does_not_promote_mutation_permission() -> None:
    read_operation = _operation()
    write_operation = _operation(
        method="create_dataset",
        capability_class=CapabilityClass.AUTHENTICATED,
        mutation_class=MutationClass.CREATE,
    )
    profile = _declared_profile(read_operation, write_operation)
    read_evidence = ProbeEvidence(
        operation_id=read_operation.id,
        deployment_url="https://catalog.example.test/api",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=ProbeResponseClass.SUCCESS,
    )

    effective = EffectiveCapabilityProfile.derive(profile, [read_evidence])

    assert effective.for_operation(read_operation.id).state is EffectiveCapabilityState.CORE
    assert effective.for_operation(write_operation.id).state is EffectiveCapabilityState.UNAVAILABLE
    assert not effective.guard(write_operation.id).allowed
