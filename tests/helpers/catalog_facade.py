from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Protocol, cast

from datasluice.contracts.catalog.protocols import (
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    CatalogOperationGuard,
    CatalogOperationRequest,
    SyncCatalogOperationExecutor,
)
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
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)

if TYPE_CHECKING:
    from datasluice.domain.catalog.models import ResultEnvelope

SOURCE_ACCESSED_AT = date(2026, 8, 15)


class SyncExecutor:
    def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[object]:
        return cast("ResultEnvelope[object]", object())

    def close(self) -> None:
        return None


class AsyncExecutor:
    async def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[object]:
        return cast("ResultEnvelope[object]", object())

    async def aclose(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class ProfileSpec:
    platform: str
    service: str
    method: str
    tier: OperationTier
    request_type: str
    response_type: str
    auth_class: AuthClass
    profile_version: str
    platform_api_version: str
    official_source_uri: str
    deployment_url: str


class ServiceProjections(Protocol):
    @property
    def normalized_sync(self) -> object: ...

    @property
    def normalized_async(self) -> object: ...

    @property
    def native_sync(self) -> object: ...

    @property
    def native_async(self) -> object: ...

    @property
    def effective_profile(self) -> EffectiveCapabilityProfile: ...


def effective_profile(spec: ProfileSpec) -> EffectiveCapabilityProfile:
    operation = OperationSpec(
        id=OperationId(platform=spec.platform, service=spec.service, method=spec.method),
        tier=spec.tier,
        request_type=spec.request_type,
        response_type=spec.response_type,
        auth_class=spec.auth_class,
        mutation_class=MutationClass.READ,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.NONE,
        capability_class=CapabilityClass.CORE,
    )
    declared = DeclaredCapabilityProfile(
        profile_version=spec.profile_version,
        schema_version="1.0",
        platform_api_version=spec.platform_api_version,
        official_source_uri=spec.official_source_uri,
        source_accessed_at=SOURCE_ACCESSED_AT,
        fixture_fingerprint="fixture-fingerprint",
        operations={operation.id: operation},
    )
    evidence = ProbeEvidence(
        operation_id=operation.id,
        deployment_url=spec.deployment_url,
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=ProbeResponseClass.SUCCESS,
    )
    return EffectiveCapabilityProfile.derive(declared, [evidence])


def facade_context(
    profile: EffectiveCapabilityProfile,
    *,
    sync_executor: SyncCatalogOperationExecutor | None = None,
    async_executor: AsyncCatalogOperationExecutor | None = None,
) -> CatalogConnectorContext:
    return CatalogConnectorContext(
        sync_executor=sync_executor if sync_executor is not None else SyncExecutor(),
        async_executor=async_executor if async_executor is not None else AsyncExecutor(),
        normalized_sync=object(),
        normalized_async=object(),
        native_sync=object(),
        native_async=object(),
        effective_profile=profile,
    )


def context_missing_sync_executor(profile: EffectiveCapabilityProfile) -> CatalogConnectorContext:
    return facade_context(profile, sync_executor=cast("SyncCatalogOperationExecutor", object()))


def context_invalid_async_executor(profile: EffectiveCapabilityProfile) -> CatalogConnectorContext:
    return facade_context(profile, async_executor=cast("AsyncCatalogOperationExecutor", object()))


def assert_retained_projections(context: CatalogConnectorContext, projections: ServiceProjections) -> None:
    assert projections.normalized_sync is context.normalized_sync
    assert projections.normalized_async is context.normalized_async
    assert projections.native_sync is context.native_sync
    assert projections.native_async is context.native_async
    assert projections.effective_profile is context.effective_profile


def assert_no_transport_escape_hatch(connector: type[object], forbidden: frozenset[str]) -> None:
    assert forbidden.isdisjoint(connector.__dict__)
    assert "transport" not in inspect.signature(connector).parameters
