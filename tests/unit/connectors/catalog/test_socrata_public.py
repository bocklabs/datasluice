"""Public-contract tests for the canonical Socrata connector façade."""

from __future__ import annotations

import inspect
from datetime import date
from typing import cast

import pytest

from datasluice.contracts.catalog.protocols import (
    CatalogConnectorContext,
    CatalogOperationGuard,
    CatalogOperationRequest,
    SyncCatalogOperationExecutor,
)
from datasluice.domain.catalog.models import ResultEnvelope
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


class SyncExecutor:
    """Minimal synchronous executor for façade construction."""

    def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[object]:
        """Provide the injected executor seam."""
        return cast(ResultEnvelope[object], object())

    def close(self) -> None:
        """Provide explicit lifecycle closure."""


class AsyncExecutor:
    """Minimal asynchronous executor for façade construction."""

    async def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[object]:
        """Provide the injected executor seam."""
        return cast(ResultEnvelope[object], object())

    async def aclose(self) -> None:
        """Provide explicit lifecycle closure."""


def _effective_socrata_profile() -> EffectiveCapabilityProfile:
    operation = OperationSpec(
        id=OperationId(platform="socrata", service="soda", method="query"),
        tier=OperationTier.NATIVE,
        request_type="SocrataQueryRequest",
        response_type="NativeRecord",
        auth_class=AuthClass.AUTHENTICATED,
        mutation_class=MutationClass.READ,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.NONE,
        capability_class=CapabilityClass.CORE,
    )
    declared = DeclaredCapabilityProfile(
        profile_version="3.0",
        schema_version="1.0",
        platform_api_version="SODA 3",
        official_source_uri="https://dev.socrata.com/docs/endpoints",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint="fixture-fingerprint",
        operations={operation.id: operation},
    )
    evidence = ProbeEvidence(
        operation_id=operation.id,
        deployment_url="https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=1",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=ProbeResponseClass.SUCCESS,
    )
    return EffectiveCapabilityProfile.derive(declared, [evidence])


def _context(*, profile: EffectiveCapabilityProfile | None = None) -> CatalogConnectorContext:
    return CatalogConnectorContext(
        sync_executor=SyncExecutor(),
        async_executor=AsyncExecutor(),
        normalized_sync=object(),
        normalized_async=object(),
        native_sync=object(),
        native_async=object(),
        effective_profile=profile or _effective_socrata_profile(),
    )


def test_socrata_package_exports_only_its_adapter_and_factory() -> None:
    """The platform package is the sole canonical Socrata publication point."""
    import datasluice.connectors.catalog.socrata as socrata

    assert socrata.__all__ == ["SocrataAdapter", "create_socrata_connector"]


def test_factory_accepts_canonical_context_and_validates_profile_identity() -> None:
    """The factory rejects non-Socrata profile evidence before exposing services."""
    from datasluice.connectors.catalog.socrata import SocrataAdapter, create_socrata_connector

    adapter = create_socrata_connector(_context())

    assert isinstance(adapter, SocrataAdapter)

    wrong_operation = OperationSpec(
        id=OperationId(platform="ckan", service="datasets", method="get"),
        tier=OperationTier.NORMALIZED,
        request_type="DatasetGetRequest",
        response_type="DatasetRecord",
        auth_class=AuthClass.PUBLIC,
        mutation_class=MutationClass.READ,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.NONE,
        capability_class=CapabilityClass.CORE,
    )
    wrong_declared = DeclaredCapabilityProfile(
        profile_version="2.11.5",
        schema_version="1.0",
        platform_api_version="Action API v3",
        official_source_uri="https://docs.ckan.org/en/2.11/api/",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint="fixture-fingerprint",
        operations={wrong_operation.id: wrong_operation},
    )
    wrong_profile = EffectiveCapabilityProfile.derive(
        wrong_declared,
        [
            ProbeEvidence(
                operation_id=wrong_operation.id,
                deployment_url="https://demo.ckan.org/api/3/action/package_list",
                credential_classification=CredentialClassification.ANONYMOUS,
                role_classification=RoleClassification.ANONYMOUS,
                observed_response_class=ProbeResponseClass.SUCCESS,
            )
        ],
    )

    with pytest.raises(ValueError, match="Socrata"):
        create_socrata_connector(_context(profile=wrong_profile))


def test_factory_requires_both_typed_executor_modes() -> None:
    """A façade cannot silently fall back to an absent executor mode."""
    from datasluice.connectors.catalog.socrata import create_socrata_connector

    context = CatalogConnectorContext(
        sync_executor=cast(SyncCatalogOperationExecutor, object()),
        async_executor=AsyncExecutor(),
        normalized_sync=object(),
        normalized_async=object(),
        native_sync=object(),
        native_async=object(),
        effective_profile=_effective_socrata_profile(),
    )

    with pytest.raises(ValueError, match="synchronous"):
        create_socrata_connector(context)


def test_adapter_exposes_injected_normalized_native_services_and_effective_profile() -> None:
    """The façade retains explicit projections for both execution modes."""
    from datasluice.connectors.catalog.socrata import create_socrata_connector

    context = _context()
    adapter = create_socrata_connector(context)

    assert adapter.normalized_sync is context.normalized_sync
    assert adapter.normalized_async is context.normalized_async
    assert adapter.native_sync is context.native_sync
    assert adapter.native_async is context.native_async
    assert adapter.effective_profile is context.effective_profile


def test_adapter_has_no_soda_two_compatibility_or_raw_request_escape_hatch() -> None:
    """The façade cannot expose SODA 2 compatibility or untyped HTTP helpers."""
    from datasluice.connectors.catalog.socrata import SocrataAdapter

    forbidden = {"SODA2Adapter", "transport", "request", "get_json", "raw_request", "raw_response"}

    assert forbidden.isdisjoint(SocrataAdapter.__dict__)
    assert "transport" not in inspect.signature(SocrataAdapter).parameters
