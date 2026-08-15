"""Public-contract tests for the canonical CKAN connector façade."""

from __future__ import annotations

import inspect
from datetime import date

import pytest

from datasluice.contracts.catalog.protocols import CatalogConnectorContext
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
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile, EffectiveCapabilityProfile, ProbeEvidence
from datasluice.domain.catalog.profiles import CredentialClassification, ProbeResponseClass, RoleClassification


class SyncExecutor:
    """Minimal synchronous executor for façade construction."""

    def execute(self, operation: object, guard: object) -> object:
        """Provide the injected executor seam."""
        return object()

    def close(self) -> None:
        """Provide explicit lifecycle closure."""


class AsyncExecutor:
    """Minimal asynchronous executor for façade construction."""

    async def execute(self, operation: object, guard: object) -> object:
        """Provide the injected executor seam."""
        return object()

    async def aclose(self) -> None:
        """Provide explicit lifecycle closure."""


def _effective_ckan_profile() -> EffectiveCapabilityProfile:
    operation = OperationSpec(
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
    declared = DeclaredCapabilityProfile(
        profile_version="2.11.5",
        schema_version="1.0",
        platform_api_version="Action API v3",
        official_source_uri="https://docs.ckan.org/en/2.11/api/",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint="fixture-fingerprint",
        operations={operation.id: operation},
    )
    evidence = ProbeEvidence(
        operation_id=operation.id,
        deployment_url="https://demo.ckan.org/api/3/action/package_list",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=ProbeResponseClass.SUCCESS,
    )
    return EffectiveCapabilityProfile.derive(declared, [evidence])


def _context(*, profile: EffectiveCapabilityProfile | None = None) -> CatalogConnectorContext:
    return CatalogConnectorContext(
        sync_executor=SyncExecutor(),  # type: ignore[arg-type]
        async_executor=AsyncExecutor(),  # type: ignore[arg-type]
        normalized_sync=object(),
        normalized_async=object(),
        native_sync=object(),
        native_async=object(),
        effective_profile=profile or _effective_ckan_profile(),
    )


def test_ckan_package_exports_only_its_adapter_and_factory() -> None:
    """The platform package is the sole canonical CKAN publication point."""
    import datasluice.connectors.catalog.ckan as ckan

    assert ckan.__all__ == ["CKANAdapter", "create_ckan_connector"]


def test_factory_accepts_canonical_context_and_validates_profile_identity() -> None:
    """The factory rejects non-CKAN profile evidence before exposing services."""
    from datasluice.connectors.catalog.ckan import CKANAdapter, create_ckan_connector

    adapter = create_ckan_connector(_context())

    assert isinstance(adapter, CKANAdapter)

    wrong_operation = OperationSpec(
        id=OperationId(platform="udata", service="datasets", method="get"),
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
        profile_version="17.3.0",
        schema_version="1.0",
        platform_api_version="uData API",
        official_source_uri="https://udata.readthedocs.io/en/stable/",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint="fixture-fingerprint",
        operations={wrong_operation.id: wrong_operation},
    )
    wrong_profile = EffectiveCapabilityProfile.derive(
        wrong_declared,
        [
            ProbeEvidence(
                operation_id=wrong_operation.id,
                deployment_url="https://example.test/api/1/datasets/",
                credential_classification=CredentialClassification.ANONYMOUS,
                role_classification=RoleClassification.ANONYMOUS,
                observed_response_class=ProbeResponseClass.SUCCESS,
            )
        ],
    )

    with pytest.raises(ValueError, match="CKAN"):
        create_ckan_connector(_context(profile=wrong_profile))


def test_adapter_exposes_injected_normalized_native_services_and_effective_profile() -> None:
    """The façade retains explicit projections for both execution modes."""
    from datasluice.connectors.catalog.ckan import create_ckan_connector

    context = _context()
    adapter = create_ckan_connector(context)

    assert adapter.normalized_sync is context.normalized_sync
    assert adapter.normalized_async is context.normalized_async
    assert adapter.native_sync is context.native_sync
    assert adapter.native_async is context.native_async
    assert adapter.effective_profile is context.effective_profile


def test_adapter_has_no_transport_default_or_raw_request_escape_hatch() -> None:
    """The façade cannot bypass the typed injected-executor boundary."""
    from datasluice.connectors.catalog.ckan import CKANAdapter

    forbidden = {"transport", "request", "get_json", "raw_request", "raw_response"}

    assert forbidden.isdisjoint(CKANAdapter.__dict__)
    assert "transport" not in inspect.signature(CKANAdapter).parameters
