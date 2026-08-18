"""Tests for public catalog connector certification."""

from __future__ import annotations

import importlib
from dataclasses import replace
from datetime import date
from typing import cast

import pytest

import datasluice.contracts.catalog as catalog
from datasluice.contracts.catalog.fakes import AsyncReferenceConnector, SyncReferenceConnector
from datasluice.contracts.catalog.fixtures import ReferenceFixtureSet, load_reference_fixture_set
from datasluice.contracts.catalog.protocols import AsyncCatalogClient, SyncCatalogClient
from datasluice.contracts.catalog.runner import catalog_contract_cases, run_catalog_contract
from datasluice.domain.catalog.extensions import (
    ActivationPolicy,
    CertificationRecord,
    ConnectorId,
    ConnectorManifest,
    OptionalInstallRequirement,
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
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile


def _profile(fixture_set: ReferenceFixtureSet) -> DeclaredCapabilityProfile:
    operation_id = OperationId(platform=fixture_set.platform, service="catalog", method="get")
    operation = OperationSpec(
        id=operation_id,
        tier=OperationTier.NORMALIZED,
        request_type="CatalogRequest",
        response_type="CatalogResponse",
        auth_class=AuthClass.PUBLIC,
        mutation_class=MutationClass.READ,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.SINGLE_RESOURCE,
        capability_class=CapabilityClass.CORE,
    )
    return DeclaredCapabilityProfile(
        profile_version=fixture_set.profile_version,
        schema_version="1",
        platform_api_version="fixture",
        official_source_uri="https://example.test/catalog",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint=fixture_set.fingerprint,
        operations={operation_id: operation},
    )


def _manifest(
    connector_id: ConnectorId, fixture_set: ReferenceFixtureSet, report_id: str | None = None
) -> ConnectorManifest:
    certification = None
    requirements: tuple[OptionalInstallRequirement, ...] = ()
    if not connector_id.is_builtin:
        requirements = (
            OptionalInstallRequirement(
                extra="acme-socrata", install_hint="Install DataSluice with `datasluice[acme-socrata]`."
            ),
        )
        certification = CertificationRecord(
            connector_id=connector_id,
            contract_schema_version="1",
            profile_version=fixture_set.profile_version,
            report_version="1",
            report_id=report_id or "sha256:pending",
        )
    return ConnectorManifest(
        connector_id=connector_id,
        entry_point="datasluice.connectors.catalog:create_connector",
        profile_version=fixture_set.profile_version,
        optional_requirements=requirements,
        certification=certification,
        activation_policy=ActivationPolicy.EXPLICIT,
    )


def _report(fixture_set: ReferenceFixtureSet):
    return run_catalog_contract(
        catalog_contract_cases(fixture_set),
        sync_client=cast(SyncCatalogClient, SyncReferenceConnector(fixture_set)),
        async_client=cast(AsyncCatalogClient, AsyncReferenceConnector(fixture_set)),
        fixture_set=fixture_set,
    )


@pytest.mark.parametrize("platform", ["ckan", "udata", "socrata"])
def test_builtin_connectors_share_one_certification_runner_and_schema(platform: str) -> None:
    """All maintained fixtures certify through the same public contract API."""
    fixture_set = load_reference_fixture_set(platform)
    report = _report(fixture_set)
    manifest = _manifest(ConnectorId.parse(f"datasluice/{platform}"), fixture_set)

    certification = catalog.certify_catalog_report(
        manifest=manifest,
        profile=_profile(fixture_set),
        fixture_set=fixture_set,
        cases=catalog_contract_cases(fixture_set),
        report=report,
        selected_connector_id=manifest.connector_id,
    )

    assert certification.connector_id == manifest.connector_id
    assert certification.report_fingerprint == report.fingerprint
    assert certification.outcome_count == len(report.outcomes)


def test_third_party_namespaced_fake_uses_the_identical_report_and_certification_schema() -> None:
    """Third parties are certified by evidence rather than built-in status."""
    fixture_set = load_reference_fixture_set("socrata")
    connector_id = ConnectorId.parse("acme/socrata")
    report = replace(_report(fixture_set), connector_id=str(connector_id))
    manifest = _manifest(connector_id, fixture_set, report_id=report.report_id)

    certification = catalog.certify_catalog_report(
        manifest=manifest,
        profile=_profile(fixture_set),
        fixture_set=fixture_set,
        cases=catalog_contract_cases(fixture_set),
        report=report,
        selected_connector_id=connector_id,
    )

    assert certification.connector_id == connector_id
    assert report.to_dict()["schema_version"] == catalog.ComplianceReport.SCHEMA_VERSION


def test_certification_rejects_implicit_activation_mismatched_bindings_and_incomplete_evidence() -> None:
    """Certification requires explicit selection and every runner-owned binding."""
    fixture_set = load_reference_fixture_set("socrata")
    connector_id = ConnectorId.parse("acme/socrata")
    report = replace(_report(fixture_set), connector_id=str(connector_id))
    manifest = _manifest(connector_id, fixture_set, report_id=report.report_id)
    profile = _profile(fixture_set)
    cases = catalog_contract_cases(fixture_set)

    with pytest.raises(ValueError, match="explicit caller selection"):
        catalog.certify_catalog_report(
            manifest=manifest,
            profile=profile,
            fixture_set=fixture_set,
            cases=cases,
            report=report,
            selected_connector_id=None,
        )
    with pytest.raises(ValueError, match="fixture fingerprint"):
        catalog.certify_catalog_report(
            manifest=manifest,
            profile=replace(profile, fixture_fingerprint="other"),
            fixture_set=fixture_set,
            cases=cases,
            report=report,
            selected_connector_id=connector_id,
        )
    with pytest.raises(ValueError, match="complete case evidence"):
        catalog.certify_catalog_report(
            manifest=manifest,
            profile=profile,
            fixture_set=fixture_set,
            cases=cases,
            report=replace(report, expected_case_ids=report.expected_case_ids[:-1]),
            selected_connector_id=connector_id,
        )


def test_catalog_exports_only_the_documented_contract_surface_without_optional_imports() -> None:
    """The public package remains inspectable and import-light."""
    assert catalog.__all__ == [
        "catalog_contract_cases",
        "run_catalog_contract",
        "certify_catalog_report",
        "CatalogContractCase",
        "CaseOutcome",
        "ComplianceReport",
        "CatalogCertification",
        "ConnectorId",
        "ConnectorManifest",
        "CertificationRecord",
        "DeclaredCapabilityProfile",
        "ReferenceCase",
        "ReferenceFixtureSet",
        "load_reference_fixture_set",
        "SyncCatalogClient",
        "AsyncCatalogClient",
    ]
    assert importlib.import_module("datasluice.contracts.catalog") is catalog
