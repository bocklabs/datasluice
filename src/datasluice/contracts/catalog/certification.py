"""Evidence-bound catalog certification validation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from datasluice.contracts.catalog.fixtures import ReferenceFixtureSet
from datasluice.contracts.catalog.report import ComplianceReport
from datasluice.contracts.catalog.runner import CatalogContractCase
from datasluice.domain.catalog.extensions import ConnectorId, ConnectorManifest
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile


@dataclass(frozen=True, slots=True)
class CatalogCertification:
    """Immutable proof that a report satisfies one manifest and fixture binding."""

    connector_id: ConnectorId
    profile_version: str
    fixture_fingerprint: str
    contract_schema_version: str
    report_fingerprint: str
    outcome_count: int


def certify_catalog_report(
    *,
    manifest: ConnectorManifest,
    profile: DeclaredCapabilityProfile,
    fixture_set: ReferenceFixtureSet,
    cases: Iterable[CatalogContractCase],
    report: ComplianceReport,
    selected_connector_id: ConnectorId | None,
) -> CatalogCertification:
    """Certify complete runner-owned evidence without discovering or activating plugins."""
    manifest.require_activation(selected_connector_id)
    if manifest.connector_id.platform != fixture_set.platform:
        raise ValueError("Certification connector platform must match the pinned fixture platform.")
    if (
        manifest.profile_version != fixture_set.profile_version
        or profile.profile_version != fixture_set.profile_version
    ):
        raise ValueError("Certification profile version must match the pinned fixture profile.")
    if profile.fixture_fingerprint != fixture_set.fingerprint:
        raise ValueError("Certification fixture fingerprint must match the pinned fixture set.")
    expected_case_ids = tuple(sorted(case.pytest_id for case in cases))
    if not expected_case_ids or len(set(expected_case_ids)) != len(expected_case_ids):
        raise ValueError("Certification requires a finite unique set of declared contract cases.")
    if (
        report.connector_id != str(manifest.connector_id)
        or report.profile_version != fixture_set.profile_version
        or report.fixture_fingerprint != fixture_set.fingerprint
        or report.contract_schema_version != str(ComplianceReport.SCHEMA_VERSION)
    ):
        raise ValueError(
            "Certification report identity must match the manifest, profile, fixture, and contract schema."
        )
    if tuple(report.expected_case_ids) != expected_case_ids or {outcome.case_id for outcome in report.outcomes} != set(
        expected_case_ids
    ):
        raise ValueError("Certification requires complete case evidence from the declared runner matrix.")
    if not report.is_compliant:
        raise ValueError("Certification requires a compliant report with every required case passing.")
    if manifest.certification is not None and (
        manifest.certification.contract_schema_version != report.contract_schema_version
        or manifest.certification.profile_version != report.profile_version
        or manifest.certification.report_id != report.report_id
    ):
        raise ValueError("Certification manifest metadata must bind the exact compliant report.")
    return CatalogCertification(
        connector_id=manifest.connector_id,
        profile_version=fixture_set.profile_version,
        fixture_fingerprint=fixture_set.fingerprint,
        contract_schema_version=str(ComplianceReport.SCHEMA_VERSION),
        report_fingerprint=report.fingerprint,
        outcome_count=len(report.outcomes),
    )
