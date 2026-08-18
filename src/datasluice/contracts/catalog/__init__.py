"""Public executable contracts for catalog connectors."""

from datasluice.contracts.catalog.certification import CatalogCertification, certify_catalog_report
from datasluice.contracts.catalog.fixtures import ReferenceCase, ReferenceFixtureSet, load_reference_fixture_set
from datasluice.contracts.catalog.protocols import AsyncCatalogClient, SyncCatalogClient
from datasluice.contracts.catalog.report import CaseOutcome, ComplianceReport
from datasluice.contracts.catalog.runner import CatalogContractCase, catalog_contract_cases, run_catalog_contract
from datasluice.domain.catalog.extensions import CertificationRecord, ConnectorId, ConnectorManifest
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile

__all__ = [
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
