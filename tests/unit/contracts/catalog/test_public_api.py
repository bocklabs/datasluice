"""Public API stability tests for the catalog contract tracer."""

from __future__ import annotations

import inspect
import os

import pytest

import datasluice
import datasluice.contracts as contracts
import datasluice.contracts.catalog as catalog
from datasluice.domain.catalog import CatalogPlatform
from datasluice.errors.catalog import NativeCatalogError, map_catalog_error


def test_catalog_contract_package_exports_only_the_documented_contract_surface() -> None:
    """The package exposes only deliberate contract types and Protocols."""
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
    assert all(hasattr(catalog, name) for name in catalog.__all__)


def test_runner_signature_and_report_schema_version_are_stable() -> None:
    """The runner and report contracts are inspectable and locked."""
    parameters = inspect.signature(catalog.run_catalog_contract).parameters

    assert list(parameters) == ["case", "sync_client", "async_client", "fixture_set"]
    assert parameters["case"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["sync_client"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["async_client"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["fixture_set"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["fixture_set"].default is None
    assert catalog.ComplianceReport.SCHEMA_VERSION == 1


def test_contracts_root_does_not_reexport_the_replacement_runner() -> None:
    """The catalog runner is available only from its explicit public package."""
    assert not hasattr(contracts, "run_catalog_contract")


@pytest.mark.skipif(
    os.environ.get("DATASLUICE_TDD_RED") == "1",
    reason="package root export implementation pending GREEN phase",
)
def test_package_root_exposes_canonical_models_and_errors_without_legacy_runtime_symbols() -> None:
    """The root boundary contains only retained data-plane and catalog contract values."""
    expected_catalog_symbols = {
        "CatalogId",
        "CatalogPlatform",
        "ResourceKind",
        "DatasetRecord",
        "NativeRecord",
        "ResultEnvelope",
        "CatalogError",
        "NativeCatalogError",
        "UnsupportedCapabilityError",
        "UnauthenticatedError",
        "ForbiddenError",
        "CatalogUnavailableError",
    }
    retired_symbols = {
        "AdapterError",
        "AdapterNotFoundError",
        "AuthenticationError",
        "CatalogCapabilities",
        "CatalogResourceLocator",
        "Portal",
        "PortalDetectionError",
        "PortalError",
        "RateLimitError",
    }

    assert expected_catalog_symbols <= set(datasluice.__all__)
    assert all(hasattr(datasluice, symbol) for symbol in expected_catalog_symbols)
    assert retired_symbols.isdisjoint(datasluice.__all__)
    assert all(not hasattr(datasluice, symbol) for symbol in retired_symbols)


def test_normalized_catalog_errors_preserve_redacted_typed_context_and_cause() -> None:
    """The root error boundary maps native failures without exposing raw metadata."""
    native = NativeCatalogError(
        "request failed",
        operation="datasets.get",
        platform=CatalogPlatform.CKAN,
        status_code=503,
        metadata={"authorization": "secret", "request_id": "request-1"},
    )
    error = map_catalog_error(native)

    assert native.metadata["authorization"] == "***"
    assert error.operation == "datasets.get"
    assert error.platform == "ckan"
    assert error.capability_state == "unavailable"
    assert error.safe_action == "Retry after the deployment is available."
    assert error.__cause__ is native
