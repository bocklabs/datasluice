"""Public API stability tests for the catalog contract tracer."""

from __future__ import annotations

import inspect
import os

import pytest

import datasluice
import datasluice.contracts as contracts
import datasluice.contracts.catalog as catalog
from datasluice.contracts.catalog.native.ckan import CKANResultItem, CKANSecretResultItem
from datasluice.domain.catalog import CatalogPlatform
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ResultEnvelope, ValueRecord
from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogNotFoundError,
    CatalogRateLimitError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
    map_catalog_error,
)


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


def test_rate_limit_mapping_forwards_native_retry_after() -> None:
    """The normalized rate-limit error keeps the platform-requested delay."""
    native = NativeCatalogError(
        "slow down",
        operation="datasets.get",
        platform=CatalogPlatform.CKAN,
        status_code=429,
        retry_after=7,
    )
    error = map_catalog_error(native)

    assert isinstance(error, CatalogRateLimitError)
    assert error.retry_after == 7.0


@pytest.mark.parametrize("retry_after", (float("nan"), float("inf")))
def test_native_errors_reject_non_finite_retry_after(retry_after: float) -> None:
    with pytest.raises(ValueError, match="Retry-After"):
        NativeCatalogError(
            "slow down",
            operation="datasets.get",
            platform=CatalogPlatform.CKAN,
            status_code=429,
            retry_after=retry_after,
        )


@pytest.mark.parametrize(
    ("status_code", "error_type", "capability_state", "safe_action"),
    [
        (401, UnauthenticatedError, "unauthorized", "Provide valid credentials and retry the operation."),
        (403, ForbiddenError, "forbidden", "Use credentials with the required scope or role."),
        (404, CatalogNotFoundError, None, "Confirm the target identifier and deployment."),
        (409, CatalogConflictError, None, "Refresh the target version token before retrying."),
        (422, CatalogValidationError, None, "Correct the request according to the platform validation details."),
        (429, CatalogRateLimitError, None, "Wait for Retry-After before retrying a safe operation."),
        (400, CatalogValidationError, None, "Correct the request before retrying."),
        (405, CatalogValidationError, None, "Correct the request before retrying."),
        (500, CatalogUnavailableError, "unavailable", "Retry after the deployment is available."),
        (503, CatalogUnavailableError, "unavailable", "Retry after the deployment is available."),
        (None, CatalogUnavailableError, "unavailable", "Retry after the deployment is available."),
    ],
)
def test_catalog_error_mapping_covers_each_normalized_status_branch(
    status_code: int | None,
    error_type: type[Exception],
    capability_state: str | None,
    safe_action: str,
) -> None:
    """Every native status branch maps to one typed safe error."""
    native = NativeCatalogError(
        "request failed",
        operation="datasets.get",
        platform=CatalogPlatform.CKAN,
        status_code=status_code,
    )

    error = map_catalog_error(native)

    assert isinstance(error, error_type)
    assert error.capability_state == capability_state
    assert error.safe_action == safe_action


def test_native_error_messages_are_redacted_and_bounded() -> None:
    """Credential-shaped message content is scrubbed before it can be logged."""
    leaked = "GET https://portal.example/api?q=dataset&apikey=super-secret&signature=abc123 failed"
    native = NativeCatalogError(leaked, operation="datasets.get", platform=CatalogPlatform.CKAN, status_code=400)

    assert "super-secret" not in str(native)
    assert "abc123" not in str(native)
    assert "apikey=***" in str(native)
    assert len(str(native)) <= 256


def test_ckan_result_union_alias_admits_value_and_mapping_record_items() -> None:
    """The broadened CKANResult alias treats scalars and mappings as legal envelope items."""
    assert CKANResultItem.__value__ == (NativeRecord | ValueRecord | MappingRecord | CKANSecretResultItem)

    envelope = ResultEnvelope(items=(ValueRecord(value=None), MappingRecord(payload={"success": True})))

    def decode(item: object) -> ValueRecord | MappingRecord:
        assert isinstance(item, dict)
        return ValueRecord.from_dict(item) if item.get("kind") == "value_record" else MappingRecord.from_dict(item)

    decoded = ResultEnvelope.from_dict(envelope.to_dict(), item_decoder=decode)
    assert [item.to_dict()["kind"] for item in decoded.items] == ["value_record", "mapping_record"]
