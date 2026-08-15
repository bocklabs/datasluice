"""Public API stability tests for the catalog contract tracer."""

from __future__ import annotations

import inspect

import datasluice.contracts as contracts
import datasluice.contracts.catalog as catalog


def test_catalog_contract_package_exports_only_the_documented_tracer_surface() -> None:
    """The package exposes only deliberate tracer types and Protocols."""
    assert catalog.__all__ == [
        "run_catalog_contract",
        "CatalogContractCase",
        "CaseOutcome",
        "ComplianceReport",
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
