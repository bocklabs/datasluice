"""Purity test: datasluice.domain imports zero optional dependencies.

Guards criterion #1 — the domain package must remain a zero-dependency
vocabulary layer so subsequent layers (ports, runtime, connectors) can depend
on it without pulling heavy optional deps into the import graph.
"""

from __future__ import annotations

from tests.helpers.import_purity import assert_import_pulls_no_distributions, assert_import_pulls_no_modules

_FORBIDDEN_OPTIONAL_MODULES = ("pyarrow", "pandas", "polars", "dlt", "duckdb", "openpyxl", "airflow")
_FORBIDDEN_DOMAIN_IMPORTS = ("datasluice.adapters", "datasluice.connectors")


def test_domain_imports_zero_optional_deps() -> None:
    assert_import_pulls_no_distributions("datasluice.domain", _FORBIDDEN_OPTIONAL_MODULES)


def test_domain_package_surface_symbols() -> None:
    import datasluice.domain as domain

    for symbol in ("Schema", "ResourceAccess", "DetectionResult", "Artifact", "SyncState", "CatalogId"):
        assert hasattr(domain, symbol), f"datasluice.domain missing {symbol}"
    assert not hasattr(domain, "CatalogCapabilities")


def test_domain_import_does_not_load_platform_or_legacy_connector_modules() -> None:
    assert_import_pulls_no_modules("datasluice.domain", _FORBIDDEN_DOMAIN_IMPORTS)
