"""Purity test: datasluice.domain imports zero optional dependencies.

Guards criterion #1 — the domain package must remain a zero-dependency
vocabulary layer so subsequent layers (ports, runtime, connectors) can depend
on it without pulling heavy optional deps into the import graph.
"""

from __future__ import annotations

import sys

_FORBIDDEN_OPTIONAL_MODULES = ("pyarrow", "pandas", "polars", "dlt", "duckdb", "openpyxl", "airflow")


def test_domain_imports_zero_optional_deps() -> None:
    for name in list(sys.modules):
        if name.split(".")[0] in _FORBIDDEN_OPTIONAL_MODULES:
            del sys.modules[name]

    import datasluice.domain  # noqa: F401

    present = [name for name in _FORBIDDEN_OPTIONAL_MODULES if name in sys.modules]
    assert present == [], f"datasluice.domain pulled optional deps: {present}"


def test_domain_package_surface_symbols() -> None:
    import datasluice.domain as domain

    for symbol in ("Schema", "ResourceAccess", "DetectionResult", "Artifact", "SyncState", "CatalogCapabilities"):
        assert hasattr(domain, symbol), f"datasluice.domain missing {symbol}"
