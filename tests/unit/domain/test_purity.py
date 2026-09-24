"""Purity test: datasluice.domain imports zero optional dependencies.

Guards criterion #1 — the domain package must remain a zero-dependency
vocabulary layer so subsequent layers (ports, runtime, connectors) can depend
on it without pulling heavy optional deps into the import graph.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _run_import_check(script: str) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(_REPO_ROOT / "src")
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr


_FORBIDDEN_OPTIONAL_MODULES = ("pyarrow", "pandas", "polars", "dlt", "duckdb", "openpyxl", "airflow")
_FORBIDDEN_DOMAIN_IMPORTS = ("datasluice.adapters", "datasluice.connectors")


def test_domain_imports_zero_optional_deps() -> None:
    script = textwrap.dedent(
        """
        import importlib
        import sys

        forbidden = %r
        for name in list(sys.modules):
            if name.split(".")[0] in forbidden:
                del sys.modules[name]
        importlib.import_module("datasluice.domain")
        present = [name for name in forbidden if name in sys.modules]
        assert present == [], present
        """
    ) % (_FORBIDDEN_OPTIONAL_MODULES,)
    _run_import_check(script)


def test_domain_package_surface_symbols() -> None:
    import datasluice.domain as domain

    for symbol in ("Schema", "ResourceAccess", "DetectionResult", "Artifact", "SyncState", "CatalogId"):
        assert hasattr(domain, symbol), f"datasluice.domain missing {symbol}"
    assert not hasattr(domain, "CatalogCapabilities")


def test_domain_import_does_not_load_platform_or_legacy_connector_modules() -> None:
    script = textwrap.dedent(
        """
        import importlib
        import sys

        forbidden = %r
        for name in list(sys.modules):
            if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden):
                del sys.modules[name]
        importlib.import_module("datasluice.domain")
        present = [name for name in forbidden if name in sys.modules]
        assert present == [], present
        """
    ) % (_FORBIDDEN_DOMAIN_IMPORTS,)
    _run_import_check(script)
