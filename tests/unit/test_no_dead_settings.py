"""Tests proving the dead Settings system is fully removed (CORR-04, D-10, D-14).

Scans every ``.py`` file under ``src/datasluice/`` for the string
``DATASLUICE_`` (environment variable references) and verifies that
``Settings`` and ``load_settings`` are no longer importable.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def test_no_datasluice_env_vars_in_source() -> None:
    src_root = Path(__file__).resolve().parents[2] / "src" / "datasluice"
    offending: list[str] = []
    for py_file in src_root.rglob("*.py"):
        if "__pycache__" in py_file.parts:
            continue
        content = py_file.read_text(encoding="utf-8")
        if "DATASLUICE_" in content:
            offending.append(str(py_file))
    assert not offending, f"DATASLUICE_ env var references found in: {offending}"


def test_settings_module_removed() -> None:
    with pytest.raises(ImportError):
        from datasluice.config.settings import Settings  # ty: ignore[unresolved-import]  # noqa: F401


def test_load_settings_removed() -> None:
    with pytest.raises(ImportError):
        from datasluice.config import load_settings  # ty: ignore[unresolved-import]  # noqa: F401
