"""Tests proving the dead Settings system is fully removed.

Scans every ``.py`` file under ``src/datasluice/`` for the string
``DATASLUICE_`` (environment variable references) and verifies that
``Settings`` and ``load_settings`` are no longer importable.

The single legitimate ``DATASLUICE_``-prefixed env var in v1 is
``DATASLUICE_NO_REDACT``;
it is allowlisted here so the scan still flags any other ``DATASLUICE_``
substring while permitting this one.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ALLOWED_DATASLUICE_ENV_VARS = frozenset({"DATASLUICE_NO_REDACT"})


def test_no_datasluice_env_vars_in_source() -> None:
    src_root = Path(__file__).resolve().parents[2] / "src" / "datasluice"
    offending: list[str] = []
    for py_file in src_root.rglob("*.py"):
        if "__pycache__" in py_file.parts:
            continue
        content = py_file.read_text(encoding="utf-8")
        found = set(re.findall(r"DATASLUICE_[A-Z_]+", content))
        unexpected = found - ALLOWED_DATASLUICE_ENV_VARS
        if unexpected:
            offending.append(f"{py_file}: {sorted(unexpected)}")
    assert not offending, f"Unexpected DATASLUICE_ env var references found in: {offending}"


def test_settings_module_removed() -> None:
    with pytest.raises(ImportError):
        from datasluice.config.settings import Settings  # ty: ignore[unresolved-import]  # noqa: F401


def test_load_settings_removed() -> None:
    with pytest.raises(ImportError):
        from datasluice.config import load_settings  # ty: ignore[unresolved-import]  # noqa: F401
