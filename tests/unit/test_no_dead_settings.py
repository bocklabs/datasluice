"""Tests proving the dead Settings system is fully removed.

Scans every ``.py`` file under ``src/datasluice/`` for the string
``DATASLUICE_`` (environment variable references) and verifies that
``Settings`` and ``load_settings`` are no longer importable.

Four legitimate ``DATASLUICE_``-prefixed env vars exist: the opt-out
redaction switch ``DATASLUICE_NO_REDACT`` plus the three platform
credential variables (``DATASLUICE_CKAN_API_TOKEN``,
``DATASLUICE_SOCRATA_APP_TOKEN``, ``DATASLUICE_UDATA_API_KEY``).
They are allowlisted here so the scan still flags any other
``DATASLUICE_`` substring while permitting exactly these four.
"""

from __future__ import annotations

import importlib
import re
from operator import attrgetter
from pathlib import Path

import pytest

ALLOWED_DATASLUICE_ENV_VARS = frozenset(
    {
        "DATASLUICE_CKAN_API_TOKEN",
        "DATASLUICE_NO_REDACT",
        "DATASLUICE_SOCRATA_APP_TOKEN",
        "DATASLUICE_UDATA_API_KEY",
    }
)


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
        importlib.import_module("datasluice.config.settings")


def test_load_settings_removed() -> None:
    with pytest.raises(AttributeError):
        _ = attrgetter("load_settings")(importlib.import_module("datasluice.config"))
