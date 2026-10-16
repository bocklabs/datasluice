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
    get_attribute = attrgetter("load_settings")
    module = importlib.import_module("datasluice.config")
    with pytest.raises(AttributeError):
        _ = get_attribute(module)
