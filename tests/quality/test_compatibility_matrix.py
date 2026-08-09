"""Compatibility matrix contract for the supported release surface.

The checked-in ``providers/apache-airflow/compatibility.json`` is the finite
source of truth for verified *external* support (Airflow, Python). The core
datasluice version is always the current source version, read from the root
``pyproject.toml`` — never a static pin — so certified support tracks each
core release automatically without manual edits.
"""

from __future__ import annotations

import itertools
import json
import os
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MANIFEST_PATH = REPO_ROOT / "providers" / "apache-airflow" / "compatibility.json"
CORE_PYPROJECT = REPO_ROOT / "pyproject.toml"
CI_PATH = REPO_ROOT / ".github" / "workflows" / "ci.yml"

_TDD_RED = os.environ.get("DATASLUICE_TDD_RED") == "1"

EXPECTED_AIRFLOW = ["3.2.2", "3.3.0"]
EXPECTED_PYTHON = ["3.12", "3.13", "3.14"]

# Published ranges the provider distribution declares.
CORE_RANGE_LO = (0, 2)
CORE_RANGE_HI = (1, 0)
AIRFLOW_RANGE_LO = (3, 2)
AIRFLOW_RANGE_HI = (4, 0)
PYTHON_MIN_RELEASE = (3, 12)

if not MANIFEST_PATH.exists():
    if _TDD_RED:
        pytest.fail("compatibility.json does not exist; expected RED collection failure", pytrace=False)
    pytest.skip("compatibility manifest pending GREEN phase", allow_module_level=True)


def _core_version() -> str:
    """Read the current core datasluice version from the root pyproject.toml."""
    match = re.search(r'^version\s*=\s*"([^"]+)"', CORE_PYPROJECT.read_text(encoding="utf-8"), re.MULTILINE)
    assert match, "could not read version from core pyproject.toml"
    return match.group(1)


def _load_manifest() -> dict[str, object]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _axis(key: str) -> list[str]:
    raw: object = _load_manifest().get(key, [])
    if not isinstance(raw, list):
        return []
    return [str(value) for value in raw]


def _version_tuple(release: object) -> tuple[int, ...]:
    nums: list[int] = []
    for part in str(release).split("."):
        m = re.match(r"\d+", part)
        nums.append(int(m.group(0)) if m else 0)
    return tuple(nums)


def test_manifest_defines_exact_external_axes() -> None:
    """The manifest pins the exact Airflow and Python supported sets and carries no core axis."""
    manifest = _load_manifest()
    assert set(manifest) == {"airflow", "python"}, ("axes", sorted(manifest))
    assert _axis("airflow") == EXPECTED_AIRFLOW, ("airflow", _axis("airflow"))
    assert _axis("python") == EXPECTED_PYTHON, ("python", _axis("python"))


def test_full_cartesian_product_is_exactly_six_tuples() -> None:
    """Expanding the external axes yields exactly the six verifiable supported tuples."""
    airflow = _axis("airflow")
    python = _axis("python")
    product = list(itertools.product(airflow, python))
    assert len(product) == len(airflow) * len(python)
    assert len(product) == 6, f"expected 6 tuples, got {len(product)}: {product}"
    assert len(set(product)) == 6, "Cartesian product must contain unique tuples"
    expected = [(a, p) for a in EXPECTED_AIRFLOW for p in EXPECTED_PYTHON]
    assert product == expected


def test_current_core_sits_inside_declared_ranges() -> None:
    """The current core version and every external value lie within the published ranges."""
    core = _version_tuple(_core_version())[:2]
    assert CORE_RANGE_LO <= core < CORE_RANGE_HI, ("core", _core_version())
    for release in _axis("airflow"):
        assert AIRFLOW_RANGE_LO <= _version_tuple(release)[:2] < AIRFLOW_RANGE_HI
    for minor in _axis("python"):
        assert _version_tuple(minor)[:2] >= PYTHON_MIN_RELEASE


def test_ci_derives_core_and_references_the_manifest() -> None:
    """CI reads the matrix from the compatibility manifest and derives the core version from pyproject."""
    ci = CI_PATH.read_text(encoding="utf-8")
    assert "compatibility.json" in ci, "CI must reference the compatibility manifest"
    assert 'Path("pyproject.toml")' in ci, "CI must derive the core version from pyproject.toml"
    assert "re.search" in ci, "CI must regex-extract the core version rather than hardcode it"
    assert "airflow" in ci
    assert "python-version" in ci or "python" in ci
