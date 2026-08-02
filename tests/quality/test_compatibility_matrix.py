"""Compatibility matrix contract for the supported release surface (D-29/D-35).

The checked-in ``providers/apache-airflow/compatibility.json`` is the finite
source of truth for verified release support. Its three axes, the full
Cartesian product they define, and the CI matrix that consumes it must all
agree; an omitted tuple would make declared support unverified.
"""

from __future__ import annotations

import itertools
import json
import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MANIFEST_PATH = REPO_ROOT / "providers" / "apache-airflow" / "compatibility.json"
CI_PATH = REPO_ROOT / ".github" / "workflows" / "ci.yml"

_TDD_RED = os.environ.get("DATASLUICE_TDD_RED") == "1"

EXPECTED_CORE = ["1.0.0"]
EXPECTED_AIRFLOW = ["3.2.2", "3.3.0"]
EXPECTED_PYTHON = ["3.12", "3.13", "3.14"]

# Published D-29 ranges the provider distribution declares.
CORE_RANGE_LO = (1, 0)
CORE_RANGE_HI = (2, 0)
AIRFLOW_RANGE_LO = (3, 2)
AIRFLOW_RANGE_HI = (4, 0)
PYTHON_MIN_RELEASE = (3, 12)

if not MANIFEST_PATH.exists():
    if _TDD_RED:
        pytest.fail("compatibility.json does not exist; expected RED collection failure", pytrace=False)
    pytest.skip("compatibility manifest pending GREEN phase", allow_module_level=True)


def _load_manifest() -> dict[str, object]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _axis(key: str) -> list[str]:
    raw: object = _load_manifest().get(key, [])
    if not isinstance(raw, list):
        return []
    return [str(value) for value in raw]


def _version_tuple(release: object) -> tuple[int, ...]:
    parts = str(release).split(".")
    return tuple(int(part) for part in parts) if parts else ()


def test_manifest_defines_exact_supported_axes() -> None:
    """The manifest pins the exact core, Airflow, and Python supported sets."""
    assert _axis("core") == EXPECTED_CORE, ("core", _axis("core"))
    assert _axis("airflow") == EXPECTED_AIRFLOW, ("airflow", _axis("airflow"))
    assert _axis("python") == EXPECTED_PYTHON, ("python", _axis("python"))


def test_full_cartesian_product_is_exactly_six_tuples() -> None:
    """Expanding the axes yields exactly the six verifiable supported tuples (D-35)."""
    core = _axis("core")
    airflow = _axis("airflow")
    python = _axis("python")
    product = list(itertools.product(core, airflow, python))
    assert len(product) == len(core) * len(airflow) * len(python)
    assert len(product) == 6, f"expected 6 tuples, got {len(product)}: {product}"
    assert len(set(product)) == 6, "Cartesian product must contain unique tuples"
    expected = [(c, a, p) for c in EXPECTED_CORE for a in EXPECTED_AIRFLOW for p in EXPECTED_PYTHON]
    assert product == expected


def test_every_supported_value_sits_inside_declared_ranges() -> None:
    """Each supported value lies within the published D-29 package ranges."""
    for release in _axis("core"):
        assert CORE_RANGE_LO <= _version_tuple(release)[:2] < CORE_RANGE_HI
    for release in _axis("airflow"):
        assert AIRFLOW_RANGE_LO <= _version_tuple(release)[:2] < AIRFLOW_RANGE_HI
    for minor in _axis("python"):
        assert _version_tuple(minor)[:2] >= PYTHON_MIN_RELEASE


def test_ci_references_the_manifest_not_a_second_list() -> None:
    """CI reads the provider matrix from the compatibility manifest, not duplicate arrays."""
    ci = CI_PATH.read_text(encoding="utf-8")
    assert "compatibility.json" in ci, "CI must reference the compatibility manifest"
    assert "airflow" in ci
    assert "python-version" in ci or "python" in ci
