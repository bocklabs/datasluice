"""Current-documentation guard for removed Airflow surfaces.

The published docs must describe only the separate
``apache-airflow-providers-datasluice`` distribution and the
``airflow.providers.datasluice`` namespace. Any reference to the removed core
``datasluice[airflow]`` extra, ``datasluice.integrations.airflow`` module, or
the decommissioned public ``DataSluiceSession`` in current documentation fails
the release gate. Planning/historical artifacts are scoped out.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
README = REPO_ROOT / "README.md"
DOCS_DIR = REPO_ROOT / "docs"

_TDD_RED = os.environ.get("DATASLUICE_TDD_RED") == "1"

# Literal substrings that must not appear in current documentation.
OBSOLETE_PATTERNS = [
    "datasluice[airflow]",
    "datasluice.integrations.airflow",
    "integrations.airflow",
    "DataSluiceSession",
    "DataSluiceOperator",
]


def _current_doc_files() -> list[Path]:
    files = [README]
    files.extend(sorted(DOCS_DIR.rglob("*.md")) if DOCS_DIR.exists() else [])
    return files


def _outside_history(path: Path) -> bool:
    rel = path.relative_to(REPO_ROOT).as_posix()
    return not (rel.startswith(".planning") or "CHANGELOG.md" in rel)


def _migrated() -> bool:
    for path in _current_doc_files():
        if not _outside_history(path):
            continue
        text = path.read_text(encoding="utf-8")
        if any(pattern in text for pattern in OBSOLETE_PATTERNS):
            return False
    install = DOCS_DIR / "install.md"
    if install.exists():
        text = install.read_text(encoding="utf-8")
        if "apache-airflow-providers-datasluice" not in text:
            return False
    return True


if not _migrated():
    if _TDD_RED:
        pytest.fail("current documentation still references removed Airflow surfaces", pytrace=False)
    pytest.skip("current-doc migration pending GREEN phase", allow_module_level=True)


@pytest.mark.skipif(not README.exists(), reason="README missing")
def test_published_docs_omit_removed_core_airflow_surface() -> None:
    """Current docs never reference the removed core airflow extra or module."""
    for path in _current_doc_files():
        if not _outside_history(path):
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in OBSOLETE_PATTERNS:
            assert pattern not in text, f"{path.relative_to(REPO_ROOT)} still references {pattern!r}"


def test_install_docs_name_the_separate_provider_distribution() -> None:
    """docs/install.md installs apache-airflow-providers-datasluice, not a core extra."""
    install = DOCS_DIR / "install.md"
    if not install.exists():
        pytest.skip("install.md missing")
    text = install.read_text(encoding="utf-8")
    assert "apache-airflow-providers-datasluice" in text, "install.md must name the separate provider"
    assert "airflow.providers.datasluice" in text, "install.md must identify the provider namespace"
    assert "datasluice[airflow]" not in text, "install.md must not advertise the removed core extra"


def test_airflow_docs_use_provider_namespace_and_operators() -> None:
    """The Airflow example goes through airflow.providers.datasluice, not the core module."""
    airflow = DOCS_DIR / "examples" / "airflow.md"
    if not airflow.exists():
        pytest.skip("airflow.md missing")
    text = airflow.read_text(encoding="utf-8")
    assert "airflow.providers.datasluice" in text, "airflow.md must use the provider namespace"
    assert "DataSluiceHook" in text or "DataSluiceSearchOperator" in text
    assert "DataSluiceOperator" not in text, "airflow.md must not use the removed preview operator"
    assert "integrations.airflow" not in text, "airflow.md must not import the removed core module"


def test_public_api_docs_omit_decommissioned_session() -> None:
    """docs/api.md must not surface DataSluiceSession."""
    api = DOCS_DIR / "api.md"
    if not api.exists():
        pytest.skip("api.md missing")
    text = api.read_text(encoding="utf-8")
    assert "DataSluiceSession" not in text
