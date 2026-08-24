"""Documentation guard for the canonical connector clean break.

The published documentation must teach only the canonical explicit
connector contract: platform packages under
``datasluice.connectors.catalog``, the public contract suite in
``datasluice.contracts.catalog``, and the retained direct-resource data
plane. Any documented route to removed modules, symbols, portal types,
URL-driven facade construction, or deleted CLI commands fails this gate.
Removed identifiers stay legal only inside this file, which defines the
negative audit, and inside the explicit per-file allowlist below.

Any detected violation fails module collection, so a documentation
regression can never surface as a green CI run with skipped gates.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
README = REPO_ROOT / "README.md"
DOCS_DIR = REPO_ROOT / "docs"
ZENSICAL = REPO_ROOT / "zensical.toml"

# Legacy Airflow surface that must stay absent from current documentation.
AIRFLOW_OBSOLETE_PATTERNS = [
    "datasluice[airflow]",
    "datasluice.integrations.airflow",
    "integrations.airflow",
    "DataSluiceSession",
    "DataSluiceOperator",
]

# Removed connector-surface identifiers: former facade framework, former
# platform module paths, retired locator machinery, and the retired session
# facade name. Banned from every scanned page, prose included.
REMOVED_IDENTIFIER_PATTERNS = [
    "adapter",
    "AdapterError",
    "AdapterNotFoundError",
    "BaseAdapter",
    "CKANAdapter",
    "SocrataAdapter",
    "UDataAdapter",
    "datasluice.adapters",
    "registry.register",
    "datagouv",
    "CatalogResourceLocator",
    "DataSluiceSession",
    "DataSluiceOperator",
    "datasluice.connectors.ckan",
    "datasluice.connectors.datagouv",
    "datasluice.connectors.socrata",
]

# Removed implicit-platform phrasing: portal auto-detection and portal-type
# selection no longer exist in any form.
REMOVED_PHRASE_PATTERNS = [
    "auto-detect",
    "auto-detected",
    "auto-detection",
    "portal type",
    "portal-type",
]

# Removed executable content, checked inside fenced code regions only.
REMOVED_PYTHON_REGION_PATTERNS = [
    'DataSluice("',
    "DataSluice('",
    "ds.search(",
    "ds.get_dataset(",
    "ds.download(",
    "ds.download_all(",
    "ds.read(",
]

REMOVED_BASH_REGION_PATTERNS = [
    "datasluice search",
    "datasluice inspect",
    "datasluice detect",
    "datasluice download",
]

# Named connector extras belong to Phase 2 packaging (PACK-01/PACK-02) and
# must not be advertised as installable from the Phase 1 package surface.
NAMED_CONNECTOR_EXTRA_RE = re.compile(r"datasluice\[(ckan|udata|socrata|all-connectors)\]")

# Canonical platform symbols and their only legal import home.
CANONICAL_PLATFORM_SYMBOLS: dict[str, str] = {
    "CKANConnector": "datasluice.connectors.catalog.ckan",
    "create_ckan_connector": "datasluice.connectors.catalog.ckan",
    "UDataConnector": "datasluice.connectors.catalog.udata",
    "create_udata_connector": "datasluice.connectors.catalog.udata",
    "SocrataConnector": "datasluice.connectors.catalog.socrata",
    "create_socrata_connector": "datasluice.connectors.catalog.socrata",
}

# Historical terms may appear only where this explicit negative-audit
# allowlist permits (repo-relative path -> allowed identifier substrings).
PATTERN_ALLOWLIST: dict[str, frozenset[str]] = {
    "CONTEXT.md": frozenset({"adapter"}),
}

_FENCE_RE = re.compile(r"```(?P<lang>[a-zA-Z0-9_-]*)[^\n]*\n(?P<body>.*?)```", re.DOTALL)
_MD_LINK_RE = re.compile(r"(?<!\!)\[[^\]]+\]\((?P<target>[^)\s]+)\)")
_IMPORT_RE = re.compile(r"from\s+(?P<module>datasluice[\w.]*)\s+import\s+(?P<names>[^\n#]+)")


@dataclass(frozen=True, slots=True)
class _Page:
    """One scanned documentation page."""

    path: Path
    text: str

    @property
    def rel(self) -> str:
        """Return the repo-relative display path."""
        return self.path.relative_to(REPO_ROOT).as_posix()

    def allowed(self, pattern: str) -> bool:
        """Return whether the allowlist permits this pattern here."""
        return pattern in PATTERN_ALLOWLIST.get(self.rel, frozenset())

    def fences(self, language: str) -> list[str]:
        """Return the bodies of fenced code blocks for one language."""
        return [match.group("body") for match in _FENCE_RE.finditer(self.text) if match.group("lang") == language]


def _doc_pages() -> list[_Page]:
    pages = []
    glossary = REPO_ROOT / "CONTEXT.md"
    if glossary.exists():
        pages.append(_Page(glossary, glossary.read_text(encoding="utf-8")))
    if README.exists():
        pages.append(_Page(README, README.read_text(encoding="utf-8")))
    if DOCS_DIR.exists():
        pages.extend(_Page(path, path.read_text(encoding="utf-8")) for path in sorted(DOCS_DIR.rglob("*.md")))
    return pages


def _zensical_text() -> str:
    if not ZENSICAL.exists():
        return ""
    return ZENSICAL.read_text(encoding="utf-8")


def _identifier_violations(pages: list[_Page]) -> list[str]:
    violations = []
    for page in pages:
        for pattern in AIRFLOW_OBSOLETE_PATTERNS + REMOVED_IDENTIFIER_PATTERNS + REMOVED_PHRASE_PATTERNS:
            if not page.allowed(pattern) and pattern in page.text:
                violations.append(f"{page.rel}: removed surface {pattern!r}")
    return violations


def _python_fence_violations(pages: list[_Page]) -> list[str]:
    violations = []
    for page in pages:
        for body in page.fences("python"):
            for pattern in REMOVED_PYTHON_REGION_PATTERNS:
                if pattern in body:
                    violations.append(f"{page.rel}: python fence uses {pattern!r}")
    return violations


def _bash_fence_violations(pages: list[_Page]) -> list[str]:
    violations = []
    for page in pages:
        for body in page.fences("bash"):
            for pattern in REMOVED_BASH_REGION_PATTERNS:
                if pattern in body:
                    violations.append(f"{page.rel}: bash fence teaches {pattern!r}")
    return violations


def _canonical_import_violations(pages: list[_Page]) -> list[str]:
    violations = []
    for page in pages:
        for symbol, package in CANONICAL_PLATFORM_SYMBOLS.items():
            if symbol in page.text and package not in page.text:
                violations.append(f"{page.rel}: {symbol!r} without its {package} import home")
        for body in page.fences("python"):
            for match in _IMPORT_RE.finditer(body):
                for symbol, package in CANONICAL_PLATFORM_SYMBOLS.items():
                    if symbol in match.group("names") and match.group("module") != package:
                        violations.append(f"{page.rel}: {symbol!r} imported from {match.group('module')!r}")
    return violations


def _named_extra_violations(pages: list[_Page]) -> list[str]:
    violations = []
    for page in pages:
        if NAMED_CONNECTOR_EXTRA_RE.search(page.text):
            violations.append(f"{page.rel}: advertises a named connector extra before Phase 2")
    return violations


def _navigation_violations() -> list[str]:
    text = _zensical_text()
    violations = []
    for term in ("datagouv", "data.gouv.fr", "BaseAdapter"):
        if term in text:
            violations.append(f"zensical.toml navigation carries {term!r}")
    for target in re.findall(r"=\s*\"([^\"]+\.md)\"", text):
        if not (DOCS_DIR / target).exists():
            violations.append(f"zensical.toml navigation entry {target!r} has no page")
    return violations


def _relative_link_violations(pages: list[_Page]) -> list[str]:
    violations = []
    for page in pages:
        for match in _MD_LINK_RE.finditer(page.text):
            target = match.group("target")
            if "://" in target or target.startswith("/"):
                continue
            if not (page.path.parent / target.split("#")[0]).resolve().exists():
                violations.append(f"{page.rel}: links to missing {target!r}")
    return violations


def _page_text(relative: str) -> str:
    path = REPO_ROOT / relative
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _install_boundary_violations() -> list[str]:
    text = _page_text("docs/install.md")
    violations = []
    if not text:
        return violations
    if "apache-airflow-providers-datasluice" not in text:
        violations.append("install.md must name the separate provider distribution")
    if "airflow.providers.datasluice" not in text:
        violations.append("install.md must identify the provider namespace")
    if "Phase 2" not in text:
        violations.append("install.md must state the Phase 2 packaging boundary")
    if "connector" not in text:
        violations.append("install.md must name the connector extras owner")
    return violations


def _api_pointer_violations() -> list[str]:
    text = _page_text("docs/api.md")
    if not text:
        return []
    if "datasluice.contracts.catalog" not in text:
        return ["api.md must point at the public contract suite"]
    if "DataSluiceSession" in text:
        return ["api.md must not surface DataSluiceSession"]
    return []


def _example_violations(relative: str, phase_label: str) -> list[str]:
    text = _page_text(relative)
    if not text:
        return []
    violations = []
    if "Reference" not in text and "reference" not in text:
        violations.append(f"{relative} must use reference fixtures/fakes")
    if phase_label not in text:
        violations.append(f"{relative} must label live endpoint work as {phase_label}")
    return violations


def _airflow_boundary_violations() -> list[str]:
    text = _page_text("docs/examples/airflow.md")
    if not text:
        return []
    violations = []
    if "airflow.providers.datasluice" not in text:
        violations.append("airflow.md must use the provider namespace")
    for term in ("DataSluiceHook", "DataSluiceSearchOperator", "DataSluiceMaterializeOperator", "integrations.airflow"):
        if term in text:
            violations.append(f"airflow.md teaches retired surface {term!r}")
    return violations


def _migration_violations() -> list[str]:
    pages = _doc_pages()
    return (
        _identifier_violations(pages)
        + _python_fence_violations(pages)
        + _bash_fence_violations(pages)
        + _canonical_import_violations(pages)
        + _named_extra_violations(pages)
        + _navigation_violations()
        + _relative_link_violations(pages)
        + _install_boundary_violations()
        + _api_pointer_violations()
        + _example_violations("docs/examples/ckan.md", "Phase 3")
        + _example_violations("docs/examples/socrata.md", "Phase 5")
        + _airflow_boundary_violations()
    )


_DOCS_VIOLATIONS = _migration_violations()
if _DOCS_VIOLATIONS:
    pytest.fail(
        f"current documentation still references removed or non-canonical surfaces: {_DOCS_VIOLATIONS[0]}",
        pytrace=False,
    )


def test_published_docs_omit_removed_core_airflow_surface() -> None:
    """Current docs never reference the removed core airflow extra or module."""
    assert not _identifier_violations(_doc_pages())


def test_install_docs_name_the_separate_provider_distribution() -> None:
    """docs/install.md installs apache-airflow-providers-datasluice, not a core extra."""
    assert not _install_boundary_violations()


def test_airflow_docs_state_provider_boundary() -> None:
    """The Airflow page stays on the provider namespace without retired operators."""
    assert not _airflow_boundary_violations()


def test_public_api_docs_omit_decommissioned_session() -> None:
    """docs/api.md must not surface DataSluiceSession."""
    assert "DataSluiceSession" not in _page_text("docs/api.md")


def test_docs_contain_no_removed_identifiers_or_portal_phrases() -> None:
    """Test 1: no removed module, symbol, identifier, or portal phrase survives."""
    assert not _identifier_violations(_doc_pages())


def test_python_fences_omit_removed_portal_calls_and_url_construction() -> None:
    """Test 1: executable python regions teach no retired portal API."""
    assert not _python_fence_violations(_doc_pages())


def test_bash_fences_omit_removed_cli_commands() -> None:
    """Test 1: executable bash regions teach no deleted CLI command."""
    assert not _bash_fence_violations(_doc_pages())


def test_navigation_omits_removed_pages_and_names() -> None:
    """Test 1: zensical navigation keeps only canonical pages and names."""
    assert not _navigation_violations()


def test_canonical_platform_symbols_import_only_from_platform_packages() -> None:
    """Test 2: canonical class/factory names appear only beside their platform import."""
    assert not _canonical_import_violations(_doc_pages())


def test_api_docs_point_at_public_contract_suite() -> None:
    """Test 3: the API page documents the public catalog contract entry point."""
    assert not _api_pointer_violations()


def test_install_docs_bound_named_connector_extras_to_phase_two() -> None:
    """Test 3: named connector extras are Phase 2 packaging, not this package surface."""
    assert not _install_boundary_violations()


def test_no_doc_advertises_named_connector_extras() -> None:
    """Test 3: no scanned page claims installable named connector extras."""
    assert not _named_extra_violations(_doc_pages())


def test_ckan_example_uses_reference_cases_and_labels_phase_three() -> None:
    """Test 4: the CKAN example is deterministic and bounded to Phase 1 behavior."""
    assert not _example_violations("docs/examples/ckan.md", "Phase 3")


def test_socrata_example_uses_reference_cases_and_labels_phase_five() -> None:
    """Test 4: the Socrata example is deterministic and bounded to Phase 1 behavior."""
    assert not _example_violations("docs/examples/socrata.md", "Phase 5")


def test_every_navigation_entry_resolves_to_a_tracked_page() -> None:
    """Exact link validation: each zensical nav target exists under docs/."""
    assert not _navigation_violations()


def test_every_relative_markdown_link_resolves() -> None:
    """Exact link validation: every relative md link in docs resolves on disk."""
    assert not _relative_link_violations(_doc_pages())
