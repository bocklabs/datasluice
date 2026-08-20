"""Provider public-boundary, metadata-surface, and documentation checks."""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[3]
_PROVIDER_ROOT = _ROOT / "providers" / "apache-airflow"
_PROVIDER_SOURCE = _PROVIDER_ROOT / "src"
_PROVIDER_TESTS = _PROVIDER_ROOT / "tests"
_PROVIDER_PACKAGE = _PROVIDER_SOURCE / "airflow" / "providers" / "datasluice"
_SAMPLE_DAG = _PROVIDER_TESTS / "dags" / "example_datasluice.py"
_AIRFLOW_DOC = _ROOT / "docs" / "examples" / "airflow.md"

_RETIRED_RUNTIME_MODULES = (
    "airflow.providers.datasluice.operators.search",
    "airflow.providers.datasluice.operators.materialize",
    "airflow.providers.datasluice.operators._xcom",
)
_RETIRED_SOURCE_FILES = (
    _PROVIDER_PACKAGE / "operators" / "search.py",
    _PROVIDER_PACKAGE / "operators" / "materialize.py",
    _PROVIDER_PACKAGE / "operators" / "_xcom.py",
)
_RETIRED_TEST_FILES = (
    _PROVIDER_TESTS / "test_hook.py",
    _PROVIDER_TESTS / "test_search_operator.py",
    _PROVIDER_TESTS / "test_materialize_operator.py",
)
_EXECUTION_PACKAGES = ("hooks", "operators")
_RETIRED_IDENTIFIERS = (
    *_RETIRED_RUNTIME_MODULES,
    "DataSluiceSearchOperator",
    "DataSluiceMaterializeOperator",
    "datasluice_default",
)
_RUNTIME_DECLARATION_KEYS = ("operators", "hooks")
_FORBIDDEN_DECLARATION_KEYS = ("hook-class-names", "connection-types")
_SMOKE_ALLOWED_AIRFLOW_IMPORTS = {
    "airflow.providers.datasluice",
    "airflow.providers.datasluice.get_provider_info",
}
_PUBLIC_CORE_MODULES = frozenset(
    {
        "datasluice.application",
        "datasluice.domain.catalog.auth",
        "datasluice.domain.catalog.ids",
        "datasluice.domain.catalog.operations",
        "datasluice.domain.catalog.profiles",
        "datasluice.runtime.clients",
        "datasluice.runtime.credentials",
    }
)
_NEGATIVE_ASSERTION_ALLOWLIST = (
    _PROVIDER_TESTS / "test_provider_metadata.py",
    _PROVIDER_TESTS / "test_public_boundary.py",
)


def _provider_python_files() -> list[Path]:
    return sorted((*_PROVIDER_SOURCE.rglob("*.py"), *_PROVIDER_TESTS.rglob("*.py")))


def _root_name(value: ast.expr) -> str | None:
    current: ast.expr = value
    while isinstance(current, ast.Attribute):
        current = current.value
    return current.id if isinstance(current, ast.Name) else None


def test_provider_python_files_use_only_public_core_imports_and_attributes() -> None:
    for path in _provider_python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        core_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "datasluice":
                core_names.update(alias.asname or alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("datasluice."):
                assert node.module in _PUBLIC_CORE_MODULES, f"{path} imports private core module {node.module!r}"
                core_names.update(alias.asname or alias.name for alias in node.names)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("datasluice."):
                        assert alias.name in _PUBLIC_CORE_MODULES, f"{path} imports private core module {alias.name!r}"
                        core_names.add(alias.asname or alias.name)
                    if alias.name == "datasluice":
                        core_names.add(alias.asname or alias.name)

        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
                root = _root_name(node.value)
                if root in core_names:
                    raise AssertionError(f"{path} accesses private core attribute {root}.{node.attr}")


def test_retired_hook_operator_modules_and_behavior_tests_are_absent() -> None:
    for path in (*_RETIRED_SOURCE_FILES, *_RETIRED_TEST_FILES):
        assert not path.exists(), f"retired runtime surface survived at {path}"


@pytest.mark.parametrize("module_name", _RETIRED_RUNTIME_MODULES)
def test_retired_runtime_modules_are_not_importable(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)


def test_runtime_packages_import_only_new_core_runtime_surfaces() -> None:
    for path in sorted(_PROVIDER_PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("datasluice"):
                        assert alias.name in _PUBLIC_CORE_MODULES, f"{path} imports private core module {alias.name!r}"
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                if node.module.startswith("datasluice"):
                    assert node.module in _PUBLIC_CORE_MODULES, f"{path} imports private core module {node.module!r}"


def test_provider_tree_imports_no_retired_runtime_module() -> None:
    for path in _provider_python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name not in _RETIRED_RUNTIME_MODULES, f"{path} imports retired module {alias.name!r}"
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                assert node.module not in _RETIRED_RUNTIME_MODULES, f"{path} imports retired module {node.module!r}"
                for alias in node.names:
                    imported = f"{node.module}.{alias.name}"
                    assert imported not in _RETIRED_RUNTIME_MODULES, f"{path} imports retired module {imported!r}"


def test_runtime_packages_export_the_new_hook_and_deferred_operator() -> None:
    hooks = importlib.import_module("airflow.providers.datasluice.hooks")
    operators = importlib.import_module("airflow.providers.datasluice.operators")

    assert hooks.__all__ == ["DatasluiceHook"]
    assert operators.__all__ == ["DatasluiceCatalogOperator"]


def test_sample_dag_is_absent() -> None:
    """The legacy sample DAG that imported removed operators is gone."""
    assert not _SAMPLE_DAG.exists(), f"legacy sample DAG survived at {_SAMPLE_DAG}"
    dags_dir = _SAMPLE_DAG.parent
    assert not dags_dir.exists() or not any(dags_dir.iterdir()), f"stray DAG files remain in {dags_dir}"


def test_smoke_module_imports_metadata_and_namespace_only() -> None:
    """The installed-provider smoke check imports the package and metadata only."""
    smoke_path = _PROVIDER_TESTS / "smoke.py"
    source = smoke_path.read_text(encoding="utf-8")
    assert "runpy" not in source, "smoke module must not execute arbitrary provider scripts"
    tree = ast.parse(source, filename=str(smoke_path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("airflow"):
                    imported.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("airflow"):
            imported.add(node.module)
    assert imported == _SMOKE_ALLOWED_AIRFLOW_IMPORTS, f"smoke.py airflow imports are {sorted(imported)}"


def test_provider_tree_references_no_retired_surface() -> None:
    """No provider source, test, DAG, metadata, or packaging text names the retired surface."""
    scanned = [
        *_provider_python_files(),
        _PROVIDER_PACKAGE / "provider.yaml",
        _PROVIDER_ROOT / "pyproject.toml",
    ]
    for path in scanned:
        if not path.exists() or path.resolve() in _NEGATIVE_ASSERTION_ALLOWLIST:
            continue
        text = path.read_text(encoding="utf-8")
        for identifier in _RETIRED_IDENTIFIERS:
            assert identifier not in text, f"{path} references retired surface {identifier!r}"


def test_provider_metadata_declares_runtime_modules_without_connection_registration() -> None:
    """Installed metadata declares runtime modules without a connection type."""
    from airflow.providers.datasluice.get_provider_info import get_provider_info

    info = get_provider_info()
    for key in _RUNTIME_DECLARATION_KEYS:
        assert info[key], f"installed provider metadata omits {key!r}"
    yaml_text = (_PROVIDER_PACKAGE / "provider.yaml").read_text(encoding="utf-8")
    for key in _RUNTIME_DECLARATION_KEYS:
        assert f"{key}:" in yaml_text, f"provider.yaml omits {key!r}"
    for key in _FORBIDDEN_DECLARATION_KEYS:
        assert key not in info, f"installed provider metadata declares {key!r}"
        assert f"{key}:" not in yaml_text, f"provider.yaml declares {key!r}"


def test_airflow_docs_state_phase_boundary_without_retired_examples() -> None:
    """Airflow docs state the Phase 1 boundary with no installation or execution example."""
    assert _AIRFLOW_DOC.exists(), f"Airflow boundary doc missing at {_AIRFLOW_DOC}"
    text = _AIRFLOW_DOC.read_text(encoding="utf-8")
    lowered = text.lower()
    for identifier in _RETIRED_IDENTIFIERS:
        assert identifier not in text, f"airflow.md references retired surface {identifier!r}"
    assert "airflow.providers.datasluice" in text, "airflow.md must stay anchored to the provider namespace"
    for banned in ("pip install", "uv add", "uv pip install"):
        assert banned not in lowered, f"airflow.md offers an installation command {banned!r}"
    assert "```" not in text, "airflow.md must remain a prose boundary note without examples"
    assert "phase 1" in lowered, "airflow.md must state the Phase 1 contract boundary"
    assert "phase 2" in lowered, "airflow.md must state the Phase 2 packaging boundary"
    assert "executor" in lowered, "airflow.md must name the executor dependency for live operators"
