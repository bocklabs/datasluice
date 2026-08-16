"""Provider public-boundary and private-core import checks."""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[3]
_PROVIDER_ROOT = _ROOT / "providers" / "apache-airflow"
_PROVIDER_SOURCE = _PROVIDER_ROOT / "src"
_PROVIDER_PACKAGE = _PROVIDER_SOURCE / "airflow" / "providers" / "datasluice"
_SAMPLE_DAGS = _PROVIDER_ROOT / "tests" / "dags"

_RETIRED_RUNTIME_MODULES = (
    "airflow.providers.datasluice.hooks.datasluice",
    "airflow.providers.datasluice.operators.search",
    "airflow.providers.datasluice.operators.materialize",
    "airflow.providers.datasluice.operators._xcom",
)
_RETIRED_SOURCE_FILES = (
    _PROVIDER_PACKAGE / "hooks" / "datasluice.py",
    _PROVIDER_PACKAGE / "operators" / "search.py",
    _PROVIDER_PACKAGE / "operators" / "materialize.py",
    _PROVIDER_PACKAGE / "operators" / "_xcom.py",
)
_RETIRED_TEST_FILES = (
    _PROVIDER_ROOT / "tests" / "test_hook.py",
    _PROVIDER_ROOT / "tests" / "test_search_operator.py",
    _PROVIDER_ROOT / "tests" / "test_materialize_operator.py",
)
_EXECUTION_PACKAGES = ("hooks", "operators")


def _python_files() -> list[Path]:
    return sorted((*_PROVIDER_SOURCE.rglob("*.py"), *_SAMPLE_DAGS.rglob("*.py")))


def _root_name(value: ast.expr) -> str | None:
    current: ast.expr = value
    while isinstance(current, ast.Attribute):
        current = current.value
    return current.id if isinstance(current, ast.Name) else None


def test_provider_and_sample_dag_use_only_public_core_imports_and_attributes() -> None:
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        core_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "datasluice":
                core_names.update(alias.asname or alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("datasluice."):
                raise AssertionError(f"{path} imports private or non-top-level core module {node.module!r}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("datasluice."):
                        raise AssertionError(f"{path} imports private or non-top-level core module {alias.name!r}")
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


def test_runtime_packages_import_neither_core_runner_nor_retired_types() -> None:
    for path in sorted(_PROVIDER_PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name.split(".")[0] != "datasluice", f"{path} imports core module {alias.name!r}"
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                assert node.module.split(".")[0] != "datasluice", f"{path} imports core module {node.module!r}"


def test_provider_source_imports_no_retired_runtime_module() -> None:
    for path in sorted(_PROVIDER_SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name not in _RETIRED_RUNTIME_MODULES, f"{path} imports retired module {alias.name!r}"
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                assert node.module not in _RETIRED_RUNTIME_MODULES, f"{path} imports retired module {node.module!r}"


def test_execution_packages_re_export_nothing_and_declare_no_wrapper() -> None:
    for package_name in _EXECUTION_PACKAGES:
        module = importlib.import_module(f"airflow.providers.datasluice.{package_name}")
        public_names = sorted(name for name in vars(module) if not name.startswith("_"))
        assert public_names == [], f"airflow.providers.datasluice.{package_name} re-exports {public_names}"

        for path in sorted((_PROVIDER_PACKAGE / package_name).rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in tree.body:
                assert not isinstance(node, ast.ClassDef), f"{path} declares class {node.name!r}"
                assert not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)), f"{path} declares {node.name!r}"
                assert not isinstance(node, (ast.Import, ast.ImportFrom)), f"{path} contains module-level imports"
