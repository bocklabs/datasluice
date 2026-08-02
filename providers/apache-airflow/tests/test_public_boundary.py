"""Provider public-boundary and private-core import checks."""

from __future__ import annotations

import ast
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
_PROVIDER_SOURCE = _ROOT / "providers" / "apache-airflow" / "src"
_SAMPLE_DAGS = _ROOT / "providers" / "apache-airflow" / "tests" / "dags"


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
