from __future__ import annotations

import ast
from pathlib import Path

CLIENT_CONSTRUCTORS = {"create_sync_client": "sync", "create_async_client": "async"}


def _own_nodes(node: ast.AST) -> list[ast.AST]:
    """Return the nodes of one function body, excluding nested function definitions."""
    collected: list[ast.AST] = []

    def walk(current: ast.AST, is_root: bool) -> None:
        collected.append(current)
        for child in ast.iter_child_nodes(current):
            if not is_root and isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            walk(child, False)

    walk(node, True)
    return collected


def _own_calls(node: ast.AST) -> list[str]:
    """Return the plain function names called in one function's own body."""
    return [item.func.id for item in _own_nodes(node) if isinstance(item, ast.Call) and isinstance(item.func, ast.Name)]


def _loop_literals(node: ast.AST) -> dict[str, list[str]]:
    """Return each for-target that iterates a literal tuple or list of strings."""
    literals: dict[str, list[str]] = {}
    for item in ast.walk(node):
        if isinstance(item, ast.For) and isinstance(item.target, ast.Name):
            try:
                values = ast.literal_eval(item.iter)
            except (ValueError, TypeError, SyntaxError):
                continue
            if isinstance(values, list | tuple) and all(isinstance(value, str) for value in values):
                literals[item.target.id] = list(values)
    return literals


def _expanded_fstrings(node: ast.AST) -> set[str]:
    """Return the string literals and one-hole f-strings a pass can produce."""
    literals = _loop_literals(node)
    names: set[str] = set()
    for item in ast.walk(node):
        if not isinstance(item, ast.JoinedStr):
            continue
        template = ""
        hole: str | None = None
        for value in item.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                template += value.value
            elif isinstance(value, ast.FormattedValue) and isinstance(value.value, ast.Name):
                if value.value.id in literals:
                    hole = value.value.id
                    template += "\x00"
                else:
                    hole = None
                    break
            else:
                hole = None
                break
        if hole is None:
            names.add(template)
        else:
            names.update(template.replace("\x00", value) for value in literals[hole])
    return names


def executed_modes(source: str, test_name: str, methods: set[str]) -> dict[str, set[str]]:
    """Return, per typed method, the client modes that a pass actually drives.

    Args:
        source: The controlled test module source.
        test_name: The recorded test that claims the coverage.
        methods: Typed method names derived from the recorded operation IDs.

    Returns:
        A mapping of each method to the ``sync`` and ``async`` modes reached by
        a pass that constructs a client of that mode and drives the method.

    Raises:
        KeyError: If the recorded test does not exist in the module.
    """
    functions = {
        node.name: node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    target = functions[test_name]
    closures: dict[ast.AST, set[ast.AST]] = {}

    def closure(node: ast.AST, seen: set[ast.AST] | None = None) -> set[ast.AST]:
        seen = set() if seen is None else seen
        if node in seen:
            return set()
        seen.add(node)
        reachable = {node}
        for name in _own_calls(node):
            if name in functions:
                reachable |= closure(functions[name], seen)
        return reachable

    def cached_closure(node: ast.AST) -> set[ast.AST]:
        if node not in closures:
            closures[node] = closure(node)
        return closures[node]

    passes = [target, *(node for node in ast.walk(target) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef))]
    modes_by_method = dict.fromkeys(methods, set())
    for current in passes:
        modes = {
            CLIENT_CONSTRUCTORS[item.func.id]
            for item in _own_nodes(current)
            if isinstance(item, ast.Call) and isinstance(item.func, ast.Name) and item.func.id in CLIENT_CONSTRUCTORS
        }
        if not modes:
            continue
        for reached in cached_closure(current):
            names = _expanded_fstrings(reached)
            names.update(
                item.func.attr
                for item in ast.walk(reached)
                if isinstance(item, ast.Call) and isinstance(item.func, ast.Attribute)
            )
            names.update(
                item.value
                for item in ast.walk(reached)
                if isinstance(item, ast.Constant) and isinstance(item.value, str)
            )
            for method in methods & names:
                modes_by_method[method] |= modes
    return modes_by_method


def typed_methods(operation_ids: set[str]) -> set[str]:
    """Return the typed method name a profile operation ID is driven through."""
    return {operation.rsplit(".", 1)[-1].replace("-", "_") for operation in operation_ids}


def unexecuted_operations(source: str, test_name: str, operation_ids: set[str], claimed_modes: list[str]) -> set[str]:
    """Return the recorded operations no pass actually drives in every claimed mode."""
    covered = executed_modes(source, test_name, typed_methods(operation_ids))
    return {
        operation for operation in operation_ids if not set(claimed_modes) <= covered[typed_methods({operation}).pop()]
    }


def controlled_test_source(root: Path) -> str:
    """Return the controlled integration test source that backs the recorded evidence."""
    return (root / "tests/integration/connectors/catalog/test_udata_controlled.py").read_text(encoding="utf-8")
