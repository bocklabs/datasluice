from __future__ import annotations

import ast
from collections.abc import Mapping
from pathlib import Path

CLIENT_CONSTRUCTORS = {"create_sync_client": "sync", "create_async_client": "async"}
CLIENT_FAMILIES = {"auth_oauth", "taxonomies", "users_tokens", "organizations_memberships"}
_TYPED_METHOD_ALIASES = {
    "add_dataset_badge": "add_badge",
    "available_dataset_badges": "available_badges",
    "delete_dataset_badge": "delete_badge",
    "get_dataset_schemas": "dataset_schemas",
    "list_dataset_schemas": "schemas",
}


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


def _dynamic_names(node: ast.AST, functions: Mapping[str, ast.AST] | None = None) -> dict[str, list[str]]:
    """Return loop variables that enumerate declared strings."""
    names = _loop_literals(node)
    for item in ast.walk(node):
        if not isinstance(item, ast.For) or not isinstance(item.target, ast.Tuple):
            continue
        iterable = item.iter
        if isinstance(iterable, ast.Name):
            iterable = next(
                (
                    assignment.value
                    for assignment in ast.walk(node)
                    if isinstance(assignment, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == iterable.id for target in assignment.targets)
                ),
                iterable,
            )
        elif isinstance(iterable, ast.Call) and isinstance(iterable.func, ast.Name) and functions:
            iterable = functions.get(iterable.func.id, iterable)
        declared = [
            child.value
            for child in ast.walk(iterable)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)
        ]
        for target in item.target.elts:
            if isinstance(target, ast.Name):
                names.setdefault(target.id, []).extend(declared)
    return names


def _string_constants(node: ast.AST) -> set[str]:
    return {item.value for item in ast.walk(node) if isinstance(item, ast.Constant) and isinstance(item.value, str)}


def _matching_call_arguments(nodes: set[ast.AST], function_name: str) -> list[list[ast.expr]]:
    """Return argument lists for calls to one helper function."""
    return [
        item.args
        for node in nodes
        for item in ast.walk(node)
        if isinstance(item, ast.Call) and isinstance(item.func, ast.Name) and item.func.id == function_name
    ]


def _bound_method_parameters(nodes: set[ast.AST], dynamic: set[str]) -> dict[str, set[str]]:
    """Bind helper method-name parameters from call arguments, propagating through wrappers."""
    functions = {
        function.name: function for function in nodes if isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    method_parameters = {
        name: {
            item.args[1].id
            for item in ast.walk(function)
            if isinstance(item, ast.Call)
            and isinstance(item.func, ast.Name)
            and item.func.id == "getattr"
            and len(item.args) == 2
            and isinstance(item.args[1], ast.Name)
        }
        for name, function in functions.items()
    }
    bound: dict[str, set[str]] = {}
    for _ in range(len(functions) + 1):
        changed = False
        for name, function in functions.items():
            parameters = [argument.arg for argument in function.args.args]
            for node in nodes:
                for arguments in _matching_call_arguments({node}, name):
                    for parameter, argument in zip(parameters, arguments, strict=False):
                        if parameter not in method_parameters[name]:
                            continue
                        values: set[str] = set()
                        if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                            values = {argument.value}
                        elif isinstance(argument, ast.Name):
                            values = bound.get(argument.id, set()) | (dynamic if argument.id in dynamic else set())
                        changed |= bool(values - bound.setdefault(parameter, set()))
                        bound[parameter].update(values)
        if not changed:
            break
    return bound


def _family_bound_parameters(nodes: set[ast.AST]) -> set[str]:
    """Return helper parameters that callers bind to a typed family attribute."""
    bound: set[str] = set()
    for function in nodes:
        if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        parameters = [argument.arg for argument in function.args.args]
        for node in nodes:
            for arguments in _matching_call_arguments({node}, function.name):
                bound.update(
                    parameters[index]
                    for index, argument in enumerate(arguments)
                    if index < len(parameters)
                    and isinstance(argument, ast.Attribute)
                    and argument.attr in CLIENT_FAMILIES
                )
    return bound


def _call_method_names(
    node: ast.AST,
    methods: set[str],
    declared: set[str],
    bound_methods: dict[str, set[str]],
    family_parameters: set[str],
) -> set[str]:
    """Return methods invoked through a typed family attribute or its dynamic dispatch."""
    names: set[str] = set()
    dynamic = _dynamic_names(node)
    for item in ast.walk(node):
        if not isinstance(item, ast.Call):
            continue
        if isinstance(item.func, ast.Attribute):
            target = item.func.value
            if isinstance(target, ast.Attribute) and target.attr in CLIENT_FAMILIES and item.func.attr in methods:
                names.add(item.func.attr)
        if (
            isinstance(item.func, ast.Name)
            and item.func.id == "getattr"
            and len(item.args) == 2
            and (
                (isinstance(item.args[0], ast.Attribute) and item.args[0].attr in CLIENT_FAMILIES)
                or (isinstance(item.args[0], ast.Name) and item.args[0].id in family_parameters)
            )
        ):
            if isinstance(item.args[1], ast.Constant) and isinstance(item.args[1].value, str):
                names.add(item.args[1].value)
            elif isinstance(item.args[1], ast.Name):
                names.update(dynamic.get(item.args[1].id, set()))
                names.update(bound_methods.get(item.args[1].id, set()))
                names.update(declared)
    return names & methods


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
    parsed = ast.parse(source)
    functions = {node.name: node for node in parsed.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)}
    target = functions[test_name]
    nested_functions = {
        node.name: node for node in ast.walk(target) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    callable_functions = {**functions, **nested_functions}
    closures: dict[ast.AST, set[ast.AST]] = {}

    def closure(node: ast.AST, seen: set[ast.AST] | None = None) -> set[ast.AST]:
        seen = set() if seen is None else seen
        if node in seen:
            return set()
        seen.add(node)
        reachable = {node}
        for name in _own_calls(node):
            if name in functions or name in nested_functions:
                reachable |= closure(functions[name] if name in functions else nested_functions[name], seen)
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
        reached_nodes = cached_closure(current) | set(nested_functions.values())
        for reached in reached_nodes:
            names = {
                name.removeprefix("auth_oauth.")
                for name in _expanded_fstrings(reached)
                if name.startswith("auth_oauth.")
            }
            family_parameters = _family_bound_parameters(reached_nodes)
            declared = {value for reached_node in reached_nodes for value in _string_constants(reached_node)}
            loop_names = set(_dynamic_names(current, callable_functions)) | set(
                _dynamic_names(reached, callable_functions)
            )
            bound_methods = _bound_method_parameters(reached_nodes, loop_names)
            names |= _call_method_names(reached, methods, declared, bound_methods, family_parameters)
            for method in methods & names:
                modes_by_method[method] |= modes
    return modes_by_method


def typed_methods(operation_ids: set[str]) -> set[str]:
    """Return the typed method name a profile operation ID is driven through."""
    return {
        _TYPED_METHOD_ALIASES.get(method := operation.rsplit(".", 1)[-1].replace("-", "_"), method)
        for operation in operation_ids
    }


def unexecuted_operations(source: str, test_name: str, operation_ids: set[str], claimed_modes: list[str]) -> set[str]:
    """Return the recorded operations no pass actually drives in every claimed mode."""
    covered = executed_modes(source, test_name, typed_methods(operation_ids))
    return {
        operation for operation in operation_ids if not set(claimed_modes) <= covered[typed_methods({operation}).pop()]
    }


def controlled_test_source(root: Path) -> str:
    """Return the controlled integration test source that backs the recorded evidence."""
    return (root / "tests/integration/connectors/catalog/test_udata_controlled.py").read_text(encoding="utf-8")
