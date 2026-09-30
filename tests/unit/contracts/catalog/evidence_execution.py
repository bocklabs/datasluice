from __future__ import annotations

import ast
from collections.abc import Iterable, Mapping
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

Def = ast.FunctionDef | ast.AsyncFunctionDef


def _own_nodes(node: ast.AST) -> list[ast.AST]:
    """Return the nodes of one function body, excluding nested function definitions."""
    collected: list[ast.AST] = []

    def walk(current: ast.AST) -> None:
        collected.append(current)
        for child in ast.iter_child_nodes(current):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            walk(child)

    walk(node)
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


def _fstring_hole(item: ast.JoinedStr, literals: Mapping[str, list[str]]) -> tuple[str, str | None]:
    """Return the literal template of one f-string and the loop name it interpolates, if any."""
    template = ""
    hole: str | None = None
    for value in item.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            template += value.value
            continue
        if isinstance(value, ast.FormattedValue) and isinstance(value.value, ast.Name) and value.value.id in literals:
            hole = value.value.id
            template += "\x00"
            continue
        return template, None
    return template, hole


def _expanded_fstrings(node: ast.AST) -> set[str]:
    """Return the string literals and one-hole f-strings a pass can produce."""
    literals = _loop_literals(node)
    names: set[str] = set()
    for item in ast.walk(node):
        if not isinstance(item, ast.JoinedStr):
            continue
        template, hole = _fstring_hole(item, literals)
        if hole is None:
            names.add(template)
        else:
            names.update(template.replace("\x00", value) for value in literals[hole])
    return names


def _assigned_value(name: str, scopes: tuple[ast.AST, ...], fallback: ast.expr) -> ast.AST:
    """Return the value the first scope binds to a name, or the fallback when unbound."""
    return next(
        (
            assignment.value
            for scope in scopes
            for assignment in ast.walk(scope)
            if isinstance(assignment, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == name for target in assignment.targets)
        ),
        fallback,
    )


def _enumerated_iterable(
    item: ast.For,
    node: ast.AST,
    functions: Mapping[str, ast.AST] | None,
    enclosing: ast.AST | None,
) -> ast.AST:
    """Return the expression a for-target enumerates, following one name or helper indirection."""
    iterable = item.iter
    if isinstance(iterable, ast.Name):
        scopes = (node, enclosing) if enclosing is not None else (node,)
        return _assigned_value(iterable.id, scopes, iterable)
    if isinstance(iterable, ast.Call) and isinstance(iterable.func, ast.Name) and functions:
        return functions.get(iterable.func.id, iterable)
    return iterable


def _dynamic_names(
    node: ast.AST, functions: Mapping[str, ast.AST] | None = None, enclosing: ast.AST | None = None
) -> dict[str, list[str]]:
    """Return loop variables that enumerate declared strings."""
    names = _loop_literals(node)
    for item in ast.walk(node):
        if not isinstance(item, ast.For) or not isinstance(item.target, ast.Tuple):
            continue
        declared = [
            child.value
            for child in ast.walk(_enumerated_iterable(item, node, functions, enclosing))
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


def _getattr_parameters(function: ast.AST) -> set[str]:
    """Return the local names one function passes to a two-argument getattr."""
    return {
        item.args[1].id
        for item in ast.walk(function)
        if isinstance(item, ast.Call)
        and isinstance(item.func, ast.Name)
        and item.func.id == "getattr"
        and len(item.args) == 2
        and isinstance(item.args[1], ast.Name)
    }


def _argument_methods(argument: ast.expr, bound: dict[str, set[str]], dynamic: set[str]) -> set[str]:
    """Return the method names one call argument can stand for."""
    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
        return {argument.value}
    if isinstance(argument, ast.Name):
        return bound.get(argument.id, set()) | (dynamic if argument.id in dynamic else set())
    return set()


def _bind_call_arguments(
    parameters: list[str],
    arguments: list[ast.expr],
    method_parameters: set[str],
    bound: dict[str, set[str]],
    dynamic: set[str],
) -> bool:
    """Fold one call's arguments into the bound method-name parameters, reporting any growth."""
    changed = False
    for parameter, argument in zip(parameters, arguments, strict=False):
        if parameter not in method_parameters:
            continue
        values = _argument_methods(argument, bound, dynamic)
        changed |= bool(values - bound.setdefault(parameter, set()))
        bound[parameter].update(values)
    return changed


def _propagate_bound_parameters(
    nodes: set[ast.AST],
    functions: Mapping[str, Def],
    method_parameters: Mapping[str, set[str]],
    bound: dict[str, set[str]],
    dynamic: set[str],
) -> bool:
    """Propagate one round of call arguments through every helper, reporting any growth."""
    changed = False
    for name, function in functions.items():
        parameters = [argument.arg for argument in function.args.args]
        for node in nodes:
            for arguments in _matching_call_arguments({node}, name):
                changed |= _bind_call_arguments(parameters, arguments, method_parameters[name], bound, dynamic)
    return changed


def _bound_method_parameters(nodes: set[ast.AST], dynamic: set[str]) -> dict[str, set[str]]:
    """Bind helper method-name parameters from call arguments, propagating through wrappers."""
    functions = {function.name: function for function in nodes if isinstance(function, Def)}
    method_parameters = {name: _getattr_parameters(function) for name, function in functions.items()}
    bound: dict[str, set[str]] = {}
    for _ in range(len(functions) + 1):
        if not _propagate_bound_parameters(nodes, functions, method_parameters, bound, dynamic):
            break
    return bound


def _family_arguments(parameters: list[str], arguments: list[ast.expr]) -> list[str]:
    """Return the parameters one call binds to a typed family attribute."""
    return [
        parameters[index]
        for index, argument in enumerate(arguments)
        if index < len(parameters) and isinstance(argument, ast.Attribute) and argument.attr in CLIENT_FAMILIES
    ]


def _function_family_parameters(function: Def, nodes: set[ast.AST]) -> set[str]:
    """Return the parameters of one function that callers bind to a typed family attribute."""
    parameters = [argument.arg for argument in function.args.args]
    bound: set[str] = set()
    for node in nodes:
        for arguments in _matching_call_arguments({node}, function.name):
            bound.update(_family_arguments(parameters, arguments))
    return bound


def _family_bound_parameters(nodes: set[ast.AST]) -> set[str]:
    """Return helper parameters that callers bind to a typed family attribute."""
    bound: set[str] = set()
    for function in nodes:
        if isinstance(function, Def):
            bound |= _function_family_parameters(function, nodes)
    return bound


def _attribute_methods(item: ast.Call, methods: set[str]) -> set[str]:
    """Return the method a call drives through a typed family attribute."""
    if not isinstance(item.func, ast.Attribute):
        return set()
    target = item.func.value
    if not isinstance(target, ast.Attribute) or target.attr not in CLIENT_FAMILIES:
        return set()
    return {item.func.attr} if item.func.attr in methods else set()


def _family_getattr_receiver(item: ast.Call, family_parameters: set[str]) -> ast.expr | None:
    """Return the receiver of a two-argument getattr aimed at a typed family."""
    if not isinstance(item.func, ast.Name) or item.func.id != "getattr" or len(item.args) != 2:
        return None
    receiver = item.args[0]
    if isinstance(receiver, ast.Attribute) and receiver.attr in CLIENT_FAMILIES:
        return receiver
    if isinstance(receiver, ast.Name) and receiver.id in family_parameters:
        return receiver
    return None


def _getattr_methods(
    item: ast.Call,
    declared: set[str],
    bound_methods: dict[str, set[str]],
    dynamic: dict[str, list[str]],
    family_parameters: set[str],
) -> set[str]:
    """Return the method names a typed family getattr can resolve to."""
    if _family_getattr_receiver(item, family_parameters) is None:
        return set()
    name = item.args[1]
    if isinstance(name, ast.Constant) and isinstance(name.value, str):
        return {name.value}
    if isinstance(name, ast.Name):
        return set(dynamic.get(name.id, ())) | bound_methods.get(name.id, set()) | declared
    return set()


def _call_method_names(
    node: ast.AST,
    methods: set[str],
    declared: set[str],
    bound_methods: dict[str, set[str]],
    family_parameters: set[str],
    enclosing: ast.AST | None = None,
) -> set[str]:
    """Return methods invoked through a typed family attribute or its dynamic dispatch."""
    names: set[str] = set()
    dynamic = _dynamic_names(node, enclosing=enclosing)
    for item in ast.walk(node):
        if not isinstance(item, ast.Call):
            continue
        names |= _attribute_methods(item, methods)
        names |= _getattr_methods(item, declared, bound_methods, dynamic, family_parameters)
    return names & methods


def _reached_nodes(
    node: ast.AST,
    functions: Mapping[str, ast.AST],
    nested: Mapping[str, ast.AST],
    seen: set[ast.AST] | None = None,
) -> set[ast.AST]:
    """Return a pass and every function it can reach through its own call graph."""
    seen = set() if seen is None else seen
    if node in seen:
        return set()
    seen.add(node)
    reachable = {node}
    for name in _own_calls(node):
        if name in functions or name in nested:
            reachable |= _reached_nodes(functions[name] if name in functions else nested[name], functions, nested, seen)
    return reachable


def _cached_reached_nodes(
    node: ast.AST,
    functions: Mapping[str, ast.AST],
    nested: Mapping[str, ast.AST],
    cache: dict[ast.AST, set[ast.AST]],
) -> set[ast.AST]:
    """Return the call closure of a pass, computing it on first use."""
    if node not in cache:
        cache[node] = _reached_nodes(node, functions, nested)
    return cache[node]


def _function_definitions(body: Iterable[ast.AST]) -> dict[str, Def]:
    """Return the functions of a module body, keyed by name."""
    return {node.name: node for node in body if isinstance(node, Def)}


def _passes(target: ast.AST) -> list[ast.AST]:
    """Return a test function together with every function nested inside it."""
    return [target, *(node for node in ast.walk(target) if isinstance(node, Def))]


def _client_modes(node: ast.AST) -> set[str]:
    """Return the client modes one pass constructs."""
    return {
        CLIENT_CONSTRUCTORS[item.func.id]
        for item in _own_nodes(node)
        if isinstance(item, ast.Call) and isinstance(item.func, ast.Name) and item.func.id in CLIENT_CONSTRUCTORS
    }


def _reached_methods(
    reached: ast.AST,
    reached_nodes: set[ast.AST],
    current: ast.AST,
    methods: set[str],
    callable_functions: Mapping[str, ast.AST],
    target: ast.AST,
) -> set[str]:
    """Return the typed method names one function of a pass reaches."""
    names = {name.removeprefix("auth_oauth.") for name in _expanded_fstrings(reached) if name.startswith("auth_oauth.")}
    family_parameters = _family_bound_parameters(reached_nodes)
    declared = {value for reached_node in reached_nodes for value in _string_constants(reached_node)}
    loop_names = set(_dynamic_names(current, callable_functions, enclosing=target)) | set(
        _dynamic_names(reached, callable_functions, enclosing=target)
    )
    bound_methods = _bound_method_parameters(reached_nodes, loop_names)
    return names | _call_method_names(reached, methods, declared, bound_methods, family_parameters, enclosing=target)


def _pass_methods(
    current: ast.AST,
    methods: set[str],
    callable_functions: Mapping[str, ast.AST],
    target: ast.AST,
    closures: dict[ast.AST, set[ast.AST]],
) -> set[str]:
    """Return the typed method names one pass reaches across its whole call closure."""
    reached_nodes = _cached_reached_nodes(current, callable_functions, callable_functions, closures)
    return {
        name
        for reached in reached_nodes
        for name in _reached_methods(reached, reached_nodes, current, methods, callable_functions, target)
    }


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
    functions = _function_definitions(parsed.body)
    target = functions[test_name]
    callable_functions = {**functions, **_function_definitions(ast.walk(target))}
    closures: dict[ast.AST, set[ast.AST]] = {}
    modes_by_method = dict.fromkeys(methods, set())
    for current in _passes(target):
        modes = _client_modes(current)
        if not modes:
            continue
        for method in methods & _pass_methods(current, methods, callable_functions, target, closures):
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
