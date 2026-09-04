"""Reconcile independently captured uData route documents without touching connector code."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

PINNED_COMMIT = "0546582058d84706812a1c37387576efc4e5ad1f"
ALLOWED_METHODS = frozenset({"DELETE", "GET", "PATCH", "POST", "PUT"})
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class PreflightError(ValueError):
    """Raised when a preflight record cannot authorize its requested scope."""


class PinnedSourceError(PreflightError):
    """Raised when a source document is not tied to the approved upstream commit."""


class ReconciliationError(PreflightError):
    """Raised when independently captured route sets disagree."""


def digest_file(path: Path) -> str:
    """Return the SHA-256 digest for a file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_commit(source_root: Path) -> str:
    """Read the exact commit checked out in an upstream source tree."""
    completed = subprocess.run(
        ["git", "-C", str(source_root), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise PinnedSourceError("source root must be a git checkout with an exact pinned commit")
    return completed.stdout.strip()


def _module_path(source_root: Path, module: str) -> Path:
    """Resolve an imported upstream module without importing application code."""
    path = source_root.joinpath(*module.split("."))
    if path.with_suffix(".py").is_file():
        return path.with_suffix(".py")
    if (path / "__init__.py").is_file():
        return path / "__init__.py"
    raise ReconciliationError(f"registered API module is missing from source checkout: {module}")


def _ancestor_packages(module: str) -> list[str]:
    """Return the ancestor packages Python executes before importing a module."""
    parts = module.split(".")
    return [".".join(parts[:index]) for index in range(1, len(parts))]


def _relative_base(module: str, level: int, *, is_package: bool) -> str:
    """Resolve an import level against either a package or plain module."""
    parts = module.split(".")
    drop = level - 1 if is_package else level
    return ".".join(parts[: max(len(parts) - drop, 1)])


def _module_imports(tree: ast.AST, module: str, *, is_package: bool) -> set[str]:
    """Return every udata module imported anywhere inside a parsed module."""
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names if alias.name.startswith("udata."))
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = _relative_base(module, node.level, is_package=is_package)
                if node.module:
                    imports.add(f"{base}.{node.module}")
                    for alias in node.names:
                        imports.add(f"{base}.{node.module}.{alias.name}")
                else:
                    for alias in node.names:
                        imports.add(f"{base}.{alias.name}")
            elif node.module and node.module.startswith("udata"):
                imports.add(node.module)
                for alias in node.names:
                    imports.add(f"{node.module}.{alias.name}")
    return {name for name in imports if len(name.split(".")) > 1}


def _stock_modules(source_root: Path) -> list[str]:
    """Discover the transitive import closure started by uData's API initializer."""
    initializer = source_root / "udata" / "api" / "__init__.py"
    try:
        tree = ast.parse(initializer.read_text(encoding="utf-8"), filename=str(initializer))
    except (OSError, SyntaxError) as error:
        raise ReconciliationError("unable to parse uData API initializer") from error
    queue = sorted(_module_imports(tree, "udata.api", is_package=True))
    closure: set[str] = set()
    while queue:
        module = queue.pop()
        if module in closure:
            continue
        candidates = [module, *_ancestor_packages(module)]
        for candidate in candidates:
            if candidate in closure:
                continue
            path = source_root.joinpath(*candidate.split("."))
            module_path = path.with_suffix(".py")
            package_path = path / "__init__.py"
            if not module_path.is_file() and not package_path.is_file():
                continue
            closure.add(candidate)
            parse_path = module_path if module_path.is_file() else package_path
            try:
                candidate_tree = ast.parse(parse_path.read_text(encoding="utf-8"), filename=str(parse_path))
            except (OSError, SyntaxError) as error:
                raise ReconciliationError(f"unable to parse stock module {candidate}") from error
            queue.extend(sorted(_module_imports(candidate_tree, candidate, is_package=not module_path.is_file())))
    return sorted(closure)


def _string_constant(node: ast.AST) -> str:
    """Read a string literal from a static source expression."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    raise ReconciliationError("stock API route declarations must use literal paths and namespaces")


def _own_http_methods(class_def: ast.ClassDef) -> set[str]:
    """Return the literal HTTP verbs implemented directly by a class."""
    return {
        item.name.upper()
        for item in class_def.body
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name.upper() in ALLOWED_METHODS
    }


def _imported_names(tree: ast.AST, module: str, *, is_package: bool) -> dict[str, str]:
    """Map imported names to the absolute udata module providing them."""
    names: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level:
            base = _relative_base(module, node.level, is_package=is_package)
            absolute = f"{base}.{node.module}" if node.module else base
        elif node.module:
            absolute = node.module
        else:
            continue
        if not absolute.startswith("udata"):
            continue
        for alias in node.names:
            names.setdefault(alias.name, absolute)
    return names


def _class_definitions(tree: ast.Module) -> dict[str, ast.ClassDef]:
    """Index top-level classes declared in one module."""
    return {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def _resolve_base_class(
    source_root: Path,
    module: str,
    tree: ast.Module,
    class_registry: dict[str, tuple[str, ast.Module, ast.ClassDef]],
    name: str,
    *,
    is_package: bool,
) -> tuple[str, ast.Module, ast.ClassDef] | None:
    """Resolve a route class base locally or through its importing statement."""
    local = _class_definitions(tree).get(name)
    if local is not None:
        return module, tree, local
    imported = _imported_names(tree, module, is_package=is_package).get(name)
    if imported is None:
        return None
    cached = class_registry.get(f"{imported}:{name}")
    if cached is not None:
        return cached
    try:
        base_path = _module_path(source_root, imported)
        base_tree = ast.parse(base_path.read_text(encoding="utf-8"), filename=str(base_path))
    except (OSError, SyntaxError, ReconciliationError):
        return None
    base_class = _class_definitions(base_tree).get(name)
    if base_class is None:
        return None
    resolved = (imported, base_tree, base_class)
    class_registry[f"{imported}:{name}"] = resolved
    return resolved


def _inherited_http_methods(
    source_root: Path,
    module: str,
    tree: ast.Module,
    class_def: ast.ClassDef,
    class_registry: dict[str, tuple[str, ast.Module, ast.ClassDef]],
    stack: frozenset[str],
    *,
    is_package: bool,
) -> set[str]:
    """Collect HTTP verbs from a class and every resolvable udata base class."""
    methods = _own_http_methods(class_def)
    key = f"{module}:{class_def.name}"
    if key in stack:
        return methods
    for base in class_def.bases:
        if not isinstance(base, ast.Name):
            continue
        resolved = _resolve_base_class(source_root, module, tree, class_registry, base.id, is_package=is_package)
        if resolved is None:
            continue
        base_module, base_tree, base_class = resolved
        base_path = _module_path(source_root, base_module)
        methods.update(
            _inherited_http_methods(
                source_root,
                base_module,
                base_tree,
                base_class,
                class_registry,
                stack | {key},
                is_package=base_path.name == "__init__.py",
            )
        )
    return methods


def _methods(node: ast.Call, route_target: ast.AST, inherited: set[str] | None = None) -> tuple[str, ...]:
    """Read a route decorator's literal HTTP methods."""
    methods = next((item.value for item in node.keywords if item.arg == "methods"), None)
    if methods is None:
        if isinstance(route_target, ast.ClassDef):
            class_methods = inherited or _own_http_methods(route_target)
            if class_methods:
                return tuple(sorted(class_methods))
            raise ReconciliationError("decorated API class exposes no HTTP methods")
        return ("GET",)
    if not isinstance(methods, (ast.List, ast.Tuple)):
        raise ReconciliationError("stock API route methods must be a literal list or tuple")
    values = tuple(_string_constant(element) for element in methods.elts)
    if not values or any(method not in ALLOWED_METHODS for method in values):
        raise ReconciliationError("stock API route methods must be supported uppercase HTTP methods")
    return values


def _namespace_prefixes(tree: ast.AST) -> dict[str, tuple[str, str]]:
    """Resolve statically declared v1, v2, and OAuth namespace prefixes."""
    prefixes = {
        "api": ("/api/1", "v1"),
        "apiv2": ("/api/2", "v2"),
        "blueprint": ("/oauth", "oauth"),
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        if not isinstance(node.value, ast.Call) or not isinstance(node.value.func, ast.Attribute):
            continue
        if node.value.func.attr != "namespace" or not isinstance(node.value.func.value, ast.Name):
            continue
        parent = prefixes.get(node.value.func.value.id)
        if parent is None or not node.value.args:
            continue
        segment = _string_constant(node.value.args[0]).strip("/")
        prefixes[node.targets[0].id] = (f"{parent[0]}/{segment}", parent[1])
    return prefixes


def _decorated_routes(
    node: ast.AST,
    *,
    prefixes: Mapping[str, tuple[str, str]],
    module: str,
    owner: str | None = None,
    inherited: set[str] | None = None,
) -> list[dict[str, str]]:
    """Extract route methods and source signatures from a decorated AST node."""
    routes: list[dict[str, str]] = []
    decorators = getattr(node, "decorator_list", ())
    for decorator in decorators:
        if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
            continue
        if decorator.func.attr != "route" or not isinstance(decorator.func.value, ast.Name):
            continue
        prefix = prefixes.get(decorator.func.value.id)
        if prefix is None:
            continue
        if not decorator.args:
            raise ReconciliationError("stock API route declaration is missing its path")
        route_path = _string_constant(decorator.args[0])
        full_path = f"{prefix[0]}/{route_path.lstrip('/')}"
        signature = f"{module}:{owner or getattr(node, 'name', '<unknown>')}"
        for method in _methods(decorator, node, inherited=inherited):
            routes.append(
                {
                    "api_version": prefix[1],
                    "method": method,
                    "path": full_path,
                    "signature": signature,
                }
            )
    return routes


def _module_routes(
    source_root: Path, module: str, class_registry: dict[str, tuple[str, ast.Module, ast.ClassDef]]
) -> list[dict[str, str]]:
    """Extract literal route declarations from one registered stock API module."""
    path = _module_path(source_root, module)
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError) as error:
        raise ReconciliationError(f"unable to parse stock API module {module}") from error
    prefixes = _namespace_prefixes(tree)
    routes: list[dict[str, str]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            routes.extend(_decorated_routes(node, prefixes=prefixes, module=module, inherited=None))
        elif isinstance(node, ast.ClassDef):
            inherited = _inherited_http_methods(
                source_root, module, tree, node, class_registry, frozenset(), is_package=path.name == "__init__.py"
            )
            routes.extend(_decorated_routes(node, prefixes=prefixes, module=module, inherited=inherited))
    return routes


def extract_source_document(source_root: Path) -> dict[str, Any]:
    """Extract independent route and signature data from the exact upstream checkout."""
    commit = _source_commit(source_root)
    if commit != PINNED_COMMIT:
        raise PinnedSourceError(f"source commit must equal pinned commit {PINNED_COMMIT}")
    class_registry: dict[str, tuple[str, ast.Module, ast.ClassDef]] = {}
    routes = [
        route for module in _stock_modules(source_root) for route in _module_routes(source_root, module, class_registry)
    ]
    route_keys = [_route_key(route) for route in routes]
    if len(route_keys) != len(set(route_keys)):
        raise ReconciliationError("pinned source extraction contains duplicate route methods")
    return {
        "schema_version": 1,
        "source_commit": commit,
        "routes": sorted(routes, key=lambda route: (*_route_key(route), route["signature"])),
    }


def write_source_document(path: Path, document: Mapping[str, Any]) -> None:
    """Write a compact source-only route document for later reconciliation."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(document), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _route_key(route: Mapping[str, Any]) -> tuple[str, str, str]:
    method = route.get("method")
    path = route.get("path")
    api_version = route.get("api_version")
    if not isinstance(method, str) or method not in ALLOWED_METHODS:
        raise ReconciliationError("route method must be a supported uppercase HTTP method")
    if not isinstance(path, str) or not path.startswith("/"):
        raise ReconciliationError("route path must be an absolute path")
    if not isinstance(api_version, str) or api_version not in {"v1", "v2", "oauth"}:
        raise ReconciliationError("route api_version must be v1, v2, or oauth")
    canonical_path = re.sub(r"\{([^}/]+)\}", r"<\1>", path)
    canonical_path = re.sub(r"<[^</:]+:([^/>]+)>", r"<\1>", canonical_path)
    return method, canonical_path, api_version


def load_route_document(path: Path, *, require_pinned_commit: bool = False) -> dict[str, Any]:
    """Load a compact independently captured route document."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ReconciliationError(f"unable to load route document {path}") from error
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ReconciliationError("route document schema_version must be 1")
    routes = document.get("routes")
    if not isinstance(routes, list):
        raise ReconciliationError("route document routes must be a list")
    keys = [_route_key(route) for route in routes if isinstance(route, dict)]
    if len(keys) != len(routes):
        raise ReconciliationError("every route must be an object")
    if len(set(keys)) != len(keys):
        raise ReconciliationError("route document contains duplicate routes")
    if require_pinned_commit and document.get("source_commit") != PINNED_COMMIT:
        raise PinnedSourceError(f"source_commit must equal pinned commit {PINNED_COMMIT}")
    return {"routes": [dict(route) for route in routes], "source_commit": document.get("source_commit")}


def reconcile_route_documents(source: Path, swagger: Path, url_map: Path) -> dict[str, Any]:
    """Fail closed unless every capture channel matches its observable source scope."""
    documents = {
        "source": load_route_document(source, require_pinned_commit=True),
        "swagger": load_route_document(swagger),
        "url_map": load_route_document(url_map),
    }
    route_sets = {name: {_route_key(route) for route in document["routes"]} for name, document in documents.items()}
    baseline = route_sets["source"]
    swagger_scope = {route for route in baseline if route[2] != "oauth"}
    disagreements = {
        "url_map": {
            "missing": sorted(baseline - route_sets["url_map"]),
            "extra": sorted(route_sets["url_map"] - baseline),
        },
        "swagger": {
            "missing": sorted(swagger_scope - route_sets["swagger"]),
            "extra": sorted(route_sets["swagger"] - swagger_scope),
        },
    }
    disagreements = {name: delta for name, delta in disagreements.items() if delta["missing"] or delta["extra"]}
    if disagreements:
        names = ", ".join(sorted(disagreements))
        raise ReconciliationError(f"route reconciliation disagrees with {names}: {disagreements}")
    return {
        "status": "reconciled",
        "route_count": len(baseline),
        "source_commit": PINNED_COMMIT,
        "input_digests": {
            "source": digest_file(source),
            "swagger": digest_file(swagger),
            "url_map": digest_file(url_map),
        },
        "routes": [dict(route) for route in documents["source"]["routes"]],
    }


def write_candidate(path: Path, result: Mapping[str, Any]) -> None:
    """Write a provisional candidate only; it is not a production profile or inventory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_preflight(path: Path, result: Mapping[str, Any]) -> None:
    """Write a decision-pending preflight record from a reconciled result."""
    digests = result["input_digests"]
    lines = [
        "---",
        "schema_version: 1",
        "status: pending",
        "decision: pending",
        f"source_commit: {result['source_commit']}",
        f"route_count: {result['route_count']}",
        f"source_digest: {digests['source']}",
        f"swagger_digest: {digests['swagger']}",
        f"url_map_digest: {digests['url_map']}",
        "targets_approved: false",
        "---",
        "",
        "# uData Phase 04 Preflight",
        "",
        "This record is pending a human approval. It authorizes no production inventory, profile, fixture,",
        "service, or drift work.",
        "",
        "## Reconciliation",
        "",
        "All three route inputs reconciled exactly. Public-target suitability remains an independent human decision.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _frontmatter(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        raise PreflightError("preflight record must start with YAML frontmatter")
    closing = text.find("\n---", 4)
    if closing < 0:
        raise PreflightError("preflight record has no closing frontmatter")
    values: dict[str, str] = {}
    for line in text[4:closing].splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            values[key.strip()] = value.strip()
    return values


def verify_preflight(path: Path) -> dict[str, bool | int | str]:
    """Verify a human-approved record and report its narrowly authorized scopes."""
    values = _frontmatter(path)
    if values.get("status") != "approved":
        raise PreflightError(f"preflight status is {values.get('status', 'missing')}, not approved")
    decision = values.get("decision")
    if decision not in {"approve-baseline-and-targets", "approve-baseline-defer-targets"}:
        raise PreflightError(f"preflight decision is {decision or 'missing'}, not an approval")
    if values.get("source_commit") != PINNED_COMMIT:
        raise PinnedSourceError(f"source_commit must equal pinned commit {PINNED_COMMIT}")
    try:
        route_count = int(values["route_count"])
    except (KeyError, ValueError) as error:
        raise PreflightError("preflight route_count must be an integer") from error
    if route_count <= 0:
        raise PreflightError("preflight route_count must be positive")
    for key in ("source_digest", "swagger_digest", "url_map_digest"):
        if not SHA256.fullmatch(values.get(key, "")):
            raise PreflightError(f"preflight {key} must be a SHA-256 digest")
    if not values.get("approved_by") or not values.get("approved_at"):
        raise PreflightError("preflight approval must name an approver and timestamp")
    targets_approved = values.get("targets_approved") == "true"
    if decision == "approve-baseline-and-targets" and not targets_approved:
        raise PreflightError("target approval decision requires targets_approved: true")
    return {
        "production_approved": True,
        "drift_approved": targets_approved,
        "route_count": route_count,
        "decision": decision,
    }


def build_parser() -> argparse.ArgumentParser:
    """Build the no-network reconciliation command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--swagger", type=Path)
    parser.add_argument("--url-map", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--preflight", type=Path)
    parser.add_argument("--verify-preflight", type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--source-output", type=Path)
    parser.add_argument("--exclude-namespace", action="append", default=[])
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the bounded local-only reconciliation or verification operation."""
    args = build_parser().parse_args(argv)
    if args.exclude_namespace and any((args.source, args.swagger, args.url_map, args.candidate, args.preflight)):
        raise SystemExit("namespace exclusion only applies to source extraction")
    if args.verify_preflight:
        print(json.dumps(verify_preflight(args.verify_preflight), sort_keys=True))
        return 0
    if args.source_root or args.source_output:
        if not args.source_root or not args.source_output:
            raise SystemExit("--source-root and --source-output are required together")
        if any((args.source, args.swagger, args.url_map, args.candidate, args.preflight)):
            raise SystemExit("source extraction cannot be combined with route reconciliation arguments")
        document = extract_source_document(args.source_root)
        if args.exclude_namespace:
            document = {
                **document,
                "routes": [
                    route
                    for route in document["routes"]
                    if not any(
                        route["path"] == namespace or route["path"].startswith(namespace + "/")
                        for namespace in args.exclude_namespace
                    )
                ],
                "excluded_namespaces": sorted(args.exclude_namespace),
            }
        write_source_document(args.source_output, document)
        print(json.dumps({key: value for key, value in document.items() if key != "routes"}, sort_keys=True))
        return 0
    if not all((args.source, args.swagger, args.url_map, args.candidate, args.preflight)):
        raise SystemExit("--source, --swagger, --url-map, --candidate, and --preflight are required together")
    result = reconcile_route_documents(args.source, args.swagger, args.url_map)
    write_candidate(args.candidate, result)
    write_preflight(args.preflight, result)
    print(json.dumps({key: value for key, value in result.items() if key != "routes"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
