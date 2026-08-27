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
ALLOWED_METHODS = frozenset({"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"})
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


def _api_modules(source_root: Path) -> list[str]:
    """Return the stock API modules registered by uData's API initializer."""
    initializer = source_root / "udata" / "api" / "__init__.py"
    try:
        tree = ast.parse(initializer.read_text(encoding="utf-8"), filename=str(initializer))
    except (OSError, SyntaxError) as error:
        raise ReconciliationError("unable to parse uData API initializer") from error
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(
                alias.name for alias in node.names if alias.name.startswith("udata.") and ".api" in alias.name
            )
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("udata."):
            if ".api" in node.module:
                modules.add(node.module)
    return sorted(modules)


def _module_path(source_root: Path, module: str) -> Path:
    """Resolve an imported upstream module without importing application code."""
    path = source_root.joinpath(*module.split("."))
    if path.with_suffix(".py").is_file():
        return path.with_suffix(".py")
    if (path / "__init__.py").is_file():
        return path / "__init__.py"
    raise ReconciliationError(f"registered API module is missing from source checkout: {module}")


def _string_constant(node: ast.AST) -> str:
    """Read a string literal from a static source expression."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    raise ReconciliationError("stock API route declarations must use literal paths and namespaces")


def _methods(node: ast.Call, route_target: ast.AST) -> tuple[str, ...]:
    """Read a route decorator's literal HTTP methods."""
    methods = next((item.value for item in node.keywords if item.arg == "methods"), None)
    if methods is None:
        if isinstance(route_target, ast.ClassDef):
            class_methods = tuple(
                item.name.upper()
                for item in route_target.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name.upper() in ALLOWED_METHODS
            )
            if class_methods:
                return class_methods
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
        for method in _methods(decorator, node):
            routes.append(
                {
                    "api_version": prefix[1],
                    "method": method,
                    "path": full_path,
                    "signature": signature,
                }
            )
    return routes


def _module_routes(source_root: Path, module: str) -> list[dict[str, str]]:
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
            routes.extend(_decorated_routes(node, prefixes=prefixes, module=module))
        elif isinstance(node, ast.ClassDef):
            routes.extend(_decorated_routes(node, prefixes=prefixes, module=module))
    return routes


def extract_source_document(source_root: Path) -> dict[str, Any]:
    """Extract independent route and signature data from the exact upstream checkout."""
    commit = _source_commit(source_root)
    if commit != PINNED_COMMIT:
        raise PinnedSourceError(f"source commit must equal pinned commit {PINNED_COMMIT}")
    routes = [route for module in _api_modules(source_root) for route in _module_routes(source_root, module)]
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
    return method, path, api_version


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
    """Fail closed unless all three independently captured route sets are identical."""
    documents = {
        "source": load_route_document(source, require_pinned_commit=True),
        "swagger": load_route_document(swagger),
        "url_map": load_route_document(url_map),
    }
    route_sets = {name: {_route_key(route) for route in document["routes"]} for name, document in documents.items()}
    baseline = route_sets["source"]
    disagreements = {
        name: {"missing": sorted(baseline - routes), "extra": sorted(routes - baseline)}
        for name, routes in route_sets.items()
        if routes != baseline
    }
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the bounded local-only reconciliation or verification operation."""
    args = build_parser().parse_args(argv)
    if args.verify_preflight:
        print(json.dumps(verify_preflight(args.verify_preflight), sort_keys=True))
        return 0
    if args.source_root or args.source_output:
        if not args.source_root or not args.source_output:
            raise SystemExit("--source-root and --source-output are required together")
        if any((args.source, args.swagger, args.url_map, args.candidate, args.preflight)):
            raise SystemExit("source extraction cannot be combined with route reconciliation arguments")
        document = extract_source_document(args.source_root)
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
