"""Capture independent route documents from the loopback uData evidence stack."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_spec = importlib.util.spec_from_file_location(
    "extract_udata_oracle", Path(__file__).with_name("extract_udata_oracle.py")
)
assert _spec is not None and _spec.loader is not None
oracle = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(oracle)

INFRASTRUCTURE_ENDPOINT_SUFFIXES = (".doc", ".specs")
INFRASTRUCTURE_PATHS = frozenset(
    {"/api/1", "/api/1/", "/api/1/swagger.json", "/api/2", "/api/2/", "/api/2/swagger.json"}
)
DEFAULT_EXCLUSION_REASONS = {
    "/api/1/proconnect": "browser SSO flow wired by app extensions, outside the stock data-API scope",
}
SWAGGER_SPECS = (("/api/1/swagger.json", "v1"), ("/api/2/swagger.json", "v2"))


def sanitize_origin(origin: str) -> str:
    """Accept only the fixed loopback evidence origin."""
    parsed = urlparse(origin)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.port != 5640
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("route capture origin must be http://127.0.0.1:5640")
    return origin


def _exclusion(
    path: str, endpoint: str | None = None, excluded_namespaces: Mapping[str, str] | None = None
) -> dict[str, str] | None:
    """Return the documented scope exclusion for one observed route, if any."""
    if path in INFRASTRUCTURE_PATHS or (endpoint and endpoint.endswith(INFRASTRUCTURE_ENDPOINT_SUFFIXES)):
        return {"path": path, "reason": "flask-restx documentation infrastructure"}
    for namespace, reason in (excluded_namespaces or {}).items():
        if path == namespace or path.startswith(namespace + "/"):
            return {"path": path, "reason": reason}
    return None


def _canonical(path: str) -> str:
    """Canonicalize a route path to the extractor's comparison spelling."""
    path = re.sub(r"\{([^}/]+)\}", r"<\1>", path)
    return re.sub(r"<[^</:]+:([^/>]+)>", r"<\1>", path)


def capture_swagger(origin: str, output: Path, excluded_namespaces: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Fetch both generated Swagger specs and emit a compact route document."""
    import urllib.request

    safe_origin = sanitize_origin(origin)
    routes: list[dict[str, str]] = []
    excluded: list[dict[str, str]] = []
    for spec_path, api_version in SWAGGER_SPECS:
        with urllib.request.urlopen(safe_origin + spec_path, timeout=30) as response:
            spec = json.loads(response.read().decode("utf-8"))
        base_path = spec.get("basePath")
        if not isinstance(base_path, str) or not base_path.startswith("/api/"):
            raise ValueError(f"unexpected Swagger basePath for {spec_path}")
        for path, operations in sorted(spec.get("paths", {}).items()):
            full_path = base_path + path
            if (exclusion := _exclusion(full_path, excluded_namespaces=excluded_namespaces)) is not None:
                excluded.append(exclusion)
                continue
            for method, operation in sorted(operations.items()):
                if method.lower() in {"get", "post", "put", "patch", "delete"}:
                    routes.append(
                        {
                            "api_version": api_version,
                            "method": method.upper(),
                            "path": _canonical(full_path),
                            "signature": f"swagger:{operation.get('operationId', '')}",
                        }
                    )
    document = {
        "schema_version": 1,
        "capture": "swagger",
        "routes": routes,
        "excluded": excluded,
        "note": "flask-restx generated Swagger cannot express the OAuth blueprint",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return document


def capture_url_map(
    raw_input: Path, output: Path, excluded_namespaces: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Convert a sanitized URL-map dump into a compact route document."""
    raw_routes = json.loads(raw_input.read_text(encoding="utf-8"))
    if not isinstance(raw_routes, list):
        raise ValueError("URL-map input must be a list of route records")
    routes: list[dict[str, str]] = []
    excluded: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for record in raw_routes:
        path = record.get("path")
        method = record.get("method")
        endpoint = record.get("endpoint")
        if not isinstance(path, str) or not isinstance(method, str):
            raise ValueError("URL-map records need path and method strings")
        if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            continue
        if path.startswith("/api/1"):
            api_version = "v1"
        elif path.startswith("/api/2"):
            api_version = "v2"
        elif path.startswith("/oauth"):
            api_version = "oauth"
        else:
            continue
        if (exclusion := _exclusion(path, endpoint, excluded_namespaces=excluded_namespaces)) is not None:
            excluded.append(exclusion)
            continue
        key = (method, _canonical(path), api_version)
        if key in seen:
            continue
        seen.add(key)
        routes.append(
            {
                "api_version": api_version,
                "method": method,
                "path": _canonical(path),
                "signature": f"url_map:{endpoint or ''}",
            }
        )
    document = {
        "schema_version": 1,
        "capture": "url_map",
        "routes": sorted(routes, key=lambda route: (route["method"], route["path"], route["api_version"])),
        "excluded": excluded,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return document


def main() -> int:
    """Run one bounded route-document capture."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    swagger_parser = subparsers.add_parser("swagger")
    swagger_parser.add_argument("--origin", required=True)
    swagger_parser.add_argument("--output", type=Path, required=True)
    swagger_parser.add_argument("--exclude-namespace", action="append", default=[])
    url_map_parser = subparsers.add_parser("url-map")
    url_map_parser.add_argument("--input", type=Path, required=True)
    url_map_parser.add_argument("--output", type=Path, required=True)
    url_map_parser.add_argument("--exclude-namespace", action="append", default=[])
    args = parser.parse_args()
    excluded_namespaces = {
        namespace: DEFAULT_EXCLUSION_REASONS.get(
            namespace, "namespace excluded by the recorded preflight scope decision"
        )
        for namespace in args.exclude_namespace
    }
    if args.command == "swagger":
        document = capture_swagger(args.origin, args.output, excluded_namespaces)
    else:
        document = capture_url_map(args.input, args.output, excluded_namespaces)
    print(json.dumps({"routes": len(document["routes"]), "excluded": len(document["excluded"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
