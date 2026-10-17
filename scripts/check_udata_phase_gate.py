"""Fail-closed uData phase gate: independent inventory, semantic, review, and cleanup checks.

The gate reads checked-in artifacts and runs the authoritative suites; it never trusts connector-reported or hand-
written totals. Every check fails closed: an unreadable artifact, a pending status, or an unverifiable claim fails the
phase.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib
import json
import re
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = Path(__file__).resolve().parents[1]
PHASE_DIR = ROOT / ".planning" / "phases" / "04-udata-connector"
PROFILE_PATH = ROOT / "src/datasluice/contracts/catalog/profiles/udata-17.6.json"
STOCK_BINDINGS_PATH = ROOT / "scripts/data/udata-17.6-stock-route-bindings.json"
COVERAGE_PATH = PHASE_DIR / "COVERAGE.md"
VALIDATION_PATH = PHASE_DIR / "04-VALIDATION.md"
CONFORMANCE_PATH = PHASE_DIR / "04-CONFORMANCE.md"
REVIEWS_ROOT = ROOT / "dev/udata-evidence/reviews"

FAMILIES = (
    "tracer",
    "datasets",
    "root_profile",
    "resource",
    "organization",
    "user",
    "oauth",
    "taxonomy",
    "activity_discussion",
    "reuse",
    "spatial",
    "dataservices",
    "posts_report",
    "topic",
    "contact_visualization",
    "jobs",
    "harvest",
    "transfers",
    "extensions",
)

REVIEWS_DIR_NAMES = {
    "organization": "organizations_memberships",
    "user": "users_tokens",
    "oauth": "auth_oauth",
    "taxonomy": "taxonomies",
    "activity_discussion": "activity_discussions",
    "reuse": "reuses",
    "posts_report": "posts_reports",
    "topic": "topics",
    "resource": "resources",
}


class GateError(Exception):
    """One gate check's fail-closed refusal to pass the phase."""


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _run(command: list[str]) -> tuple[int, str]:
    completed = subprocess.run(command, capture_output=True, text=True, cwd=ROOT)
    return completed.returncode, (completed.stdout + completed.stderr)


def profile_operations(path: Path = PROFILE_PATH) -> list[str]:
    profile = json.loads(path.read_text(encoding="utf-8"))
    operations = [operation["id"] for operation in profile["operations"]]
    if len(operations) != len(set(operations)):
        raise SystemExit("GATE FAIL: the profile declares duplicate operations")
    return operations


def coverage_inventory() -> set[str]:
    """Return the declared INTEGRATE rows, not an independent upstream oracle."""
    rows = re.findall(
        r"^\|\s*(\w+)\.([\w.-]+)\s*\|\s*INTEGRATE\s*\|",
        COVERAGE_PATH.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if not rows:
        raise GateError("GATE FAIL: the coverage matrix is empty or unreadable")
    return {f"{platform}.{name}" for platform, name in rows}


def surface_completeness() -> tuple[int, int]:
    """Return the surface score only after the independent route bijection and quality checks pass."""
    require_stock_bijection()
    code, output = _run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/quality/test_catalog_surface.py::test_locked_coverage_matrix_matches_profiles_exactly",
            "tests/unit/contracts/catalog/test_udata_profile.py::test_profile_covers_each_udata_integrate_capability_exactly_once",
            "-q",
            "--no-header",
            "-p",
            "no:cacheprovider",
        ]
    )
    if code != 0:
        print(output[-4000:])
        raise GateError("GATE FAIL: the locked matrix and the profile diverge")
    inventory = coverage_inventory()
    return len(inventory), len(inventory)


def require_stock_bijection() -> None:
    """Compare approved upstream captures, coverage routes, and explicit profile route bindings."""
    _review_gate()
    oracle = importlib.import_module("extract_udata_oracle")
    directory = PHASE_DIR / "oracle"
    try:
        _, rows, coverage_routes, source_commit = _bijection_inputs(oracle, directory)
        identifiers = _profile_identifiers()
        _validate_bindings(oracle, identifiers, rows, coverage_routes, source_commit)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        raise GateError(f"independent upstream bijection cannot be verified: {error}") from error


def _bijection_inputs(oracle: Any, directory: Path) -> tuple[set[Any], list[Any], list[Any], str]:
    """Return the approved route set, coverage rows, their routes, and the source commit."""
    candidate = directory / "udata-oracle-candidate.json"
    approval = oracle.verify_preflight(PHASE_DIR / "04-PREFLIGHT.md", candidate)
    result = oracle.reconcile_route_documents(
        directory / "udata-source-routes.json",
        directory / "udata-swagger-routes.json",
        directory / "udata-urlmap-routes.json",
    )
    if result != json.loads(candidate.read_text(encoding="utf-8")):
        raise GateError("approved candidate does not match the independent upstream captures")
    routes = {oracle._route_key(route) for route in result["routes"]}
    if len(routes) != approval["route_count"]:
        raise GateError("approved route count does not match the independent upstream captures")
    rows = re.findall(
        r"^\|\s*\d+\s*\|\s*`([^`]+)`\s*\|\s*(v1|v2|oauth)\s*\|\s*([A-Z]+)\s*\|\s*`([^`]+)`"
        r"\s*\|[^|]*\|[^|]*\|\s*INTEGRATE\s*\|",
        COVERAGE_PATH.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    coverage_routes = [
        oracle._route_key({"api_version": version, "method": method, "path": path}) for _, version, method, path in rows
    ]
    if (
        len(coverage_routes) != len(routes)
        or set(coverage_routes) != routes
        or len({row[0] for row in rows}) != len(rows)
        or {row[0] for row in rows} != coverage_inventory()
    ):
        raise GateError("coverage rows do not form a bijection with the independent upstream routes")
    return routes, rows, coverage_routes, result["source_commit"]


def _profile_identifiers() -> list[str]:
    profile = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
    identifiers = [operation["id"] for operation in profile["operations"]]
    if any(not isinstance(identifier, str) or not identifier for identifier in identifiers):
        raise GateError("profile operation IDs must be non-empty strings")
    if len(identifiers) != len(set(identifiers)):
        raise GateError("profile declares duplicate operations")
    return identifiers


def _validate_bindings(
    oracle: Any,
    identifiers: list[str],
    rows: list[Any],
    coverage_routes: list[Any],
    source_commit: str,
) -> None:
    """Require the binding artifact to partition the profile and match coverage exactly."""
    document = json.loads(STOCK_BINDINGS_PATH.read_text(encoding="utf-8"))
    bindings = _binding_sets(document, source_commit)
    _require_partition(document, identifiers)
    _require_coverage_bijection(oracle, bindings, rows, coverage_routes)


def _binding_sets(document: dict[str, Any], source_commit: str) -> list[dict[str, Any]]:
    """Validate the binding header and return the binding rows."""
    profile = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
    if (
        document["schema_version"] != 1
        or document["source_commit"] != source_commit
        or document["profile_version"] != profile["profile_version"]
    ):
        raise GateError("stock route bindings do not match the approved source and profile versions")
    bindings = document["bindings"]
    non_route_operations = document["non_route_operations"]
    if not isinstance(bindings, list) or not isinstance(non_route_operations, dict):
        raise GateError("stock route bindings require a list and explicit non-route profile operations")
    if any(not isinstance(reason, str) or not reason.strip() for reason in non_route_operations.values()):
        raise GateError("non-route profile operations require a reason")
    return bindings


def _require_partition(document: dict[str, Any], identifiers: list[str]) -> None:
    """Require route owners and declared non-route operations to partition the profile."""
    owners = [binding["operation_id"] for binding in document["bindings"]]
    if any(not isinstance(owner, str) or owner not in identifiers for owner in owners):
        raise GateError("stock route bindings reference an undeclared profile operation")
    non_route_operations = document["non_route_operations"]
    if set(owners) & non_route_operations.keys() or set(identifiers) != set(owners) | non_route_operations.keys():
        raise GateError("stock route owners and non-route operations must partition the declared profile operations")


def _require_coverage_bijection(
    oracle: Any,
    bindings: list[dict[str, Any]],
    rows: list[Any],
    coverage_routes: list[Any],
) -> None:
    """Require every bound capability to name the same route as its coverage row."""
    capabilities = [binding["capability_id"] for binding in bindings]
    if any(not isinstance(capability, str) or not capability for capability in capabilities):
        raise GateError("stock route bindings require non-empty capability IDs")
    bound_routes = [oracle._route_key(binding) for binding in bindings]
    if len(set(capabilities)) != len(bindings) or len(set(bound_routes)) != len(bindings):
        raise GateError("stock route bindings contain duplicate capabilities or routes")
    expected = dict(zip((row[0] for row in rows), coverage_routes, strict=True))
    actual = dict(zip(capabilities, bound_routes, strict=True))
    if actual == expected:
        return
    missing = sorted(expected.keys() - actual.keys())
    extra = sorted(actual.keys() - expected.keys())
    mismatched = sorted(
        capability for capability in actual.keys() & expected.keys() if actual[capability] != expected[capability]
    )
    raise GateError(
        f"stock route bindings do not form an independent upstream bijection: "
        f"missing={missing}, extra={extra}, wrong-route={mismatched}"
    )


_RECORDLESS_FAMILIES = frozenset({"tracer", "datasets", "root_profile"})


def _review_gate() -> Any:
    """Import the review gate module from ``scripts/`` regardless of install layout."""
    scripts_dir = str(ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    import check_udata_review_gate as review_gate  # type: ignore[import-not-found]

    return review_gate


def _family_semantic_gaps(family: str, evidence: dict[str, object]) -> list[str]:
    """Return the reasons one family lacks semantic coverage."""
    record = evidence.get(f"controlled_{family}_evidence")
    if record is None and family not in _RECORDLESS_FAMILIES:
        return ["no controlled evidence record"]
    review_gate = _review_gate()
    try:
        review_gate.verify_current_review(REVIEWS_ROOT, REVIEWS_DIR_NAMES.get(family, family), ROOT)
    except (review_gate.ReviewGateError, OSError, ValueError) as error:
        return [f"no verified current-head review — {error}"]
    return []


def semantic_conformance() -> tuple[int, int]:
    """Return (families with green controlled evidence and a passed review, families).

    A family is covered when its controlled evidence record exists and its current-head review passes. The three pre-
    convention families (tracer, datasets, root_profile) predate controlled evidence records; their controlled proof
    lives in the shared controlled-stack tests, so the receipt alone carries their review credit.
    """
    evidence = json.loads(
        (ROOT / "src/datasluice/contracts/catalog/fixtures/udata/evidence.json").read_text(encoding="utf-8")
    )
    covered = 0
    total = 0
    for family in FAMILIES:
        total += 1
        gaps = _family_semantic_gaps(family, evidence)
        if gaps:
            for gap in gaps:
                print(f"semantic gap: {family} — {gap}")
            continue
        covered += 1
    return covered, total


def require_validation_authoritative(path: Path = VALIDATION_PATH) -> None:
    text = path.read_text(encoding="utf-8")
    if re.search(r"status:\s*(pending|draft)", text, re.IGNORECASE):
        raise GateError("GATE FAIL: 04-VALIDATION.md still contains pending or draft status rows")
    if "⬜" in text:
        raise GateError("GATE FAIL: 04-VALIDATION.md still contains unchecked validation cells")


def require_review_current(reviews_root: Path = REVIEWS_ROOT, repo_root: Path = ROOT) -> None:
    """Require every family's review receipt to verify at the current merge head.

    The reviewed SHA must equal the merge head exactly: a family whose newest
    receipt is bound to any other sha, or that has no receipt at all, fails
    the gate closed.
    """
    review_gate = _review_gate()
    for family in FAMILIES:
        dir_name = REVIEWS_DIR_NAMES.get(family, family)
        try:
            review_gate.verify_current_review(reviews_root, dir_name, repo_root)
        except (review_gate.ReviewGateError, OSError, ValueError) as err:
            raise GateError(f"GATE FAIL: {family} is not reviewed at the merge head — {err}") from err


_REVIEW_ARTIFACT_NAMES = ("source-review.md", "current-thread.json", "review-receipt.json")


def _require_cleanup_directories() -> None:
    for root in (PHASE_DIR, REVIEWS_ROOT):
        if not root.is_dir():
            raise GateError(f"GATE FAIL: cleanup directory is missing: {root}")


def _recovery_markers() -> list[str]:
    return [
        str(path)
        for root in (PHASE_DIR, REVIEWS_ROOT)
        for path in root.rglob("*")
        if ".review-recovery" in path.name or "recovery" in path.name.lower()
    ]


def _incomplete_review_directory() -> Path | None:
    for directory in REVIEWS_ROOT.glob("*/*"):
        if not directory.is_dir() or re.fullmatch(r"[0-9a-f]{40}", directory.name) is None:
            continue
        if any(not (directory / name).is_file() for name in _REVIEW_ARTIFACT_NAMES):
            return directory
    return None


def _require_single_worktree() -> None:
    code, output = _run(["git", "worktree", "list", "--porcelain"])
    if code != 0:
        raise GateError("GATE FAIL: git worktree state cannot be read")
    entries = [block for block in output.split("\n\n") if block.strip()]
    if len(entries) != 1 or f"worktree {ROOT.resolve()}" not in entries[0].splitlines():
        raise GateError(f"GATE FAIL: extra git worktrees remain:\n{output}")


def _require_clean_status() -> None:
    code, output = _run(["git", "status", "--porcelain"])
    if code != 0:
        raise GateError("GATE FAIL: git working-tree state cannot be read")
    if output.strip():
        raise GateError(f"GATE FAIL: the working tree is not clean:\n{output}")


def require_cleanup() -> None:
    _require_cleanup_directories()
    markers = _recovery_markers()
    if markers:
        raise GateError(f"GATE FAIL: review recovery markers remain: {markers}")
    incomplete = _incomplete_review_directory()
    if incomplete is not None:
        raise GateError(f"GATE FAIL: incomplete review directory remains: {incomplete}")
    _require_single_worktree()
    _require_clean_status()


_FIXED_BOUNDARY_NAME = "candidate wheel member datasluice/connectors/catalog/udata/clients.py"
_FIXED_BOUNDARY_VALUE = "http://127.0.0.1:5640"
_OAUTH_ROUTE_TABLE = "candidate wheel member datasluice/connectors/catalog/udata/wire/oauth.py"
_FAKES_SENTINEL = "candidate wheel member datasluice/contracts/catalog/fakes.py"


def _is_fixed_safety_boundary(name: str, key: str, value: str) -> bool:
    """Return whether one literal is a declared safety boundary, not a retained secret."""
    if name == _OAUTH_ROUTE_TABLE and key == "access_token" and value in {"udata/oauth.access-token", "/oauth/token"}:
        return True
    return name == _FAKES_SENTINEL and key == "access_token" and value == "never-report-this"


def _control_origin_nodes(tree: ast.AST, name: str) -> set[ast.AST]:
    """Return the nodes of the one allowlisted loopback-origin constant in the connector client."""
    if name != _FIXED_BOUNDARY_NAME:
        return set()
    boundary: set[ast.AST] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if (
            isinstance(target, ast.Name)
            and target.id == "_CONTROLLED_ORIGIN"
            and isinstance(node.value, ast.Constant)
            and node.value.value == _FIXED_BOUNDARY_VALUE
        ):
            boundary.update((node, node.value))
    return boundary


def _scan_python_literals(content: bytes, name: str, review_gate: Any) -> None:
    """Reject secret-shaped literals, mapping pairs, and constant assignments in one source member."""
    text = content.decode("utf-8")
    tree = ast.parse(text)
    boundary = _control_origin_nodes(tree, name)
    for node in ast.walk(tree):
        if node in boundary:
            continue
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            review_gate.validate_artifact_content(node.value, name, reject_payload_keys=False)
        elif isinstance(node, ast.Dict):
            _scan_mapping_literals(node, name, review_gate)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)) and isinstance(node.value, ast.Constant):
            _scan_constant_assignment(node, name, review_gate)


def _scan_mapping_literals(node: ast.Dict, name: str, review_gate: Any) -> None:
    for key, value in zip(node.keys, node.values, strict=True):
        if not (
            isinstance(key, ast.Constant)
            and isinstance(key.value, str)
            and isinstance(value, ast.Constant)
            and isinstance(value.value, str)
        ):
            continue
        if _is_fixed_safety_boundary(name, key.value, value.value):
            continue
        review_gate.validate_artifact_content(json.dumps({key.value: value.value}), name, reject_payload_keys=False)


def _scan_constant_assignment(node: ast.Assign | ast.AnnAssign, name: str, review_gate: Any) -> None:
    if not isinstance(node.value, ast.Constant) or not isinstance(node.value.value, str):
        return
    for target in node.targets if isinstance(node, ast.Assign) else (node.target,):
        if isinstance(target, ast.Name):
            review_gate.validate_artifact_content(
                json.dumps({target.id.lower(): node.value.value}), name, reject_payload_keys=False
            )


def _artifact_check(review_gate: Any) -> Callable[[bytes, str], None]:
    def check(content: bytes, name: str) -> None:
        if name.endswith(".py"):
            _scan_python_literals(content, name, review_gate)
        else:
            review_gate.validate_artifact_content(content.decode("utf-8"), name, reject_payload_keys=False)

    return check


def _require_regular_file(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise GateError(f"retained artifact is not a regular file or contains a symbolic link: {path}")


def _scan_retained_artifacts(check: Callable[[bytes, str], None]) -> None:
    """Scan the retained oracle, review, and fixture trees without following anything unusual."""
    for root in (PHASE_DIR / "oracle", REVIEWS_ROOT, ROOT / "src/datasluice/contracts/catalog/fixtures/udata"):
        if not root.is_dir() or root.is_symlink():
            raise GateError(f"retained artifact directory is missing or unsafe: {root}")
        for directory, _, filenames in root.walk():
            for filename in filenames:
                path = directory / filename
                _require_regular_file(path)
                check(path.read_bytes(), str(path.relative_to(ROOT)))


def _scan_candidate_wheel(candidate_wheel: Path | None, check: Callable[[bytes, str], None]) -> None:
    """Scan every candidate-wheel member without extraction."""
    if candidate_wheel is None:
        raise GateError("a candidate wheel is required for the artifact safety check")
    with zipfile.ZipFile(candidate_wheel) as archive:
        members = archive.infolist()
        if not members or len({member.filename for member in members}) != len(members):
            raise GateError("candidate wheel is empty or contains duplicate members")
        for member in members:
            if not member.is_dir():
                check(archive.read(member), f"candidate wheel member {member.filename}")


def require_artifact_safety(candidate_wheel: Path | None = None) -> None:
    """Scan every retained artifact and every candidate-wheel member without extraction."""
    try:
        check = _artifact_check(_review_gate())
        _scan_retained_artifacts(check)
        _scan_candidate_wheel(candidate_wheel, check)
    except (OSError, ValueError, SyntaxError, zipfile.BadZipFile, RuntimeError) as error:
        raise GateError(f"artifact safety check failed: {error}") from error


def require_prohibitions_resolved() -> None:
    code, output = _run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/quality/test_docs_guard.py",
            "tests/quality/test_docs_guard.py::test_docs_contain_no_removed_identifiers_or_portal_phrases",
            "-q",
            "--no-header",
            "-p",
            "no:cacheprovider",
        ]
    )
    if code != 0:
        print(output[-3000:])
        raise GateError("GATE FAIL: the retired-surface prohibition checks do not pass")


def require_concurrency_resolved() -> None:
    text = CONFORMANCE_PATH.read_text(encoding="utf-8")
    required = (
        "upload",
        "bulk",
        "job-wait",
        "parallel-read",
        "cancellation",
        "ownership",
    )
    missing = [cell for cell in required if cell not in text]
    if missing:
        raise GateError(f"GATE FAIL: the concurrency edge lacks final dispositions for: {missing}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail-closed uData phase gate.")
    parser.add_argument("--require-validation-authoritative", action="store_true")
    parser.add_argument("--require-review-current", action="store_true")
    parser.add_argument("--require-cleanup", action="store_true")
    parser.add_argument("--require-prohibitions-resolved", action="store_true")
    parser.add_argument("--require-concurrency-resolved", action="store_true")
    parser.add_argument("--all", action="store_true", help="Run every check.")
    parser.add_argument("--candidate-wheel", type=Path)
    args = parser.parse_args()

    failures: list[str] = []
    try:
        surface_covered, surface_total = surface_completeness()
        print(f"surface_completeness: {surface_covered}/{surface_total} = {surface_covered / surface_total:.0%}")
        if surface_covered != surface_total:
            failures.append("surface coverage is not exactly 100%")
    except (GateError, OSError, ValueError) as error:
        failures.append(f"surface-completeness: {error}")
    try:
        semantic_covered, semantic_total = semantic_conformance()
        print(f"semantic_conformance: {semantic_covered}/{semantic_total} = {semantic_covered / semantic_total:.0%}")
        if semantic_covered != semantic_total:
            failures.append("semantic coverage is not exactly 100%")
    except (GateError, OSError, ValueError) as error:
        failures.append(f"semantic-conformance: {error}")

    checks: list[tuple[str, Callable[[], None], bool]] = [
        ("validation-authoritative", require_validation_authoritative, args.require_validation_authoritative),
        ("review-current", require_review_current, True),
        ("artifact-safety", lambda: require_artifact_safety(args.candidate_wheel), True),
        ("cleanup", require_cleanup, True),
        ("prohibitions-resolved", require_prohibitions_resolved, args.require_prohibitions_resolved),
        ("concurrency-resolved", require_concurrency_resolved, args.require_concurrency_resolved),
    ]
    for name, check, enabled in checks:
        if not (enabled or args.all):
            continue
        try:
            check()
            print(f"{name}: OK")
        except GateError as error:
            failures.append(f"{name}: {error}")

    if failures:
        for failure in failures:
            print(f"GATE FAIL: {failure}")
        return 1
    print("PHASE GATE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
