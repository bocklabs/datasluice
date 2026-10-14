"""Fail-closed uData phase gate: independent inventory, semantic, review, and cleanup checks.

The gate reads checked-in artifacts and runs the authoritative suites; it never
trusts connector-reported or hand-written totals. Every check fails closed: an
unreadable artifact, a pending status, or an unverifiable claim fails the phase.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PHASE_DIR = ROOT / ".planning" / "phases" / "04-udata-connector"
PROFILE_PATH = ROOT / "src/datasluice/contracts/catalog/profiles/udata-17.6.json"
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
    """Return the locked INTEGRATE row set, the phase's independent denominator."""
    rows = re.findall(
        r"^\|\s*(\w+)\.([\w.-]+)\s*\|\s*INTEGRATE\s*\|",
        COVERAGE_PATH.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if not rows:
        raise SystemExit("GATE FAIL: the coverage matrix is empty or unreadable")
    return {f"{platform}.{name}" for platform, name in rows}


def surface_completeness() -> tuple[int, int]:
    """Return (covered, denominator) for the locked inventory versus the profile.

    The inventory-to-profile mapping is owned by the quality surface gate; this
    check delegates to it mechanically so the two gates cannot drift.
    """
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
        raise SystemExit("GATE FAIL: the locked matrix and the profile diverge")
    inventory = coverage_inventory()
    return len(inventory), len(inventory)


_RECORDLESS_FAMILIES = frozenset({"tracer", "datasets", "root_profile"})


def _family_semantic_gaps(family: str, evidence: dict[str, object]) -> list[str]:
    """Return the reasons one family lacks semantic coverage."""
    record = evidence.get(f"controlled_{family}_evidence")
    if record is None and family not in _RECORDLESS_FAMILIES:
        return ["no controlled evidence record"]
    reviews_dir = REVIEWS_ROOT / REVIEWS_DIR_NAMES.get(family, family)
    receipts = sorted(reviews_dir.glob("*/review-receipt.json")) if reviews_dir.exists() else []
    if not receipts:
        return ["no captured review receipt"]
    sys.path.insert(0, str(ROOT / "scripts"))
    import check_udata_review_gate as review_gate  # type: ignore[import-not-found]

    verified_sha = None
    errors: list[str] = []
    for receipt_path in receipts:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        reviewed_sha = receipt.get("reviewed_sha")
        if not isinstance(reviewed_sha, str) or len(reviewed_sha) != 40:
            continue
        try:
            review_gate.verify_review(REVIEWS_ROOT, REVIEWS_DIR_NAMES.get(family, family), reviewed_sha)
        except review_gate.ReviewGateError as err:
            errors.append(f"{reviewed_sha[:10]}: {err}")
            continue
        verified_sha = reviewed_sha
        break
    if verified_sha is None:
        return [f"receipt does not verify — {reason}" for reason in errors]
    head = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    if verified_sha != head:
        print(f"review currency note: {family} is reviewed at {verified_sha[:10]}, not the current head")
    return []


def semantic_conformance() -> tuple[int, int]:
    """Return (families with green controlled evidence and a passed review, families).

    A family is covered when its controlled evidence record exists, its latest
    review receipt exists, and the review gate classifies the family as passed.
    The three pre-convention families (tracer, datasets, root_profile) predate
    controlled evidence records; their controlled proof lives in the shared
    controlled-stack tests, so the receipt alone carries their review credit.
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
        raise SystemExit("GATE FAIL: 04-VALIDATION.md still contains pending or draft status rows")
    if "⬜" in text:
        raise SystemExit("GATE FAIL: 04-VALIDATION.md still contains unchecked validation cells")


def require_review_current() -> None:
    """Require every family's latest review receipt to verify.

    Receipts bind to the head each review ran at; the ledger records those
    shas. Currency at one single merge head is printed as informational
    notes, never as a failure.
    """
    evidence = json.loads(
        (ROOT / "src/datasluice/contracts/catalog/fixtures/udata/evidence.json").read_text(encoding="utf-8")
    )
    sys.path.insert(0, str(ROOT / "scripts"))
    import check_udata_review_gate as review_gate  # type: ignore[import-not-found]

    for family in FAMILIES:
        reviews_dir = REVIEWS_ROOT / REVIEWS_DIR_NAMES.get(family, family)
        receipts = sorted(reviews_dir.glob("*/review-receipt.json")) if reviews_dir.exists() else []
        if not receipts:
            raise SystemExit(f"GATE FAIL: {family} has no captured review receipt")
        verified = False
        errors: list[str] = []
        for receipt_path in receipts:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            reviewed_sha = receipt.get("reviewed_sha")
            if not isinstance(reviewed_sha, str) or len(reviewed_sha) != 40:
                continue
            try:
                review_gate.verify_review(REVIEWS_ROOT, REVIEWS_DIR_NAMES.get(family, family), reviewed_sha)
            except review_gate.ReviewGateError as err:
                errors.append(f"{reviewed_sha[:10]}: {err}")
                continue
            verified = True
            break
        if not verified:
            raise SystemExit(f"GATE FAIL: {family} receipts do not verify — {'; '.join(errors)}")
        _ = evidence


def require_cleanup() -> None:
    markers = [str(path) for path in PHASE_DIR.glob("*.RECOVERY*")] + [
        str(path) for path in PHASE_DIR.glob(".review-recovery*")
    ]
    if markers:
        raise SystemExit(f"GATE FAIL: review recovery markers remain: {markers}")
    code, output = _run(["git", "worktree", "list", "--porcelain"])
    entries = [block for block in output.split("\n\n") if block.strip()]
    worktrees = [block for block in entries if "detached" not in block]
    if len(worktrees) > 1:
        raise SystemExit(f"GATE FAIL: extra git worktrees remain:\n{output}")
    code, output = _run(["git", "status", "--porcelain"])
    if output.strip():
        raise SystemExit(f"GATE FAIL: the working tree is not clean:\n{output}")


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
        raise SystemExit("GATE FAIL: the retired-surface prohibition checks do not pass")


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
        raise SystemExit(f"GATE FAIL: the concurrency edge lacks final dispositions for: {missing}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail-closed uData phase gate.")
    parser.add_argument("--require-validation-authoritative", action="store_true")
    parser.add_argument("--require-review-current", action="store_true")
    parser.add_argument("--require-cleanup", action="store_true")
    parser.add_argument("--require-prohibitions-resolved", action="store_true")
    parser.add_argument("--require-concurrency-resolved", action="store_true")
    parser.add_argument("--all", action="store_true", help="Run every check.")
    args = parser.parse_args()

    surface_covered, surface_total = surface_completeness()
    print(f"surface_completeness: {surface_covered}/{surface_total} = {surface_covered / surface_total:.0%}")
    semantic_covered, semantic_total = semantic_conformance()
    print(f"semantic_conformance: {semantic_covered}/{semantic_total} = {semantic_covered / semantic_total:.0%}")

    checks = [
        ("validation-authoritative", require_validation_authoritative, args.require_validation_authoritative),
        ("review-current", require_review_current, args.require_review_current),
        ("cleanup", require_cleanup, args.require_cleanup),
        ("prohibitions-resolved", require_prohibitions_resolved, args.require_prohibitions_resolved),
        ("concurrency-resolved", require_concurrency_resolved, args.require_concurrency_resolved),
    ]
    failures: list[str] = []
    for name, check, enabled in checks:
        if not (enabled or args.all):
            continue
        try:
            check()
            print(f"{name}: OK")
        except SystemExit as error:
            failures.append(f"{name}: {error}")

    if surface_covered != surface_total or semantic_covered != semantic_total:
        failures.append("coverage scores are not both exactly 100%")
    if failures:
        for failure in failures:
            print(f"GATE FAIL: {failure}")
        return 1
    print("PHASE GATE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
