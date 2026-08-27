"""Capture and verify immutable, SHA-bound uData review evidence."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SHA = re.compile(r"^[0-9a-f]{40}$")
SENSITIVE = re.compile(r"(?:authorization\s*:|bearer\s+|api[_-]?key\s*[:=]|https?://[^\s/]+:[^\s@]+@)", re.IGNORECASE)
REDACTION_VIOLATION = re.compile(
    r"\b(?:raw[_ -]?(?:request|response)|credential(?:s)?|token(?:s)?)\s*[:=]", re.IGNORECASE
)
URL = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
RESOLVED = frozenset({"fixed", "invalid", "not_actionable"})


class ReviewGateError(ValueError):
    """Raised when review provenance is incomplete, unsafe, or stale."""


def digest_file(path: Path) -> str:
    """Return a file's SHA-256 digest."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_sha(value: str, name: str) -> None:
    if not SHA.fullmatch(value):
        raise ReviewGateError(f"{name} must be a lowercase 40-character SHA")


def _safe_artifact(path: Path) -> None:
    if not path.is_file():
        raise ReviewGateError(f"review artifact is missing: {path.name}")
    content = path.read_text(encoding="utf-8")
    if SENSITIVE.search(content) or REDACTION_VIOLATION.search(content):
        raise ReviewGateError(f"review artifact contains sensitive content: {path.name}")
    for match in URL.finditer(content):
        parsed = match.group().rstrip(".,;:)]}")
        hostname = re.match(r"https?://([^/:?#]+)", parsed, re.IGNORECASE)
        if hostname is None:
            raise ReviewGateError(f"review artifact contains an invalid origin: {path.name}")
        try:
            address = ipaddress.ip_address(hostname.group(1).strip("[]"))
        except ValueError:
            continue
        if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved:
            raise ReviewGateError(f"review artifact contains a private origin: {path.name}")


def _git_head(repo_root: Path) -> str:
    """Read the current repository HEAD for reviewed-head equality."""
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise ReviewGateError("review verification requires a git repository with a current HEAD")
    head = completed.stdout.strip()
    _validate_sha(head, "current HEAD")
    return head


def _validate_threads(path: Path, reviewed_sha: str) -> None:
    _safe_artifact(path)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as error:
        raise ReviewGateError("current thread export is not valid JSON") from error
    if (
        not isinstance(document, dict)
        or document.get("schema_version") != 1
        or document.get("reviewed_sha") != reviewed_sha
    ):
        raise ReviewGateError("current thread export is not bound to the reviewed SHA")
    threads = document.get("threads")
    if not isinstance(threads, list) or not threads:
        raise ReviewGateError("current thread export must contain classified findings")
    for thread in threads:
        if not isinstance(thread, dict) or thread.get("classification") not in RESOLVED:
            raise ReviewGateError("current thread export has an unresolved or unclassified finding")


def _validate_timestamp(value: object) -> None:
    """Require a parseable UTC timestamp in the immutable receipt."""
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ReviewGateError("receipt timestamp must be a UTC ISO-8601 value")
    try:
        timestamp = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise ReviewGateError("receipt timestamp must be a UTC ISO-8601 value") from error
    if timestamp.tzinfo != UTC:
        raise ReviewGateError("receipt timestamp must be a UTC ISO-8601 value")


def capture_review(
    *,
    reviews_root: Path,
    family: str,
    reviewed_sha: str,
    base_sha: str,
    reviewer_id: str,
    author_ids: tuple[str, ...],
    source_review: Path,
    current_threads: Path,
    created_at: str | None = None,
    parent_receipt: str | None,
    post_fix: bool,
) -> Path:
    """Copy redacted reviewer artifacts into a write-once SHA directory."""
    _validate_sha(reviewed_sha, "reviewed_sha")
    _validate_sha(base_sha, "base_sha")
    if not family or "/" in family or not reviewer_id or not author_ids:
        raise ReviewGateError("family, reviewer, and implementation authors are required")
    if reviewer_id in author_ids:
        raise ReviewGateError("reviewer must differ from every implementation author")
    if any(not author_id for author_id in author_ids):
        raise ReviewGateError("implementation author IDs must be non-empty opaque values")
    if post_fix and parent_receipt is None:
        raise ReviewGateError("post-fix review requires a parent receipt")
    if parent_receipt is not None and not re.fullmatch(r"[0-9a-f]{64}", parent_receipt):
        raise ReviewGateError("parent receipt must be a SHA-256 digest")
    _safe_artifact(source_review)
    _validate_threads(current_threads, reviewed_sha)
    directory = reviews_root / family / reviewed_sha
    if directory.exists():
        raise ReviewGateError(f"review directory already exists for SHA {reviewed_sha}")
    directory.mkdir(parents=True)
    copied_source = directory / "source-review.md"
    copied_threads = directory / "current-thread.json"
    shutil.copyfile(source_review, copied_source)
    shutil.copyfile(current_threads, copied_threads)
    timestamp = created_at or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    _validate_timestamp(timestamp)
    receipt = {
        "schema_version": 1,
        "reviewed_sha": reviewed_sha,
        "base_sha": base_sha,
        "reviewer_id": reviewer_id,
        "author_ids": list(author_ids),
        "source_review_sha256": digest_file(copied_source),
        "current_thread_sha256": digest_file(copied_threads),
        "created_at": timestamp,
        "parent_receipt": parent_receipt,
        "post_fix": post_fix,
    }
    (directory / "review-receipt.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return directory


def verify_review(reviews_root: Path, family: str, reviewed_sha: str) -> dict[str, str]:
    """Verify a complete immutable review receipt for the current reviewed SHA."""
    _validate_sha(reviewed_sha, "reviewed_sha")
    directory = reviews_root / family / reviewed_sha
    for name in ("source-review.md", "current-thread.json", "review-receipt.json"):
        if not (directory / name).is_file():
            raise ReviewGateError(f"review artifact is missing: {name}")
    source = directory / "source-review.md"
    threads = directory / "current-thread.json"
    _safe_artifact(source)
    _validate_threads(threads, reviewed_sha)
    try:
        receipt: Any = json.loads((directory / "review-receipt.json").read_text(encoding="utf-8"))
    except ValueError as error:
        raise ReviewGateError("receipt is not valid JSON") from error
    if not isinstance(receipt, dict) or receipt.get("schema_version") != 1:
        raise ReviewGateError("receipt schema is invalid")
    if receipt.get("reviewed_sha") != reviewed_sha:
        raise ReviewGateError("receipt reviewed SHA does not match directory")
    _validate_sha(str(receipt.get("base_sha", "")), "base_sha")
    reviewer = receipt.get("reviewer_id")
    authors = receipt.get("author_ids")
    if (
        not isinstance(reviewer, str)
        or not reviewer
        or not isinstance(authors, list)
        or not authors
        or any(not isinstance(author, str) or not author for author in authors)
        or reviewer in authors
    ):
        raise ReviewGateError("reviewer must differ from every implementation author")
    if receipt.get("source_review_sha256") != digest_file(source) or receipt.get(
        "current_thread_sha256"
    ) != digest_file(threads):
        raise ReviewGateError("review artifact digest does not match receipt")
    _validate_timestamp(receipt.get("created_at"))
    post_fix = receipt.get("post_fix")
    parent = receipt.get("parent_receipt")
    if not isinstance(post_fix, bool):
        raise ReviewGateError("receipt post-fix status is missing")
    if post_fix and (not isinstance(parent, str) or not re.fullmatch(r"[0-9a-f]{64}", parent)):
        raise ReviewGateError("post-fix review requires a parent receipt")
    if not post_fix and parent is not None:
        raise ReviewGateError("initial review cannot claim post-fix parent receipt provenance")
    return {"status": "passed", "reviewed_sha": reviewed_sha}


def verify_current_review(reviews_root: Path, family: str, repo_root: Path) -> dict[str, str]:
    """Verify that the immutable review directory is bound to the current HEAD."""
    return verify_review(reviews_root, family, _git_head(repo_root))


def build_parser() -> argparse.ArgumentParser:
    """Build the write-once capture and current-HEAD verification commands."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("capture")
    capture.add_argument("--reviews-root", type=Path, required=True)
    capture.add_argument("--family", required=True)
    capture.add_argument("--reviewed-sha", required=True)
    capture.add_argument("--base-sha", required=True)
    capture.add_argument("--reviewer-id", required=True)
    capture.add_argument("--author-id", action="append", required=True)
    capture.add_argument("--source-review", type=Path, required=True)
    capture.add_argument("--current-thread", type=Path, required=True)
    capture.add_argument("--parent-receipt")
    capture.add_argument("--post-fix", action="store_true")
    verify = commands.add_parser("verify")
    verify.add_argument("--reviews-root", type=Path, required=True)
    verify.add_argument("--family", required=True)
    verify.add_argument("--repo-root", type=Path, default=Path.cwd())
    return parser


def main() -> int:
    """Capture or verify one bounded, immutable review receipt."""
    args = build_parser().parse_args()
    if args.command == "capture":
        directory = capture_review(
            reviews_root=args.reviews_root,
            family=args.family,
            reviewed_sha=args.reviewed_sha,
            base_sha=args.base_sha,
            reviewer_id=args.reviewer_id,
            author_ids=tuple(args.author_id),
            source_review=args.source_review,
            current_threads=args.current_thread,
            parent_receipt=args.parent_receipt,
            post_fix=args.post_fix,
        )
        print(directory)
        return 0
    print(json.dumps(verify_current_review(args.reviews_root, args.family, args.repo_root), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
