"""Capture and verify immutable, SHA-bound uData review evidence."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import re
import shutil
import socket
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

SHA = re.compile(r"^[0-9a-f]{40}$")
SENSITIVE = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"authorization\s*[\"']?\s*:\s*[\"']?\s*(?:bearer|token|basic)\s+[A-Za-z0-9._~+/=-]{8,}",
        r"bearer\s+[A-Za-z0-9._~+/=-]{16,}",
        r"(?:\"api[_-]?key\"|api[_-]?key)\s*[:=]\s*[\"']?\s*\S{8,}",
        r"https?://[^\s/:]+:[^\s/@]+@",
        r"[\"'](?:password|client[_-]?secret|access[_-]?token|refresh[_-]?token|secret)[\"']\s*:\s*[\"'][^\"'\s]+[\"']",
        r"[\"'](?:api[_-]?key|token|credentials?)[\"']\s*:\s*[\"'][^\"'\s]+[\"']",
    )
)
REDACTION_VIOLATION = re.compile(
    r"\b(?:raw[_ -]?(?:request|response)|credential(?:s)?|token(?:s)?)[\"']?\s*[:=]", re.IGNORECASE
)
URL = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
RESOLVED = frozenset({"fixed", "invalid", "not_actionable"})
PRIVATE_HOSTS = frozenset({"localhost"})
PRIVATE_HOST_SUFFIXES = (".localhost", ".local", ".internal", ".home.arpa")
_SOURCE_REVIEW = "source-review.md"
_CURRENT_THREAD = "current-thread.json"
_REVIEW_RECEIPT = "review-receipt.json"
_TIMESTAMP_ERROR = "receipt timestamp must be a UTC ISO-8601 value"
_REVIEWER_ERROR = "reviewer must differ from every implementation author"


class ReviewGateError(ValueError):
    """Raised when review provenance is incomplete, unsafe, or stale."""


def _contains_sensitive(content: str) -> bool:
    return any(pattern.search(content) for pattern in SENSITIVE)


def digest_file(path: Path) -> str:
    """Return a file's SHA-256 digest."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_sha(value: str, name: str) -> None:
    if not SHA.fullmatch(value):
        raise ReviewGateError(f"{name} must be a lowercase 40-character SHA")


def _private_origin(content: str) -> bool:
    """Report whether any URL in the content names a private or reserved origin."""
    for match in URL.finditer(content):
        parsed = match.group().rstrip(".,;:)]}\"'`")
        try:
            host = urlsplit(parsed).hostname
        except ValueError:
            return True
        if host is None:
            continue
        host = unquote(host).lower().rstrip(".")
        if host in PRIVATE_HOSTS or host.endswith(PRIVATE_HOST_SUFFIXES) or "." not in host and ":" not in host:
            return True
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            try:
                address = ipaddress.ip_address(socket.inet_aton(host))
            except OSError:
                continue
        if not address.is_global or address.is_multicast:
            return True
    return False


def _safe_artifact(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise ReviewGateError(f"review artifact is missing: {path.name}")
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ReviewGateError(f"review artifact cannot be read: {path.name}") from error
    validate_artifact_content(content, path.name)


def validate_artifact_content(content: str, name: str, *, reject_payload_keys: bool = True) -> None:
    """Reject secrets, raw payloads, and private origins in retained text."""
    values = [content]
    try:
        document = json.loads(content)
    except ValueError:
        pass
    else:
        values.append(json.dumps(document, ensure_ascii=False))
        pending = [document]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
            elif isinstance(value, str):
                values.append(value)
    for value in values:
        if _contains_sensitive(value) or reject_payload_keys and REDACTION_VIOLATION.search(value):
            raise ReviewGateError(f"review artifact contains sensitive content: {name}")
        if _private_origin(value):
            raise ReviewGateError(f"review artifact contains a private origin: {name}")


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
        raise ReviewGateError(_TIMESTAMP_ERROR)
    try:
        timestamp = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise ReviewGateError(_TIMESTAMP_ERROR) from error
    if timestamp.tzinfo != UTC:
        raise ReviewGateError(_TIMESTAMP_ERROR)


def _validate_capture_metadata(
    reviews_root: Path,
    family: str,
    reviewed_sha: str,
    base_sha: str,
    reviewer_id: str,
    author_ids: tuple[str, ...],
    parent_receipt: str | None,
    post_fix: bool,
) -> None:
    _validate_sha(reviewed_sha, "reviewed_sha")
    _validate_sha(base_sha, "base_sha")
    if not family or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", family):
        raise ReviewGateError("family, reviewer, and implementation authors are required")
    if not reviewer_id or not author_ids:
        raise ReviewGateError("family, reviewer, and implementation authors are required")
    if reviewer_id in author_ids:
        raise ReviewGateError(_REVIEWER_ERROR)
    if any(not author_id for author_id in author_ids):
        raise ReviewGateError("implementation author IDs must be non-empty opaque values")
    if post_fix and parent_receipt is None:
        raise ReviewGateError("post-fix review requires a parent receipt")
    if not post_fix and parent_receipt is not None:
        raise ReviewGateError("initial review cannot claim post-fix parent receipt provenance")
    if parent_receipt is not None and not re.fullmatch(r"[0-9a-f]{64}", parent_receipt):
        raise ReviewGateError("parent receipt must be a SHA-256 digest")
    _validate_parent(reviews_root, family, {"post_fix": post_fix, "parent_receipt": parent_receipt}, reviewed_sha)


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
    _validate_capture_metadata(
        reviews_root, family, reviewed_sha, base_sha, reviewer_id, author_ids, parent_receipt, post_fix
    )
    _safe_artifact(source_review)
    _validate_threads(current_threads, reviewed_sha)
    timestamp = created_at or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    _validate_timestamp(timestamp)
    receipt = {
        "schema_version": 1,
        "reviewed_sha": reviewed_sha,
        "base_sha": base_sha,
        "reviewer_id": reviewer_id,
        "author_ids": list(author_ids),
        "source_review_sha256": digest_file(source_review),
        "current_thread_sha256": digest_file(current_threads),
        "created_at": timestamp,
        "parent_receipt": parent_receipt,
        "post_fix": post_fix,
    }
    receipt_text = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    validate_artifact_content(receipt_text, _REVIEW_RECEIPT)
    directory = reviews_root / family / reviewed_sha
    if directory.parent.is_symlink():
        raise ReviewGateError("review family directory must not be a symbolic link")
    directory.parent.mkdir(parents=True, exist_ok=True)
    if directory.exists():
        raise ReviewGateError(f"review directory already exists for SHA {reviewed_sha}")
    staging = Path(tempfile.mkdtemp(prefix=".review-recovery-", dir=directory.parent))
    try:
        source = staging / _SOURCE_REVIEW
        threads = staging / _CURRENT_THREAD
        shutil.copyfile(source_review, source)
        shutil.copyfile(current_threads, threads)
        _safe_artifact(source)
        _validate_threads(threads, reviewed_sha)
        if (
            digest_file(source) != receipt["source_review_sha256"]
            or digest_file(threads) != receipt["current_thread_sha256"]
        ):
            raise ReviewGateError("review inputs changed during capture")
        (staging / _REVIEW_RECEIPT).write_text(receipt_text, encoding="utf-8")
        if directory.exists():
            raise ReviewGateError(f"review directory already exists for SHA {reviewed_sha}")
        staging.rename(directory)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return directory


def _validate_review_identity(receipt: dict[str, Any]) -> None:
    reviewer = receipt.get("reviewer_id")
    authors = receipt.get("author_ids")
    if not isinstance(reviewer, str) or not reviewer or not isinstance(authors, list) or not authors:
        raise ReviewGateError(_REVIEWER_ERROR)
    if any(not isinstance(author, str) or not author for author in authors) or reviewer in authors:
        raise ReviewGateError(_REVIEWER_ERROR)


def _validate_parent(reviews_root: Path, family: str, receipt: dict[str, Any], reviewed_sha: str) -> None:
    post_fix = receipt.get("post_fix")
    parent = receipt.get("parent_receipt")
    if not isinstance(post_fix, bool):
        raise ReviewGateError("receipt post-fix status is missing")
    if post_fix and (not isinstance(parent, str) or not re.fullmatch(r"[0-9a-f]{64}", parent)):
        raise ReviewGateError("post-fix review requires a parent receipt")
    if not post_fix and parent is not None:
        raise ReviewGateError("initial review cannot claim post-fix parent receipt provenance")
    if post_fix and not any(
        path.parent.name != reviewed_sha
        and not path.is_symlink()
        and not path.parent.is_symlink()
        and parent == digest_file(path)
        for path in (reviews_root / family).glob(f"*/{_REVIEW_RECEIPT}")
    ):
        raise ReviewGateError("post-fix parent receipt does not match any recorded receipt in the same family")


def verify_review(reviews_root: Path, family: str, reviewed_sha: str) -> dict[str, str]:
    """Verify a complete immutable review receipt for the current reviewed SHA."""
    _validate_sha(reviewed_sha, "reviewed_sha")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", family):
        raise ReviewGateError("review family must be a single directory name")
    directory = reviews_root / family / reviewed_sha
    if directory.parent.is_symlink() or directory.is_symlink():
        raise ReviewGateError("review directory must not be a symbolic link")
    for name in (_SOURCE_REVIEW, _CURRENT_THREAD, _REVIEW_RECEIPT):
        if not (directory / name).is_file():
            raise ReviewGateError(f"review artifact is missing: {name}")
    source = directory / _SOURCE_REVIEW
    threads = directory / _CURRENT_THREAD
    _safe_artifact(source)
    _validate_threads(threads, reviewed_sha)
    try:
        receipt: Any = json.loads((directory / _REVIEW_RECEIPT).read_text(encoding="utf-8"))
    except ValueError as error:
        raise ReviewGateError("receipt is not valid JSON") from error
    if not isinstance(receipt, dict) or receipt.get("schema_version") != 1:
        raise ReviewGateError("receipt schema is invalid")
    if receipt.get("reviewed_sha") != reviewed_sha:
        raise ReviewGateError("receipt reviewed SHA does not match directory")
    _validate_sha(str(receipt.get("base_sha", "")), "base_sha")
    _validate_review_identity(receipt)
    if receipt.get("source_review_sha256") != digest_file(source) or receipt.get(
        "current_thread_sha256"
    ) != digest_file(threads):
        raise ReviewGateError("review artifact digest does not match receipt")
    _validate_timestamp(receipt.get("created_at"))
    _validate_parent(reviews_root, family, receipt, reviewed_sha)
    receipt_text = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    validate_artifact_content(receipt_text, _REVIEW_RECEIPT)
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
    else:
        print(json.dumps(verify_current_review(args.reviews_root, args.family, args.repo_root), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
