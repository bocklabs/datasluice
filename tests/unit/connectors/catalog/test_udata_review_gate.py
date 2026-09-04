"""Fail-closed tests for SHA-bound independent uData review evidence."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[4] / "scripts" / "check_udata_review_gate.py"
if not _MODULE_PATH.is_file():
    pytest.skip("scripts/check_udata_review_gate.py requires a full repository checkout", allow_module_level=True)
_spec = importlib.util.spec_from_file_location("check_udata_review_gate", _MODULE_PATH)
assert _spec is not None and _spec.loader is not None
review_gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(review_gate)

HEAD = "a" * 40
BASE = "b" * 40


def _threads(*, classification: str = "fixed") -> dict[str, object]:
    return {
        "schema_version": 1,
        "reviewed_sha": HEAD,
        "threads": [
            {
                "id": "F-1",
                "classification": classification,
                "rationale": "checked against pinned source",
                "regression_test": "tests/unit/test_example.py::test_regression",
            }
        ],
    }


def _capture(
    tmp_path: Path,
    *,
    reviewer: str = "reviewer-2",
    authors: tuple[str, ...] = ("author-1",),
    post_fix: bool = False,
    parent_receipt: str | None = None,
) -> Path:
    source = tmp_path / "source-review-input.md"
    threads = tmp_path / "thread-input.json"
    source.write_text("# Independent review\n\nNo unresolved findings.\n", encoding="utf-8")
    threads.write_text(json.dumps(_threads()), encoding="utf-8")
    return review_gate.capture_review(
        reviews_root=tmp_path / "reviews",
        family="tracer",
        reviewed_sha=HEAD,
        base_sha=BASE,
        reviewer_id=reviewer,
        author_ids=authors,
        source_review=source,
        current_threads=threads,
        created_at="2026-08-26T00:00:00Z",
        parent_receipt=parent_receipt,
        post_fix=post_fix,
    )


def test_capture_and_verify_accepts_complete_separate_reviewer_evidence(tmp_path: Path) -> None:
    directory = _capture(tmp_path)

    result = review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)

    assert result["status"] == "passed"
    assert result["reviewed_sha"] == HEAD
    assert directory == tmp_path / "reviews" / "tracer" / HEAD


def test_capture_refuses_existing_sha_directory(tmp_path: Path) -> None:
    _capture(tmp_path)

    with pytest.raises(review_gate.ReviewGateError, match="already exists"):
        _capture(tmp_path)


def test_capture_rejects_self_review(tmp_path: Path) -> None:
    with pytest.raises(review_gate.ReviewGateError, match="reviewer"):
        _capture(tmp_path, reviewer="author-1")


@pytest.mark.parametrize("missing", ["source-review.md", "current-thread.json", "review-receipt.json"])
def test_verify_rejects_missing_artifact(tmp_path: Path, missing: str) -> None:
    directory = _capture(tmp_path)
    (directory / missing).unlink()

    with pytest.raises(review_gate.ReviewGateError, match="missing"):
        review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)


def test_verify_rejects_wrong_sha_and_checksum_mismatch(tmp_path: Path) -> None:
    directory = _capture(tmp_path)
    with pytest.raises(review_gate.ReviewGateError, match="missing"):
        review_gate.verify_review(tmp_path / "reviews", "tracer", "c" * 40)

    (directory / "source-review.md").write_text("tampered", encoding="utf-8")
    with pytest.raises(review_gate.ReviewGateError, match="digest"):
        review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)


@pytest.mark.parametrize("classification", ["valid", "partially_valid", "unclassified"])
def test_verify_rejects_current_unfixed_or_unclassified_finding(tmp_path: Path, classification: str) -> None:
    directory = _capture(tmp_path)
    threads = directory / "current-thread.json"
    threads.write_text(json.dumps(_threads(classification=classification)), encoding="utf-8")
    receipt = json.loads((directory / "review-receipt.json").read_text(encoding="utf-8"))
    receipt["current_thread_sha256"] = review_gate.digest_file(threads)
    (directory / "review-receipt.json").write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(review_gate.ReviewGateError, match="finding"):
        review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)


def test_capture_rejects_secret_shaped_review_content(tmp_path: Path) -> None:
    source = tmp_path / "source.md"
    threads = tmp_path / "threads.json"
    source.write_text("Authorization: Bearer definitely-secret", encoding="utf-8")
    threads.write_text(json.dumps(_threads()), encoding="utf-8")

    with pytest.raises(review_gate.ReviewGateError, match="sensitive"):
        review_gate.capture_review(
            reviews_root=tmp_path / "reviews",
            family="tracer",
            reviewed_sha=HEAD,
            base_sha=BASE,
            reviewer_id="reviewer-2",
            author_ids=("author-1",),
            source_review=source,
            current_threads=threads,
            created_at="2026-08-26T00:00:00Z",
            parent_receipt=None,
            post_fix=False,
        )


def test_capture_rejects_unredacted_raw_response_content(tmp_path: Path) -> None:
    source = tmp_path / "source.md"
    threads = tmp_path / "threads.json"
    source.write_text("raw_response: retained", encoding="utf-8")
    threads.write_text(json.dumps(_threads()), encoding="utf-8")

    with pytest.raises(review_gate.ReviewGateError, match="sensitive"):
        review_gate.capture_review(
            reviews_root=tmp_path / "reviews",
            family="tracer",
            reviewed_sha=HEAD,
            base_sha=BASE,
            reviewer_id="reviewer-2",
            author_ids=("author-1",),
            source_review=source,
            current_threads=threads,
            created_at="2026-08-26T00:00:00Z",
            parent_receipt=None,
            post_fix=False,
        )


def test_verify_rejects_post_fix_review_without_parent_receipt(tmp_path: Path) -> None:
    directory = _capture(tmp_path)
    receipt_path = directory / "review-receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["post_fix"] = True
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(review_gate.ReviewGateError, match="parent receipt"):
        review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)


def test_verify_current_review_uses_the_repository_head(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _capture(tmp_path)
    monkeypatch.setattr(review_gate, "_git_head", lambda path: HEAD)

    result = review_gate.verify_current_review(tmp_path / "reviews", "tracer", tmp_path)

    assert result["reviewed_sha"] == HEAD
