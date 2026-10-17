from __future__ import annotations

import importlib.util
import json
import shutil
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[4] / "scripts" / "check_udata_review_gate.py"
if not _MODULE_PATH.is_file():
    pytest.skip("scripts/check_udata_review_gate.py requires a full repository checkout", allow_module_level=True)
_spec = importlib.util.spec_from_file_location("check_udata_review_gate", _MODULE_PATH)
assert _spec is not None
assert _spec.loader is not None
review_gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(review_gate)

HEAD = "a" * 40
BASE = "b" * 40


def _threads(*, reviewed_sha: str = HEAD, classification: str = "fixed") -> dict[str, object]:
    return {
        "schema_version": 1,
        "reviewed_sha": reviewed_sha,
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
    reviewed_sha: str = HEAD,
    created_at: str = "2026-08-26T00:00:00Z",
    post_fix: bool = False,
    parent_receipt: str | None = None,
) -> Path:
    source = tmp_path / "source-review-input.md"
    threads = tmp_path / "thread-input.json"
    source.write_text("# Independent review\n\nNo unresolved findings.\n", encoding="utf-8")
    threads.write_text(json.dumps(_threads(reviewed_sha=reviewed_sha)), encoding="utf-8")
    return review_gate.capture_review(
        reviews_root=tmp_path / "reviews",
        family="tracer",
        reviewed_sha=reviewed_sha,
        base_sha=BASE,
        reviewer_id=reviewer,
        author_ids=authors,
        source_review=source,
        current_threads=threads,
        created_at=created_at,
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


def test_capture_rejects_json_exported_credentials(tmp_path: Path) -> None:
    source = tmp_path / "source.md"
    threads = tmp_path / "threads.json"
    sample = "abcdefgh" + "12345678"
    key_field = "api_" + "key"
    source.write_text(
        f'"{key_field}": "{sample}",\n"authorization": "Bearer {sample}"\n',
        encoding="utf-8",
    )
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


def test_capture_accepts_security_prose_without_credentials(tmp_path: Path) -> None:
    source = tmp_path / "source.md"
    source.write_text(
        "Bearer tokens must not be leaked on redirect, so strip the Authorization: header.\n"
        "The authorization check must run before I/O, and `assert 'X-API-KEY' not in headers` pins it.\n",
        encoding="utf-8",
    )
    threads = tmp_path / "threads.json"
    threads.write_text(json.dumps(_threads()), encoding="utf-8")

    directory = review_gate.capture_review(
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

    assert review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)["status"] == "passed"
    assert directory.is_dir()


@pytest.mark.parametrize(
    "private_url",
    [
        "http://localhost/api/1/site/",
        "http://localhost./api/1/site/",
        "http://stack.internal/api",
        "http://127.0.0.1:5640/api",
        "http://127.0.0.1./api",
        "http://10.0.0.5/api",
        "http://[::1]/api",
        "http://127.1/api",
        "http://0177.0.0.1/api",
        "http://0x7f.0.0.1/api",
        "http://%31%32%37.0.0.1/api",
        "http://metadata/api",
    ],
)
def test_capture_rejects_private_origin_urls(tmp_path: Path, private_url: str) -> None:
    source = tmp_path / "source.md"
    threads = tmp_path / "threads.json"
    source.write_text(f"evidence endpoint: {private_url}\n", encoding="utf-8")
    threads.write_text(json.dumps(_threads()), encoding="utf-8")

    with pytest.raises(review_gate.ReviewGateError, match="private origin"):
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


def test_capture_accepts_public_https_origins(tmp_path: Path) -> None:
    source = tmp_path / "source.md"
    threads = tmp_path / "threads.json"
    source.write_text("evidence endpoint: https://www.data.gouv.fr/api/1/site/\n", encoding="utf-8")
    threads.write_text(json.dumps(_threads()), encoding="utf-8")

    directory = review_gate.capture_review(
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

    assert directory.is_dir()


def test_capture_failure_leaves_no_review_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(review_gate.ReviewGateError, match="timestamp"):
        _capture(tmp_path, created_at="not-a-timestamp")
    assert not (tmp_path / "reviews" / "tracer" / HEAD).exists()

    def _fail_copyfile(source: Path, target: Path) -> None:
        raise OSError("injected copy failure")

    monkeypatch.setattr(review_gate.shutil, "copyfile", _fail_copyfile)

    with pytest.raises(OSError, match="injected copy failure"):
        _capture(tmp_path)
    assert not (tmp_path / "reviews" / "tracer" / HEAD).exists()


@pytest.mark.parametrize(
    ("post_fix", "parent_receipt", "message"),
    [
        (True, None, "requires a parent receipt"),
        (False, "0" * 64, "cannot claim post-fix parent receipt"),
    ],
)
def test_capture_rejects_contradictory_parent_receipt_provenance(
    tmp_path: Path, post_fix: bool, parent_receipt: str | None, message: str
) -> None:
    with pytest.raises(review_gate.ReviewGateError, match=message):
        _capture(tmp_path, post_fix=post_fix, parent_receipt=parent_receipt)


def test_capture_rejects_post_fix_parent_receipt_matching_no_recorded_receipt(tmp_path: Path) -> None:
    with pytest.raises(review_gate.ReviewGateError, match="does not match any recorded receipt"):
        _capture(tmp_path, reviewed_sha="c" * 40, post_fix=True, parent_receipt="0" * 64)


def test_capture_and_verify_accepts_post_fix_review_with_recorded_parent(tmp_path: Path) -> None:
    parent = _capture(tmp_path)
    child_sha = "c" * 40
    _capture(
        tmp_path,
        reviewed_sha=child_sha,
        post_fix=True,
        parent_receipt=review_gate.digest_file(parent / "review-receipt.json"),
    )

    assert review_gate.verify_review(tmp_path / "reviews", "tracer", child_sha)["status"] == "passed"
    assert review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)["status"] == "passed"


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


def test_capture_rejects_parent_from_another_family(tmp_path: Path) -> None:
    parent = _capture(tmp_path)
    digest = review_gate.digest_file(parent / "review-receipt.json")
    (tmp_path / "reviews" / "tracer").rename(tmp_path / "reviews" / "other-family")
    with pytest.raises(review_gate.ReviewGateError, match="same family"):
        _capture(tmp_path, reviewed_sha="c" * 40, post_fix=True, parent_receipt=digest)
    assert not (tmp_path / "reviews" / "tracer" / ("c" * 40)).exists()


def test_verify_rejects_parent_moved_to_another_family(tmp_path: Path) -> None:
    parent = _capture(tmp_path)
    child_sha = "c" * 40
    _capture(
        tmp_path,
        reviewed_sha=child_sha,
        post_fix=True,
        parent_receipt=review_gate.digest_file(parent / "review-receipt.json"),
    )
    other = tmp_path / "reviews" / "other-family"
    other.mkdir()
    parent.rename(other / HEAD)
    with pytest.raises(review_gate.ReviewGateError, match="same family"):
        review_gate.verify_review(tmp_path / "reviews", "tracer", child_sha)


def test_missing_parent_is_not_rewritten_or_reconstructed(tmp_path: Path) -> None:
    directory = _capture(tmp_path)
    receipt_path = directory / "review-receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt.update(post_fix=True, parent_receipt="0" * 64)
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    original = {path.name: path.read_bytes() for path in directory.iterdir()}
    with pytest.raises(review_gate.ReviewGateError, match="does not match any recorded receipt"):
        review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == original


def test_capture_publishes_only_after_both_copies_validate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_copy = shutil.copyfile

    def copy(source: Path, target: Path) -> None:
        assert not (tmp_path / "reviews" / "tracer" / HEAD).exists()
        original_copy(source, target)

    monkeypatch.setattr(review_gate.shutil, "copyfile", copy)
    directory = _capture(tmp_path)
    assert {path.name for path in directory.iterdir()} == {
        "source-review.md",
        "current-thread.json",
        "review-receipt.json",
    }
    assert not list(directory.parent.glob(".review-recovery-*"))


def test_changed_capture_input_is_rejected_and_retryable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_copy = shutil.copyfile

    def copy(source: Path, target: Path) -> None:
        original_copy(source, target)
        if target.name == "source-review.md":
            target.write_text("Changed during capture", encoding="utf-8")

    with monkeypatch.context() as scoped:
        scoped.setattr(review_gate.shutil, "copyfile", copy)
        with pytest.raises(review_gate.ReviewGateError, match="changed during capture"):
            _capture(tmp_path)
    assert not (tmp_path / "reviews" / "tracer" / HEAD).exists()
    assert not list((tmp_path / "reviews" / "tracer").glob(".review-recovery-*"))
    assert _capture(tmp_path).is_dir()


def test_verify_rejects_path_traversal_family(tmp_path: Path) -> None:
    _capture(tmp_path)
    with pytest.raises(review_gate.ReviewGateError, match="single directory"):
        review_gate.verify_review(tmp_path / "reviews", "../reviews/tracer", HEAD)


def test_verify_rejects_symbolic_link_to_another_family(tmp_path: Path) -> None:
    _capture(tmp_path)
    (tmp_path / "reviews" / "alias").symlink_to(tmp_path / "reviews" / "tracer", target_is_directory=True)
    with pytest.raises(review_gate.ReviewGateError, match="symbolic link"):
        review_gate.verify_review(tmp_path / "reviews", "alias", HEAD)


@pytest.mark.parametrize("metadata", ["reviewer_id", "author_ids"])
def test_verify_rejects_private_origins_in_receipt_metadata(tmp_path: Path, metadata: str) -> None:
    directory = _capture(tmp_path)
    path = directory / "review-receipt.json"
    receipt = json.loads(path.read_text(encoding="utf-8"))
    receipt[metadata] = ["http://127.0.0.1"] if metadata == "author_ids" else "http://127.0.0.1"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(review_gate.ReviewGateError, match="private origin"):
        review_gate.verify_review(tmp_path / "reviews", "tracer", HEAD)
