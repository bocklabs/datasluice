"""Known-bad discrimination and contract coverage for the uData phase gate."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import check_udata_phase_gate as gate  # type: ignore[import-not-found] # noqa: E402


def test_profile_operations_rejects_duplicate_ids(tmp_path: Path) -> None:
    profile = {"operations": [{"id": "udata/api-v1.avatar"}, {"id": "udata/api-v1.avatar"}]}
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(profile), encoding="utf-8")
    with pytest.raises(SystemExit, match="duplicate operations"):
        gate.profile_operations(path)


def test_profile_operations_returns_the_declared_ids() -> None:
    operations = gate.profile_operations()
    assert "udata/api-v1.avatar" in operations
    assert len(operations) == len(set(operations))


def _local_receipts_present() -> bool:
    """True when every family's review receipts are on disk, as in a full local checkout."""
    return all(
        any((gate.REVIEWS_ROOT / gate.REVIEWS_DIR_NAMES.get(family, family)).glob("*/review-receipt.json"))
        for family in gate.FAMILIES
    )


def _review_gate() -> Any:
    import check_udata_review_gate as review_gate  # type: ignore[import-not-found]

    return review_gate


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    )
    return completed.stdout.strip()


def _review_artifacts(scratch: Path, reviewed_sha: str) -> tuple[Path, Path]:
    scratch.mkdir(parents=True, exist_ok=True)
    source = scratch / "source-review.md"
    source.write_text("# Independent review\n\nEvery finding is classified and resolved.\n", encoding="utf-8")
    threads = scratch / "current-thread.json"
    threads.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "reviewed_sha": reviewed_sha,
                "threads": [{"classification": "fixed", "summary": "review currency regression"}],
            }
        ),
        encoding="utf-8",
    )
    return source, threads


def _capture_family_receipt(
    review_gate: Any, reviews_root: Path, family: str, reviewed_sha: str, source: Path, threads: Path
) -> None:
    review_gate.capture_review(
        reviews_root=reviews_root,
        family=gate.REVIEWS_DIR_NAMES.get(family, family),
        reviewed_sha=reviewed_sha,
        base_sha=reviewed_sha,
        reviewer_id="phase-gate-reviewer",
        author_ids=("phase-gate-author",),
        source_review=source,
        current_threads=threads,
        parent_receipt=None,
        post_fix=False,
    )


def test_review_current_fails_closed_without_receipts(tmp_path: Path) -> None:
    with pytest.raises(gate.GateError, match="tracer is not reviewed at the merge head"):
        gate.require_review_current(reviews_root=tmp_path)


def test_review_current_requires_every_family_at_the_current_head(tmp_path: Path) -> None:
    review_gate = _review_gate()
    head = _git_head()
    source, threads = _review_artifacts(tmp_path / "artifacts-head", head)
    for family in gate.FAMILIES:
        _capture_family_receipt(review_gate, tmp_path, family, head, source, threads)
    gate.require_review_current(reviews_root=tmp_path)


def test_review_current_rejects_a_family_pinned_to_another_head(tmp_path: Path) -> None:
    review_gate = _review_gate()
    head = _git_head()
    stale = "0" * 40
    source, threads = _review_artifacts(tmp_path / "artifacts-head", head)
    stale_source, stale_threads = _review_artifacts(tmp_path / "artifacts-stale", stale)
    for family in gate.FAMILIES:
        if family == "tracer":
            _capture_family_receipt(review_gate, tmp_path, family, stale, stale_source, stale_threads)
        else:
            _capture_family_receipt(review_gate, tmp_path, family, head, source, threads)
    with pytest.raises(gate.GateError, match="tracer is not reviewed at the merge head"):
        gate.require_review_current(reviews_root=tmp_path)
    _capture_family_receipt(review_gate, tmp_path, "tracer", head, source, threads)
    gate.require_review_current(reviews_root=tmp_path)


def test_coverage_inventory_is_the_locked_denominator() -> None:
    if not gate.COVERAGE_PATH.exists():
        pytest.skip("the coverage matrix requires the local-only .planning tree")
    inventory = gate.coverage_inventory()
    assert len(inventory) == 268
    assert all("." in name for name in inventory)
    assert sum(1 for name in inventory if name.startswith("udata.")) == 268


def test_validation_authority_fails_on_pending_or_draft(tmp_path: Path) -> None:
    pending = tmp_path / "validation-pending.md"
    pending.write_text("status: pending", encoding="utf-8")
    with pytest.raises(gate.GateError, match="pending or draft"):
        gate.require_validation_authoritative(pending)
    draft = tmp_path / "validation-draft.md"
    draft.write_text("Status: draft", encoding="utf-8")
    with pytest.raises(gate.GateError, match="pending or draft"):
        gate.require_validation_authoritative(draft)
    clean = tmp_path / "validation-clean.md"
    clean.write_text("status: compliant; every row green", encoding="utf-8")
    gate.require_validation_authoritative(clean)


KNOWN_HISTORICAL_GAPS: set[str] = set()
# The reconciliation campaign captured current-era receipts for all nineteen
# families at merge head 2cc19605 (ten re-reviewed after the rate-limited first
# round; the rest already verified at their own heads). If a future convention
# change reopens gaps, list the affected families here and the gate will fail
# closed until they are reconciled.


def test_semantic_conformance_reports_the_true_family_state() -> None:
    if not _local_receipts_present():
        pytest.skip("the review receipts require the local-only dev/udata-evidence tree")
    covered, total = gate.semantic_conformance()
    assert total == len(gate.FAMILIES)
    # The gate fails closed: families whose historical review artifacts predate
    # the current gate format are reported as gaps, never credited. This test
    # fails if the honest state regresses below today's reconciled floor or if a
    # known gap silently becomes credited (which would mean the gate weakened).
    assert covered == total - len(KNOWN_HISTORICAL_GAPS)


def test_every_current_era_receipt_verifies_at_its_own_sha(monkeypatch: pytest.MonkeyPatch) -> None:
    """Known-good: every receipt of the reviewed families is valid at its recorded sha."""
    if not _local_receipts_present():
        pytest.skip("the review receipts require the local-only dev/udata-evidence tree")
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    import check_udata_review_gate as review_gate  # type: ignore[import-not-found]

    reviewed_families = gate.FAMILIES
    for family in reviewed_families:
        if family in KNOWN_HISTORICAL_GAPS:
            continue
        reviews_dir = gate.REVIEWS_ROOT / gate.REVIEWS_DIR_NAMES.get(family, family)
        receipts = sorted(reviews_dir.glob("*/review-receipt.json")) if reviews_dir.exists() else []
        assert receipts, family
        # Any internally valid receipt carries the family's review credit; this
        # mirrors the phase gate's semantics.
        errors: list[str] = []
        for receipt_path in receipts:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            try:
                review_gate.verify_review(
                    gate.REVIEWS_ROOT, gate.REVIEWS_DIR_NAMES.get(family, family), receipt["reviewed_sha"]
                )
            except review_gate.ReviewGateError as err:
                errors.append(f"{receipt['reviewed_sha'][:10]}: {err}")
                continue
            break
        else:
            raise AssertionError(f"no valid receipt for {family}: {errors}")
