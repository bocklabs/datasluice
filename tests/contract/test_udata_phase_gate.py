"""Known-bad discrimination and contract coverage for the uData phase gate."""

from __future__ import annotations

import json
import sys
from pathlib import Path

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


def test_coverage_inventory_is_the_locked_denominator() -> None:
    inventory = gate.coverage_inventory()
    assert len(inventory) == 268
    assert all("." in name for name in inventory)
    assert sum(1 for name in inventory if name.startswith("udata.")) == 268


def test_validation_authority_fails_on_pending_or_draft(tmp_path: Path) -> None:
    pending = tmp_path / "validation-pending.md"
    pending.write_text("status: pending", encoding="utf-8")
    with pytest.raises(SystemExit, match="pending or draft"):
        gate.require_validation_authoritative(pending)
    draft = tmp_path / "validation-draft.md"
    draft.write_text("Status: draft", encoding="utf-8")
    with pytest.raises(SystemExit, match="pending or draft"):
        gate.require_validation_authoritative(draft)
    clean = tmp_path / "validation-clean.md"
    clean.write_text("status: compliant; every row green", encoding="utf-8")
    gate.require_validation_authoritative(clean)


KNOWN_HISTORICAL_GAPS = {
    # Families whose latest review follows a pre-convention format the current
    # gate refuses: inline-review-era receipts without source-review.md
    # (organization, user, oauth, taxonomy, activity_discussion, reuse,
    # posts_report, topic), a pre-classification thread export (dataservices),
    # a post-fix parent chain the validator cannot match (datasets), and a
    # family with no receipt directory at all (resource). Reconciling these is
    # the documented 04-14 debt; the gate fails closed until it is paid.
    "datasets",
    "resource",
    "organization",
    "user",
    "oauth",
    "taxonomy",
    "activity_discussion",
    "reuse",
    "dataservices",
    "posts_report",
    "topic",
}


def test_semantic_conformance_reports_the_true_family_state() -> None:
    covered, total = gate.semantic_conformance()
    assert total == len(gate.FAMILIES)
    # The gate fails closed: families whose historical review artifacts predate
    # the current gate format are reported as gaps, never credited. This test
    # fails if the honest state regresses below today's reconciled floor or if a
    # known gap silently becomes credited (which would mean the gate weakened).
    assert covered == total - len(KNOWN_HISTORICAL_GAPS)


def test_every_current_era_receipt_verifies_at_its_own_sha() -> None:
    """Known-good: every receipt of the reviewed families is valid at its recorded sha."""
    sys.path.insert(0, str(ROOT / "scripts"))
    import check_udata_review_gate as review_gate  # type: ignore[import-not-found]

    reviewed_families = gate.FAMILIES
    for family in reviewed_families:
        if family in KNOWN_HISTORICAL_GAPS:
            continue
        reviews_dir = gate.REVIEWS_ROOT / gate.REVIEWS_DIR_NAMES.get(family, family)
        receipts = sorted(reviews_dir.glob("*/review-receipt.json")) if reviews_dir.exists() else []
        assert receipts, family
        latest = json.loads(receipts[-1].read_text(encoding="utf-8"))
        review_gate.verify_review(gate.REVIEWS_ROOT, family, latest["reviewed_sha"])
