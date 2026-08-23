"""D-20/D-25 verify-time evidence gates over the versioned CKAN corpus.

G1: every purge-class action family carries at least one receipt-bearing
    authenticated-success row tied to a manifest-registered operation.
G2: the versioned bulk-run provenance is internally consistent with the
    runtime bulk invariants (plan size, ordered indexes, settled state).
G3: every tracked evidence artifact round-trips sanitization — no
    credential-shaped substrings and no JWT/seed-token literals anywhere.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.contracts.catalog.fixtures import load_reference_fixture_set
from datasluice.domain.catalog.redaction import contains_credential_content

FIXTURES = Path("src/datasluice/contracts/catalog/fixtures/ckan")
PURGE_ACTIONS = ("dataset_purge", "organization_purge", "group_purge")
JWT_PATTERN = re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}")


def _cases() -> list[dict[str, object]]:
    document = json.loads((FIXTURES / "cases.json").read_text(encoding="utf-8"))
    cases = document["cases"] if isinstance(document, dict) and "cases" in document else document
    assert isinstance(cases, list)
    return [case for case in cases if isinstance(case, dict)]


def _purge_family_operation_ids() -> dict[str, str]:
    families: dict[str, str] = {}
    for entry in CKAN_ACTIONS.entries:
        if entry.name in PURGE_ACTIONS:
            families[entry.name] = str(entry.owning_operation_id)
    return families


def test_g1_every_purge_family_carries_a_receipt_bearing_row() -> None:
    """Each purge family owns an authenticated-success row with receipt metadata."""
    cases = _cases()
    for action_name, owning_id in _purge_family_operation_ids().items():
        receipt_rows = [
            case
            for case in cases
            if case.get("operation") == owning_id
            and case.get("outcome") == "authenticated-success"
            and isinstance(case.get("receipt_metadata"), dict)
        ]
        assert receipt_rows, f"G1: no receipt-bearing row for purge family {action_name} ({owning_id})"
        for row in receipt_rows:
            metadata = row["receipt_metadata"]
            assert isinstance(metadata, dict)
            assert set(metadata) <= {"receipt_id_shape", "actor_role_class", "operation_id"}
            assert metadata.get("receipt_id_shape") == "mutation-receipt-v1"
            assert isinstance(metadata.get("actor_role_class"), str)


def test_g2_bulk_evidence_matches_recorded_plan_and_bulk_invariants() -> None:
    """The versioned bulk run is plan-consistent and index-monotonic per phase."""
    evidence = json.loads((FIXTURES / "evidence.json").read_text(encoding="utf-8"))
    stack = evidence["controlled_stack"]
    bulk = stack["bulk_run"]

    assert bulk["bulk_count"] >= 1
    assert {phase["mode"] for phase in bulk["phases"]} == {"create", "delete"}
    for phase in bulk["phases"]:
        indexes = phase["item_receipt_indexes"]
        assert indexes == list(range(bulk["bulk_count"])), "receipt indexes must be complete and ordered"
        assert phase["state"] == "completed"
        assert phase["succeeded"] == bulk["bulk_count"]
        assert phase["failed"] == 0
        assert phase["checkpoints_recorded"] >= 1
        assert phase["last_checkpoint_settled"] == bulk["bulk_count"]
        outcomes = phase["item_outcomes"]
        assert len(outcomes) == bulk["bulk_count"]
        assert all(outcome == "succeeded" for outcome in outcomes)

    assert stack["ckan_version_reported"].startswith("2.11.")
    assert stack["identity_presence"] == {
        "datasluice-sysadmin": True,
        "datasluice-org-admin": True,
        "datasluice-user": True,
    }
    for image in stack["images"]:
        assert "@sha256:" in image["digest"]


def test_g3_tracked_evidence_round_trips_sanitization() -> None:
    """No credential shapes or token literals survive into tracked artifacts."""
    tracked = (FIXTURES / "cases.json").read_text(encoding="utf-8") + (FIXTURES / "evidence.json").read_text(
        encoding="utf-8"
    )
    assert not JWT_PATTERN.search(tracked), "a JWT-shaped literal reached a tracked fixture"

    document = json.loads((FIXTURES / "evidence.json").read_text(encoding="utf-8"))

    def walk(value: object) -> None:
        if isinstance(value, str):
            assert not contains_credential_content(value), f"credential-shaped string: {value[:40]!r}"
        elif isinstance(value, dict):
            for key, item in value.items():
                walk(key)
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(document)

    loader_set = load_reference_fixture_set("ckan")
    platform_version = loader_set.evidence["platform_version"]
    assert isinstance(platform_version, str)
    assert platform_version.startswith("CKAN 2.11.")
