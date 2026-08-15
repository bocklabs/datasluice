"""Contract tests for the pinned CKAN capability profile."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parents[4]
_PROFILE_PATH = _ROOT / "src/datasluice/contracts/catalog/profiles/ckan-2.11.json"
_EVIDENCE_PATH = _ROOT / "tests/fixtures/catalog/ckan/evidence.json"
_CASES_PATH = _ROOT / "tests/fixtures/catalog/ckan/cases.json"
_EXPECTED_OPERATION_IDS = {
    "ckan/action-api-v3.discovery-help-and-status",
    "ckan/action-api-v3.dataset-list-show-search",
    "ckan/action-api-v3.dataset-create-update-patch-delete-purge",
    "ckan/action-api-v3.resource-list-show-create-update-patch-delete-upload",
    "ckan/action-api-v3.organization-list-show-create-update-delete-members",
    "ckan/action-api-v3.group-list-show-create-update-delete-members",
    "ckan/action-api-v3.user-list-show-create-update-delete-token-management",
    "ckan/action-api-v3.tags-vocabularies-and-licenses",
    "ckan/action-api-v3.relationships-followers-and-activity",
    "ckan/action-api-v3.resource-views",
    "ckan/datastore-extension.query-and-record-crud",
    "ckan/filestore.upload-and-resource-file-replacement",
    "ckan/plugin-provided-action-and-extension-probes",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_profile_covers_each_ckan_integrate_capability_exactly_once() -> None:
    profile = _read_json(_PROFILE_PATH)
    operations = profile["operations"]

    assert isinstance(operations, list)
    operation_ids = [operation["id"] for operation in operations]
    assert set(operation_ids) == _EXPECTED_OPERATION_IDS
    assert len(operation_ids) == len(set(operation_ids))


def test_profile_explicitly_opts_out_only_the_legacy_api() -> None:
    profile = _read_json(_PROFILE_PATH)

    assert profile["opt_out_operations"] == [
        {
            "id": "ckan/legacy-api.unsupported",
            "reason": "Locked project scope supports the latest stable Action API only.",
        }
    ]
    assert all("legacy" not in operation["id"] for operation in profile["operations"])


def test_evidence_pins_official_read_observation_and_controlled_mutation_boundary() -> None:
    profile = _read_json(_PROFILE_PATH)
    evidence = _read_json(_EVIDENCE_PATH)

    assert evidence["platform_version"] == "CKAN 2.11.5"
    assert evidence["api_version"] == "Action API v3"
    assert evidence["official_source_uri"] == "https://docs.ckan.org/en/2.11/api/"
    assert evidence["source_accessed_at"] == "2026-08-15"
    assert evidence["public_read"] == {
        "deployment_url": "https://demo.ckan.org/api/3/action/package_list",
        "accessed_at": "2026-08-15",
        "response_class": "success",
        "sanitized": True,
    }
    assert evidence["mutation_evidence"] == "controlled-environment-only"
    assert "credential" not in json.dumps(evidence).lower()
    assert "raw_body" not in json.dumps(evidence).lower()
    assert profile["fixture_fingerprint"] == hashlib.sha256(_CASES_PATH.read_bytes()).hexdigest()


def test_cases_cover_required_effective_capability_outcomes() -> None:
    cases = _read_json(_CASES_PATH)
    outcomes = {case["outcome"] for case in cases["cases"]}

    assert outcomes == {
        "core",
        "optional",
        "authenticated-success",
        "missing-credentials",
        "invalid-credentials",
        "forbidden",
        "deployment-disabled",
        "unavailable",
    }
