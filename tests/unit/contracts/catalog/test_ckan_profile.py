"""Contract tests for the pinned CKAN capability profile."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parents[4]
_PROFILE_PATH = _ROOT / "src/datasluice/contracts/catalog/profiles/ckan-2.11.json"
_EVIDENCE_PATH = _ROOT / "src/datasluice/contracts/catalog/fixtures/ckan/evidence.json"
_CASES_PATH = _ROOT / "src/datasluice/contracts/catalog/fixtures/ckan/cases.json"
_V2_OPERATION_IDS = frozenset(
    {
        "ckan/action-api-v3.discovery-help-and-status",
        "ckan/action-api-v3.dataset-list-show-search",
        "ckan/action-api-v3.dataset-create-update-patch-delete-purge",
        "ckan/action-api-v3.dataset-collaborators",
        "ckan/action-api-v3.resource-list-show-create-update-patch-delete-upload",
        "ckan/action-api-v3.organization-list-show-search",
        "ckan/action-api-v3.organization-create-update-delete-members",
        "ckan/action-api-v3.group-list-show-search",
        "ckan/action-api-v3.group-create-update-delete-members",
        "ckan/action-api-v3.user-list-show",
        "ckan/action-api-v3.user-create-update-delete-token-management",
        "ckan/action-api-v3.tags-vocabularies-licenses-list-show",
        "ckan/action-api-v3.tags-vocabularies-licenses-create-update-delete",
        "ckan/action-api-v3.relationships-follows",
        "ckan/action-api-v3.activity",
        "ckan/action-api-v3.resource-views",
        "ckan/datastore-extension.query-and-record-crud",
        "ckan/datastore-extension.sql-search",
        "ckan/filestore.upload-and-resource-file-replacement",
        "ckan/action-api-v3.jobs-and-task-status",
        "ckan/action-api-v3.config-options",
        "ckan/plugin-provided-action-and-extension-probes",
    }
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _operations_by_id(profile: dict[str, Any]) -> dict[str, dict[str, Any]]:
    operations = profile["operations"]
    assert isinstance(operations, list)
    operation_ids = [operation["id"] for operation in operations]
    assert len(operation_ids) == len(set(operation_ids))
    assert set(operation_ids) == _V2_OPERATION_IDS
    return {operation["id"]: operation for operation in operations}


def test_profile_declares_exactly_the_v2_evidence_distinct_operations() -> None:
    profile = _read_json(_PROFILE_PATH)

    operations = _operations_by_id(profile)
    assert len(operations) == len(_V2_OPERATION_IDS)


def test_sql_search_is_evidence_distinct_from_datastore_record_crud() -> None:
    profile = _read_json(_PROFILE_PATH)

    operations = _operations_by_id(profile)
    sql_search = operations["ckan/datastore-extension.sql-search"]
    record_crud = operations["ckan/datastore-extension.query-and-record-crud"]
    assert sql_search["capability"] == "optional"
    assert sql_search["mutation"] == "read"
    assert record_crud["mutation"] == "update"
    assert sql_search["evidence_requirement"] == "deployment-probe"
    assert record_crud["evidence_requirement"] == "controlled-environment-only"


def test_dataset_collaborators_are_separate_from_dataset_mutations() -> None:
    profile = _read_json(_PROFILE_PATH)

    operations = _operations_by_id(profile)
    collaborators = operations["ckan/action-api-v3.dataset-collaborators"]
    mutations = operations["ckan/action-api-v3.dataset-create-update-patch-delete-purge"]
    assert collaborators["capability"] == "optional"
    assert collaborators["authentication"] == "authenticated"
    assert mutations["capability"] == "authenticated"


def test_activity_is_separate_from_relationships_and_optional_per_d06() -> None:
    profile = _read_json(_PROFILE_PATH)

    operations = _operations_by_id(profile)
    relationships = operations["ckan/action-api-v3.relationships-follows"]
    activity = operations["ckan/action-api-v3.activity"]
    assert relationships["capability"] == "core"
    assert relationships["mutation"] == "update"
    assert activity["capability"] == "optional"
    assert activity["mutation"] == "read"
    assert relationships["evidence_requirement"] == "controlled-environment-only"
    assert activity["evidence_requirement"] == "deployment-probe"


def test_resource_views_declare_the_optional_view_plugin_tier() -> None:
    profile = _read_json(_PROFILE_PATH)

    views = _operations_by_id(profile)["ckan/action-api-v3.resource-views"]
    assert views["capability"] == "optional"
    assert views["authentication"] == "authenticated"
    assert views["mutation"] == "update"


def test_entity_reads_split_from_writes_with_distinct_tiers() -> None:
    profile = _read_json(_PROFILE_PATH)

    operations = _operations_by_id(profile)
    read_write_pairs = (
        (
            "ckan/action-api-v3.organization-list-show-search",
            "ckan/action-api-v3.organization-create-update-delete-members",
        ),
        (
            "ckan/action-api-v3.group-list-show-search",
            "ckan/action-api-v3.group-create-update-delete-members",
        ),
        (
            "ckan/action-api-v3.user-list-show",
            "ckan/action-api-v3.user-create-update-delete-token-management",
        ),
        (
            "ckan/action-api-v3.tags-vocabularies-licenses-list-show",
            "ckan/action-api-v3.tags-vocabularies-licenses-create-update-delete",
        ),
    )
    for read_id, write_id in read_write_pairs:
        reads, writes = operations[read_id], operations[write_id]
        assert reads["capability"] == "core"
        assert reads["authentication"] == "public"
        assert reads["mutation"] == "read"
        assert reads["evidence_requirement"] == "anonymous-read"
        assert writes["capability"] in {"admin", "authenticated"}
        assert writes["evidence_requirement"] == "controlled-environment-only"


def test_jobs_config_and_plugin_probes_are_evidence_distinct() -> None:
    profile = _read_json(_PROFILE_PATH)

    operations = _operations_by_id(profile)
    jobs = operations["ckan/action-api-v3.jobs-and-task-status"]
    config = operations["ckan/action-api-v3.config-options"]
    probes = operations["ckan/plugin-provided-action-and-extension-probes"]
    assert jobs["capability"] == "core"
    assert jobs["authentication"] == "authenticated"
    assert jobs["mutation"] == "update"
    assert config["capability"] == "admin"
    assert config["authentication"] == "privileged"
    assert config["mutation"] == "admin"
    assert probes["capability"] == "optional"
    assert probes["authentication"] == "public"
    assert probes["mutation"] == "read"
    assert probes["evidence_requirement"] == "deployment-probe"


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


def test_cases_reference_only_v2_operation_ids_and_cover_each_one() -> None:
    cases = _read_json(_CASES_PATH)
    referenced = {case["operation"] for case in cases["cases"]}

    assert referenced == _V2_OPERATION_IDS


def test_regenerated_fingerprint_script_is_idempotent() -> None:
    import os
    import subprocess
    import sys

    before = _PROFILE_PATH.read_bytes()
    environment = {**os.environ, "PYTHONPATH": str(_ROOT / "src")}
    try:
        result = subprocess.run(
            [sys.executable, "scripts/regenerate_fixture_fingerprint.py", "--platform", "ckan"],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            check=False,
            env=environment,
        )
        assert result.returncode == 0, result.stderr
        first_pass = _PROFILE_PATH.read_bytes()
        subprocess.run(
            [sys.executable, "scripts/regenerate_fixture_fingerprint.py", "--platform", "ckan"],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            check=True,
            env=environment,
        )
        assert _PROFILE_PATH.read_bytes() == first_pass
        assert hashlib.sha256(_CASES_PATH.read_bytes()).hexdigest() in result.stdout
        assert before == first_pass
    finally:
        _PROFILE_PATH.write_bytes(before)
