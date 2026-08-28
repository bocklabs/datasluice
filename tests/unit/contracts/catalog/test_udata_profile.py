"""Contract tests for the pinned uData capability profile."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parents[4]
_PROFILE_PATH = _ROOT / "src/datasluice/contracts/catalog/profiles/udata-17.6.json"
_EVIDENCE_PATH = _ROOT / "src/datasluice/contracts/catalog/fixtures/udata/evidence.json"
_CASES_PATH = _ROOT / "src/datasluice/contracts/catalog/fixtures/udata/cases.json"
_EXPECTED_OPERATION_IDS = {
    "udata/api-v1.root-and-effective-profile-probe",
    "udata/api-v1.dataset-list-search-show-create-update-delete",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete",
    "udata/api-v1.organizations-and-memberships",
    "udata/api-v1.users-me-and-api-token-management",
    "udata/api-v1.authentication-and-oauth-flows",
    "udata/api-v1.taxonomies-licenses-frequencies-formats-badges-and-schemas",
    "udata/api-v1.followers-activities-discussions-and-reuses",
    "udata/api-v1.topics-territories-contact-points-and-dataservices",
    "udata/api-v1.harvest-moderation-and-admin-operations",
    "udata/deployment-plugin-and-configuration-dependent-routes",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_profile_covers_each_udata_integrate_capability_exactly_once() -> None:
    """Every planned uData contract family and dataset route is declared exactly once."""
    profile = _read_json(_PROFILE_PATH)
    operations = profile["operations"]

    assert isinstance(operations, list)
    operation_ids = [operation["id"] for operation in operations]
    assert set(_EXPECTED_OPERATION_IDS).issubset(set(operation_ids))
    assert set(operation_ids) == _EXPECTED_OPERATION_IDS | _DATASET_ROUTE_OPERATION_IDS
    assert len(operation_ids) == len(set(operation_ids))


_DATASET_ROUTE_OPERATION_IDS = {
    "udata/api-v1.list-datasets",
    "udata/api-v1.create-dataset",
    "udata/api-v1.recent-datasets-atom",
    "udata/api-v1.get-dataset",
    "udata/api-v1.update-dataset",
    "udata/api-v1.delete-dataset",
    "udata/api-v1.feature-dataset",
    "udata/api-v1.unfeature-dataset",
    "udata/api-v1.rdf-dataset",
    "udata/api-v1.rdf-dataset-format",
    "udata/api-v1.suggest-datasets",
    "udata/api-v2.search-datasets",
    "udata/api-v2.list-datasets",
    "udata/api-v2.get-dataset",
    "udata/api-v2.get-dataset-extras",
    "udata/api-v2.update-dataset-extras",
    "udata/api-v2.delete-dataset-extras",
}


def test_evidence_pins_official_read_observation_and_controlled_mutation_boundary() -> None:
    """Public evidence is sanitized, while mutations require a controlled instance."""
    profile = _read_json(_PROFILE_PATH)
    evidence = _read_json(_EVIDENCE_PATH)

    assert profile["profile_version"] == "17.6.0"
    assert profile["platform"] == "udata"
    assert evidence["platform_version"] == "uData 17.6.0"
    assert evidence["official_source_uri"] == "https://udata.readthedocs.io/en/17.6/"
    assert evidence["source_accessed_at"] == "2026-08-27"
    assert evidence["public_read"] == {
        "deployment_url": "https://www.data.gouv.fr/api/1/datasets/",
        "accessed_at": "2026-08-27",
        "response_class": "success",
        "sanitized": True,
    }
    assert evidence["mutation_evidence"] == "controlled-environment-only"
    assert "credential" not in json.dumps(evidence).lower()
    assert "raw_body" not in json.dumps(evidence).lower()
    assert profile["fixture_fingerprint"] == hashlib.sha256(_CASES_PATH.read_bytes()).hexdigest()


def test_deployment_dependent_routes_require_observed_effective_evidence() -> None:
    """Declared plugin/configuration routes never claim universal availability."""
    profile = _read_json(_PROFILE_PATH)
    deployment_operation = next(
        operation
        for operation in profile["operations"]
        if operation["id"] == "udata/deployment-plugin-and-configuration-dependent-routes"
    )

    assert deployment_operation == {
        "id": "udata/deployment-plugin-and-configuration-dependent-routes",
        "capability": "optional",
        "authentication": "public",
        "mutation": "read",
        "evidence_requirement": "deployment-probe",
    }


def test_cases_cover_required_effective_capability_outcomes() -> None:
    """Fixture cases distinguish public, auth, role, and deployment states."""
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
