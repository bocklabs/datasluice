"""Contract tests for the pinned Socrata capability profile."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parents[4]
_PROFILE_PATH = _ROOT / "src/datasluice/contracts/catalog/profiles/socrata-soda3.json"
_EVIDENCE_PATH = _ROOT / "tests/fixtures/catalog/socrata/evidence.json"
_CASES_PATH = _ROOT / "tests/fixtures/catalog/socrata/cases.json"
_EXPECTED_OPERATION_IDS = {
    "socrata/soda-v3-query",
    "socrata/soda-v3-export",
    "socrata/soda-v3-row-create-update-upsert-delete",
    "socrata/soda-v3-soql-query-types-and-format-negotiation",
    "socrata/catalog-discovery-and-view-metadata",
    "socrata/asset-dataset-metadata-and-permission-management",
    "socrata/user-current-identity-and-permission-probe",
    "socrata/application-token-basic-auth-and-oauth",
    "socrata/async-request-status-rate-limit-and-request-id",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_profile_covers_each_socrata_integrate_capability_exactly_once() -> None:
    """Every integrated SODA 3 and native capability has one declaration."""
    profile = _read_json(_PROFILE_PATH)
    operations = profile["operations"]

    assert isinstance(operations, list)
    operation_ids = [operation["id"] for operation in operations]
    assert set(operation_ids) == _EXPECTED_OPERATION_IDS
    assert len(operation_ids) == len(set(operation_ids))


def test_profile_explicitly_opts_out_soda_two_resource_endpoints() -> None:
    """Legacy SODA 2 endpoints cannot enter the public SODA 3 contract."""
    profile = _read_json(_PROFILE_PATH)

    assert profile["opt_out_operations"] == [
        {
            "id": "socrata/soda-v2-legacy-resource-endpoints",
            "reason": "Locked project scope supports the latest stable SODA 3 profile only.",
        }
    ]
    assert all("soda-v2" not in operation["id"] for operation in profile["operations"])


def test_evidence_pins_official_soda_three_sources_and_legacy_observation_boundary() -> None:
    """Official support and deployed legacy-read evidence remain distinct."""
    profile = _read_json(_PROFILE_PATH)
    evidence = _read_json(_EVIDENCE_PATH)

    assert profile["profile_version"] == "3.0"
    assert profile["platform"] == "socrata"
    assert evidence["platform_version"] == "Socrata SODA 3.0"
    assert evidence["official_sources"] == {
        "endpoints": "https://dev.socrata.com/docs/endpoints",
        "authentication": "https://dev.socrata.com/docs/authentication",
        "response_codes": "https://dev.socrata.com/docs/response-codes.html",
    }
    assert evidence["source_accessed_at"] == "2026-08-15"
    assert evidence["legacy_observation"] == {
        "deployment_url": "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=1",
        "accessed_at": "2026-08-15",
        "response_class": "success",
        "sanitized": True,
        "classification": "deployed-legacy-observation-not-soda3-availability",
    }
    assert evidence["mutation_evidence"] == "controlled-environment-only"
    assert "credential" not in json.dumps(evidence).lower()
    assert "raw_body" not in json.dumps(evidence).lower()
    assert profile["fixture_fingerprint"] == hashlib.sha256(_CASES_PATH.read_bytes()).hexdigest()


def test_cases_cover_auth_permissions_async_rate_and_deployment_outcomes() -> None:
    """Fixture cases preserve Socrata-specific capability and failure states."""
    cases = _read_json(_CASES_PATH)
    outcomes = {case["outcome"] for case in cases["cases"]}
    credential_classes = {case.get("credential_class") for case in cases["cases"]}
    covered_operations = {case["operation"] for case in cases["cases"]}

    assert outcomes == {
        "core",
        "optional",
        "authenticated-success",
        "missing-credentials",
        "invalid-credentials",
        "forbidden",
        "deployment-disabled",
        "unavailable",
        "async-pending",
        "rate-limited",
    }
    assert {"application-token", "basic", "oauth"}.issubset(credential_classes)
    assert "socrata/user-current-identity-and-permission-probe" in covered_operations
    assert "socrata/async-request-status-rate-limit-and-request-id" in covered_operations
