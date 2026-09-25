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
    "udata/api-v1.resource-reads",
    "udata/api-v1.resource-mutations",
    "udata/api-v1.resource-destructive-mutations",
    "udata/api-v1.organizations-and-memberships",
    "udata/api-v1.users-me-and-api-token-management",
    "udata/api-v1.authentication-and-oauth-flows",
    "udata/api-v1.taxonomies-licenses-frequencies-formats-badges-and-schemas",
    "udata/api-v1.followers-activities-discussions-and-reuses",
    "udata/api-v1.topics-territories-contact-points-and-dataservices",
    "udata/api-v1.harvest-moderation-and-admin-operations",
    "udata/deployment-plugin-and-configuration-dependent-routes",
}
_RESOURCE_ROUTE_OPERATION_IDS = {
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-create",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-reorder",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-new",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-replace",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-community-new",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-community-replace",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-update",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-delete",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-list",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-create",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-get",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-update",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-delete",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-extras-update",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-extras-delete",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-redirect",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-get",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-types",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-v2-dataset-get",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-v2-resource-list",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-v2-resource-get",
    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-v2-extras-get",
}
_ORGANIZATION_ROUTE_OPERATION_IDS = {
    "udata/api-v1.list-organizations",
    "udata/api-v1.create-organization",
    "udata/api-v1.get-organization",
    "udata/api-v1.update-organization",
    "udata/api-v1.delete-organization",
    "udata/api-v1.organization-datasets-csv",
    "udata/api-v1.organization-dataservices-csv",
    "udata/api-v1.organization-discussions-csv",
    "udata/api-v1.organization-datasets-resources-csv",
    "udata/api-v1.rdf-organization",
    "udata/api-v1.rdf-organization-format",
    "udata/api-v1.available-organization-badges",
    "udata/api-v1.add-organization-badge",
    "udata/api-v1.delete-organization-badge",
    "udata/api-v1.get-organization-contact-point",
    "udata/api-v1.suggest-org-contact-points",
    "udata/api-v1.list-membership-requests",
    "udata/api-v1.membership-request",
    "udata/api-v1.accept-membership",
    "udata/api-v1.refuse-membership",
    "udata/api-v1.cancel-membership",
    "udata/api-v1.invite-organization-member",
    "udata/api-v1.update-organization-member",
    "udata/api-v1.delete-organization-member",
    "udata/api-v1.list-organization-assignments",
    "udata/api-v1.sync-member-assignments",
    "udata/api-v1.suggest-organizations",
    "udata/api-v1.organization-logo",
    "udata/api-v1.resize-organization-logo",
    "udata/api-v1.list-organization-datasets",
    "udata/api-v1.list-organization-reuses",
    "udata/api-v1.list-organization-discussions",
    "udata/api-v1.org-roles",
    "udata/api-v2.search-organizations",
    "udata/api-v2.get-organization-extras",
    "udata/api-v2.update-organization-extras",
    "udata/api-v2.delete-organization-extras",
    "udata/api-v1.list-organization-followers",
    "udata/api-v1.follow-organization",
    "udata/api-v1.unfollow-organization",
}
_USER_ROUTE_OPERATION_IDS = {
    "udata/api-v1.accept-org-invitation",
    "udata/api-v1.create-api-token",
    "udata/api-v1.create-user",
    "udata/api-v1.delete-me",
    "udata/api-v1.delete-user",
    "udata/api-v1.follow-user",
    "udata/api-v1.get-me",
    "udata/api-v1.get-user",
    "udata/api-v1.get-user-contact-point",
    "udata/api-v1.list-api-tokens",
    "udata/api-v1.list-org-invitations",
    "udata/api-v1.list-user-followers",
    "udata/api-v1.list-users",
    "udata/api-v1.my-avatar",
    "udata/api-v1.my-datasets",
    "udata/api-v1.my-metrics",
    "udata/api-v1.my-org-community-resources",
    "udata/api-v1.my-org-datasets",
    "udata/api-v1.my-org-discussions",
    "udata/api-v1.my-org-reuses",
    "udata/api-v1.my-reuses",
    "udata/api-v1.refuse-org-invitation",
    "udata/api-v1.revoke-api-token",
    "udata/api-v1.rotate-user-password",
    "udata/api-v1.suggest-users",
    "udata/api-v1.unfollow-user",
    "udata/api-v1.update-me",
    "udata/api-v1.update-user",
    "udata/api-v1.user-avatar",
    "udata/api-v1.user-roles",
    "udata/api-v2.my-org-topics",
}


_OAUTH_ROUTE_OPERATION_IDS = {
    "udata/oauth.access-token",
    "udata/oauth.authorize",
    "udata/oauth.authorize-post",
    "udata/oauth.client-info",
    "udata/oauth.oauth-error",
    "udata/oauth.revoke-token",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_profile_covers_each_udata_integrate_capability_exactly_once() -> None:
    """Every planned uData contract family and dataset route is declared exactly once."""
    profile = _read_json(_PROFILE_PATH)
    operations = profile["operations"]

    assert isinstance(operations, list)
    operation_ids = [operation["id"] for operation in operations]
    assert (
        set(operation_ids)
        == _EXPECTED_OPERATION_IDS
        | _DATASET_ROUTE_OPERATION_IDS
        | _RESOURCE_ROUTE_OPERATION_IDS
        | _ORGANIZATION_ROUTE_OPERATION_IDS
        | _USER_ROUTE_OPERATION_IDS
        | _OAUTH_ROUTE_OPERATION_IDS
    )
    assert len(operation_ids) == len(set(operation_ids))


def test_user_and_token_create_operations_declare_their_policy_metadata() -> None:
    operations = {operation["id"]: operation for operation in _read_json(_PROFILE_PATH)["operations"]}

    assert operations["udata/api-v1.create-api-token"] == {
        "id": "udata/api-v1.create-api-token",
        "capability": "authenticated",
        "authentication": "authenticated",
        "mutation": "create",
        "evidence_requirement": "controlled-environment-only",
    }
    assert operations["udata/api-v1.create-user"] == {
        "id": "udata/api-v1.create-user",
        "capability": "admin",
        "authentication": "privileged",
        "mutation": "create",
        "evidence_requirement": "controlled-environment-only",
    }


_DATASET_ROUTE_OPERATION_IDS = {
    "udata/api-v1.set_site",
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


def test_controlled_organization_evidence_covers_every_route_in_both_modes() -> None:
    controlled = _read_json(_EVIDENCE_PATH)["controlled_organization_evidence"]
    evidence = controlled["route_differential"]
    read_operations = evidence["read_operations"]
    mutation_operations = evidence["mutation_operations"]
    reads = set(read_operations)
    mutations = set(mutation_operations)

    assert "test_controlled_organization_read_matrix_matches_raw_routes" in controlled["test_ids"]
    assert "test_controlled_organization_mutations_match_raw_routes_in_both_modes" in controlled["test_ids"]
    assert evidence["read_modes"] == ["sync", "async"]
    assert evidence["mutation_modes"] == ["sync", "async"]
    assert len(read_operations) == 21
    assert len(reads) == len(read_operations)
    assert len(mutation_operations) == 19
    assert len(mutations) == len(mutation_operations)
    assert reads.isdisjoint(mutations)
    assert reads | mutations == _ORGANIZATION_ROUTE_OPERATION_IDS


def test_controlled_user_evidence_covers_every_route_in_both_modes() -> None:
    controlled = _read_json(_EVIDENCE_PATH)["controlled_user_evidence"]
    evidence = controlled["route_differential"]
    reads = evidence["read_operations"]
    mutations = evidence["mutation_operations"]

    assert evidence["read_modes"] == ["sync", "async"]
    assert evidence["mutation_modes"] == ["sync", "async"]
    assert len(reads) == 17
    assert len(mutations) == 14
    assert len(set(reads)) == len(reads)
    assert len(set(mutations)) == len(mutations)
    assert set(reads).isdisjoint(mutations)
    assert set(reads) | set(mutations) == _USER_ROUTE_OPERATION_IDS
    assert controlled["sanitized"] is True
    assert controlled["local_only"] is True
    assert (
        controlled["controlled_test_sha256"]
        == hashlib.sha256(
            (_ROOT / "tests/integration/connectors/catalog/test_udata_controlled.py").read_bytes()
        ).hexdigest()
    )
    assert (
        controlled["wheel_test_sha256"]
        == hashlib.sha256((_ROOT / "tests/e2e/test_udata_wheel.py").read_bytes()).hexdigest()
    )


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
