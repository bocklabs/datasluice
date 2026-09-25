from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
from typing import Any

from tests.unit.contracts.catalog.evidence_execution import (
    controlled_test_source,
    unexecuted_operations,
)

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


# Each recorded evidence family is claimed by the controlled test that drives it;
# the read and mutation matrices are separate tests for the user and organization
# families and one combined test for the OAuth family.
_EVIDENCE_CLAIMING_TESTS = {
    "controlled_oauth_evidence": {
        "read": "test_controlled_oauth_routes_match_raw_semantics_in_both_modes",
        "mutation": "test_controlled_oauth_routes_match_raw_semantics_in_both_modes",
    },
    "controlled_user_evidence": {
        "read": "test_controlled_user_reads_match_raw_routes_in_both_modes",
        "mutation": "test_controlled_user_mutations_match_raw_routes_in_both_modes",
    },
    "controlled_organization_evidence": {
        "read": "test_controlled_organization_read_matrix_matches_raw_routes",
        "mutation": "test_controlled_organization_mutations_match_raw_routes_in_both_modes",
    },
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


def test_controlled_oauth_evidence_covers_every_route_in_both_modes() -> None:
    """The recorded coverage must match what the named tests actually execute.

    Asserting the recorded lists against themselves would ratify a false claim, so
    this reads the controlled test and requires every claimed operation and mode to
    be present in it.
    """
    controlled = _read_json(_EVIDENCE_PATH)["controlled_oauth_evidence"]
    evidence = controlled["route_differential"]
    reads = evidence["read_operations"]
    mutations = evidence["mutation_operations"]

    assert set(reads) | set(mutations) == _OAUTH_ROUTE_OPERATION_IDS
    assert set(reads).isdisjoint(mutations)
    assert controlled["sanitized"] is True
    assert controlled["local_only"] is True
    assert controlled["stack_version"] == "17.6.0"

    controlled_source = (_ROOT / "tests/integration/connectors/catalog/test_udata_controlled.py").read_text()
    wheel_source = (_ROOT / "tests/e2e/test_udata_wheel.py").read_text()
    for test_id in controlled["test_ids"]:
        assert f"def {test_id}(" in controlled_source, test_id
    assert f"def {controlled['wheel_test_id']}(" in wheel_source, controlled["wheel_test_id"]

    # Every claimed operation must actually be driven by the controlled test,
    # either by its route name or by a direct call to the typed method.
    for operation in set(reads) | set(mutations):
        method = operation.rsplit(".", 1)[-1].replace("-", "_")
        assert f'"{method}"' in controlled_source or f"auth_oauth.{method}(" in controlled_source, operation

    # Both modes are claimed for both families, so both client families must appear
    # in the read and the mutation pass.
    assert controlled_source.count("create_async_client(") >= 2, "async read and mutation passes are required"
    assert "asyncio.run(run_mutations_async())" in controlled_source
    assert "asyncio.run(run_async())" in controlled_source
    assert evidence["read_modes"] == ["sync", "async"]
    assert evidence["mutation_modes"] == ["sync", "async"]

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


def _without_async_passes(source: str, test_name: str) -> str:
    """Return the controlled source with every async pass of one test removed."""
    target = next(
        node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == test_name
    )
    async_passes = [node for node in ast.walk(target) if isinstance(node, ast.AsyncFunctionDef)]
    assert async_passes, f"{test_name} must drive an async pass"
    lines = source.splitlines(keepends=True)
    first = min(pass_.lineno for pass_ in async_passes)
    last = max((pass_.end_lineno or pass_.lineno) for pass_ in async_passes)
    return "".join(lines[: first - 1] + lines[last:])


def test_controlled_evidence_coverage_is_derived_from_executed_passes() -> None:
    """Every recorded route must be driven by a real client pass in every claimed mode.

    A recorded list compared against itself ratifies a false claim, so the recorded
    coverage is checked against the controlled test's parsed structure: each claimed
    read and mutation must be reached by a pass that constructs a client of every
    claimed mode. The recorded SHA digests stay as tamper-evidence, but the gate
    is load-bearing only because of this structural derivation.
    """
    source = controlled_test_source(_ROOT)
    evidence_document = _read_json(_EVIDENCE_PATH)
    unexecuted: dict[str, set[str]] = {}

    for family, tests in _EVIDENCE_CLAIMING_TESTS.items():
        differential = evidence_document[family]["route_differential"]
        for kind in ("read", "mutation"):
            unexecuted[f"{family}.{kind}"] = unexecuted_operations(
                source,
                tests[kind],
                set(differential[f"{kind}_operations"]),
                differential[f"{kind}_modes"],
            )

    expected_empty = {f"{family}.{kind}" for family in _EVIDENCE_CLAIMING_TESTS for kind in ("read", "mutation")}
    assert {family: operations for family, operations in unexecuted.items() if operations} == {}
    assert set(unexecuted) == expected_empty


def test_the_execution_gate_fails_when_the_async_mutation_pass_is_removed() -> None:
    """The regression: gut the async OAuth mutation pass and the gate must catch it.

    This is the falsification the reviewer performed. Keeping the recorded lists and
    rebinding the digests must not be enough to keep the suite green once the code
    that executes the async mutation pass is gone.
    """
    source = controlled_test_source(_ROOT)
    differential = _read_json(_EVIDENCE_PATH)["controlled_oauth_evidence"]["route_differential"]
    mutations = set(differential["mutation_operations"])
    modes = differential["mutation_modes"]
    test = _EVIDENCE_CLAIMING_TESTS["controlled_oauth_evidence"]["mutation"]

    gutted = _without_async_passes(source, test)
    assert gutted != source, "the async OAuth mutation pass was not found to remove"
    assert not unexecuted_operations(source, test, mutations, modes)
    assert unexecuted_operations(gutted, test, mutations, modes) == mutations


def test_the_execution_gate_fails_when_a_user_async_mutation_pass_is_removed() -> None:
    """The same structural gate holds for the user family, not just OAuth."""
    source = controlled_test_source(_ROOT)
    differential = _read_json(_EVIDENCE_PATH)["controlled_user_evidence"]["route_differential"]
    mutations = set(differential["mutation_operations"])
    modes = differential["mutation_modes"]
    test = _EVIDENCE_CLAIMING_TESTS["controlled_user_evidence"]["mutation"]

    gutted = _without_async_passes(source, test)

    assert not unexecuted_operations(source, test, mutations, modes)
    assert unexecuted_operations(gutted, test, mutations, modes)
