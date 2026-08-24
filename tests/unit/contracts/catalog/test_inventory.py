"""Contract tests for catalog operation and capability profile values."""

from datetime import date

import pytest

from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    DeclaredCapabilityProfile,
    EffectiveCapabilityProfile,
    EffectiveCapabilityState,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)


def _operation(
    *,
    method: str = "get_dataset",
    capability_class: CapabilityClass = CapabilityClass.CORE,
    mutation_class: MutationClass = MutationClass.READ,
) -> OperationSpec:
    operation_id = OperationId(platform="ckan", service="datasets", method=method)
    return OperationSpec(
        id=operation_id,
        tier=OperationTier.NATIVE,
        request_type="DatasetRequest",
        response_type="DatasetResponse",
        auth_class=AuthClass.PUBLIC,
        mutation_class=mutation_class,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.SINGLE_RESOURCE,
        capability_class=capability_class,
    )


def _declared_profile(*operations: OperationSpec) -> DeclaredCapabilityProfile:
    return DeclaredCapabilityProfile(
        profile_version="1.0",
        schema_version="1.0",
        platform_api_version="3.0",
        official_source_uri="https://docs.ckan.org/en/latest/api/",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint="sha256:fixture",
        operations={operation.id: operation for operation in operations},
    )


def test_operation_spec_has_complete_typed_operation_taxonomy() -> None:
    operation = _operation()

    assert operation.id == OperationId(platform="ckan", service="datasets", method="get_dataset")
    assert operation.tier is OperationTier.NATIVE
    assert operation.request_type == "DatasetRequest"
    assert operation.response_type == "DatasetResponse"
    assert operation.auth_class is AuthClass.PUBLIC
    assert operation.mutation_class is MutationClass.READ
    assert operation.idempotency is Idempotency.SAFE
    assert operation.concurrency is ConcurrencyRequirement.NONE
    assert operation.atomicity is Atomicity.SINGLE_RESOURCE
    assert operation.capability_class is CapabilityClass.CORE


def test_declared_profile_rejects_duplicate_or_missing_operation_ids() -> None:
    operation = _operation()

    with pytest.raises(ValueError, match="missing operation IDs"):
        DeclaredCapabilityProfile(
            profile_version="1.0",
            schema_version="1.0",
            platform_api_version="3.0",
            official_source_uri="https://docs.ckan.org/en/latest/api/",
            source_accessed_at=date(2026, 8, 15),
            fixture_fingerprint="sha256:fixture",
            operations={},
        )

    with pytest.raises(ValueError, match="duplicate operation ID"):
        DeclaredCapabilityProfile(
            profile_version="1.0",
            schema_version="1.0",
            platform_api_version="3.0",
            official_source_uri="https://docs.ckan.org/en/latest/api/",
            source_accessed_at=date(2026, 8, 15),
            fixture_fingerprint="sha256:fixture",
            operations={OperationId(platform="ckan", service="datasets", method="alias"): operation},
        )


@pytest.mark.parametrize(
    ("capability_class", "response_class", "expected_state"),
    [
        (CapabilityClass.CORE, ProbeResponseClass.SUCCESS, EffectiveCapabilityState.CORE),
        (CapabilityClass.OPTIONAL, ProbeResponseClass.SUCCESS, EffectiveCapabilityState.OPTIONAL),
        (CapabilityClass.AUTHENTICATED, ProbeResponseClass.SUCCESS, EffectiveCapabilityState.AUTHENTICATED),
        (CapabilityClass.CORE, ProbeResponseClass.UNAUTHORIZED, EffectiveCapabilityState.UNAUTHORIZED),
        (CapabilityClass.CORE, ProbeResponseClass.FORBIDDEN, EffectiveCapabilityState.FORBIDDEN),
        (CapabilityClass.CORE, ProbeResponseClass.UNAVAILABLE, EffectiveCapabilityState.UNAVAILABLE),
        (
            CapabilityClass.CORE,
            ProbeResponseClass.DEPLOYMENT_DISABLED,
            EffectiveCapabilityState.DEPLOYMENT_DISABLED,
        ),
    ],
)
def test_effective_profile_preserves_operation_specific_probe_evidence(
    capability_class: CapabilityClass,
    response_class: ProbeResponseClass,
    expected_state: EffectiveCapabilityState,
) -> None:
    operation = _operation(capability_class=capability_class)
    profile = _declared_profile(operation)
    evidence = ProbeEvidence(
        operation_id=operation.id,
        deployment_url="https://catalog.example.test/api?token=discard-me",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=response_class,
    )

    effective = EffectiveCapabilityProfile.derive(profile, [evidence])
    capability = effective.for_operation(operation.id)

    assert capability.state is expected_state
    assert capability.evidence == evidence
    assert "token" not in capability.evidence.deployment_url


@pytest.mark.parametrize(
    ("response_class", "expected_remedy"),
    [
        (ProbeResponseClass.UNSUPPORTED, "Use a connector profile that declares the operation."),
        (ProbeResponseClass.DEPLOYMENT_DISABLED, "Enable the capability on the target deployment."),
        (ProbeResponseClass.UNAUTHORIZED, "Provide credentials with access to the operation."),
    ],
)
def test_unavailable_states_produce_safe_pre_dispatch_guard_decisions(
    response_class: ProbeResponseClass,
    expected_remedy: str,
) -> None:
    operation = _operation()
    profile = _declared_profile(operation)
    effective = EffectiveCapabilityProfile.derive(
        profile,
        [
            ProbeEvidence(
                operation_id=operation.id,
                deployment_url="https://catalog.example.test/api",
                credential_classification=CredentialClassification.ANONYMOUS,
                role_classification=RoleClassification.ANONYMOUS,
                observed_response_class=response_class,
            )
        ],
    )

    decision = effective.guard(operation.id)

    assert not decision.allowed
    assert decision.remedy == expected_remedy


def test_public_read_evidence_does_not_promote_mutation_permission() -> None:
    read_operation = _operation()
    write_operation = _operation(
        method="create_dataset",
        capability_class=CapabilityClass.AUTHENTICATED,
        mutation_class=MutationClass.CREATE,
    )
    profile = _declared_profile(read_operation, write_operation)
    read_evidence = ProbeEvidence(
        operation_id=read_operation.id,
        deployment_url="https://catalog.example.test/api",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=ProbeResponseClass.SUCCESS,
    )

    effective = EffectiveCapabilityProfile.derive(profile, [read_evidence])

    assert effective.for_operation(read_operation.id).state is EffectiveCapabilityState.CORE
    assert effective.for_operation(write_operation.id).state is EffectiveCapabilityState.UNAVAILABLE
    assert not effective.guard(write_operation.id).allowed


def test_ckan_manifest_datasets_group_holds_exactly_the_documented_twenty_actions() -> None:
    """Manifest-driven completeness: role splits and kind agreement, no duplicated tuple."""
    from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
    from datasluice.connectors.catalog.ckan.mapping import RECORD_KINDS, RESULT_KINDS

    entries = [entry for entry in CKAN_ACTIONS.entries if entry.group == "datasets"]
    assert len(entries) == 20
    assert {entry.owning_operation_id for entry in entries} == {
        "ckan/action-api-v3.dataset-list-show-search",
        "ckan/action-api-v3.dataset-create-update-patch-delete-purge",
        "ckan/action-api-v3.dataset-collaborators",
    }
    reads = [entry for entry in entries if entry.mutation_class == "read"]
    standards = [entry for entry in entries if entry.mutation_class == "standard"]
    destructive = [entry for entry in entries if entry.mutation_class == "destructive"]
    assert len(reads) == 7
    assert len(standards) == 12
    assert len(destructive) == 1
    collaborator_reads = [e for e in reads if e.owning_operation_id.endswith("dataset-collaborators")]
    collaborator_mutations = [e for e in standards if e.owning_operation_id.endswith("dataset-collaborators")]
    assert len(collaborator_reads) == 2
    assert len(collaborator_mutations) == 2
    purge = next(entry for entry in entries if entry.name == "dataset_purge")
    assert purge.mutation_class == "destructive"
    for entry in entries:
        spec = RESULT_KINDS.get(entry.name)
        assert spec is not None, f"{entry.name} is absent from the mapping truth table"
        outcome, family = spec
        assert outcome == entry.result_kind
        if family is not None:
            assert family in RECORD_KINDS


def test_relationships_follows_manifest_holds_exactly_the_thirty_one_core_actions() -> None:
    """Staged completeness gate: the core relationships-follows id owns exactly 31 entries."""
    from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
    from datasluice.connectors.catalog.ckan.mapping import RECORD_KINDS, RESULT_KINDS

    entries = [
        entry
        for entry in CKAN_ACTIONS.entries
        if entry.owning_operation_id == "ckan/action-api-v3.relationships-follows"
    ]
    assert len(entries) == 31
    assert {entry.group for entry in entries} == {"relationships_activity"}
    reads = [entry for entry in entries if entry.mutation_class == "read"]
    mutations = [entry for entry in entries if entry.mutation_class != "read"]
    assert len(reads) == 22
    assert len(mutations) == 9
    assert all(entry.mutation_class == "standard" for entry in mutations)
    assert sum(1 for entry in entries if entry.name.startswith("package_relationship")) == 4
    assert sum(1 for entry in entries if entry.result_kind == "value") == 13
    assert sum(1 for entry in entries if entry.result_kind == "record-list") == 9
    assert sum(1 for entry in entries if entry.result_kind == "mapping") == 9
    for entry in entries:
        spec = RESULT_KINDS.get(entry.name)
        assert spec is not None, f"{entry.name} is absent from the mapping truth table"
        outcome, family = spec
        assert outcome == entry.result_kind
        if family is not None:
            assert family in RECORD_KINDS


def test_inventory_complete() -> None:
    """Assert three-way completeness of the exhaustive 157-action CKAN inventory.

    The checked-in ``action_manifest.json`` is the single source of registered
    action names (D-22): registry, mapping truth table, declared v2 profile
    operations, and the typed methods of BOTH projection modes must agree with
    it exactly. Extending or regenerating the manifest requires a reviewed
    manifest amendment — this gate fails on any addition or omission.
    """
    import json
    from importlib import resources

    from datasluice.connectors.catalog.ckan.clients import (
        _AsyncDiscoveryService,
        _SyncDiscoveryService,
        declared_ckan_profile,
    )
    from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
    from datasluice.connectors.catalog.ckan.mapping import RECORD_KINDS, RESULT_KINDS
    from datasluice.connectors.catalog.ckan.services.datasets import AsyncDatasetsService, SyncDatasetsService
    from datasluice.connectors.catalog.ckan.services.datastore import AsyncDatastoreService, SyncDatastoreService
    from datasluice.connectors.catalog.ckan.services.extensions import AsyncExtensionsService, SyncExtensionsService
    from datasluice.connectors.catalog.ckan.services.groups import AsyncGroupsService, SyncGroupsService
    from datasluice.connectors.catalog.ckan.services.organizations import (
        AsyncOrganizationsService,
        SyncOrganizationsService,
    )
    from datasluice.connectors.catalog.ckan.services.relationships_activity import (
        AsyncRelationshipsActivityService,
        SyncRelationshipsActivityService,
    )
    from datasluice.connectors.catalog.ckan.services.resources import AsyncResourcesService, SyncResourcesService
    from datasluice.connectors.catalog.ckan.services.users import AsyncUsersService, SyncUsersService
    from datasluice.connectors.catalog.ckan.services.views import AsyncViewsService, SyncViewsService
    from datasluice.connectors.catalog.ckan.services.vocabularies_licenses import (
        AsyncVocabulariesLicensesService,
        SyncVocabulariesLicensesService,
    )

    group_projections = {
        "action_discovery": (_SyncDiscoveryService, _AsyncDiscoveryService),
        "datasets": (SyncDatasetsService, AsyncDatasetsService),
        "resources": (SyncResourcesService, AsyncResourcesService),
        "organizations": (SyncOrganizationsService, AsyncOrganizationsService),
        "groups": (SyncGroupsService, AsyncGroupsService),
        "users": (SyncUsersService, AsyncUsersService),
        "vocabularies_licenses": (SyncVocabulariesLicensesService, AsyncVocabulariesLicensesService),
        "relationships_activity": (SyncRelationshipsActivityService, AsyncRelationshipsActivityService),
        "views": (SyncViewsService, AsyncViewsService),
        "datastore": (SyncDatastoreService, AsyncDatastoreService),
        "extensions": (SyncExtensionsService, AsyncExtensionsService),
    }

    document = json.loads(
        resources.files("datasluice.connectors.catalog.ckan")
        .joinpath("action_manifest.json")
        .read_text(encoding="utf-8")
    )
    manifest_names = {item["name"] for item in document["actions"]}
    registry_names = {entry.name for entry in CKAN_ACTIONS.entries}
    assert len(manifest_names) == 157
    assert len(registry_names) == 157
    assert manifest_names ^ registry_names == frozenset()
    assert set(RESULT_KINDS) == manifest_names, (
        f"RESULT_KINDS carries stale keys absent from the manifest: {sorted(set(RESULT_KINDS) - manifest_names)}"
    )

    declared_operations = {str(operation_id) for operation_id in declared_ckan_profile().operations}
    valid_mutation_classes = {"read", "standard", "destructive"}
    valid_result_kinds = {"record", "record-list", "value", "value-list", "mapping", "token-secret"}
    for entry in CKAN_ACTIONS.entries:
        assert entry.owning_operation_id in declared_operations, f"{entry.name} rides an undeclared v2 id"
        assert entry.mutation_class in valid_mutation_classes, f"{entry.name} carries an invalid mutation class"
        assert entry.result_kind in valid_result_kinds, f"{entry.name} carries an invalid result kind"
        assert entry.group in group_projections, f"{entry.name} belongs to an unknown group"
        spec = RESULT_KINDS.get(entry.name)
        assert spec is not None, f"{entry.name} is absent from the mapping truth table"
        outcome, family = spec
        assert outcome == entry.result_kind, f"{entry.name} disagrees with the mapping truth table"
        if family is not None:
            assert family in RECORD_KINDS, f"{entry.name} names an unknown record family"
        sync_type, async_type = group_projections[entry.group]
        sync_method = getattr(sync_type, entry.name, None)
        async_method = getattr(async_type, entry.name, None)
        assert callable(sync_method), f"{sync_type.__name__} misses the typed method {entry.name}"
        assert callable(async_method), f"{async_type.__name__} misses the typed method {entry.name}"

    filestore_entries = [
        entry
        for entry in CKAN_ACTIONS.entries
        if entry.owning_operation_id == "ckan/filestore.upload-and-resource-file-replacement"
    ]
    assert filestore_entries == []
    assert "filestore" not in {entry.group for entry in CKAN_ACTIONS.entries}

    counted = sum(len([entry for entry in CKAN_ACTIONS.entries if entry.group == group]) for group in group_projections)
    assert counted == 157
