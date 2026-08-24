"""Unit tests for domain models."""

from __future__ import annotations

from datetime import date

import pytest

from datasluice.domain import Dataset, License, Organization, Query, Resource, SearchResult
from datasluice.domain.catalog.models import MappingRecord, ResultEnvelope, ValueRecord
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
    EvidenceProvenance,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.exceptions import DataSluiceError


def test_license_defaults() -> None:
    license_ = License(id="CC-BY-4.0")
    assert license_.id == "CC-BY-4.0"
    assert license_.title is None
    assert license_.url is None


def test_resource_normalize_format() -> None:
    assert Resource.normalize_format("text/csv") == "CSV"
    assert Resource.normalize_format("application/json") == "JSON"
    assert Resource.normalize_format("csv") == "CSV"
    assert Resource.normalize_format(None) is None


def test_resource_defaults() -> None:
    resource = Resource(id="abc-123")
    assert resource.id == "abc-123"
    assert resource.url is None
    assert resource.extra == {}


def test_organization_defaults() -> None:
    org = Organization(id="org-1")
    assert org.id == "org-1"
    assert org.extra == {}


def test_dataset_defaults() -> None:
    dataset = Dataset(id="ds-1")
    assert dataset.id == "ds-1"
    assert dataset.resources == []
    assert dataset.tags == []


def test_query_defaults() -> None:
    query = Query()
    assert query.text is None
    assert query.limit == 100
    assert query.offset == 0


def test_search_result_iteration() -> None:
    result = SearchResult(datasets=[Dataset(id="a"), Dataset(id="b")], total=2)
    assert len(result) == 2
    ids = [d.id for d in result]
    assert ids == ["a", "b"]


@pytest.mark.parametrize("scalar", [True, False, 7, 3.14, "text", None])
def test_value_record_round_trips_every_scalar_through_strict_schema_v1(
    scalar: None | bool | int | float | str,
) -> None:
    record = ValueRecord(value=scalar)

    envelope = record.to_dict()

    assert envelope == {"schema_version": 1, "kind": "value_record", "value": scalar}
    assert type(envelope["value"]) is type(scalar)
    decoded = ValueRecord.from_dict(envelope)
    assert decoded == record
    assert type(decoded.value) is type(scalar)


def test_value_record_rejects_non_scalar_values_with_the_domain_contract_error() -> None:
    with pytest.raises(DataSluiceError):
        ValueRecord(value=[1, 2])  # ty: ignore[invalid-argument-type]
    with pytest.raises(DataSluiceError):
        ValueRecord(value={"nested": "object"})  # ty: ignore[invalid-argument-type]


def test_value_record_rejects_non_finite_floats_and_foreign_envelope_keys() -> None:
    with pytest.raises(DataSluiceError):
        ValueRecord(value=float("inf"))
    with pytest.raises(DataSluiceError):
        ValueRecord(value=float("nan"))
    with pytest.raises(DataSluiceError):
        ValueRecord.from_dict({"schema_version": 1, "kind": "value_record", "value": 1, "extra": "key"})
    with pytest.raises(DataSluiceError):
        ValueRecord.from_dict({"schema_version": 2, "kind": "value_record", "value": 1})


def test_mapping_record_round_trips_an_arbitrary_json_object_losslessly() -> None:
    payload = {"status": "ok", "counts": {"datasets": 3, "tags": [1, 2]}, "site_title": "Demo"}
    record = MappingRecord(payload=payload)

    assert MappingRecord.from_dict(record.to_dict()) == record
    assert record.to_dict() == {"schema_version": 1, "kind": "mapping_record", "payload": payload}


def test_mapping_record_freezes_its_interior_and_rejects_non_object_payloads() -> None:
    from types import MappingProxyType

    record = MappingRecord(payload={"nested": {"key": "value"}})
    assert isinstance(record.payload, MappingProxyType)
    with pytest.raises(TypeError):
        record.payload["nested"]["key"] = "mutated"  # ty: ignore[invalid-assignment]
    with pytest.raises(DataSluiceError):
        MappingRecord(payload=[1, 2, 3])  # ty: ignore[invalid-argument-type]


def test_result_envelope_accepts_value_and_mapping_records_as_first_class_items() -> None:
    envelope = ResultEnvelope(items=(ValueRecord(value=42), MappingRecord(payload={"ok": True})))

    assert [item.kind for item in envelope.items] == ["value_record", "mapping_record"]
    restored = ResultEnvelope.from_dict(envelope.to_dict(), item_decoder=_decode_item)
    assert restored.items == envelope.items


def _decode_item(item: object) -> object:
    kind = item.get("kind") if isinstance(item, dict) else None
    if kind == "value_record":
        return ValueRecord.from_dict(item)
    if kind == "mapping_record":
        return MappingRecord.from_dict(item)
    raise AssertionError(f"unexpected item kind {kind!r}")


def _profile_with_one_operation() -> DeclaredCapabilityProfile:
    operation_id = OperationId(platform="ckan", service="action-api-v3", method="status")
    operation = OperationSpec(
        id=operation_id,
        tier=OperationTier.NORMALIZED,
        request_type="CatalogRequest",
        response_type="CatalogResponse",
        auth_class=AuthClass.PUBLIC,
        mutation_class=MutationClass.READ,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.SINGLE_RESOURCE,
        capability_class=CapabilityClass.CORE,
    )
    return DeclaredCapabilityProfile(
        profile_version="2.11.5",
        schema_version="1.0",
        platform_api_version="Action API v3",
        official_source_uri="https://docs.ckan.org/en/2.11/api/",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint="fingerprint",
        operations={operation_id: operation},
    )


def test_probe_evidence_constructs_with_historical_five_arguments_and_defaults_provenance() -> None:
    evidence = ProbeEvidence(
        operation_id=OperationId(platform="ckan", service="action-api-v3", method="status"),
        deployment_url="https://demo.ckan.org/api/3/action/status_show",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=ProbeResponseClass.SUCCESS,
    )

    assert evidence.provenance is EvidenceProvenance.VERIFIED_LINE


def test_probe_evidence_rejects_unknown_provenance_values() -> None:
    with pytest.raises(ValueError):
        ProbeEvidence(
            operation_id=OperationId(platform="ckan", service="action-api-v3", method="status"),
            deployment_url="https://demo.ckan.org/api/3/action/status_show",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=ProbeResponseClass.SUCCESS,
            provenance="bogus",  # ty: ignore[invalid-argument-type]
        )


def test_effective_capability_profile_exposes_evidence_provenance_end_to_end() -> None:
    operation_id = OperationId(platform="ckan", service="action-api-v3", method="status")
    evidence = ProbeEvidence(
        operation_id=operation_id,
        deployment_url="https://foreign.example/api/3/action/status_show",
        credential_classification=CredentialClassification.ANONYMOUS,
        role_classification=RoleClassification.ANONYMOUS,
        observed_response_class=ProbeResponseClass.SUCCESS,
        provenance=EvidenceProvenance.UNVERIFIED,
    )

    effective = EffectiveCapabilityProfile.derive(_profile_with_one_operation(), [evidence])
    capability = effective.for_operation(operation_id)

    assert capability.evidence is not None
    assert capability.evidence.provenance is EvidenceProvenance.UNVERIFIED
