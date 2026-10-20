from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.domain.catalog import (
    CatalogId,
    CatalogPlatform,
    DatasetRecord,
    NativeRecord,
    OrganizationRecord,
    PageInfo,
    PlatformMetadata,
    ResourceKind,
    ResourceRecord,
    ResultEnvelope,
    UserRecord,
    WarningRecord,
)
from datasluice.domain.catalog.models import MappingRecord, ValueRecord
from datasluice.domain.catalog.redaction import MAX_METADATA_ENTRIES, MAX_TEXT_LENGTH, REDACTED, TRUNCATED
from datasluice.exceptions import DataSluiceError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


def _native(payload: Mapping[str, object]) -> NativeRecord:
    return NativeRecord(
        platform=CatalogPlatform.UDATA,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(CatalogPlatform.UDATA, ResourceKind.DATASET, "private-record-identifier"),
        payload=payload,
        extensions={"udata.dataset": payload},
    )


@pytest.mark.parametrize("factory", [MappingRecord, _native])
def test_serialization_bounds_fields_sequences_keys_and_total_output_without_changing_payload(
    factory: Callable[[Mapping[str, object]], NativeRecord | MappingRecord],
) -> None:
    payload = {
        "rows": [{f"field_{index}": ["x" * 1024] * 64 for index in range(64)}] * 64,
        "k" * 4096: "retained",
        **{f"field_{index}": index for index in range(64)},
    }
    record = factory(payload)

    serialized = record.to_dict()
    projected = cast("dict[str, object]", serialized["payload"])
    assert len(projected) <= MAX_METADATA_ENTRIES
    assert TRUNCATED in json.dumps(projected)
    assert len(json.dumps(serialized).encode()) < 200_000
    assert all(len(key) <= MAX_TEXT_LENGTH for key in projected)
    assert len(cast("tuple[object, ...]", record.payload["rows"])) == 64
    assert "k" * 4096 in record.payload


@pytest.mark.parametrize("factory", [MappingRecord, _native])
def test_serialization_bounds_fields_and_keys_without_deep_payloads(
    factory: Callable[[Mapping[str, object]], NativeRecord | MappingRecord],
) -> None:
    """A flat payload reaches the field-count and key-length bounds the deep-rows payload cannot."""
    payload = {"k" * 4096: "retained", **{f"field_{index}": index for index in range(64)}}
    record = factory(payload)

    projected = cast("dict[str, object]", record.to_dict()["payload"])
    assert len(projected) == MAX_METADATA_ENTRIES
    assert all(len(key) <= MAX_TEXT_LENGTH for key in projected)
    assert record.payload["k" * 4096] == "retained"


@pytest.mark.parametrize("factory", [MappingRecord, _native])
def test_serialization_excludes_private_fields_and_sanitizes_field_names(
    factory: Callable[[Mapping[str, object]], NativeRecord | MappingRecord],
) -> None:
    payload = {
        "email": "private@example.test",
        "phone": "+33123456789",
        "private_origin": "http://internal.example.test/",
        "contact": {"name": "Private Person"},
        "nested": {"internal_notes": "private-note", "accessToken": "private-token"},
        "https://example.test/?token=key-secret": "public",
        "title": "Public title",
    }
    record = factory(payload)

    serialized = json.dumps(record.to_dict())
    for secret in (
        "private@example.test",
        "+33123456789",
        "internal.example",
        "Private Person",
        "private-note",
        "key-secret",
    ):
        assert secret not in serialized
    assert REDACTED in serialized
    assert record.payload["email"] == "private@example.test"


@pytest.mark.parametrize("factory", [MappingRecord, _native])
def test_evidence_contains_structure_but_no_field_names_identifiers_or_values(
    factory: Callable[[Mapping[str, object]], NativeRecord | MappingRecord],
) -> None:
    record = factory({"private-field-name": "Private Person", "nested": [{"email": "private@example.test"}]})

    evidence = record.to_evidence_dict()
    serialized = json.dumps(evidence)
    for private in (
        "private-field-name",
        "Private Person",
        "nested",
        "email",
        "private@example.test",
        "private-record-identifier",
    ):
        assert private not in serialized
    assert "object" in serialized
    assert "string" in serialized
    equivalent = factory({"other": "Other Person", "list": [{"other": "another@example.test"}]})
    assert evidence == equivalent.to_evidence_dict()


def test_evidence_bounds_deep_wide_records_and_result_collections() -> None:
    payload: dict[str, object] = {"values": ["secret"] * 1000}
    for _ in range(20):
        payload = {"nested": payload}
    assert TRUNCATED in json.dumps(MappingRecord(payload).to_evidence_dict())
    record = MappingRecord({"values": ["secret"] * 1000})
    envelope = ResultEnvelope(items=(record,) * 1000)

    evidence = envelope.to_evidence_dict()
    assert len(cast("list[object]", evidence["items"])) <= MAX_METADATA_ENTRIES
    assert evidence["item_count"] == 1000
    assert len(json.dumps(evidence)) < 200_000
    assert "secret" not in json.dumps(evidence)


def test_scalar_evidence_excludes_arbitrary_private_text() -> None:
    record = ValueRecord("private@example.test")

    assert record.to_evidence_dict() == {"schema_version": 1, "kind": "value_record_evidence", "value": "string"}


def test_serialization_bounds_large_integer_values() -> None:
    record = MappingRecord({"value": 10**10000})

    assert json.dumps(record.to_dict())
    assert record.payload["value"] == 10**10000


def test_native_serialization_bounds_identity_and_individual_keys_without_mutation() -> None:
    identifier = "https://example.test/?token=private-token" + "x" * 4096
    key = "k" * 4096
    record = NativeRecord(
        platform=CatalogPlatform.UDATA,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(CatalogPlatform.UDATA, ResourceKind.DATASET, identifier),
        payload={key: "value"},
    )

    serialized = record.to_dict()
    assert "private-token" not in json.dumps(serialized)
    assert len(cast("dict[str, str]", serialized["id"])["value"]) <= MAX_TEXT_LENGTH
    assert list(cast("dict[str, object]", serialized["payload"])) == [key[:MAX_TEXT_LENGTH]]
    assert record.id.value == identifier
    assert key in record.payload


def test_normalized_record_evidence_excludes_names_urls_extensions_and_envelope_metadata() -> None:
    dataset_id = CatalogId(CatalogPlatform.UDATA, ResourceKind.DATASET, "private-dataset")
    records = (
        DatasetRecord(dataset_id, "private-name", "private-description", {"example.test": {"private-key": "private"}}),
        ResourceRecord(
            CatalogId(CatalogPlatform.UDATA, ResourceKind.RESOURCE, "private-resource"),
            dataset_id,
            "private-name",
            "http://private-origin/",
        ),
        OrganizationRecord(CatalogId(CatalogPlatform.UDATA, ResourceKind.ORGANIZATION, "private-org"), "private-name"),
        UserRecord(CatalogId(CatalogPlatform.UDATA, ResourceKind.USER, "private-user"), "private-user", "private-name"),
    )
    envelope = ResultEnvelope(
        items=records,
        page=PageInfo(next_cursor="http://private-origin/?token=private-token"),
        warnings=(WarningRecord("private-code", "private-message"),),
        platform=PlatformMetadata(CatalogPlatform.UDATA, deployment="http://private-origin/"),
    )

    assert "private" not in json.dumps(envelope.to_evidence_dict())


def test_evidence_rejects_items_without_an_evidence_serializer() -> None:
    class UnsupportedRecord:
        def to_dict(self) -> dict[str, object]:
            return {"content": "private-content"}

    envelope = ResultEnvelope(items=(UnsupportedRecord(),))

    with pytest.raises(DataSluiceError, match="result_envelope.evidence"):
        envelope.to_evidence_dict()


def test_record_representations_exclude_runtime_payload_and_extension_values() -> None:
    for record in (MappingRecord({"text": "private-content"}), _native({"text": "private-content"})):
        assert "private-content" not in repr(record)
