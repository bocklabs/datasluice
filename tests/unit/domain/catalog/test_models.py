"""Tests for immutable catalog domain values."""

from __future__ import annotations

import dataclasses
import json
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

import pytest

from datasluice.domain.catalog import (
    CatalogId,
    CatalogPlatform,
    DatasetRecord,
    NativeRecord,
    PageInfo,
    PlatformMetadata,
    ResourceKind,
    ResultEnvelope,
    WarningRecord,
)
from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.redaction import MAX_TEXT_LENGTH, REDACTED
from datasluice.exceptions import DataSluiceError

if TYPE_CHECKING:
    from collections.abc import Mapping

_CREDENTIAL_PAYLOAD: dict[str, object] = {
    "id": "dataset-1",
    "title": "Weather",
    "token": "live-token-plaintext",
    "api_key": "live-api-key-plaintext",
    "password": "live-password-plaintext",
    "body": '{"raw": "response body text"}',
    "headers": {"Authorization": "Bearer live-bearer-plaintext"},
    "nested": {"client_secret": "live-client-secret", "safe": "kept"},
    "rows": [{"credential": "live-row-credential", "name": "kept"}],
}


def _assign(target: object, field: str, value: object) -> None:
    """Assign a read-only field so the frozen-dataclass rejection is what the assertion observes."""
    setattr(target, field, value)


def _dataset() -> DatasetRecord:
    return DatasetRecord(
        id=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather"),
        name="Weather",
        description="Public weather records",
        extensions={"example.org": {"publisher": {"contacts": ["ops@example.org"]}}},
    )


def test_catalog_id_requires_typed_platform_and_resource_kind_and_round_trips() -> None:
    identifier = CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather")

    assert identifier.to_dict() == {
        "schema_version": 1,
        "kind": "catalog_id",
        "platform": "ckan",
        "resource_kind": "dataset",
        "value": "weather",
    }
    assert CatalogId.from_dict(identifier.to_dict()) == identifier

    # Runtime validation is what these assert, so the invalid values pass through untyped kwargs.
    invalid: tuple[dict[str, Any], ...] = (
        {"platform": "ckan", "resource_kind": ResourceKind.DATASET, "value": "weather"},
        {"platform": CatalogPlatform.CKAN, "resource_kind": "dataset", "value": "weather"},
    )
    for kwargs in invalid:
        with pytest.raises(DataSluiceError):
            CatalogId(**kwargs)
    with pytest.raises(DataSluiceError):
        NativeRecord(
            platform=CatalogPlatform.UDATA,
            resource_kind=ResourceKind.DATASET,
            id=identifier,
            payload={"id": "weather"},
        )


def test_native_and_normalized_records_are_recursively_immutable_and_thaw_to_fresh_values() -> None:
    native = NativeRecord(
        platform=CatalogPlatform.CKAN,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather"),
        payload={"tags": [{"name": "climate"}], "owner": {"name": "City"}},
        extensions={"example.org": {"state": ["published"]}},
    )
    dataset = _dataset()

    assert isinstance(native.payload, MappingProxyType)
    assert isinstance(native.payload["tags"], tuple)
    assert isinstance(dataset.extensions, MappingProxyType)
    with pytest.raises(TypeError):
        cast("dict[str, object]", native.payload)["state"] = "active"
    with pytest.raises(dataclasses.FrozenInstanceError):
        _assign(dataset, "name", "Other")

    serialized = native.to_dict()
    serialized_payload = cast("dict[str, list[dict[str, str]]]", serialized["payload"])
    serialized_payload["tags"][0]["name"] = "changed"
    native_payload = cast("Mapping[str, tuple[Mapping[str, str], ...]]", native.payload)
    assert native_payload["tags"][0]["name"] == "climate"
    assert NativeRecord.from_dict(native.to_dict()) == native
    assert DatasetRecord.from_dict(dataset.to_dict()) == dataset


def test_result_envelopes_preserve_items_page_warnings_and_platform_metadata() -> None:
    dataset = _dataset()
    envelope = ResultEnvelope(
        items=(dataset,),
        page=PageInfo(cursor="one", next_cursor="two", total_items=3),
        warnings=(WarningRecord(code="partial", message="One result omitted"),),
        platform=PlatformMetadata(platform=CatalogPlatform.CKAN, api_version="3"),
    )

    serialized = envelope.to_dict()
    assert serialized == {
        "schema_version": 1,
        "kind": "result_envelope",
        "items": [dataset.to_dict()],
        "page": {"schema_version": 1, "kind": "page_info", "cursor": "one", "next_cursor": "two", "total_items": 3},
        "warnings": [{"schema_version": 1, "kind": "warning", "code": "partial", "message": "One result omitted"}],
        "platform": {
            "schema_version": 1,
            "kind": "platform_metadata",
            "platform": "ckan",
            "api_version": "3",
            "deployment": None,
            "extensions": {},
        },
    }
    assert ResultEnvelope.from_dict(serialized, item_decoder=DatasetRecord.from_dict) == envelope


@pytest.mark.parametrize(
    "value",
    [
        {"schema_version": 2, "kind": "catalog_id", "platform": "ckan", "resource_kind": "dataset", "value": "x"},
        {"schema_version": 1, "kind": "wrong", "platform": "ckan", "resource_kind": "dataset", "value": "x"},
        {"schema_version": 1, "kind": "catalog_id", "platform": "not valid", "resource_kind": "dataset", "value": "x"},
        {"schema_version": 1, "kind": "catalog_id", "platform": "ckan", "resource_kind": "not valid", "value": "x"},
    ],
)
def test_catalog_id_rejects_malformed_versions_kinds_and_values(value: object) -> None:
    with pytest.raises(DataSluiceError):
        CatalogId.from_dict(value)


@pytest.mark.parametrize(
    "value",
    [
        {"bad_namespace": {"field": "value"}},
        {"example.org": {1: "value"}},
        {"example.org": {"value": float("nan")}},
        {"example.org": {"value": float("inf")}},
    ],
)
def test_records_reject_invalid_extensions_and_json_values(value: object) -> None:
    record_id = CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather")
    # Runtime validation is the assertion; the rejected value reaches the constructor untyped.
    with pytest.raises(DataSluiceError):
        DatasetRecord(id=record_id, name="Weather", extensions=cast("Mapping[str, object]", value))


def _credential_record() -> NativeRecord:
    return NativeRecord(
        platform=CatalogPlatform.CKAN,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "dataset-1"),
        payload=_CREDENTIAL_PAYLOAD,
        extensions={"portal.test": dict(_CREDENTIAL_PAYLOAD)},
    )


def test_native_record_serialization_redacts_credential_payload_and_extension_keys() -> None:
    serialized = _credential_record().to_dict()

    payload = cast("dict[str, object]", serialized["payload"])
    extensions = cast("dict[str, object]", serialized["extensions"])
    for key in ("token", "api_key", "password", "body"):
        assert payload[key] == REDACTED, f"payload.{key} must serialize as the shared redacted marker"
    for key in ("token", "api_key", "password", "body"):
        assert cast("dict[str, object]", extensions["portal.test"])[key] == REDACTED


def test_native_record_serialization_scrubs_credential_shaped_content_at_every_depth() -> None:
    serialized = _credential_record().to_dict()

    payload = cast("dict[str, object]", serialized["payload"])
    assert payload["nested"] == {"client_secret": REDACTED, "safe": "kept"}
    assert payload["rows"] == [{"credential": REDACTED, "name": "kept"}]
    assert "live-bearer-plaintext" not in str(serialized)
    for plaintext in (
        "live-token-plaintext",
        "live-api-key-plaintext",
        "live-password-plaintext",
        "live-client-secret",
        "live-row-credential",
    ):
        assert plaintext not in str(serialized), f"{plaintext} must never reach a retained artifact"


def test_native_record_serialization_keeps_every_non_credential_field_without_an_entry_cap() -> None:
    payload = {f"field_{index}": index for index in range(64)}
    record = NativeRecord(
        platform=CatalogPlatform.CKAN,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "dataset-1"),
        payload=payload,
    )

    serialized = cast("dict[str, object]", record.to_dict()["payload"])
    assert serialized == payload
    assert len(serialized) == 64


def test_native_record_serialization_redacts_credential_content_smuggled_into_a_benign_value() -> None:
    record = NativeRecord(
        platform=CatalogPlatform.CKAN,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "dataset-1"),
        payload={"description": "see https://portal.test/?token=live-query-plaintext for access"},
    )

    serialized = cast("dict[str, object]", record.to_dict()["payload"])
    assert "live-query-plaintext" not in cast("str", serialized["description"])
    assert REDACTED in cast("str", serialized["description"])


def test_native_record_serialization_bounds_retained_text_length() -> None:
    record = NativeRecord(
        platform=CatalogPlatform.CKAN,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "dataset-1"),
        payload={"description": "x" * (MAX_TEXT_LENGTH * 4)},
    )

    serialized = cast("dict[str, object]", record.to_dict()["payload"])
    assert len(cast("str", serialized["description"])) == MAX_TEXT_LENGTH


def test_mapping_record_serialization_redacts_credential_keys_and_keeps_the_rest() -> None:
    record = MappingRecord(payload=_CREDENTIAL_PAYLOAD)

    payload = cast("dict[str, object]", record.to_dict()["payload"])
    for key in ("token", "api_key", "password", "body"):
        assert payload[key] == REDACTED
    assert payload["id"] == "dataset-1"
    assert payload["title"] == "Weather"
    assert payload["nested"] == {"client_secret": REDACTED, "safe": "kept"}
    assert "live-token-plaintext" not in str(record.to_dict())


def test_record_payload_redaction_is_idempotent() -> None:
    record = _credential_record()

    once = record.to_dict()
    restored = NativeRecord.from_dict(once)
    assert restored.to_dict() == once
    assert restored.to_dict() == NativeRecord.from_dict(restored.to_dict()).to_dict()


def test_result_envelope_serialization_redacts_record_payloads_recursively() -> None:
    envelope = ResultEnvelope(items=(_credential_record(),))

    serialized = json.dumps(envelope.to_dict())
    for plaintext in ("live-token-plaintext", "live-client-secret", "live-bearer-plaintext"):
        assert plaintext not in serialized
    assert REDACTED in serialized
