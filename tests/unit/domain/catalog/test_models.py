"""Tests for immutable catalog domain values."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from types import MappingProxyType
from typing import cast

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
from datasluice.exceptions import DataSluiceError


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

    with pytest.raises(DataSluiceError):
        CatalogId("ckan", ResourceKind.DATASET, "weather")  # ty: ignore[invalid-argument-type]: runtime validation
    with pytest.raises(DataSluiceError):
        CatalogId(CatalogPlatform.CKAN, "dataset", "weather")  # ty: ignore[invalid-argument-type]: runtime validation
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
        cast(dict[str, object], native.payload)["state"] = "active"
    with pytest.raises(dataclasses.FrozenInstanceError):
        dataset.name = "Other"  # ty: ignore[invalid-assignment]: frozen dataclass assertion

    serialized = native.to_dict()
    serialized_payload = cast(dict[str, list[dict[str, str]]], serialized["payload"])
    serialized_payload["tags"][0]["name"] = "changed"
    native_payload = cast(Mapping[str, tuple[Mapping[str, str], ...]], native.payload)
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
    with pytest.raises(DataSluiceError):
        DatasetRecord(
            id=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather"),
            name="Weather",
            extensions=value,  # ty: ignore[invalid-argument-type]: runtime validation assertion
        )
