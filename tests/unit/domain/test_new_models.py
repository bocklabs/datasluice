"""Unit tests for the six new frozen domain models added."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import cast

import pytest

from datasluice.application import DirectResourceLocator
from datasluice.domain import (
    Artifact,
    ArtifactProvenance,
    DetectionResult,
    Digest,
    HttpDownload,
    LocalFile,
    ObjectStorage,
    QueryAccess,
    Resource,
    ResourceAccess,
    Schema,
    StreamAccess,
    SyncState,
)
from datasluice.domain.catalog import CatalogId, CatalogPlatform, DatasetRecord, ResourceKind
from datasluice.domain.detection import DetectionEvidence


def test_schema_defaults() -> None:
    schema = Schema(name="resources")
    assert schema.name == "resources"
    assert list(schema.columns) == []
    assert schema.version == "1"
    assert schema.extra == {}


def test_schema_is_frozen() -> None:
    schema = Schema(name="s")
    with pytest.raises(dataclasses.FrozenInstanceError):
        schema.name = "other"  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime


def test_resource_access_subclass_kind_defaults() -> None:
    assert HttpDownload(url="https://x").kind == "http_download"
    assert ObjectStorage(uri="s3://b/k").kind == "object_storage"
    assert QueryAccess(endpoint="e").kind == "query"
    assert StreamAccess(url="u").kind == "stream"
    assert LocalFile(path="/p").kind == "local_file"


def test_http_download_defaults() -> None:
    http = HttpDownload(url="https://example.com/file.csv")
    assert http.url == "https://example.com/file.csv"
    assert http.method == "GET"
    assert http.kind == "http_download"
    assert http.extra == {}


def test_resource_access_is_frozen() -> None:
    http = HttpDownload(url="https://x")
    with pytest.raises(dataclasses.FrozenInstanceError):
        http.url = "https://y"  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime
    with pytest.raises(dataclasses.FrozenInstanceError):
        http.kind = "other"  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime


def test_resource_access_is_base_of_subclasses() -> None:
    assert issubclass(HttpDownload, ResourceAccess)
    assert issubclass(ObjectStorage, ResourceAccess)
    assert issubclass(QueryAccess, ResourceAccess)
    assert issubclass(StreamAccess, ResourceAccess)
    assert issubclass(LocalFile, ResourceAccess)


def test_detection_result_defaults() -> None:
    result = DetectionResult(portal_type="ckan")
    assert result.portal_type == "ckan"
    assert result.confidence == 0.0
    assert list(result.evidence) == []
    assert result.extra == {}


def test_detection_result_accepts_none_portal_type() -> None:
    result = DetectionResult(portal_type=None, confidence=0.9, evidence=[DetectionEvidence(check="api", matched=True)])
    assert result.portal_type is None
    assert result.confidence == 0.9
    assert result.evidence[0].check == "api"


def test_detection_result_is_frozen() -> None:
    result = DetectionResult(portal_type="ckan")
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.confidence = 1.0  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime


def test_detection_evidence_is_frozen() -> None:
    evidence = DetectionEvidence(check="api", matched=True)
    with pytest.raises(dataclasses.FrozenInstanceError):
        evidence.matched = False  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime


def test_detection_result_confidence_range_enforced() -> None:
    with pytest.raises(ValueError):
        DetectionResult(portal_type="ckan", confidence=-0.1)
    with pytest.raises(ValueError):
        DetectionResult(portal_type="ckan", confidence=1.5)
    DetectionResult(portal_type="ckan", confidence=0.0)
    DetectionResult(portal_type="ckan", confidence=1.0)


def test_detection_result_evidence_is_immutable() -> None:
    result = DetectionResult(portal_type="ckan", evidence=[DetectionEvidence(check="api", matched=True)])
    with pytest.raises((AttributeError, TypeError)):
        result.evidence.append(DetectionEvidence(check="other", matched=False))  # type: ignore


def test_detection_result_extra_is_immutable() -> None:
    result = DetectionResult(portal_type="ckan", extra={"k": "v"})
    with pytest.raises((TypeError, AttributeError)):
        result.extra["k"] = "other"  # type: ignore


def test_schema_extra_and_columns_are_immutable() -> None:
    schema = Schema(name="s", columns=[{"id": "c"}], extra={"k": "v"})
    with pytest.raises((AttributeError, TypeError)):
        schema.columns.append({"id": "other"})  # type: ignore
    with pytest.raises((TypeError, AttributeError)):
        schema.extra["k"] = "other"  # type: ignore


def test_catalog_models_are_immutable_and_versioned() -> None:
    identifier = CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather")
    record = DatasetRecord(id=identifier, name="Weather", extensions={"example.org": {"status": "published"}})

    assert CatalogId.from_dict(identifier.to_dict()) == identifier
    assert DatasetRecord.from_dict(record.to_dict()) == record
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.name = "Other"  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime
    with pytest.raises(TypeError):
        cast(dict[str, object], record.extensions)["example.org"] = {}


def test_resource_access_kind_not_overridable() -> None:
    http = HttpDownload(url="https://x")
    assert http.kind == "http_download"
    with pytest.raises(TypeError):
        HttpDownload(url="https://x", kind="object_storage")  # type: ignore


def test_resource_is_kw_only() -> None:
    with pytest.raises(TypeError):
        Resource("id")  # type: ignore


def test_artifact_defaults() -> None:
    artifact = _artifact()
    assert artifact.uri == "file:///tmp/out.parquet"
    assert artifact.media_type == "application/x-parquet"
    assert artifact.size == 0
    assert artifact.metadata == {}
    assert artifact.extensions == {}


def test_artifact_is_frozen() -> None:
    artifact = _artifact()
    with pytest.raises(dataclasses.FrozenInstanceError):
        artifact.size = 1  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime


def _artifact() -> Artifact:
    digest = Digest(algorithm="sha256", value="0" * 64)
    provenance = ArtifactProvenance(
        source_locator=DirectResourceLocator(uri="file:///tmp/source.csv"),
        resource_identity="1" * 64,
        created_at=datetime(2026, 8, 1, tzinfo=UTC),
        materialization_mode="parquet",
    )
    return Artifact(
        uri="file:///tmp/out.parquet",
        media_type="application/x-parquet",
        size=0,
        content_digest=digest,
        blob_digest=digest,
        provenance=provenance,
    )


def test_sync_state_defaults() -> None:
    state = SyncState()
    assert state.cursor == {}
    assert state.partitions == {}
    assert state.last_synced_at is None
    assert state.extra == {}


def test_sync_state_is_frozen() -> None:
    state = SyncState(cursor={"res-1": "2026-01-01T00:00:00Z"})
    with pytest.raises(dataclasses.FrozenInstanceError):
        state.cursor = {"res-1": "2026-02-01T00:00:00Z"}  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime
