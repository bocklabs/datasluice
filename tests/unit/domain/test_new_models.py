"""Unit tests for the six new frozen domain models added in Phase 2."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime

import pytest

from datasluice.application import DirectResourceLocator
from datasluice.domain import (
    Artifact,
    ArtifactProvenance,
    CatalogCapabilities,
    DetectionResult,
    Digest,
    HttpDownload,
    LocalFile,
    ObjectStorage,
    QueryAccess,
    ResourceAccess,
    Schema,
    StreamAccess,
    SyncState,
)
from datasluice.domain.detection import DetectionEvidence


def test_schema_defaults() -> None:
    schema = Schema(name="resources")
    assert schema.name == "resources"
    assert schema.columns == []
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
    assert result.evidence == []
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


def test_catalog_capabilities_defaults() -> None:
    caps = CatalogCapabilities()
    assert caps.supports_search is True
    assert caps.supports_organizations is False
    assert caps.supports_faceted_search is False
    assert caps.supported_query_fields == frozenset()
    assert caps.unsupported_query_fields == frozenset()
    assert caps.notes == {}


def test_catalog_capabilities_is_frozen() -> None:
    caps = CatalogCapabilities()
    with pytest.raises(dataclasses.FrozenInstanceError):
        caps.supports_search = False  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime
