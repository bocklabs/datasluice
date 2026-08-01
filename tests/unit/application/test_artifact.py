"""Artifact schema-v1 producer and codec contracts."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from datasluice.domain import Artifact, LocalFile, Resource
from datasluice.exceptions import DataSluiceError
from datasluice.sync._hashing import logical_sha256
from datasluice.sync.materialize import materialize


class _RawReader:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read_bytes(self, resource: Any) -> bytes:
        return self.payload


class _ParquetReader:
    def __init__(self) -> None:
        import pyarrow as pa

        self.table = pa.table({"id": [1, 2], "name": ["Ada", "Lin"]})

    def open(self, resource: Any) -> Any:
        from datasluice.data.batch_stream import BatchStream

        return BatchStream(iter(self.table.to_batches()), self.table.schema)


def _fixture() -> dict[str, object]:
    return json.loads(Path("tests/fixtures/contracts/artifact-v1.json").read_text())


def test_raw_materialize_returns_one_canonical_artifact(tmp_path: Path) -> None:
    payload = b"artifact producer raw payload"
    resource = Resource(
        id="raw-artifact",
        name="raw-artifact",
        media_type="application/octet-stream",
        access=LocalFile(path=str(tmp_path / "source.bin")),
    )

    created_at = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    artifact = materialize(
        resource,
        reader=_RawReader(payload),
        destination_uri=f"file://{tmp_path}/destination",
        mode="raw",
        created_at=created_at,
    )

    assert isinstance(artifact, Artifact)
    assert artifact.content_digest == artifact.blob_digest
    assert artifact.provenance.created_at == created_at
    payload_dict = artifact.to_dict()
    provenance = cast(dict[str, object], payload_dict["provenance"])
    assert provenance["materialization_mode"] == "raw"


def test_parquet_materialize_reports_independent_logical_and_blob_digests(tmp_path: Path) -> None:
    reader = _ParquetReader()
    resource = Resource(
        id="parquet-artifact",
        name="parquet-artifact",
        format="CSV",
        media_type="text/csv",
        access=LocalFile(path=str(tmp_path / "source.csv")),
    )

    artifact = materialize(
        resource,
        reader=reader,
        destination_uri=f"file://{tmp_path}/destination",
        created_at=datetime(2026, 8, 1, 12, 1, tzinfo=UTC),
    )

    assert isinstance(artifact, Artifact)
    assert artifact.content_digest.value == logical_sha256(reader.table)
    assert (
        artifact.blob_digest.value
        == hashlib.sha256(Path(artifact.uri.removeprefix("file://")).read_bytes()).hexdigest()
    )
    assert artifact.content_digest != artifact.blob_digest


def test_artifact_codec_round_trips_the_locked_golden_payload() -> None:
    payload = _fixture()

    assert Artifact.from_dict(payload).to_dict() == payload


def test_artifact_codec_deeply_freezes_and_freshly_thaws_values() -> None:
    artifact = Artifact.from_dict(_fixture())
    first = artifact.to_dict()
    second = artifact.to_dict()
    first_metadata = cast(dict[str, object], first["metadata"])
    first_extensions = cast(dict[str, object], first["extensions"])
    second_metadata = cast(dict[str, object], second["metadata"])
    second_extensions = cast(dict[str, object], second["extensions"])

    assert isinstance(first_metadata, dict)
    assert isinstance(first_extensions, dict)
    assert isinstance(second_metadata, dict)
    assert isinstance(second_extensions, dict)
    first_metadata["record_count"] = 99
    first_extension = cast(dict[str, object], first_extensions["org.datasluice.contract"])
    second_extension = cast(dict[str, object], second_extensions["org.datasluice.contract"])
    first_extension["fixture"] = "changed"

    assert second_metadata["record_count"] == 2
    assert second_extension["fixture"] == "artifact"
    with pytest.raises(TypeError):
        cast(dict[str, object], artifact.metadata)["record_count"] = 99


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("schema_version", 2),
        ("kind", "other"),
        ("unexpected", "value"),
        ("size", True),
        ("content_digest", {"algorithm": "sha256", "value": "upper"}),
        ("provenance.created_at", "2026-08-01T00:00:00"),
        ("uri", "https://user:password@example.test/output.parquet"),
    ],
)
def test_artifact_codec_rejects_invalid_core_values_without_echoing_them(path: str, value: object) -> None:
    payload = _fixture()
    if path == "provenance.created_at":
        provenance = cast(dict[str, object], payload["provenance"])
        provenance["created_at"] = value
    elif path == "unexpected":
        payload[path] = value
    else:
        payload[path] = value

    with pytest.raises(DataSluiceError) as exc_info:
        Artifact.from_dict(payload)

    assert str(value) not in str(exc_info.value)
