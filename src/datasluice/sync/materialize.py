"""Idempotent resource materialization to fsspec destinations."""

from __future__ import annotations

import hashlib
import os
import random
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from datasluice._uri import sanitize_uri
from datasluice.exceptions import DataSluiceError, DownloadError
from datasluice.logging import get_logger
from datasluice.sync._identity import canonical_identity

if TYPE_CHECKING:
    from datasluice.domain import Artifact

LegacyArtifactRecord = tuple[str, str, int, str]

_IDEMPOTENT_MATERIALIZE_READY = True
_ARTIFACT_HEALTH_READY = True

logger = get_logger("sync.materialize")


def materialize_artifact(
    resource: Any,
    *,
    destination_uri: str,
    source_locator: Any,
    reader: Any | None = None,
    stream: Any | None = None,
    mode: str = "parquet",
    transforms: tuple[str, ...] = (),
) -> Artifact:
    """Materialize one public resource and return a strict Artifact envelope."""
    if (reader is None) == (stream is None):
        raise DataSluiceError("Artifact materialization requires exactly one reader or stream")
    if reader is not None:
        return materialize(
            resource,
            reader=reader,
            destination_uri=destination_uri,
            mode=mode,
            source_locator=source_locator,
            transforms=transforms,
        )
    return _materialize_stream(
        resource,
        stream=stream,
        destination_uri=destination_uri,
        mode=mode,
        source_locator=source_locator,
        transforms=transforms,
    )


def materialize(
    resource: Any,
    *,
    reader: Any,
    destination_uri: str,
    mode: str = "parquet",
    stored_checksum: str | None = None,
    stored_artifact: Artifact | None = None,
    source_locator: Any | None = None,
    created_at: datetime | None = None,
    transforms: tuple[str, ...] = (),
) -> Artifact:
    """Materialize a resource and return its canonical Artifact."""
    from datasluice.io.filesystem import open_filesystem

    base_uri = destination_uri.rstrip("/")
    fs = open_filesystem(base_uri)
    fs.makedirs(base_uri, exist_ok=True)
    identity = canonical_identity(resource)

    if mode == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        from datasluice.integrations.arrow import to_arrow
        from datasluice.sync._hashing import logical_sha256

        with reader.open(resource) as stream:
            table = to_arrow(stream)
        content_digest = logical_sha256(table)
        final_uri = f"{base_uri}/{identity}.parquet"
        media_type = "application/x-parquet"
        existing = _existing_record(fs, final_uri, media_type, content_digest, stored_checksum)
        if existing is not None:
            uri, existing_media_type, size, existing_content_digest = existing
            blob_digest = _blob_digest_from_fs(fs, uri)
            if _is_current_artifact(
                stored_artifact,
                uri=uri,
                media_type=existing_media_type,
                size=size,
                content_digest=existing_content_digest,
                blob_digest=blob_digest,
                mode=mode,
            ):
                assert stored_artifact is not None
                return stored_artifact
            return _artifact(
                resource,
                uri=uri,
                media_type=existing_media_type,
                size=size,
                content_digest=existing_content_digest,
                blob_digest=blob_digest,
                mode=mode,
                source_locator=source_locator,
                created_at=created_at,
                transforms=transforms,
            )
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        payload = sink.getvalue().to_pybytes()
    elif mode == "raw":
        payload = _read_raw(resource, reader)
        content_digest = hashlib.sha256(payload).hexdigest()
        final_uri = f"{base_uri}/{identity}.bin"
        media_type = _raw_media_type(resource)
        existing = _existing_record(fs, final_uri, media_type, content_digest, stored_checksum)
        if existing is not None:
            uri, existing_media_type, size, existing_content_digest = existing
            if _is_current_artifact(
                stored_artifact,
                uri=uri,
                media_type=existing_media_type,
                size=size,
                content_digest=existing_content_digest,
                blob_digest=existing_content_digest,
                mode=mode,
            ):
                assert stored_artifact is not None
                return stored_artifact
            return _artifact(
                resource,
                uri=uri,
                media_type=existing_media_type,
                size=size,
                content_digest=existing_content_digest,
                blob_digest=existing_content_digest,
                mode=mode,
                source_locator=source_locator,
                created_at=created_at,
                transforms=transforms,
            )
    else:
        raise ValueError(f"Unsupported materialize mode {mode!r}; expected 'parquet' or 'raw'")

    blob_digest = hashlib.sha256(payload).hexdigest()
    _atomic_pipe(fs, final_uri, payload)
    return _artifact(
        resource,
        uri=final_uri,
        media_type=media_type,
        size=len(payload),
        content_digest=content_digest,
        blob_digest=blob_digest,
        mode=mode,
        source_locator=source_locator,
        created_at=created_at,
        transforms=transforms,
    )


def _materialize_stream(
    resource: Any,
    *,
    stream: Any,
    destination_uri: str,
    mode: str,
    source_locator: Any | None,
    created_at: datetime | None = None,
    transforms: tuple[str, ...] = (),
) -> Artifact:
    if mode != "parquet":
        raise DataSluiceError("Transformed Artifact materialization supports only parquet mode")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.integrations.arrow import to_arrow
    from datasluice.io.filesystem import open_filesystem
    from datasluice.sync._hashing import logical_sha256

    base_uri = destination_uri.rstrip("/")
    fs = open_filesystem(base_uri)
    fs.makedirs(base_uri, exist_ok=True)
    table = to_arrow(stream)
    content_digest = logical_sha256(table)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    payload = sink.getvalue().to_pybytes()
    final_uri = f"{base_uri}/{canonical_identity(resource)}.parquet"
    blob_digest = hashlib.sha256(payload).hexdigest()
    _atomic_pipe(fs, final_uri, payload)
    return _artifact(
        resource,
        uri=final_uri,
        media_type="application/x-parquet",
        size=len(payload),
        content_digest=content_digest,
        blob_digest=blob_digest,
        mode=mode,
        source_locator=source_locator,
        created_at=created_at,
        transforms=transforms,
    )


def _blob_digest(uri: str) -> str:
    from datasluice.io.filesystem import open_filesystem

    fs = open_filesystem(uri)
    with fs.open(uri, "rb") as source:
        return hashlib.sha256(source.read()).hexdigest()


def materialize_checkpointed(
    resource: Any,
    *,
    stream: Any,
    destination_uri: str,
    start_batch_index: int,
    on_batch_persisted: Any,
    source_locator: Any | None = None,
    created_at: datetime | None = None,
    transforms: tuple[str, ...] = (),
) -> Artifact:
    """Stage cursor-bearing Parquet batches and atomically publish one final artifact."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.io.filesystem import open_filesystem
    from datasluice.sync._hashing import logical_sha256

    if type(start_batch_index) is not int or start_batch_index < 0:
        raise DataSluiceError("Checkpointed materialization requires a non-negative start batch index")
    base_uri = destination_uri.rstrip("/")
    fs = open_filesystem(base_uri)
    fs.makedirs(base_uri, exist_ok=True)
    identity = canonical_identity(resource)
    partial_uri = f"{base_uri}/.datasluice-partial/{identity}"
    fs.makedirs(partial_uri, exist_ok=True)
    for batch_index in range(start_batch_index):
        shard_uri = _batch_shard_uri(partial_uri, batch_index)
        if not fs.exists(shard_uri):
            raise DataSluiceError(
                f"Corrupt continuation for resource {resource.id!r}: completed shard {batch_index} is missing"
            )

    next_batch_index = start_batch_index
    with stream:
        for batch, cursor in stream.iter_batches_with_cursors():
            if cursor.next_batch_index != next_batch_index + 1:
                raise DataSluiceError(
                    f"Corrupt batch cursor for resource {resource.id!r}: expected "
                    f"{next_batch_index + 1}, got {cursor.next_batch_index}"
                )
            shard_uri = _batch_shard_uri(partial_uri, next_batch_index)
            _publish_batch_shard(fs, shard_uri, batch)
            on_batch_persisted(cursor)
            next_batch_index = cursor.next_batch_index

    if next_batch_index == 0:
        # Valid empty Parquet: the cursor reader yielded no batches because
        # the file has no non-empty row groups. Publish a zero-row table that
        # retains the source schema (CR-08) — the previous code raised, so a
        # schema-bearing empty Parquet could not be synchronized.
        table = pa.Table.from_batches([], schema=stream.schema)
    else:
        shard_uris = [_batch_shard_uri(partial_uri, index) for index in range(next_batch_index)]
        for batch_index, shard_uri in enumerate(shard_uris):
            if not fs.exists(shard_uri):
                raise DataSluiceError(
                    f"Corrupt continuation for resource {resource.id!r}: completed shard {batch_index} is missing"
                )
        tables = []
        for shard_uri in shard_uris:
            with fs.open(shard_uri, "rb") as source:
                tables.append(pq.read_table(source))
        table = pa.concat_tables(tables)
    checksum = logical_sha256(table)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    payload = sink.getvalue().to_pybytes()
    final_uri = f"{base_uri}/{identity}.parquet"
    blob_digest = hashlib.sha256(payload).hexdigest()
    _atomic_pipe(fs, final_uri, payload)
    return _artifact(
        resource,
        uri=final_uri,
        media_type="application/x-parquet",
        size=len(payload),
        content_digest=checksum,
        blob_digest=blob_digest,
        mode="parquet",
        source_locator=source_locator,
        created_at=created_at,
        transforms=transforms,
    )


def cleanup_checkpointed(resource: Any, *, destination_uri: str) -> None:
    """Remove checkpoint shards after the completed state is durably committed."""
    from datasluice.io.filesystem import open_filesystem

    base_uri = destination_uri.rstrip("/")
    partial_uri = f"{base_uri}/.datasluice-partial/{canonical_identity(resource)}"
    try:
        fs = open_filesystem(base_uri)
        fs.rm(partial_uri, recursive=True)
    except OSError as exc:
        logger.warning(
            "Failed to remove partial shards for resource %r at %r: %s",
            resource.id,
            sanitize_uri(partial_uri),
            exc,
        )


def destination_health(resource: Any, record: Artifact | LegacyArtifactRecord, *, destination_uri: str) -> bool:
    """Verify that a completed artifact exists at the current destination with matching bytes."""
    from datasluice.io.filesystem import open_filesystem

    base_uri = destination_uri.rstrip("/")
    from datasluice.domain import Artifact

    if isinstance(record, Artifact):
        suffix = "bin" if record.provenance.materialization_mode == "raw" else "parquet"
        expected_uri = f"{base_uri}/{canonical_identity(resource)}.{suffix}"
        final_uri = record.uri
        media_type = record.media_type
        expected_size = record.size
        content_digest = record.content_digest.value
        blob_digest = record.blob_digest.value
    else:
        expected_uri = f"{base_uri}/{canonical_identity(resource)}.parquet"
        final_uri, media_type, expected_size, content_digest = record
        blob_digest = None
    if final_uri != expected_uri:
        return False
    try:
        fs = open_filesystem(base_uri)
        if not fs.exists(final_uri) or int(fs.info(final_uri)["size"]) != expected_size:
            return False
        return _destination_checksum(fs, final_uri, media_type) == content_digest and (
            blob_digest is None or _blob_digest_from_fs(fs, final_uri) == blob_digest
        )
    except Exception:
        return False


def _batch_shard_uri(partial_uri: str, batch_index: int) -> str:
    return f"{partial_uri}/{batch_index:020d}.parquet"


def _publish_batch_shard(fs: Any, shard_uri: str, batch: Any) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_batches([batch]), sink)
    if fs.exists(shard_uri):
        fs.rm(shard_uri)
    _atomic_pipe(fs, shard_uri, sink.getvalue().to_pybytes())


def _atomic_pipe(fs: Any, final_uri: str, payload: bytes) -> None:
    tmp_uri = f"{final_uri}.tmp.{os.getpid()}.{random.randint(0, 1 << 32)}"
    try:
        fs.pipe_file(tmp_uri, payload)
        fs.mv(tmp_uri, final_uri)
    except OSError as exc:
        try:
            if fs.exists(tmp_uri):
                fs.rm(tmp_uri)
        except OSError:
            pass
        raise DownloadError(f"Failed to atomically publish {sanitize_uri(final_uri)!r}: {exc}") from exc


def _existing_record(
    fs: Any,
    final_uri: str,
    media_type: str,
    checksum: str,
    stored_checksum: str | None,
) -> tuple[str, str, int, str] | None:
    if stored_checksum != checksum or not fs.exists(final_uri):
        return None
    try:
        size = int(fs.info(final_uri)["size"])
        if _destination_checksum(fs, final_uri, media_type) != checksum:
            return None
    except Exception:
        return None
    return final_uri, media_type, size, checksum


def _destination_checksum(fs: Any, final_uri: str, media_type: str) -> str:
    if media_type == "application/x-parquet":
        import pyarrow.parquet as pq

        from datasluice.sync._hashing import logical_sha256

        with fs.open(final_uri, "rb") as source:
            return logical_sha256(pq.read_table(source))
    with fs.open(final_uri, "rb") as source:
        return hashlib.sha256(source.read()).hexdigest()


def _blob_digest_from_fs(fs: Any, uri: str) -> str:
    with fs.open(uri, "rb") as source:
        return hashlib.sha256(source.read()).hexdigest()


def _artifact(
    resource: Any,
    *,
    uri: str,
    media_type: str,
    size: int,
    content_digest: str,
    blob_digest: str,
    mode: str,
    source_locator: Any | None,
    created_at: datetime | None,
    transforms: tuple[str, ...],
) -> Artifact:
    from datasluice.domain import Artifact, ArtifactProvenance, Digest

    return Artifact(
        uri=sanitize_uri(uri),
        media_type=media_type,
        size=size,
        content_digest=Digest(algorithm="sha256", value=content_digest),
        blob_digest=Digest(algorithm="sha256", value=blob_digest),
        provenance=ArtifactProvenance(
            source_locator=_source_locator(resource, source_locator),
            resource_identity=canonical_identity(resource),
            created_at=created_at or datetime.now(UTC),
            materialization_mode=mode,
            transforms=transforms,
        ),
    )


def _source_locator(resource: Any, source_locator: Any | None) -> Any:
    if source_locator is not None:
        return source_locator
    from datasluice.application import DirectResourceLocator

    access = resource.access
    uri = getattr(access, "url", None) or getattr(access, "uri", None) or resource.url
    if uri is None and access is not None and getattr(access, "kind", None) == "local_file":
        uri = Path(access.path).expanduser().resolve().as_uri()
    if not isinstance(uri, str) or not uri:
        raise DataSluiceError("Artifact materialization requires a serializable source locator")
    return DirectResourceLocator(uri=uri, format=resource.format, media_type=resource.media_type)


def _is_current_artifact(
    value: Artifact | None,
    *,
    uri: str,
    media_type: str,
    size: int,
    content_digest: str,
    blob_digest: str,
    mode: str,
) -> bool:
    from datasluice.domain import Artifact

    return (
        isinstance(value, Artifact)
        and value.uri == sanitize_uri(uri)
        and value.media_type == media_type
        and value.size == size
        and value.content_digest.value == content_digest
        and value.blob_digest.value == blob_digest
        and value.provenance.materialization_mode == mode
    )


def _raw_media_type(resource: Any) -> str:
    if resource.media_type:
        return str(resource.media_type)
    return {
        "CSV": "text/csv",
        "JSON": "application/json",
        "JSONL": "application/x-ndjson",
        "XML": "application/xml",
    }.get(resource.format or "", "application/octet-stream")


def _read_raw(resource: Any, reader: Any) -> bytes:
    access = resource.access
    if hasattr(reader, "read_bytes"):
        return bytes(reader.read_bytes(resource))
    if access is not None and access.kind == "local_file":
        with open(access.path, "rb") as source:
            return source.read()
    if access is not None and access.kind == "object_storage":
        from datasluice.io.filesystem import open_filesystem

        fs = open_filesystem(access.uri)
        return bytes(fs.cat_file(access.uri))
    transport = getattr(reader, "transport", None)
    url = getattr(access, "url", None) or resource.url
    if transport is not None and url is not None:
        return bytes(transport.download(url))
    raise ValueError(f"Resource {resource.id!r} cannot be read in raw mode")
