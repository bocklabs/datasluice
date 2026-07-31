"""Idempotent resource materialization to fsspec destinations."""

from __future__ import annotations

import hashlib
import os
import random
from typing import Any

from datasluice.exceptions import DataSluiceError, DownloadError
from datasluice.sync._identity import canonical_identity

_IDEMPOTENT_MATERIALIZE_READY = True


def materialize(
    resource: Any,
    *,
    reader: Any,
    destination_uri: str,
    mode: str = "parquet",
    stored_checksum: str | None = None,
) -> tuple[str, str, int, str]:
    """Materialize a resource and return its URI, media type, size, and checksum."""
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
        checksum = logical_sha256(table)
        final_uri = f"{base_uri}/{identity}.parquet"
        media_type = "application/x-parquet"
        existing = _existing_record(fs, final_uri, media_type, checksum, stored_checksum)
        if existing is not None:
            return existing
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        payload = sink.getvalue().to_pybytes()
    elif mode == "raw":
        payload = _read_raw(resource, reader)
        checksum = hashlib.sha256(payload).hexdigest()
        final_uri = f"{base_uri}/{identity}.bin"
        media_type = _raw_media_type(resource)
        existing = _existing_record(fs, final_uri, media_type, checksum, stored_checksum)
        if existing is not None:
            return existing
    else:
        raise ValueError(f"Unsupported materialize mode {mode!r}; expected 'parquet' or 'raw'")

    tmp_uri = f"{base_uri}/.{identity}.tmp.{os.getpid()}.{random.randint(0, 1 << 32)}"
    try:
        fs.pipe_file(tmp_uri, payload)
        fs.mv(tmp_uri, final_uri)
    except OSError as exc:
        try:
            if fs.exists(tmp_uri):
                fs.rm(tmp_uri)
        except OSError:
            pass
        raise DownloadError(f"Failed to materialize resource {resource.id!r} to {final_uri!r}: {exc}") from exc
    return final_uri, media_type, len(payload), checksum


def materialize_checkpointed(
    resource: Any,
    *,
    stream: Any,
    destination_uri: str,
    start_batch_index: int,
    on_batch_persisted: Any,
) -> tuple[str, str, int, str]:
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
        raise DataSluiceError(f"Cannot finalize empty checkpointed resource {resource.id!r}")
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
    _atomic_pipe(fs, final_uri, payload)
    try:
        fs.rm(partial_uri, recursive=True)
    except OSError as exc:
        raise DownloadError(
            f"Published resource {resource.id!r} but failed to remove partial shards at {partial_uri!r}: {exc}"
        ) from exc
    return final_uri, "application/x-parquet", len(payload), checksum


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
        raise DownloadError(f"Failed to atomically publish {final_uri!r}: {exc}") from exc


def _existing_record(
    fs: Any,
    final_uri: str,
    media_type: str,
    checksum: str,
    stored_checksum: str | None,
) -> tuple[str, str, int, str] | None:
    if stored_checksum != checksum or not fs.exists(final_uri):
        return None
    size = int(fs.info(final_uri)["size"])
    return final_uri, media_type, size, checksum


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
