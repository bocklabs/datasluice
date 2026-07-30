"""Idempotent resource materialization to fsspec destinations."""

from __future__ import annotations

import hashlib
import os
import random
from typing import Any

from datasluice.exceptions import DownloadError

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

    if mode == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        from datasluice.integrations.arrow import to_arrow
        from datasluice.sync._hashing import logical_sha256

        with reader.open(resource) as stream:
            table = to_arrow(stream)
        checksum = logical_sha256(table)
        final_uri = f"{base_uri}/{resource.id}.parquet"
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
        final_uri = f"{base_uri}/{resource.id}.bin"
        media_type = _raw_media_type(resource)
        existing = _existing_record(fs, final_uri, media_type, checksum, stored_checksum)
        if existing is not None:
            return existing
    else:
        raise ValueError(f"Unsupported materialize mode {mode!r}; expected 'parquet' or 'raw'")

    tmp_uri = f"{base_uri}/.{resource.id}.tmp.{os.getpid()}.{random.randint(0, 1 << 32)}"
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
