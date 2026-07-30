"""Idempotent resource materialization to fsspec destinations."""

from __future__ import annotations

import hashlib
import json
import os
import random
from typing import Any


def materialize(
    resource: Any,
    *,
    reader: Any,
    destination_uri: str,
    mode: str = "parquet",
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

        with reader.open(resource) as stream:
            table = to_arrow(stream)
        checksum = _logical_sha256(table)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        payload = sink.getvalue().to_pybytes()
        final_uri = f"{base_uri}/{resource.id}.parquet"
        media_type = "application/x-parquet"
    elif mode == "raw":
        payload = _read_raw(resource, reader)
        checksum = hashlib.sha256(payload).hexdigest()
        final_uri = f"{base_uri}/{resource.id}.bin"
        media_type = resource.media_type or "application/octet-stream"
    else:
        raise ValueError(f"Unsupported materialize mode {mode!r}; expected 'parquet' or 'raw'")

    tmp_uri = f"{base_uri}/.{resource.id}.tmp.{os.getpid()}.{random.randint(0, 1 << 32)}"
    fs.pipe_file(tmp_uri, payload)
    fs.mv(tmp_uri, final_uri)
    return final_uri, media_type, len(payload), checksum


def _logical_sha256(table: Any) -> str:
    schema = [[field.name, str(field.type), field.nullable] for field in table.schema]
    payload = json.dumps([schema, table.to_pylist()], sort_keys=True, default=str, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


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
