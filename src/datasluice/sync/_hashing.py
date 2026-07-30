"""Serialization-stable logical hashing for Arrow tables."""

from __future__ import annotations

import datetime
import hashlib
import json
from typing import Any


def logical_sha256(table: Any) -> str:
    """Return a SHA-256 digest over an Arrow table's schema and logical rows."""
    digest = hashlib.sha256()
    digest.update(_schema_fingerprint(table.schema))
    for batch in table.to_batches(max_chunksize=65536):
        for row in batch.to_pylist():
            encoded = {key: _encode(value) for key, value in row.items()}
            digest.update(json.dumps(encoded, sort_keys=True, separators=(",", ":")).encode())
            digest.update(b"\n")
    return digest.hexdigest()


def _schema_fingerprint(schema: Any) -> bytes:
    return json.dumps([[field.name, str(field.type), field.nullable] for field in schema]).encode()


def _encode(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return {"__t": value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {"__b": bytes(value).hex()}
    if isinstance(value, list):
        return [_encode(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _encode(item) for key, item in value.items()}
    return {"__s": str(value)}
