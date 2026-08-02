"""Bounded JSON encoding for provider XCom values."""

from __future__ import annotations

import json

from datasluice import DataSluiceError

MAX_XCOM_BYTES = 49_152


def validate_xcom_payload[T](payload: T) -> T:
    """Validate one deterministic, compact JSON XCom payload."""
    try:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise DataSluiceError("XCom payload must be JSON serializable") from exc
    if len(encoded) > MAX_XCOM_BYTES:
        raise DataSluiceError(f"XCom payload exceeds the {MAX_XCOM_BYTES}-byte limit")
    return payload
