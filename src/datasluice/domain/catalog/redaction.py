"""Pure bounded redaction primitives for catalog output values."""

from __future__ import annotations

import re
from collections.abc import Mapping

REDACTED = "***"
TRUNCATED = "[TRUNCATED]"
MAX_METADATA_ENTRIES = 32
MAX_METADATA_DEPTH = 8
MAX_TEXT_LENGTH = 256
SENSITIVE_PARTS = frozenset(
    {
        "authorization",
        "credential",
        "token",
        "secret",
        "password",
        "passwd",
        "pwd",
        "cookie",
        "api_key",
        "apikey",
        "private_key",
        "access_key",
        "consumer_key",
        "client_key",
        "signature",
        "body",
        "header",
    }
)
CREDENTIAL_QUERY_RE = re.compile(
    r"(?i)([?&;][^=&;\s]*(?:api[_-]?key|token|secret|password|passwd|credential|authorization|signature)"
    r"[^=&;\s]*)=[^&;\s]+"
)
AUTH_SCHEME_RE = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")


def contains_credential_content(value: object) -> bool:
    """Return whether a string contains a credential-shaped value."""
    return isinstance(value, str) and bool(CREDENTIAL_QUERY_RE.search(value) or AUTH_SCHEME_RE.search(value))


def redact_string(value: str) -> str:
    """Return a bounded string with credential-shaped content replaced."""
    scrubbed = CREDENTIAL_QUERY_RE.sub(r"\1=***", value)
    return AUTH_SCHEME_RE.sub(r"\1 ***", scrubbed)[:MAX_TEXT_LENGTH]


def redact_mapping(value: Mapping[str, object], *, _depth: int = 0) -> dict[str, object]:
    """Return a recursively bounded redacted copy of a metadata mapping."""
    if _depth >= MAX_METADATA_DEPTH:
        return {TRUNCATED: TRUNCATED}
    redacted: dict[str, object] = {}
    for key, nested in tuple(value.items())[:MAX_METADATA_ENTRIES]:
        if not isinstance(key, str) or not key:
            raise ValueError("Redacted metadata keys must be non-empty strings.")
        normalized = key.lower().replace("-", "_")
        if any(part in normalized for part in SENSITIVE_PARTS):
            redacted[key] = REDACTED
        else:
            redacted[key] = redact_value(nested, _depth=_depth + 1)
    if len(value) > MAX_METADATA_ENTRIES:
        redacted.popitem()
        redacted[TRUNCATED] = TRUNCATED
    return redacted


def redact_value(value: object, *, _depth: int = 0) -> object:
    """Return a recursively bounded redacted representation of one value."""
    if isinstance(value, str):
        return redact_string(value)
    if value is None or isinstance(value, bool | int | float):
        return value
    if _depth >= MAX_METADATA_DEPTH:
        return TRUNCATED
    if isinstance(value, Mapping):
        return redact_mapping(value, _depth=_depth)
    if isinstance(value, tuple | list):
        items = tuple(redact_value(item, _depth=_depth + 1) for item in value[:MAX_METADATA_ENTRIES])
        return (*items[:-1], TRUNCATED) if len(value) > MAX_METADATA_ENTRIES else items
    return repr(value)[:MAX_TEXT_LENGTH]
