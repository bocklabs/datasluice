"""Pure bounded redaction primitives for catalog output values."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

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
SENSITIVE_QUERY_KEYS = frozenset(
    {
        "access-key",
        "access_key",
        "api-key",
        "api_key",
        "apikey",
        "authorization",
        "client-key",
        "client_key",
        "consumer-key",
        "consumer_key",
        "cookie",
        "credential",
        "passwd",
        "password",
        "private-key",
        "private_key",
        "pwd",
        "secret",
        "signature",
        "token",
    }
)

_QUERY_KEYWORDS = "|".join(re.escape(part) for part in sorted(SENSITIVE_QUERY_KEYS, key=len, reverse=True))
CREDENTIAL_QUERY_RE = re.compile(rf"(?i)((?:[?&;]|\b)[^=&;\s]*(?:{_QUERY_KEYWORDS})[^=&;\s]*)=[^&;\s]+")
AUTH_SCHEME_RE = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
USERINFO_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^@/\s]*):([^@\s]*)@")
_SCAN_MARGIN = 64


def _output_window(value: str) -> str:
    return value[: MAX_TEXT_LENGTH + _SCAN_MARGIN]


def contains_credential_content(value: object) -> bool:
    """Return whether a string contains a credential-shaped value."""
    if not isinstance(value, str):
        return False
    window_size = MAX_TEXT_LENGTH + _SCAN_MARGIN
    step = MAX_TEXT_LENGTH
    for offset in range(0, len(value) or 1, step):
        window = value[offset : offset + window_size]
        if USERINFO_RE.search(window) or CREDENTIAL_QUERY_RE.search(window) or AUTH_SCHEME_RE.search(window):
            return True
    return False


def redact_string(value: str) -> str:
    """Return a bounded string with credential-shaped content replaced."""
    scrubbed = USERINFO_RE.sub(r"\1:***@", _output_window(value))
    scrubbed = CREDENTIAL_QUERY_RE.sub(r"\1=***", scrubbed)
    return AUTH_SCHEME_RE.sub(r"\1 ***", scrubbed)[:MAX_TEXT_LENGTH]


def _render_unsupported(value: object) -> str:
    try:
        rendered = repr(value)
    except Exception:
        return TRUNCATED
    return redact_string(rendered)


def _bounded_entry(value: object, *, _depth: int) -> object:
    redacted = redact_value(value, _depth=_depth)
    if redacted is None or isinstance(redacted, str | bool | int | float | Mapping | tuple | list):
        return redacted
    return _render_unsupported(redacted)


def redact_mapping(value: Mapping[str, object], *, _depth: int = 0) -> dict[str, object]:
    """Return a recursively bounded redacted copy of a metadata mapping."""
    if _depth >= MAX_METADATA_DEPTH:
        return {TRUNCATED: TRUNCATED}
    redacted: dict[str, object] = {}
    for key, nested in tuple(value.items())[:MAX_METADATA_ENTRIES]:
        if not isinstance(key, str) or not key:
            continue
        normalized = key.lower().replace("-", "_")
        if any(part in normalized for part in SENSITIVE_PARTS):
            redacted[key] = REDACTED
        else:
            redacted[key] = _bounded_entry(nested, _depth=_depth + 1)
    if len(value) > MAX_METADATA_ENTRIES:
        if redacted:
            redacted.popitem()
        redacted[TRUNCATED] = TRUNCATED
    return redacted


def redact_value(value: object, *, _depth: int = 0) -> object:
    """Return a total bounded redacted representation of one value."""
    if isinstance(value, str):
        return redact_string(value)
    if isinstance(value, bytes | bytearray):
        return redact_string(bytes(value).decode("utf-8", errors="replace"))
    if value is None or isinstance(value, bool | int | float):
        return value
    if _depth >= MAX_METADATA_DEPTH:
        return TRUNCATED
    if isinstance(value, Mapping):
        return redact_mapping(value, _depth=_depth)
    if isinstance(value, Sequence):
        items = tuple(redact_value(item, _depth=_depth + 1) for item in value[:MAX_METADATA_ENTRIES])
        return (*items[:-1], TRUNCATED) if len(value) > MAX_METADATA_ENTRIES else items
    return value
