"""URI display sanitizer: redact userinfo and sensitive query values (CR-07).

Query-position API keys, bearer tokens, signed-URL signatures, and
``user:password@`` userinfo routinely end up embedded inside URI strings
that flow into log records and exception messages. The package
:class:`datasluice.logging.RedactingFilter` only inspects named dict
keys, so it cannot redact a secret buried inside a positional URL string.
This module provides :func:`sanitize_uri` for every call site that needs
a URI for human-facing display. The unsanitized URI is still used for
actual I/O.
"""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_SENSITIVE_QUERY_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "token",
        "access_token",
        "refresh_token",
        "secret",
        "signature",
        "credential",
        "password",
        "x-api-key",
        "x-api-token",
        "x-auth-token",
    }
)

_REDACTED = "***"


def sanitize_uri(uri: str) -> str:
    """Return *uri* with userinfo and sensitive query values redacted for safe display.

    Strips ``user:password@`` from the netloc (keeping host and port) and
    replaces the value of every sensitive query parameter (``api_key``,
    ``token``, ``signature``, …) with ``"***"``. The returned value is
    suitable only for logs, exception text, and other human-facing display;
    never reuse it for actual I/O, because the redaction discards
    credentials the destination filesystem still needs.

    Strings without a scheme are returned unchanged so this helper is safe to
    call on path-like state keys, bare paths, or opaque identifiers.
    """
    try:
        parts = urlsplit(uri)
    except ValueError:
        return uri
    if not parts.scheme:
        return uri
    if parts.hostname is None:
        netloc = parts.netloc.rsplit("@", 1)[-1]
    else:
        try:
            port = parts.port
        except ValueError:
            port = None
        hostname = parts.hostname
        if ":" in hostname:
            hostname = f"[{hostname}]"
        netloc = hostname if port is None else f"{hostname}:{port}"
    redacted: list[tuple[str, str]] = []
    if parts.query:
        try:
            redacted = [
                (name, _REDACTED if name.lower() in _SENSITIVE_QUERY_KEYS else value)
                for name, value in parse_qsl(parts.query, keep_blank_values=True)
            ]
        except ValueError:
            redacted = []
    query = urlencode(redacted, doseq=True, safe="*")
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))
