from __future__ import annotations

import ipaddress
import socket
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit, urlunsplit

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
        "sig",
        "x-amz-signature",
        "x-amz-credential",
        "x-amz-security-token",
        "x-goog-signature",
        "x-goog-credential",
    }
)

_REDACTED = "***"


def _netloc_without_userinfo(parts: SplitResult) -> str:
    """Return the netloc of *parts* with any ``user:password@`` prefix removed."""
    if parts.hostname is None:
        return parts.netloc.rsplit("@", 1)[-1]
    try:
        port = parts.port
    except ValueError:
        port = None
    hostname = parts.hostname
    if ":" in hostname:
        hostname = f"[{hostname}]"
    return hostname if port is None else f"{hostname}:{port}"


def _redacted_query(query: str) -> str:
    """Return *query* re-encoded with every sensitive parameter value replaced."""
    if not query:
        return ""
    try:
        pairs = parse_qsl(query, keep_blank_values=True)
    except ValueError:
        return ""
    return urlencode(
        [(name, _REDACTED if name.lower() in _SENSITIVE_QUERY_KEYS else value) for name, value in pairs],
        doseq=True,
        safe="*",
    )


def sanitize_uri(uri: str) -> str:
    """Return *uri* with userinfo and sensitive query values redacted for safe display.

    Strips ``user:password@`` from the netloc (keeping host and port) and replaces the value of every sensitive query
    parameter (``api_key``, ``token``, ``signature``, …) with ``"***"``. The returned value is suitable only for logs,
    exception text, and other human-facing display; never reuse it for actual I/O, because the redaction discards
    credentials the destination filesystem still needs.

    Strings without a scheme are returned unchanged so this helper is safe to call on path-like state keys, bare paths,
    or opaque identifiers.
    """
    try:
        parts = urlsplit(uri)
    except ValueError:
        return uri
    if not parts.scheme:
        return uri
    return urlunsplit(
        (parts.scheme, _netloc_without_userinfo(parts), parts.path, _redacted_query(parts.query), parts.fragment)
    )


def validate_http_target(value: object, *, allow_private: bool = False, resolve: bool = False) -> str:
    """Validate HTTP syntax and target locality.

    Set ``resolve=True`` to check DNS answers.
    """
    if not isinstance(value, str) or not value:
        raise ValueError("missing")
    if len(value) > 2048:
        raise ValueError("length")
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError("format")
    host = _http_target_host(value)
    if not allow_private and (_private_http_host(host) or (resolve and _resolves_to_private_address(host))):
        raise ValueError("locality")
    return value


def _http_target_host(value: str) -> str:
    """Return the validated host.

    Reject an invalid scheme, userinfo, port, or host.
    """
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
    except ValueError:
        raise ValueError("host") from None
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("port") from None
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("scheme")
    if "@" in parsed.netloc:
        raise ValueError("userinfo")
    if port is not None and port < 1:
        raise ValueError("port")
    if not host or "\\" in host or "%" in host:
        raise ValueError("host")
    return host


def _resolves_to_private_address(host: str) -> bool:
    """Return whether DNS resolves *host* to a non-public address.

    Return ``False`` if DNS cannot resolve the host. This check does not prevent later DNS rebinding.
    """
    try:
        answers = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError):
        return False
    for answer in answers:
        try:
            address = ipaddress.ip_address(answer[4][0])
        except ValueError:
            return True
        if not address.is_global or address.is_multicast:
            return True
    return False


def _private_http_host(host: str) -> bool:
    host = host.lower().rstrip(".")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        try:
            address = ipaddress.IPv4Address(socket.inet_aton(host))
        except OSError:
            return (
                host in {"localhost", "ip6-localhost", "ip6-loopback"}
                or "." not in host
                or host.endswith(
                    (
                        ".localhost",
                        ".local",
                        ".lan",
                        ".internal",
                        ".intranet",
                        ".corp",
                        ".localdomain",
                        ".home.arpa",
                        ".onion",
                    )
                )
            )
    return not address.is_global or address.is_multicast
