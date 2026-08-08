"""Redirect handling that strips credentials on cross-origin or scheme-downgrade redirects."""

from __future__ import annotations

import urllib.parse
import urllib.request
from typing import IO, TYPE_CHECKING

from datasluice._uri import sanitize_uri
from datasluice.logging import SENSITIVE_HEADERS, get_logger

if TYPE_CHECKING:
    from http.client import HTTPMessage

    from datasluice.domain.credentials import CredentialScope

logger = get_logger("transport.redirect")

_DEFAULT_PORTS = {"http": 80, "https": 443, "ws": 80, "wss": 443}


def _effective_port(scheme: str, port: int | None) -> int | None:
    """Normalize an explicit port against the scheme default (None when default)."""
    if port is None:
        return None
    return None if port == _DEFAULT_PORTS.get(scheme) else port


def _same_origin(old: urllib.parse.ParseResult, new: urllib.parse.ParseResult) -> bool:
    """Return True when both URLs share hostname (case-insensitive) and effective port."""
    old_host = (old.hostname or "").lower()
    new_host = (new.hostname or "").lower()
    return old_host == new_host and _effective_port(old.scheme, old.port) == _effective_port(new.scheme, new.port)


class CredentialAwareRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Strip sensitive headers when a redirect crosses origins or downgrades to plain HTTP.

    Args:
        credential_scope: Optional host-scoped credential policy. When omitted, any
            cross-origin redirect strips credentials (zero-config safety).
    """

    def __init__(self, credential_scope: CredentialScope | None = None) -> None:
        self.credential_scope = credential_scope

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        """Return the follow-up request, stripping sensitive headers when required."""
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_req is None:
            return None

        old_parsed = urllib.parse.urlparse(req.full_url)
        new_parsed = urllib.parse.urlparse(newurl)
        same_origin = _same_origin(old_parsed, new_parsed)
        scheme_downgrade = old_parsed.scheme == "https" and new_parsed.scheme == "http"
        # A redirect to a non-web scheme (ftp:, file:, gopher:, …) must never
        # carry credentials, regardless of origin.
        target_unsafe = new_parsed.scheme not in {"http", "https", "ws", "wss"}

        scope = self.credential_scope
        if scope is not None:
            host_allowed = new_parsed.hostname in scope.allowed_hosts
            scheme_allowed = new_parsed.scheme in scope.allowed_schemes
            should_strip = (
                scheme_downgrade or target_unsafe or not (host_allowed and scheme_allowed and scope.send_on_redirect)
            )
        else:
            should_strip = scheme_downgrade or target_unsafe or not same_origin

        if should_strip:
            new_req.headers = {k: v for k, v in new_req.headers.items() if k.lower() not in SENSITIVE_HEADERS}
            logger.debug("Stripped sensitive headers on redirect to %s", sanitize_uri(newurl))
        return new_req
