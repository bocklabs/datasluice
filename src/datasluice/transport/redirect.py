"""Redirect handling that strips credentials on cross-origin or scheme-downgrade redirects."""

from __future__ import annotations

import urllib.parse
import urllib.request
from typing import IO, TYPE_CHECKING

from datasluice.logging import get_logger

if TYPE_CHECKING:
    from http.client import HTTPMessage

    from datasluice.domain.credentials import CredentialScope

logger = get_logger("transport.redirect")

SENSITIVE_HEADERS = frozenset({"authorization", "cookie", "x-api-key", "x-auth-token"})


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
        same_origin = old_parsed.netloc == new_parsed.netloc
        scheme_downgrade = old_parsed.scheme == "https" and new_parsed.scheme == "http"

        scope = self.credential_scope
        if scope is not None:
            host_allowed = new_parsed.hostname in scope.allowed_hosts
            scheme_allowed = new_parsed.scheme in scope.allowed_schemes
            should_strip = scheme_downgrade or not host_allowed or not scheme_allowed or not scope.send_on_redirect
        else:
            should_strip = scheme_downgrade or not same_origin

        if should_strip:
            new_req.headers = {k: v for k, v in new_req.headers.items() if k.lower() not in SENSITIVE_HEADERS}
            logger.debug("Stripped sensitive headers on redirect to %s", newurl)
        return new_req
