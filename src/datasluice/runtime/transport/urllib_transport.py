"""Verified-stdlib synchronous catalog transport."""

from __future__ import annotations

import ssl
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPException, HTTPMessage
from typing import IO
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import (
    HTTPErrorProcessor,
    HTTPHandler,
    HTTPRedirectHandler,
    HTTPSHandler,
    OpenerDirector,
    Request,
)

from datasluice.domain import CredentialScope
from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.transport.base import (
    CatalogTransport,
    RuntimeRequest,
    RuntimeResponse,
    TransportFailure,
    drop_body_transfer_headers,
    redirect_method_and_body,
    strip_sensitive_redirect_headers,
)

_CREDENTIAL_PARTS = (
    "api_key",
    "apikey",
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
    "authorization",
    "signature",
)
_REDIRECT_CODES = frozenset({301, 302, 303, 307, 308})
_ALLOWED_SCHEMES = frozenset({"http", "https"})


def _retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(0.0, (parsed - datetime.now(UTC)).total_seconds())


def _origin(url: str) -> tuple[str, str, int | None]:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        return "", "", None
    return parsed.scheme.lower(), parsed.hostname or "", port or (443 if parsed.scheme == "https" else 80)


def _redacted_redirect_url(url: str) -> str:
    """Render *url* for exception surfaces with credential-shaped query params removed."""
    try:
        parsed = urlsplit(url)
        query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if not any(part in key.lower() for part in _CREDENTIAL_PARTS)
            ]
        )
    except ValueError:
        return "<unparseable-redirect-target>"
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _tls_context(policy: TLSPolicy) -> ssl.SSLContext:
    """Return a verified context, or an unverified one built from public APIs."""
    if policy.verify:
        return ssl.create_default_context()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


class _NoRedirect(HTTPRedirectHandler):
    """Return redirect responses so the runtime can sanitize every hop."""

    def redirect_request(
        self, req: Request, fp: IO[bytes], code: int, msg: str, headers: HTTPMessage, newurl: str
    ) -> Request | None:
        """Disable urllib's implicit redirect behavior."""
        return None


def _build_opener(context: ssl.SSLContext) -> OpenerDirector:
    """Assemble an opener restricted to plain HTTP(S), with no file or FTP access."""
    opener = OpenerDirector()
    opener.add_handler(_NoRedirect())
    opener.add_handler(HTTPErrorProcessor())
    opener.add_handler(HTTPHandler())
    opener.add_handler(HTTPSHandler(context=context))
    return opener


class UrllibCatalogTransport(CatalogTransport):
    """Synchronous urllib transport with runtime-owned redirect security.

    Redirect targets are limited to ``http`` and ``https`` and are requested
    verbatim, so presigned query strings survive every hop; sensitive headers
    are re-evaluated against the target origin and any configured
    :class:`CredentialScope`. Note that urllib applies its timeout to each
    individual socket operation (connect and read) rather than bounding the
    whole request; callers needing a hard deadline must enforce it themselves.
    """

    def __init__(
        self,
        *,
        tls_policy: TLSPolicy | None = None,
        budget: TimeBudget | None = None,
        max_redirects: int = 10,
        credential_scope: CredentialScope | None = None,
    ) -> None:
        self._tls_policy = tls_policy or TLSPolicy()
        self._budget = budget or TimeBudget()
        self._max_redirects = max_redirects
        self._credential_scope = credential_scope
        self._opener = _build_opener(_tls_context(self._tls_policy))
        self._closed = False

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one request, following bounded and sanitized redirects."""
        if self._closed:
            raise TransportFailure("The urllib catalog transport is closed.")
        current = request
        for _ in range(self._max_redirects + 1):
            try:
                response = self._opener.open(
                    Request(current.url, data=current.body, headers=dict(current.headers), method=current.method),
                    timeout=self._budget.connect,
                )
                status = response.status
                headers = dict(response.headers.items())
                body = response.read()
            except HTTPError as exc:
                status, headers, body = exc.code, dict(exc.headers.items()), exc.read()
            except HTTPException as exc:
                raise TransportFailure("urllib lost the catalog connection mid-response.") from exc
            except (URLError, OSError) as exc:
                raise TransportFailure("urllib could not complete the catalog request.") from exc
            location = next((value for key, value in headers.items() if key.lower() == "location"), None)
            if status not in _REDIRECT_CODES or location is None:
                return RuntimeResponse(
                    status_code=status,
                    headers=headers,
                    body=body,
                    retry_after=_retry_after(_header(headers, "retry-after")),
                )
            try:
                next_url = urljoin(current.url, location)
            except ValueError as exc:
                raise TransportFailure(
                    f"urllib received an unusable redirect target {_redacted_redirect_url(location)!r}."
                ) from exc
            if urlsplit(next_url).scheme.lower() not in _ALLOWED_SCHEMES:
                raise TransportFailure(f"Refusing to follow non-HTTP redirect to {_redacted_redirect_url(next_url)!r}.")
            headers_for_next = dict(current.headers)
            if not self._retains_credentials(current.url, next_url):
                headers_for_next = strip_sensitive_redirect_headers(headers_for_next)
            next_method, next_body = redirect_method_and_body(current.method, status, current.body)
            if next_body is None:
                headers_for_next = drop_body_transfer_headers(headers_for_next)
            current = RuntimeRequest(method=next_method, url=next_url, headers=headers_for_next, body=next_body)
        raise TransportFailure("Catalog redirect limit exceeded.")

    def close(self) -> None:
        """Mark the transport closed; urllib has no persistent pool."""
        self._closed = True

    def _retains_credentials(self, current_url: str, next_url: str) -> bool:
        """Decide whether credential-bearing headers survive this hop."""
        scope = self._credential_scope
        if scope is None:
            return _origin(current_url) == _origin(next_url)
        scheme, host, _ = _origin(next_url)
        return scope.send_on_redirect and scheme in scope.allowed_schemes and host in scope.allowed_hosts


def _header(headers: dict[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name), None)
