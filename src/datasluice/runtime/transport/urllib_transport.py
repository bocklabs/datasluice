"""Verified-stdlib synchronous catalog transport."""

from __future__ import annotations

import ssl
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPMessage
from typing import IO
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest, RuntimeResponse, TransportFailure

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
    parsed = urlsplit(url)
    return parsed.scheme.lower(), parsed.hostname or "", parsed.port or (443 if parsed.scheme == "https" else 80)


def _redacted_redirect_url(url: str) -> str:
    parsed = urlsplit(url)
    query = urlencode(
        [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if not any(part in key.lower() for part in _CREDENTIAL_PARTS)
        ]
    )
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


class _NoRedirect(HTTPRedirectHandler):
    """Return redirect responses so the runtime can sanitize every hop."""

    def redirect_request(
        self, req: Request, fp: IO[bytes], code: int, msg: str, headers: HTTPMessage, newurl: str
    ) -> Request | None:
        """Disable urllib's implicit redirect behavior."""
        return None


class UrllibCatalogTransport(CatalogTransport):
    """Synchronous urllib transport with runtime-owned redirect security."""

    def __init__(
        self, *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None, max_redirects: int = 10
    ) -> None:
        self._tls_policy = tls_policy or TLSPolicy()
        self._budget = budget or TimeBudget()
        self._max_redirects = max_redirects
        context = ssl.create_default_context() if self._tls_policy.verify else ssl._create_unverified_context()
        self._opener = build_opener(_NoRedirect(), HTTPSHandler(context=context))
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
            next_url = urljoin(current.url, location)
            headers_for_next = dict(current.headers)
            if _origin(current.url) != _origin(next_url):
                headers_for_next = {
                    key: value for key, value in headers_for_next.items() if key.lower() != "authorization"
                }
                next_url = _redacted_redirect_url(next_url)
            current = RuntimeRequest(method=current.method, url=next_url, headers=headers_for_next, body=current.body)
        raise TransportFailure("Catalog redirect limit exceeded.")

    def close(self) -> None:
        """Mark the transport closed; urllib has no persistent pool."""
        self._closed = True


def _header(headers: dict[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name), None)
