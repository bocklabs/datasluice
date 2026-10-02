"""Verified-stdlib synchronous catalog transport."""

from __future__ import annotations

import ssl
from collections.abc import Iterator, Mapping
from email.message import Message
from http.client import HTTPException, HTTPMessage
from typing import IO
from urllib.error import HTTPError
from urllib.parse import urljoin, urlsplit
from urllib.request import (
    HTTPDefaultErrorHandler,
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
from datasluice.runtime.transport._shared import (
    ALLOWED_REDIRECT_SCHEMES,
    _enforce_body_limit,
    _next_redirect_request,
    _redacted_redirect_url,
    _retry_after,
)
from datasluice.runtime.transport.base import (
    CatalogTransport,
    RedirectPolicy,
    RuntimeRequest,
    RuntimeResponse,
    RuntimeStreamResponse,
    TransportFailure,
)

_REDIRECT_CODES = frozenset({301, 302, 303, 307, 308})


def _tls_context(policy: TLSPolicy) -> ssl.SSLContext:
    """Return a context that validates peer certificates and hostnames."""
    if not policy.verify:
        raise ValueError("TLS certificate and hostname verification cannot be disabled.")
    context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
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
    opener.add_handler(HTTPDefaultErrorHandler())
    opener.add_handler(HTTPErrorProcessor())
    opener.add_handler(HTTPHandler())
    opener.add_handler(HTTPSHandler(context=context))
    return opener


class UrllibCatalogTransport(CatalogTransport):
    """Synchronous urllib transport with runtime-owned redirect security.

    Redirect targets are limited to ``http`` and ``https`` and are requested
    verbatim, so presigned query strings survive every hop; sensitive headers
    are re-evaluated against the target origin and any configured
    :class:`CredentialScope`. A hop whose normalized origin differs from the
    current one refuses to carry a request body whatever the scope allows, so
    a cross-origin 307/308 never relays a body. Note that urllib applies its
    timeout to each individual socket operation (connect and read) rather than
    bounding the whole request; callers needing a hard deadline must enforce it
    themselves.
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
        if request.files:
            raise TransportFailure(
                "Multipart requests require the httpx transport; install datasluice[http] or inject an httpx transport."
            )
        current = request
        for _ in range(self._max_redirects + 1):
            status, headers, body = self._read_current(current)
            if current.redirect_policy is RedirectPolicy.NO_FOLLOW:
                return _runtime_response(status, headers, body)
            next_request = self._redirect_request(current, status, headers)
            if next_request is None:
                return _runtime_response(status, headers, body)
            current = next_request
        raise TransportFailure("Catalog redirect limit exceeded.")

    def _read_current(self, request: RuntimeRequest) -> tuple[int, dict[str, str], bytes]:
        try:
            response = self._opener.open(
                Request(request.url, data=request.body, headers=dict(request.headers), method=request.method),
                timeout=min(self._budget.read, self._budget.total),
            )
            try:
                return (
                    response.status,
                    _header_map(response.headers),
                    _read_response_body(response, request.max_response_bytes),
                )
            finally:
                close = getattr(response, "close", None)
                if callable(close):
                    close()
        except HTTPError as exc:
            try:
                return exc.code, _header_map(exc.headers), _read_response_body(exc, request.max_response_bytes)
            finally:
                exc.close()
        except HTTPException as exc:
            raise TransportFailure("urllib lost the catalog connection mid-response.") from exc
        except OSError as exc:
            raise TransportFailure("urllib could not complete the catalog request.") from exc

    def _redirect_request(
        self, request: RuntimeRequest, status: int, headers: Mapping[str, str]
    ) -> RuntimeRequest | None:
        location = next((value for key, value in headers.items() if key.lower() == "location"), None)
        if status not in _REDIRECT_CODES or location is None:
            return None
        try:
            next_url = urljoin(request.url, location)
        except ValueError as exc:
            raise TransportFailure(
                f"urllib received an unusable redirect target {_redacted_redirect_url(location)!r}."
            ) from exc
        if urlsplit(next_url).scheme.lower() not in ALLOWED_REDIRECT_SCHEMES:
            raise TransportFailure(f"Refusing to follow non-HTTP redirect to {_redacted_redirect_url(next_url)!r}.")
        return _next_redirect_request(request, status, next_url, self._credential_scope, "urllib")

    def close(self) -> None:
        """Mark the transport closed; urllib has no persistent pool."""
        self._closed = True

    def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
        """Open one no-follow response without pre-buffering its body."""
        if self._closed:
            raise TransportFailure("The urllib catalog transport is closed.")
        if request.files:
            raise TransportFailure(
                "Multipart requests require the httpx transport; install datasluice[http] or inject an httpx transport."
            )
        if request.redirect_policy is not RedirectPolicy.NO_FOLLOW:
            raise ValueError("Streaming catalog requests must explicitly disable redirect following.")
        try:
            response = self._opener.open(
                Request(request.url, data=request.body, headers=dict(request.headers), method=request.method),
                timeout=min(self._budget.read, self._budget.total),
            )
            status = response.status
            headers = _header_map(response.headers)
        except HTTPError as exc:
            response = exc
            status = exc.code
            headers = _header_map(exc.headers)
        except HTTPException as exc:
            raise TransportFailure("urllib lost the catalog connection before streaming its response.") from exc
        except OSError as exc:
            raise TransportFailure("urllib could not open the catalog response stream.") from exc

        def chunks() -> Iterator[bytes]:
            try:
                while chunk := response.read(64 * 1024):
                    yield chunk
            except (HTTPException, OSError) as exc:
                raise TransportFailure("urllib lost the catalog connection mid-response.") from exc

        return RuntimeStreamResponse(
            status_code=status,
            headers=headers,
            chunks=chunks(),
            close_callback=response.close,
            retry_after=_retry_after(_header(headers, "retry-after")),
        )


def _header_map(headers: Mapping[str, str] | Message[str, str]) -> dict[str, str]:
    """Preserve duplicate response headers as comma-joined values."""
    result: dict[str, str] = {}
    for key, value in headers.items():
        existing = next((name for name in result if name.lower() == key.lower()), None)
        if existing is None:
            result[key] = value
        else:
            result[existing] = f"{result[existing]},{value}"
    return result


def _read_response_body(response: object, max_bytes: int | None) -> bytes:
    """Read a response body with an optional incremental byte ceiling."""
    read = getattr(response, "read", None)
    if not callable(read):
        raise TransportFailure("urllib returned a response without a readable body.")
    if max_bytes is None:
        body = read()
        if not isinstance(body, bytes):
            raise TransportFailure("urllib returned a non-byte catalog response body.")
        return body
    parts: list[bytes] = []
    size = 0
    while True:
        chunk = read(64 * 1024)
        if not isinstance(chunk, bytes):
            raise TransportFailure("urllib yielded a non-byte catalog response chunk.")
        if not chunk:
            return b"".join(parts)
        size += len(chunk)
        _enforce_body_limit(size, max_bytes)
        parts.append(chunk)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name), None)


def _runtime_response(status: int, headers: dict[str, str], body: bytes) -> RuntimeResponse:
    return RuntimeResponse(
        status_code=status,
        headers=headers,
        body=body,
        retry_after=_retry_after(_header(headers, "retry-after")),
    )
