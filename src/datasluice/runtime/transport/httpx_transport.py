"""Optional httpx transports with a common runtime response shape."""

from __future__ import annotations

from typing import Any, cast
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.transport.base import (
    RuntimeRequest,
    RuntimeResponse,
    TransportFailure,
    strip_sensitive_redirect_headers,
)
from datasluice.runtime.transport.urllib_transport import _retry_after

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


class HttpxCatalogTransport:
    """Optional pooled synchronous httpx transport."""

    def __init__(
        self,
        *,
        tls_policy: TLSPolicy | None = None,
        budget: TimeBudget | None = None,
        transport: object | None = None,
        max_redirects: int = 10,
    ) -> None:
        import httpx

        policy = tls_policy or TLSPolicy()
        budget = budget or TimeBudget()
        self._client: Any = httpx.Client(
            timeout=httpx.Timeout(connect=budget.connect, read=budget.read, write=budget.write, pool=10.0),
            limits=httpx.Limits(),
            verify=policy.verify,
            transport=cast(Any, transport),
            follow_redirects=False,
        )
        self._max_redirects = max_redirects
        self._closed = False

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send through httpx and follow redirects under runtime control."""
        if self._closed:
            raise TransportFailure("The httpx catalog transport is closed.")
        current = request
        for _ in range(self._max_redirects + 1):
            try:
                response = self._client.request(
                    current.method,
                    current.url,
                    headers=dict(current.headers),
                    content=current.body,
                    follow_redirects=False,
                )
            except Exception as exc:
                if exc.__class__.__module__.startswith("httpx"):
                    raise TransportFailure("httpx could not complete the catalog request.") from exc
                raise
            location = response.headers.get("location")
            if not response.is_redirect or location is None:
                return RuntimeResponse(
                    response.status_code,
                    dict(response.headers),
                    response.content,
                    _retry_after(response.headers.get("retry-after")),
                )
            next_url = urljoin(current.url, location)
            headers = dict(current.headers)
            if _origin(current.url) != _origin(next_url):
                headers = strip_sensitive_redirect_headers(headers)
                next_url = _redacted_redirect_url(next_url)
            response.close()
            current = RuntimeRequest(current.method, next_url, headers, current.body)
        raise TransportFailure("Catalog redirect limit exceeded.")

    def close(self) -> None:
        """Close the httpx pool once."""
        if not self._closed:
            self._closed = True
            self._client.close()


class AsyncHttpxCatalogTransport:
    """Optional pooled asynchronous httpx transport."""

    def __init__(
        self,
        *,
        tls_policy: TLSPolicy | None = None,
        budget: TimeBudget | None = None,
        transport: object | None = None,
        max_redirects: int = 10,
    ) -> None:
        import httpx

        policy = tls_policy or TLSPolicy()
        budget = budget or TimeBudget()
        self._client: Any = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=budget.connect, read=budget.read, write=budget.write, pool=10.0),
            limits=httpx.Limits(),
            verify=policy.verify,
            transport=cast(Any, transport),
            follow_redirects=False,
        )
        self._max_redirects = max_redirects
        self._closed = False

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send asynchronously through httpx without sync delegation."""
        if self._closed:
            raise TransportFailure("The async httpx catalog transport is closed.")
        current = request
        for _ in range(self._max_redirects + 1):
            try:
                response = await self._client.request(
                    current.method,
                    current.url,
                    headers=dict(current.headers),
                    content=current.body,
                    follow_redirects=False,
                )
            except Exception as exc:
                if exc.__class__.__module__.startswith("httpx"):
                    raise TransportFailure("httpx could not complete the catalog request.") from exc
                raise
            location = response.headers.get("location")
            if not response.is_redirect or location is None:
                return RuntimeResponse(
                    response.status_code,
                    dict(response.headers),
                    response.content,
                    _retry_after(response.headers.get("retry-after")),
                )
            next_url = urljoin(current.url, location)
            headers = dict(current.headers)
            if _origin(current.url) != _origin(next_url):
                headers = strip_sensitive_redirect_headers(headers)
                next_url = _redacted_redirect_url(next_url)
            await response.aclose()
            current = RuntimeRequest(current.method, next_url, headers, current.body)
        raise TransportFailure("Catalog redirect limit exceeded.")

    async def aclose(self) -> None:
        """Close the asynchronous httpx pool once."""
        if not self._closed:
            self._closed = True
            await self._client.aclose()
