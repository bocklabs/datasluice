"""Optional httpx transports with a common runtime response shape."""

from __future__ import annotations

from typing import Any, cast
from urllib.parse import urljoin, urlsplit

from datasluice.domain import CredentialScope
from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.transport.base import (
    AsyncRuntimeStreamResponse,
    RedirectPolicy,
    RuntimeRequest,
    RuntimeResponse,
    RuntimeStreamResponse,
    TransportFailure,
    drop_body_transfer_headers,
    redirect_method_and_body,
    strip_sensitive_redirect_headers,
)
from datasluice.runtime.transport.urllib_transport import _origin, _redacted_redirect_url, _retry_after

_ALLOWED_REDIRECT_SCHEMES = frozenset({"http", "https"})


def _require_plain_http_target(url: str) -> None:
    """Reject redirect targets outside plain HTTP(S) or with malformed ports."""
    parsed = urlsplit(url)
    if parsed.scheme.lower() not in _ALLOWED_REDIRECT_SCHEMES:
        raise ValueError(f"non-HTTP redirect target scheme {parsed.scheme!r}")
    _ = parsed.port


def _build_request(client: Any, request: RuntimeRequest) -> Any:
    if request.files:
        return client.build_request(
            request.method,
            request.url,
            headers=dict(drop_body_transfer_headers(request.headers)),
            data=None,
            files=[(part.field_name, (part.file_name, part.data, part.content_type)) for part in request.files],
        )
    return client.build_request(
        request.method,
        request.url,
        headers=dict(request.headers),
        content=request.body,
    )


def _read_body(response: Any, max_bytes: int | None) -> bytes:
    if max_bytes is None:
        return response.content
    body = bytearray()
    for chunk in response.iter_bytes():
        if len(body) + len(chunk) > max_bytes:
            raise TransportFailure("The catalog response exceeds its configured byte limit.")
        body.extend(chunk)
    return bytes(body)


class HttpxCatalogTransport:
    """Optional pooled synchronous httpx transport."""

    def __init__(
        self,
        *,
        tls_policy: TLSPolicy | None = None,
        budget: TimeBudget | None = None,
        transport: object | None = None,
        max_redirects: int = 10,
        credential_scope: CredentialScope | None = None,
    ) -> None:
        import httpx

        self._httpx = httpx
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
        self._credential_scope = credential_scope
        self._closed = False

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send through httpx and follow redirects under runtime control."""
        if self._closed:
            raise TransportFailure("The httpx catalog transport is closed.")
        current = request
        for _ in range(self._max_redirects + 1):
            try:
                response = self._client.send(
                    _build_request(self._client, current),
                    stream=current.max_response_bytes is not None,
                    follow_redirects=False,
                )
            except self._httpx.HTTPError as exc:
                raise TransportFailure("httpx could not complete the catalog request.") from exc
            if current.redirect_policy is RedirectPolicy.NO_FOLLOW:
                try:
                    return RuntimeResponse(
                        response.status_code,
                        dict(response.headers),
                        _read_body(response, current.max_response_bytes),
                        _retry_after(response.headers.get("retry-after")),
                    )
                finally:
                    response.close()
            location = response.headers.get("location")
            if not response.is_redirect or location is None:
                try:
                    return RuntimeResponse(
                        response.status_code,
                        dict(response.headers),
                        _read_body(response, current.max_response_bytes),
                        _retry_after(response.headers.get("retry-after")),
                    )
                finally:
                    response.close()
            try:
                next_url = urljoin(current.url, location)
                _require_plain_http_target(next_url)
            except ValueError as exc:
                response.close()
                raise TransportFailure(
                    f"httpx received an unusable redirect target {_redacted_redirect_url(location)!r}."
                ) from exc
            response.close()
            headers = dict(current.headers)
            if not _retains_credentials(self._credential_scope, current.url, next_url):
                headers = strip_sensitive_redirect_headers(headers)
            next_method, next_body, next_files = redirect_method_and_body(
                current.method, response.status_code, current.body, current.files
            )
            if next_body is None and not next_files:
                headers = drop_body_transfer_headers(headers)
            current = RuntimeRequest(
                next_method,
                next_url,
                headers,
                next_body,
                next_files,
                current.redirect_policy,
                current.max_response_bytes,
            )
        raise TransportFailure("Catalog redirect limit exceeded.")

    def close(self) -> None:
        """Close the httpx pool once."""
        if not self._closed:
            self._closed = True
            self._client.close()

    def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
        """Send one no-follow request while leaving its response body unbuffered."""
        if self._closed:
            raise TransportFailure("The httpx catalog transport is closed.")
        if request.redirect_policy is not RedirectPolicy.NO_FOLLOW:
            raise ValueError("Streaming catalog requests must explicitly disable redirect following.")
        try:
            if request.files:
                prepared = self._client.build_request(
                    request.method,
                    request.url,
                    headers=dict(drop_body_transfer_headers(request.headers)),
                    data=None,
                    files=[(part.field_name, (part.file_name, part.data, part.content_type)) for part in request.files],
                )
            else:
                prepared = self._client.build_request(
                    request.method,
                    request.url,
                    headers=dict(request.headers),
                    content=request.body,
                )
            response = self._client.send(prepared, stream=True, follow_redirects=False)
        except self._httpx.HTTPError as exc:
            raise TransportFailure("httpx could not open the catalog response stream.") from exc
        return RuntimeStreamResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            chunks=response.iter_bytes(),
            close_callback=response.close,
            retry_after=_retry_after(response.headers.get("retry-after")),
        )


class AsyncHttpxCatalogTransport:
    """Optional pooled asynchronous httpx transport."""

    def __init__(
        self,
        *,
        tls_policy: TLSPolicy | None = None,
        budget: TimeBudget | None = None,
        transport: object | None = None,
        max_redirects: int = 10,
        credential_scope: CredentialScope | None = None,
    ) -> None:
        import httpx

        self._httpx = httpx
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
        self._credential_scope = credential_scope
        self._closed = False

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send asynchronously through httpx without sync delegation."""
        if self._closed:
            raise TransportFailure("The async httpx catalog transport is closed.")
        current = request
        for _ in range(self._max_redirects + 1):
            try:
                response = await self._client.send(
                    _build_request(self._client, current),
                    stream=current.max_response_bytes is not None,
                    follow_redirects=False,
                )
            except self._httpx.HTTPError as exc:
                raise TransportFailure("httpx could not complete the catalog request.") from exc
            if current.redirect_policy is RedirectPolicy.NO_FOLLOW:
                try:
                    return RuntimeResponse(
                        response.status_code,
                        dict(response.headers),
                        _read_body(response, current.max_response_bytes),
                        _retry_after(response.headers.get("retry-after")),
                    )
                finally:
                    await response.aclose()
            location = response.headers.get("location")
            if not response.is_redirect or location is None:
                try:
                    return RuntimeResponse(
                        response.status_code,
                        dict(response.headers),
                        _read_body(response, current.max_response_bytes),
                        _retry_after(response.headers.get("retry-after")),
                    )
                finally:
                    await response.aclose()
            try:
                next_url = urljoin(current.url, location)
                _require_plain_http_target(next_url)
            except ValueError as exc:
                await response.aclose()
                raise TransportFailure(
                    f"httpx received an unusable redirect target {_redacted_redirect_url(location)!r}."
                ) from exc
            await response.aclose()
            headers = dict(current.headers)
            if not _retains_credentials(self._credential_scope, current.url, next_url):
                headers = strip_sensitive_redirect_headers(headers)
            next_method, next_body, next_files = redirect_method_and_body(
                current.method, response.status_code, current.body, current.files
            )
            if next_body is None and not next_files:
                headers = drop_body_transfer_headers(headers)
            current = RuntimeRequest(
                next_method,
                next_url,
                headers,
                next_body,
                next_files,
                current.redirect_policy,
                current.max_response_bytes,
            )
        raise TransportFailure("Catalog redirect limit exceeded.")

    async def aclose(self) -> None:
        """Close the asynchronous httpx pool once."""
        if not self._closed:
            self._closed = True
            await self._client.aclose()

    async def send_stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
        """Send one no-follow request while leaving its response body unbuffered."""
        if self._closed:
            raise TransportFailure("The async httpx catalog transport is closed.")
        if request.redirect_policy is not RedirectPolicy.NO_FOLLOW:
            raise ValueError("Streaming catalog requests must explicitly disable redirect following.")
        try:
            if request.files:
                prepared = self._client.build_request(
                    request.method,
                    request.url,
                    headers=dict(drop_body_transfer_headers(request.headers)),
                    data=None,
                    files=[(part.field_name, (part.file_name, part.data, part.content_type)) for part in request.files],
                )
            else:
                prepared = self._client.build_request(
                    request.method,
                    request.url,
                    headers=dict(request.headers),
                    content=request.body,
                )
            response = await self._client.send(prepared, stream=True, follow_redirects=False)
        except self._httpx.HTTPError as exc:
            raise TransportFailure("httpx could not open the catalog response stream.") from exc
        return AsyncRuntimeStreamResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            chunks=response.aiter_bytes(),
            close_callback=response.aclose,
            retry_after=_retry_after(response.headers.get("retry-after")),
        )


def _retains_credentials(scope: CredentialScope | None, current_url: str, next_url: str) -> bool:
    """Decide whether credential-bearing headers survive this hop."""
    if scope is None:
        return _origin(current_url) == _origin(next_url)
    scheme, host, _ = _origin(next_url)
    return scope.send_on_redirect and scheme in scope.allowed_schemes and host in scope.allowed_hosts
