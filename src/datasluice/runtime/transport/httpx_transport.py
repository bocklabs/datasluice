"""Optional httpx transports with a common runtime response shape."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin, urlsplit

from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.transport._shared import (
    ALLOWED_REDIRECT_SCHEMES,
    _enforce_body_limit,
    _follow_location,
    _next_redirect_request,
    _redacted_redirect_url,
    _retry_after,
)
from datasluice.runtime.transport.base import (
    AsyncRuntimeStreamResponse,
    RedirectPolicy,
    RuntimeRequest,
    RuntimeResponse,
    RuntimeStreamResponse,
    TransportError,
    drop_body_transfer_headers,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from datasluice.domain import CredentialScope


def _require_plain_http_target(url: str) -> None:
    """Reject redirect targets outside plain HTTP(S) or with malformed ports."""
    parsed = urlsplit(url)
    if parsed.scheme.lower() not in ALLOWED_REDIRECT_SCHEMES:
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


def _redirect_request(
    request: RuntimeRequest, status: int, location: str, credential_scope: CredentialScope | None
) -> RuntimeRequest:
    try:
        next_url = urljoin(request.url, location)
        _require_plain_http_target(next_url)
    except ValueError as exc:
        raise TransportError(
            f"httpx received an unusable redirect target {_redacted_redirect_url(location)!r}."
        ) from exc
    return _next_redirect_request(request, status, next_url, credential_scope, "httpx")


def _runtime_response(response: Any, request: RuntimeRequest) -> RuntimeResponse:
    try:
        return RuntimeResponse(
            response.status_code,
            dict(response.headers),
            _read_body(response, request.max_response_bytes),
            _retry_after(response.headers.get("retry-after")),
        )
    finally:
        response.close()


async def _runtime_response_async(response: Any, request: RuntimeRequest, httpx: Any) -> RuntimeResponse:
    try:
        try:
            body = await _read_body_async(response, request.max_response_bytes)
        except httpx.HTTPError as exc:
            raise TransportError("httpx could not read the catalog response.") from exc
        return RuntimeResponse(
            response.status_code,
            dict(response.headers),
            body,
            _retry_after(response.headers.get("retry-after")),
        )
    finally:
        await response.aclose()


def _read_body(response: Any, max_bytes: int | None) -> bytes:
    if max_bytes is None:
        return response.content
    body = bytearray()
    for chunk in response.iter_bytes():
        _enforce_body_limit(len(body) + len(chunk), max_bytes)
        body.extend(chunk)
    return bytes(body)


async def _read_body_async(response: Any, max_bytes: int | None) -> bytes:
    if max_bytes is None:
        return await response.aread()
    body = bytearray()
    async for chunk in response.aiter_bytes():
        _enforce_body_limit(len(body) + len(chunk), max_bytes)
        body.extend(chunk)
    return bytes(body)


class _HttpxTransportBase:
    """Shared httpx client construction behind the synchronous and asynchronous loops.

    Both loops apply the one base-transport redirect policy: sensitive
    headers are re-evaluated against the target origin and any configured
    :class:`CredentialScope`, and a hop whose normalized origin differs from
    the current one refuses to carry a request body or multipart parts what
    the scope allows, so a cross-origin 307/308 never relays either.
    """

    def __init__(
        self,
        label: str,
        client_type: Any,
        *,
        tls_policy: TLSPolicy | None,
        budget: TimeBudget | None,
        transport: object | None,
        max_redirects: int,
        credential_scope: CredentialScope | None,
    ) -> None:
        import httpx

        self._httpx = httpx
        self._label = label
        policy = tls_policy or TLSPolicy()
        budget = budget or TimeBudget()
        self._client: Any = client_type(
            timeout=httpx.Timeout(connect=budget.connect, read=budget.read, write=budget.write, pool=10.0),
            limits=httpx.Limits(),
            verify=policy.verify,
            transport=cast("Any", transport),
            follow_redirects=False,
        )
        self._max_redirects = max_redirects
        self._credential_scope = credential_scope
        self._closed = False

    def _assert_open(self) -> None:
        if self._closed:
            raise TransportError(f"The {self._label} catalog transport is closed.")

    def _assert_streamable(self, request: RuntimeRequest) -> None:
        self._assert_open()
        if request.redirect_policy is not RedirectPolicy.NO_FOLLOW:
            raise ValueError("Streaming catalog requests must explicitly disable redirect following.")

    def _stream_metadata(self, response: Any) -> tuple[int, dict[str, str], float | None]:
        return (
            response.status_code,
            dict(response.headers),
            _retry_after(response.headers.get("retry-after")),
        )

    def _stream_chunks(self, response: Any) -> Iterator[bytes]:
        try:
            yield from response.iter_bytes()
        except self._httpx.HTTPError as exc:
            raise TransportError("httpx could not read the catalog response stream.") from exc

    async def _stream_chunks_async(self, response: Any) -> AsyncIterator[bytes]:
        try:
            async for chunk in response.aiter_bytes():
                yield chunk
        except self._httpx.HTTPError as exc:
            raise TransportError("httpx could not read the catalog response stream.") from exc


class HttpxCatalogTransport(_HttpxTransportBase):
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

        super().__init__(
            "httpx",
            httpx.Client,
            tls_policy=tls_policy,
            budget=budget,
            transport=transport,
            max_redirects=max_redirects,
            credential_scope=credential_scope,
        )

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send through httpx and follow redirects under runtime control."""
        self._assert_open()
        current = request
        for _ in range(self._max_redirects + 1):
            try:
                response = self._client.send(
                    _build_request(self._client, current),
                    stream=current.max_response_bytes is not None,
                    follow_redirects=False,
                )
            except self._httpx.HTTPError as exc:
                raise TransportError("httpx could not complete the catalog request.") from exc
            location = _follow_location(current, response)
            if location is None:
                return _runtime_response(response, current)
            try:
                current = _redirect_request(current, response.status_code, location, self._credential_scope)
            finally:
                response.close()
        raise TransportError("Catalog redirect limit exceeded.")

    def close(self) -> None:
        """Close the httpx pool once."""
        if not self._closed:
            self._closed = True
            self._client.close()

    def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
        """Send one no-follow request while leaving its response body unbuffered."""
        self._assert_streamable(request)
        try:
            response = self._client.send(_build_request(self._client, request), stream=True, follow_redirects=False)
        except self._httpx.HTTPError as exc:
            raise TransportError("httpx could not open the catalog response stream.") from exc
        status_code, headers, retry_after = self._stream_metadata(response)
        return RuntimeStreamResponse(
            status_code=status_code,
            headers=headers,
            chunks=self._stream_chunks(response),
            close_callback=response.close,
            retry_after=retry_after,
        )


class AsyncHttpxCatalogTransport(_HttpxTransportBase):
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

        super().__init__(
            "async httpx",
            httpx.AsyncClient,
            tls_policy=tls_policy,
            budget=budget,
            transport=transport,
            max_redirects=max_redirects,
            credential_scope=credential_scope,
        )

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send asynchronously through httpx without sync delegation."""
        self._assert_open()
        current = request
        for _ in range(self._max_redirects + 1):
            try:
                response = await self._client.send(
                    _build_request(self._client, current),
                    stream=current.max_response_bytes is not None,
                    follow_redirects=False,
                )
            except self._httpx.HTTPError as exc:
                raise TransportError("httpx could not complete the catalog request.") from exc
            location = _follow_location(current, response)
            if location is None:
                return await _runtime_response_async(response, current, self._httpx)
            try:
                current = _redirect_request(current, response.status_code, location, self._credential_scope)
            finally:
                await response.aclose()
        raise TransportError("Catalog redirect limit exceeded.")

    async def aclose(self) -> None:
        """Close the asynchronous httpx pool once."""
        if not self._closed:
            self._closed = True
            await self._client.aclose()

    async def send_stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
        """Send one no-follow request while leaving its response body unbuffered."""
        self._assert_streamable(request)
        try:
            response = await self._client.send(
                _build_request(self._client, request), stream=True, follow_redirects=False
            )
        except self._httpx.HTTPError as exc:
            raise TransportError("httpx could not open the catalog response stream.") from exc
        status_code, headers, retry_after = self._stream_metadata(response)
        return AsyncRuntimeStreamResponse(
            status_code=status_code,
            headers=headers,
            chunks=self._stream_chunks_async(response),
            close_callback=response.aclose,
            retry_after=retry_after,
        )
