from __future__ import annotations

from collections.abc import Callable, Mapping
from types import TracebackType
from typing import Self

import httpx

from datasluice.domain import CredentialScope
from datasluice.runtime.transport.base import (
    AsyncRuntimeStreamResponse,
    RuntimeRequest,
    RuntimeResponse,
    RuntimeStreamResponse,
    UploadPart,
)
from datasluice.runtime.transport.httpx_transport import (
    AsyncHttpxCatalogTransport,
    HttpxCatalogTransport,
)

Responder = Callable[[httpx.Request], httpx.Response]
Match = Callable[[httpx.Request], bool]

CSV_BYTES = b"a,b\n1,2"

UPLOAD_PARTS = (
    UploadPart(field_name="upload", file_name="data.csv", content_type="text/csv", data=CSV_BYTES),
    UploadPart(field_name="meta", file_name="meta.json", content_type="application/json", data=b'{"ok": true}'),
)


def at_path(path: str) -> Match:
    return lambda request: request.url.path == path


def at_host(host: str) -> Match:
    return lambda request: request.url.host == host


def fixed(status: int, *, content: bytes = b"", headers: Mapping[str, str] | None = None) -> Responder:
    def responder(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(status, content=content, headers=dict(headers or {}))

    return responder


def redirect_to(location: str, status: int, match: Match) -> Responder:
    def responder(request: httpx.Request) -> httpx.Response:
        if match(request):
            return httpx.Response(status, headers={"Location": location})
        return httpx.Response(200, content=b"redirected")

    return responder


def redirect_then_fail(message: str, location: str, status: int, origin: str) -> Responder:
    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.host != origin:
            raise AssertionError(message)
        return httpx.Response(status, headers={"Location": location})

    return responder


def redirect_loop(status: int, prefix: str) -> Responder:
    hops: list[int] = []

    def responder(request: httpx.Request) -> httpx.Response:
        hops.append(len(hops))
        return httpx.Response(status, headers={"Location": f"{prefix}{len(hops)}"})

    return responder


def header_view(request: httpx.Request) -> dict[str, str]:
    return {key.lower(): value for key, value in request.headers.items()}


class SyncProbe:
    def __init__(
        self, responder: Responder, *, max_redirects: int = 10, credential_scope: CredentialScope | None = None
    ):
        self.requests: list[httpx.Request] = []
        self._responder = responder
        self._transport = HttpxCatalogTransport(
            transport=httpx.MockTransport(self._respond),
            max_redirects=max_redirects,
            credential_scope=credential_scope,
        )

    def _respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._responder(request)

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._transport.send(request)

    def stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
        return self._transport.send_stream(request)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class AsyncProbe:
    def __init__(
        self, responder: Responder, *, max_redirects: int = 10, credential_scope: CredentialScope | None = None
    ):
        self.requests: list[httpx.Request] = []
        self._responder = responder
        self._transport = AsyncHttpxCatalogTransport(
            transport=httpx.MockTransport(self._respond),
            max_redirects=max_redirects,
            credential_scope=credential_scope,
        )

    def _respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._responder(request)

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return await self._transport.send(request)

    async def stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
        return await self._transport.send_stream(request)

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()
