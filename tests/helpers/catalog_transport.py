"""Independent loopback transports used only by catalog contract tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import SplitResult, urlsplit
from urllib.request import Request, urlopen

if TYPE_CHECKING:
    from collections.abc import Mapping
    from http.client import HTTPResponse


def _loopback_url(url: str) -> SplitResult:
    """Return the parsed parts of one explicit loopback HTTP URL, rejecting anything else."""
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port is None:
        raise ValueError("Catalog test transports only connect to explicit loopback HTTP URLs.")
    return parsed


def _request_target(parsed: SplitResult) -> str:
    """Return the origin-form request target for one parsed loopback URL."""
    target = parsed.path or "/"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    return target


def _status_code(status_line: bytes) -> int:
    """Return the numeric status carried by one HTTP status line."""
    parts = status_line.decode("ascii").rstrip("\r\n").split(" ", 2)
    if len(parts) < 2 or not parts[1].isdigit():
        raise RuntimeError("Loopback fixture returned an invalid HTTP status line.")
    return int(parts[1])


async def _response_headers(reader: asyncio.StreamReader) -> dict[str, str]:
    """Read the header block of one buffered loopback response."""
    headers: dict[str, str] = {}
    while line := await reader.readline():
        if line == b"\r\n":
            break
        key, value = line.decode("ascii").rstrip("\r\n").split(":", 1)
        headers[key] = value.strip()
    return headers


@dataclass(frozen=True, slots=True)
class LoopbackResponse:
    """A fully buffered response captured from the fixture-owned socket."""

    status: int
    headers: Mapping[str, str]
    body: bytes


class SyncLoopbackTransport:
    """A test-only synchronous urllib transport for localhost fixture servers."""

    def __init__(self) -> None:
        self.close_count = 0
        self.closed = False

    def get(self, url: str, *, headers: Mapping[str, str] | None = None) -> LoopbackResponse:
        """Issue one loopback GET without using the asynchronous transport."""
        if self.closed:
            raise RuntimeError("The synchronous loopback transport is closed.")
        _loopback_url(url)
        request = Request(url, headers=dict(headers or {}), method="GET")
        with urlopen(request, timeout=2) as response:
            return LoopbackResponse(
                status=response.status,
                headers=dict(response.headers.items()),
                body=_sync_body(response),
            )

    def close(self) -> None:
        """Close the test transport exactly once."""
        if not self.closed:
            self.closed = True
            self.close_count += 1


class AsyncLoopbackTransport:
    """A test-only asyncio stream transport for localhost fixture servers."""

    def __init__(self) -> None:
        self.close_count = 0
        self.closed = False
        self._writer: asyncio.StreamWriter | None = None

    def _request(self, parsed: SplitResult, headers: Mapping[str, str] | None) -> str:
        """Return the HTTP/1.1 request text for one parsed loopback URL."""
        request_headers = {"Host": parsed.netloc, "Connection": "close", **dict(headers or {})}
        return "".join(
            [
                f"GET {_request_target(parsed)} HTTP/1.1\r\n",
                *(f"{key}: {value}\r\n" for key, value in request_headers.items()),
                "\r\n",
            ]
        )

    def _release_writer(self, writer: asyncio.StreamWriter) -> None:
        """Forget *writer* as the retained stream when it is still the retained one."""
        if self._writer is writer:
            self._writer = None

    async def get(self, url: str, *, headers: Mapping[str, str] | None = None) -> LoopbackResponse:
        """Issue one loopback GET through an independent asyncio socket stream."""
        if self.closed:
            raise RuntimeError("The asynchronous loopback transport is closed.")
        parsed = _loopback_url(url)
        reader, writer = await asyncio.open_connection(parsed.hostname, parsed.port)
        self._writer = writer
        request = self._request(parsed, headers)
        try:
            writer.write(request.encode("ascii"))
            await writer.drain()
            status = _status_code(await reader.readline())
            response_headers = await _response_headers(reader)
            body = await _async_body(reader, response_headers)
            return LoopbackResponse(status=status, headers=response_headers, body=body)
        finally:
            writer.close()
            await writer.wait_closed()
            self._release_writer(writer)

    async def aclose(self) -> None:
        """Release an active stream and mark this transport closed once."""
        if not self.closed:
            self.closed = True
            self.close_count += 1
            if self._writer is not None:
                self._writer.close()
                await self._writer.wait_closed()
                self._writer = None


def _header(headers: Mapping[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name), None)


def _sync_body(response: HTTPResponse) -> bytes:
    return response.read()


async def _async_body(reader: asyncio.StreamReader, headers: Mapping[str, str]) -> bytes:
    if _header(headers, "transfer-encoding") == "chunked":
        parts = bytearray()
        while True:
            size = int((await reader.readline()).split(b";", 1)[0], 16)
            if size == 0:
                while (await reader.readline()) not in (b"", b"\r\n", b"\n"):
                    continue
                return bytes(parts)
            parts.extend(await reader.readexactly(size))
            await reader.readexactly(2)
    content_length = int(_header(headers, "content-length") or "0")
    return await reader.readexactly(content_length)
