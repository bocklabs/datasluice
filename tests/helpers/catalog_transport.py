"""Independent loopback transports used only by catalog contract tests."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


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
        parsed = urlsplit(url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port is None:
            raise ValueError("Catalog test transports only connect to explicit loopback HTTP URLs.")
        request = Request(url, headers=dict(headers or {}), method="GET")
        with urlopen(request, timeout=2) as response:  # noqa: S310
            response_headers = dict(response.headers.items())
            return LoopbackResponse(
                status=response.status, headers=response_headers, body=_sync_body(response, response_headers)
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

    async def get(self, url: str, *, headers: Mapping[str, str] | None = None) -> LoopbackResponse:
        """Issue one loopback GET through an independent asyncio socket stream."""
        if self.closed:
            raise RuntimeError("The asynchronous loopback transport is closed.")
        parsed = urlsplit(url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port is None:
            raise ValueError("Catalog test transports only connect to explicit loopback HTTP URLs.")
        reader, writer = await asyncio.open_connection(parsed.hostname, parsed.port)
        self._writer = writer
        target = parsed.path or "/"
        if parsed.query:
            target = f"{target}?{parsed.query}"
        request_headers = {"Host": parsed.netloc, "Connection": "close", **dict(headers or {})}
        request = "".join(
            [f"GET {target} HTTP/1.1\r\n", *(f"{key}: {value}\r\n" for key, value in request_headers.items()), "\r\n"]
        )
        try:
            writer.write(request.encode("ascii"))
            await writer.drain()
            status_line = await reader.readline()
            parts = status_line.decode("ascii").rstrip("\r\n").split(" ", 2)
            if len(parts) < 2 or not parts[1].isdigit():
                raise RuntimeError("Loopback fixture returned an invalid HTTP status line.")
            response_headers: dict[str, str] = {}
            while line := await reader.readline():
                if line == b"\r\n":
                    break
                key, value = line.decode("ascii").rstrip("\r\n").split(":", 1)
                response_headers[key] = value.strip()
            body = await _async_body(reader, response_headers)
            return LoopbackResponse(status=int(parts[1]), headers=response_headers, body=body)
        finally:
            writer.close()
            await writer.wait_closed()
            if self._writer is writer:
                self._writer = None

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


def _decode_chunked(body: bytes) -> bytes:
    decoded = bytearray()
    remaining = body
    while True:
        line, _, remaining = remaining.partition(b"\r\n")
        size = int(line.split(b";", 1)[0], 16)
        if size == 0:
            return bytes(decoded)
        decoded.extend(remaining[:size])
        remaining = remaining[size + 2 :]


def _sync_body(response: object, headers: Mapping[str, str]) -> bytes:
    body = response.read()  # ty: ignore[unresolved-attribute]
    return _decode_chunked(body) if _header(headers, "transfer-encoding") == "chunked" else body


async def _async_body(reader: asyncio.StreamReader, headers: Mapping[str, str]) -> bytes:
    if _header(headers, "transfer-encoding") == "chunked":
        parts = bytearray()
        while True:
            size = int((await reader.readline()).split(b";", 1)[0], 16)
            if size == 0:
                await reader.readline()
                return bytes(parts)
            parts.extend(await reader.readexactly(size))
            await reader.readexactly(2)
    return await reader.readexactly(int(_header(headers, "content-length") or "0"))
