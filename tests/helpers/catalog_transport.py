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
        request = Request(url, headers=dict(headers or {}), method="GET")
        with urlopen(request, timeout=2) as response:  # noqa: S310
            return LoopbackResponse(
                status=response.status, headers=dict(response.headers.items()), body=response.read()
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
            body = await reader.readexactly(int(response_headers.get("Content-Length", "0")))
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
