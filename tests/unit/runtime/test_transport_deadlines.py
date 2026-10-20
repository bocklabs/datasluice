from __future__ import annotations

import asyncio

import httpx
import pytest

from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import BudgetExhaustedError
from datasluice.runtime.resilience import DeadlineMonitor
from datasluice.runtime.transport.base import RuntimeRequest
from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport, HttpxCatalogTransport


def test_httpx_stops_a_response_at_the_operation_deadline() -> None:
    now = [0.0]
    closed = []

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            yield b"first"
            now[0] = 2.0
            yield b"late"
            pytest.fail("The transport consumed bytes after the deadline.")

        def close(self) -> None:
            closed.append(True)

    deadline = DeadlineMonitor(TimeBudget(connect=1, read=1, write=1, total=1), clock=lambda: now[0])
    transport = HttpxCatalogTransport(transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Stream())))
    request = RuntimeRequest("GET", "https://example.test", max_response_bytes=100, deadline=deadline)
    try:
        with pytest.raises(BudgetExhaustedError):
            transport.send(request)
    finally:
        transport.close()
    assert closed == [True]


def test_async_httpx_stops_a_response_at_the_operation_deadline() -> None:
    now = [0.0]
    closed = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"first"
            now[0] = 2.0
            yield b"late"
            pytest.fail("The transport consumed bytes after the deadline.")

        async def aclose(self) -> None:
            closed.append(True)

    async def run() -> None:
        deadline = DeadlineMonitor(TimeBudget(connect=1, read=1, write=1, total=1), clock=lambda: now[0])
        transport = AsyncHttpxCatalogTransport(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Stream()))
        )
        request = RuntimeRequest("GET", "https://example.test", max_response_bytes=100, deadline=deadline)
        try:
            with pytest.raises(BudgetExhaustedError):
                await transport.send(request)
        finally:
            await transport.aclose()

    asyncio.run(run())
    assert closed == [True]


def test_redirect_preserves_the_operation_deadline() -> None:
    now = [0.0]
    calls = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert all(0 < value <= 1 for value in request.extensions["timeout"].values())
        now[0] = 2.0
        return httpx.Response(302, headers={"Location": "/next"})

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(respond))
    deadline = DeadlineMonitor(TimeBudget(connect=1, read=1, write=1, total=1), clock=lambda: now[0])
    request = RuntimeRequest("GET", "https://example.test", deadline=deadline)
    try:
        with pytest.raises(BudgetExhaustedError):
            transport.send(request)
    finally:
        transport.close()
    assert len(calls) == 1
