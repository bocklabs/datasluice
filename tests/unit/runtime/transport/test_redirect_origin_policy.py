"""Cross-origin redirect body refusal pinned against an explicit CredentialScope opt-in.

``CredentialScope.send_on_redirect`` authorizes relaying credential headers
to an allowed host; it never authorizes relaying a request body to a
different origin. These tests hold that asymmetry down in the urllib
transport and in both httpx loops.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, cast
from urllib.request import Request

import httpx
import pytest

from datasluice.domain import CredentialScope
from datasluice.runtime.transport.base import RuntimeRequest, TransportFailure
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from tests.helpers.httpx_probe import UPLOAD_PARTS, AsyncProbe, SyncProbe

_ALLOWED_SCOPE = CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True)
_BODY = b'{"token": "redirect-body-secret"}'
_HEADERS = {"Content-Type": "application/json", "Authorization": "Bearer secret"}
_ORIGIN = "example.test"


def _hop(status: int, *, allow_target: bool) -> Any:
    def responder(request: httpx.Request) -> httpx.Response:
        if request.url.host != _ORIGIN:
            if not allow_target:
                raise AssertionError("The cross-origin follow-up must never be dispatched.")
            return httpx.Response(200, content=b"redirected")
        return httpx.Response(status, headers={"Location": "https://other.test/next"})

    return responder


class _FakeResponse:
    def __init__(self, status: int, headers: Mapping[str, str], body: bytes = b"") -> None:
        self.status = status
        self.headers = headers
        self._body = body
        self._offset = 0
        self.closed = False

    def read(self, size: int | None = None) -> bytes:
        if size is None:
            chunk = self._body[self._offset :]
            self._offset = len(self._body)
            return chunk
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def close(self) -> None:
        self.closed = True


class _RecordingOpener:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.requests: list[Request] = []
        self._responses = responses

    def open(self, request: Request, *, timeout: float) -> _FakeResponse:
        del timeout
        self.requests.append(request)
        return self._responses[len(self.requests) - 1]


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_cross_origin_body_redirect_refuses_even_with_scope_opt_in(status: int) -> None:
    with SyncProbe(_hop(status, allow_target=False), credential_scope=_ALLOWED_SCOPE) as probe:
        with pytest.raises(TransportFailure, match="different redirect origin") as excinfo:
            probe.send(RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY))

        assert len(probe.requests) == 1
        assert "httpx" in str(excinfo.value)

    assert "redirect-body-secret" not in str(excinfo.value)


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_cross_origin_multipart_redirect_refuses_even_with_scope_opt_in(status: int) -> None:
    with SyncProbe(_hop(status, allow_target=False), credential_scope=_ALLOWED_SCOPE) as probe:
        with pytest.raises(TransportFailure, match="different redirect origin"):
            probe.send(RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), files=UPLOAD_PARTS))

        assert len(probe.requests) == 1


@pytest.mark.parametrize("status", [307, 308])
def test_async_httpx_cross_origin_body_redirect_refuses_even_with_scope_opt_in(status: int) -> None:
    probe = AsyncProbe(_hop(status, allow_target=False), credential_scope=_ALLOWED_SCOPE)

    async def send() -> None:
        async with probe:
            with pytest.raises(TransportFailure, match="different redirect origin"):
                await probe.send(RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY))

    asyncio.run(send())

    assert len(probe.requests) == 1


@pytest.mark.parametrize("status", [307, 308])
def test_urllib_cross_origin_body_redirect_refuses_even_with_scope_opt_in(status: int) -> None:
    opener = _RecordingOpener([_FakeResponse(status, {"Location": "https://other.test/next"})])
    transport = UrllibCatalogTransport(credential_scope=_ALLOWED_SCOPE)
    cast(Any, transport)._opener = opener

    with pytest.raises(TransportFailure, match="different redirect origin") as excinfo:
        transport.send(RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY))

    assert len(opener.requests) == 1
    assert opener.requests[0].data == _BODY
    assert "urllib" in str(excinfo.value)
    assert "redirect-body-secret" not in str(excinfo.value)


def test_httpx_scope_opt_in_still_relays_authorization_once_the_body_is_dropped() -> None:
    with SyncProbe(_hop(302, allow_target=True), credential_scope=_ALLOWED_SCOPE) as probe:
        response = probe.send(RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY))

    follow_up = probe.requests[1]
    forwarded = {key.lower(): value for key, value in follow_up.headers.items()}
    assert response.status_code == 200
    assert follow_up.method == "GET"
    assert forwarded["authorization"] == "Bearer secret"


def test_urllib_scope_opt_in_still_relays_authorization_once_the_body_is_dropped() -> None:
    opener = _RecordingOpener([_FakeResponse(302, {"Location": "https://other.test/next"}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport(credential_scope=_ALLOWED_SCOPE)
    cast(Any, transport)._opener = opener

    transport.send(RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert forwarded["authorization"] == "Bearer secret"
    assert opener.requests[1].data is None
