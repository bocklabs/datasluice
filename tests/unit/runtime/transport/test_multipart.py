"""Multipart transport contract tests for UploadPart and RuntimeRequest.files."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError

import pytest

httpx = pytest.importorskip("httpx")

from datasluice.runtime.transport.base import (
    RuntimeRequest,
    UploadPart,
)
from datasluice.runtime.transport.httpx_transport import (
    AsyncHttpxCatalogTransport,
    HttpxCatalogTransport,
)
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport

_PARTS = (
    UploadPart(field_name="upload", file_name="data.csv", content_type="text/csv", data=b"a,b\n1,2"),
    UploadPart(field_name="meta", file_name="meta.json", content_type="application/json", data=b'{"ok": true}'),
)


def test_runtime_request_accepts_files_with_none_body() -> None:
    request = RuntimeRequest("POST", "https://example.test/upload", files=_PARTS)

    assert request.files == _PARTS
    assert request.body is None


def test_runtime_request_rejects_body_and_files_together() -> None:
    with pytest.raises(ValueError, match="cannot carry a byte body and multipart parts together"):
        RuntimeRequest("POST", "https://example.test/upload", {}, b"payload", _PARTS)


def test_runtime_request_freezes_parts_into_a_tuple() -> None:
    parts = list(_PARTS)
    request = RuntimeRequest("POST", "https://example.test/upload", files=parts)  # ty: ignore[invalid-argument-type]

    parts.append(UploadPart(field_name="extra", data=b"x"))
    assert request.files == _PARTS


def test_upload_part_is_frozen_and_validates_its_fields() -> None:
    part = _PARTS[0]
    with pytest.raises(FrozenInstanceError):
        part.data = b"mutated"  # ty: ignore[invalid-assignment]
    with pytest.raises(ValueError, match="non-empty"):
        UploadPart(field_name="", data=b"x")
    with pytest.raises(ValueError, match="bytes"):
        UploadPart(field_name="upload", data="text")  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="file names"):
        UploadPart(field_name="upload", data=b"x", file_name=5)  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="content types"):
        UploadPart(field_name="upload", data=b"x", content_type=[])  # ty: ignore[invalid-argument-type]


def test_reprs_render_field_names_and_lengths_but_never_part_bytes() -> None:
    secret = b"super-secret-upload-bytes"
    part = UploadPart(field_name="upload", file_name="data.csv", content_type="text/csv", data=secret)
    request = RuntimeRequest("POST", "https://example.test/upload", files=(part,))

    part_rendered = repr(part)
    request_rendered = repr(request)

    assert "super-secret-upload-bytes" not in part_rendered
    assert str(len(secret)) in part_rendered
    assert "upload" in part_rendered and "data.csv" in part_rendered
    assert "super-secret-upload-bytes" not in request_rendered
    assert len(request_rendered) < 200


def test_httpx_sends_multipart_through_the_files_channel() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"stored")

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(RuntimeRequest("POST", "https://example.test/upload", files=_PARTS))
    finally:
        transport.close()

    wire = seen[0]
    assert response.body == b"stored"
    assert wire.headers["content-type"].startswith("multipart/form-data")
    assert "boundary=" in wire.headers["content-type"]
    assert b'name="upload"' in wire.content
    assert b'filename="data.csv"' in wire.content
    assert b"a,b\n1,2" in wire.content
    assert b'filename="meta.json"' in wire.content
    assert b'{"ok": true}' in wire.content


def test_async_httpx_sends_multipart_through_the_files_channel() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"stored")

    async def send() -> None:
        transport = AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler))
        try:
            await transport.send(RuntimeRequest("POST", "https://example.test/upload", files=_PARTS))
        finally:
            await transport.aclose()

    asyncio.run(send())

    wire = seen[0]
    assert wire.headers["content-type"].startswith("multipart/form-data")
    assert b'name="upload"' in wire.content
    assert b"a,b\n1,2" in wire.content


@pytest.mark.parametrize("status", [301, 302, 303])
def test_httpx_redirect_downgrade_drops_files(status: int) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(status, headers={"Location": "/next"})
        return httpx.Response(200)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(RuntimeRequest("POST", "https://example.test/start", files=_PARTS))
    finally:
        transport.close()

    follow_up = seen[1]
    assert response.status_code == 200
    assert follow_up.method == "GET"
    assert follow_up.read() == b""
    assert "content-type" not in follow_up.headers


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_redirect_preserves_files(status: int) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(status, headers={"Location": "/next"})
        return httpx.Response(200)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(RuntimeRequest("POST", "https://example.test/start", files=_PARTS))
    finally:
        transport.close()

    follow_up = seen[1]
    assert response.status_code == 200
    assert follow_up.method == "POST"
    assert follow_up.headers["content-type"].startswith("multipart/form-data")
    assert b"a,b\n1,2" in follow_up.read()


async def _async_redirect_follow_up(status: int) -> tuple[str, bytes]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(status, headers={"Location": "/next"})
        return httpx.Response(200)

    transport = AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        await transport.send(RuntimeRequest("POST", "https://example.test/start", files=_PARTS))
    finally:
        await transport.aclose()

    return seen[1].method, seen[1].read()


@pytest.mark.parametrize("status", [301, 302, 303])
def test_async_httpx_redirect_downgrade_drops_files(status: int) -> None:
    method, body = asyncio.run(_async_redirect_follow_up(status))

    assert method == "GET"
    assert body == b""


@pytest.mark.parametrize("status", [307, 308])
def test_async_httpx_redirect_preserves_files(status: int) -> None:
    method, body = asyncio.run(_async_redirect_follow_up(status))

    assert method == "POST"
    assert b"a,b\n1,2" in body


def test_urllib_rejects_multipart_with_actionable_message_naming_the_extra() -> None:
    transport = UrllibCatalogTransport()
    try:
        with pytest.raises(ValueError, match=r"datasluice\[http\]") as excinfo:
            transport.send(RuntimeRequest("POST", "https://example.test/upload", files=_PARTS))
    finally:
        transport.close()

    assert "httpx transport" in str(excinfo.value)
