"""Multipart transport contract tests for UploadPart and RuntimeRequest.files."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from io import BytesIO
from typing import cast

import pytest

httpx = pytest.importorskip("httpx")

from datasluice.runtime.transport.base import (
    RedirectPolicy,
    RuntimeRequest,
    TransportFailure,
    UploadPart,
    UploadStream,
)
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from tests.helpers.httpx_probe import (
    CSV_BYTES,
    UPLOAD_PARTS,
    AsyncProbe,
    SyncProbe,
    at_path,
    fixed,
    redirect_to,
)

_PARTS = UPLOAD_PARTS
_STREAMED = BytesIO(b"a,b\n")


def stream_part(source: BytesIO) -> UploadPart:
    return UploadPart(
        field_name="upload", file_name="data.csv", content_type="text/csv", data=cast(UploadStream, source)
    )


def test_runtime_request_accepts_files_with_none_body() -> None:
    request = RuntimeRequest("POST", "https://example.test/upload", files=_PARTS)

    assert request.files == _PARTS
    assert request.body is None


def test_runtime_request_rejects_body_and_files_together() -> None:
    with pytest.raises(ValueError, match="cannot carry a byte body and multipart parts together"):
        RuntimeRequest("POST", "https://example.test/upload", {}, b"payload", _PARTS)


def test_runtime_request_rejects_stream_parts_with_followed_redirects() -> None:
    part = stream_part(_STREAMED)

    with pytest.raises(ValueError, match="one-shot streams require RedirectPolicy.NO_FOLLOW"):
        RuntimeRequest("POST", "https://example.test/upload", files=(part,))

    request = RuntimeRequest(
        "POST", "https://example.test/upload", files=(part,), redirect_policy=RedirectPolicy.NO_FOLLOW
    )
    assert request.files == (part,)


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
    assert "upload" in part_rendered
    assert "data.csv" in part_rendered
    assert "super-secret-upload-bytes" not in request_rendered
    assert len(request_rendered) < 200


def test_httpx_sends_multipart_through_the_files_channel() -> None:
    with SyncProbe(fixed(200, content=b"stored")) as probe:
        response = probe.send(RuntimeRequest("POST", "https://example.test/upload", files=_PARTS))

    wire = probe.requests[0]
    assert response.body == b"stored"
    assert wire.headers["content-type"].startswith("multipart/form-data")
    assert "boundary=" in wire.headers["content-type"]
    assert b'name="upload"' in wire.content
    assert b'filename="data.csv"' in wire.content
    assert CSV_BYTES in wire.content
    assert b'filename="meta.json"' in wire.content
    assert b'{"ok": true}' in wire.content


def test_async_httpx_sends_multipart_through_the_files_channel() -> None:
    probe = AsyncProbe(fixed(200, content=b"stored"))

    async def send() -> None:
        async with probe:
            await probe.send(RuntimeRequest("POST", "https://example.test/upload", files=_PARTS))

    asyncio.run(send())

    wire = probe.requests[0]
    assert wire.headers["content-type"].startswith("multipart/form-data")
    assert b'name="upload"' in wire.content
    assert CSV_BYTES in wire.content


def test_httpx_transmits_stream_part_bytes_exactly_once() -> None:
    source = BytesIO(CSV_BYTES)
    part = stream_part(source)

    with SyncProbe(fixed(200, content=b"stored")) as probe:
        response = probe.send(
            RuntimeRequest(
                "POST", "https://example.test/upload", files=(part,), redirect_policy=RedirectPolicy.NO_FOLLOW
            )
        )

    assert response.body == b"stored"
    assert CSV_BYTES in probe.requests[0].content
    assert source.tell() == len(CSV_BYTES)


def test_async_httpx_transmits_stream_part_bytes_exactly_once() -> None:
    source = BytesIO(CSV_BYTES)
    part = stream_part(source)
    probe = AsyncProbe(fixed(200, content=b"stored"))

    async def send() -> None:
        async with probe:
            await probe.send(
                RuntimeRequest(
                    "POST", "https://example.test/upload", files=(part,), redirect_policy=RedirectPolicy.NO_FOLLOW
                )
            )

    asyncio.run(send())

    assert CSV_BYTES in probe.requests[0].content
    assert source.tell() == len(CSV_BYTES)


@pytest.mark.parametrize("status", [301, 302, 303])
def test_httpx_redirect_downgrade_drops_files(status: int) -> None:
    with SyncProbe(redirect_to("/next", status, at_path("/start"))) as probe:
        response = probe.send(RuntimeRequest("POST", "https://example.test/start", files=_PARTS))

    follow_up = probe.requests[1]
    assert response.status_code == 200
    assert follow_up.method == "GET"
    assert follow_up.read() == b""
    assert "content-type" not in follow_up.headers


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_redirect_preserves_files(status: int) -> None:
    with SyncProbe(redirect_to("/next", status, at_path("/start"))) as probe:
        response = probe.send(RuntimeRequest("POST", "https://example.test/start", files=_PARTS))

    follow_up = probe.requests[1]
    assert response.status_code == 200
    assert follow_up.method == "POST"
    assert follow_up.headers["content-type"].startswith("multipart/form-data")
    assert CSV_BYTES in follow_up.read()


async def _async_redirect_follow_up(status: int) -> tuple[str, bytes]:
    probe = AsyncProbe(redirect_to("/next", status, at_path("/start")))
    async with probe:
        await probe.send(RuntimeRequest("POST", "https://example.test/start", files=_PARTS))

    return probe.requests[1].method, probe.requests[1].read()


@pytest.mark.parametrize("status", [301, 302, 303])
def test_async_httpx_redirect_downgrade_drops_files(status: int) -> None:
    method, body = asyncio.run(_async_redirect_follow_up(status))

    assert method == "GET"
    assert body == b""


@pytest.mark.parametrize("status", [307, 308])
def test_async_httpx_redirect_preserves_files(status: int) -> None:
    method, body = asyncio.run(_async_redirect_follow_up(status))

    assert method == "POST"
    assert CSV_BYTES in body


def test_urllib_rejects_multipart_with_actionable_message_naming_the_extra() -> None:
    transport = UrllibCatalogTransport()
    try:
        request = RuntimeRequest("POST", "https://example.test/upload", files=_PARTS)
        with pytest.raises(TransportFailure, match=r"datasluice\[http\]") as excinfo:
            transport.send(request)
    finally:
        transport.close()

    assert "httpx transport" in str(excinfo.value)
