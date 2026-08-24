"""Regression coverage for Parquet delivered over HTTP through the data plane.

Guards the C16 fix: gzip-compressed Parquet served without a ``Content-Encoding``
header is detected by magic bytes and decompressed before format dispatch, the
same body served WITH ``Content-Encoding: gzip`` is decompressed through the
header hint, and uncompressed bodies take the seekable fast path without ever
entering the decompression branch.
"""

from __future__ import annotations

import gzip
import io

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("httpx")

import pyarrow as pa
import pyarrow.parquet as pq

from datasluice.data.access import DataPlaneResourceReader
from datasluice.domain import HttpDownload, Resource
from datasluice.runtime.transport.base import RuntimeResponse
from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from tests.helpers.http_server import MockResponse, start_test_server


def _parquet_bytes() -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), buffer)
    return buffer.getvalue()


def _open_table(transport: HttpxCatalogTransport | UrllibCatalogTransport, base: str) -> pa.Table:
    resource = Resource(
        id="http-parquet",
        url=f"{base}/data.parquet",
        format="PARQUET",
        access=HttpDownload(url=f"{base}/data.parquet"),
    )
    with DataPlaneResourceReader(transport=transport).open(resource) as stream:
        return pa.Table.from_batches(list(stream.iter_batches()), schema=stream.schema)


def _serve_parquet(response: MockResponse) -> pa.Table:
    """Read one scripted Parquet response through the data plane with teardown-safe cleanup."""
    server, base = start_test_server({"/data.parquet": response})
    transport = HttpxCatalogTransport()
    try:
        return _open_table(transport, base)
    finally:
        transport.close()
        server.shutdown()
        server.server_close()


def test_gzip_http_parquet_without_content_encoding_header_parses() -> None:
    """Magic-byte sniffing decompresses gzipped HTTP Parquet lacking Content-Encoding."""
    table = _serve_parquet(MockResponse(body=gzip.compress(_parquet_bytes())))

    assert table.num_rows == 3
    assert table.column("name").to_pylist() == ["a", "b", "c"]


def test_gzip_http_parquet_with_content_encoding_header_parses() -> None:
    """A Content-Encoding: gzip header drives decompression of the still-encoded body.

    The stdlib transport is used because httpx decodes Content-Encoding
    transparently before the data plane sees the response, which leaves no
    encoded body for the header hint to act on; urllib delivers the raw bytes.
    """
    response = MockResponse(body=gzip.compress(_parquet_bytes()), headers={"Content-Encoding": "gzip"})
    server, base = start_test_server({"/data.parquet": response})
    transport = UrllibCatalogTransport()
    try:
        table = _open_table(transport, base)

        assert table.num_rows == 3
        assert table.column("id").to_pylist() == [1, 2, 3]
    finally:
        transport.close()
        server.shutdown()
        server.server_close()


def test_stale_content_encoding_header_does_not_double_decode() -> None:
    """A decoded body carrying a stale Content-Encoding hint is read uncompressed.

    Body magic bytes win over the header hint, so transports that hand back an
    already-decoded payload while keeping the response header are not double-decoded.
    """

    class _StaleHintTransport:
        def send(self, request: object) -> RuntimeResponse:
            return RuntimeResponse(status_code=200, headers={"Content-Encoding": "gzip"}, body=_parquet_bytes())

        def close(self) -> None:
            return None

    resource = Resource(
        id="stale-hint-parquet",
        url="https://catalog.example.test/data.parquet",
        format="PARQUET",
        access=HttpDownload(url="https://catalog.example.test/data.parquet"),
    )
    with DataPlaneResourceReader(transport=_StaleHintTransport()).open(resource) as stream:
        table = pa.Table.from_batches(list(stream.iter_batches()), schema=stream.schema)

    assert table.num_rows == 3
    assert table.column("id").to_pylist() == [1, 2, 3]


def test_uncompressed_http_parquet_takes_seekable_fast_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Uncompressed HTTP Parquet is read directly; the decompression branch is never entered."""

    def _forbidden_decompression(source: object, content_encoding: str | None = None) -> object:
        raise AssertionError(
            f"decompression branch entered for an uncompressed body (content_encoding={content_encoding!r})"
        )

    monkeypatch.setattr("datasluice.data.access.apply_compression", _forbidden_decompression)
    server, base = start_test_server({"/data.parquet": MockResponse(body=_parquet_bytes())})
    transport = HttpxCatalogTransport()
    try:
        table = _open_table(transport, base)

        assert table.num_rows == 3
        assert table.column("id").to_pylist() == [1, 2, 3]
        assert table.column("name").to_pylist() == ["a", "b", "c"]
    finally:
        transport.close()
        server.shutdown()
        server.server_close()
