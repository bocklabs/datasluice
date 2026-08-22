"""Regression coverage for Parquet delivered over HTTP through the data plane.

Guards the C16 fix: gzip-compressed Parquet served without a ``Content-Encoding``
header is detected by magic bytes and decompressed before format dispatch, while
uncompressed bodies take the seekable fast path.
"""

from __future__ import annotations

import gzip
import io

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("httpx")

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from datasluice.data.access import DataPlaneResourceReader  # noqa: E402
from datasluice.domain import HttpDownload, Resource  # noqa: E402
from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport  # noqa: E402
from tests.helpers.http_server import MockResponse, start_test_server  # noqa: E402


def _parquet_bytes() -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), buffer)
    return buffer.getvalue()


def _open_table(transport: HttpxCatalogTransport, base: str) -> pa.Table:
    resource = Resource(
        id="http-parquet",
        url=f"{base}/data.parquet",
        format="PARQUET",
        access=HttpDownload(url=f"{base}/data.parquet"),
    )
    with DataPlaneResourceReader(transport=transport).open(resource) as stream:
        return pa.Table.from_batches(list(stream.iter_batches()), schema=stream.schema)


def test_gzip_http_parquet_without_content_encoding_header_parses() -> None:
    """Magic-byte sniffing decompresses gzipped HTTP Parquet lacking Content-Encoding."""
    server, base = start_test_server({"/data.parquet": MockResponse(body=gzip.compress(_parquet_bytes()))})
    transport = HttpxCatalogTransport()
    try:
        table = _open_table(transport, base)

        assert table.num_rows == 3
        assert table.column("name").to_pylist() == ["a", "b", "c"]
    finally:
        transport.close()
        server.shutdown()
        server.server_close()


def test_uncompressed_http_parquet_takes_seekable_fast_path() -> None:
    """Uncompressed HTTP Parquet bodies are read directly without decompression."""
    server, base = start_test_server({"/data.parquet": MockResponse(body=_parquet_bytes())})
    transport = HttpxCatalogTransport()
    try:
        table = _open_table(transport, base)

        assert table.num_rows == 3
        assert table.column("id").to_pylist() == [1, 2, 3]
    finally:
        transport.close()
        server.shutdown()
        server.server_close()
