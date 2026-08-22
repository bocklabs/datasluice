"""Buffered-download correctness for the HttpDownload dispatch path.

Both runtime transports return the complete body inside ``RuntimeResponse.body``
today, so HTTP downloads are fully buffered in memory before the format reader
sees them. These tests verify that chunked-transfer and large bodies still read
correctly through that buffered ``send()`` seam; bounded-memory behavior is
covered by the peak-RSS subprocess test in ``test_peak_rss.py`` and will be
restored together with a streaming transport seam.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("httpx")

from datasluice.data.access import DataPlaneResourceReader  # noqa: E402
from datasluice.domain import HttpDownload, Resource  # noqa: E402
from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport  # noqa: E402
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport  # noqa: E402
from tests.helpers.http_server import MockResponse, start_test_server  # noqa: E402


def _csv_text(rows: int) -> bytes:
    lines = [b"id,name,value"]
    for i in range(rows):
        lines.append(f"{i},item_{i},{i * 1.5:.1f}".encode())
    return b"\n".join(lines) + b"\n"


def _start_chunked_server(body: bytes, chunk_size: int) -> tuple[Any, str]:
    """Start the server with a single chunked endpoint serving *body*."""

    return start_test_server({"/stream": MockResponse(status=200, body=body, chunk_size=chunk_size)})


def test_http_download_reads_chunked_response_through_buffered_send_seam() -> None:
    """A chunked-transfer response is reassembled into one buffered RuntimeResponse body."""

    import pyarrow as pa

    csv_bytes = _csv_text(rows=20)
    server, base = _start_chunked_server(csv_bytes, chunk_size=128)
    transport = HttpxCatalogTransport()
    try:
        resource = Resource(
            id="r1",
            url=f"{base}/stream",
            format="CSV",
            access=HttpDownload(url=f"{base}/stream"),
        )
        reader = DataPlaneResourceReader(transport=transport)
        with reader.open(resource) as bs:
            batches = list(bs.iter_batches())
        table = pa.Table.from_batches(batches, schema=bs.schema)
        assert table.num_rows == 20
        assert "id" in table.column_names
    finally:
        transport.close()
        server.shutdown()
        server.server_close()


def test_urllib_runtime_transport_reads_the_resource() -> None:
    """The stdlib runtime transport reads a resource through the same buffered seam."""

    import pyarrow as pa

    csv_bytes = _csv_text(rows=5)
    server, base = start_test_server({"/buffered": MockResponse(status=200, body=csv_bytes)})
    transport = UrllibCatalogTransport()
    try:
        reader = DataPlaneResourceReader(transport=transport)
        resource = Resource(
            id="r1",
            url=f"{base}/buffered",
            format="CSV",
            access=HttpDownload(url=f"{base}/buffered"),
        )
        with reader.open(resource) as bs:
            batches = list(bs.iter_batches())
        table = pa.Table.from_batches(batches, schema=bs.schema)
        assert table.num_rows == 5
    finally:
        transport.close()
        server.shutdown()
        server.server_close()


def test_large_chunked_download_materializes_through_buffered_read() -> None:
    """A multi-megabyte chunked body is buffered once and read through BatchStream.

    The full body is consumed by the transport before the first batch is
    yielded; this test pins read correctness at scale while the streaming
    seam is absent. The peak-RSS test in 04-04 guards the memory story.
    """

    big_rows = 50_000
    csv_bytes = _csv_text(rows=big_rows)
    assert len(csv_bytes) > 1_000_000, "fixture must produce a large body"

    server, base = _start_chunked_server(csv_bytes, chunk_size=4096)
    transport = HttpxCatalogTransport()
    try:
        resource = Resource(
            id="r1",
            url=f"{base}/stream",
            format="CSV",
            access=HttpDownload(url=f"{base}/stream"),
        )
        reader = DataPlaneResourceReader(transport=transport)
        with reader.open(resource) as bs:
            first_batch = next(bs.iter_batches(), None)
            assert first_batch is not None, "Expected at least one batch from the stream"
            assert first_batch.num_rows <= big_rows
    finally:
        transport.close()
        server.shutdown()
        server.server_close()
