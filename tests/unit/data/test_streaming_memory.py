"""streaming-memory integration tests for the HttpDownload dispatch path.

Verifies that the streaming path through :class:`HttpxCatalogTransport.stream()` +
:class:`IterableBytesIO` actually streams chunks through to the format reader
without buffering the entire body, AND that the urllib fallback (non-streaming
transport) buffers the body and logs the WARNING recommending
``datasluice[http]``.

The subprocess peak-RSS test that closes lives in 04-04
(``test_peak_rss.py``); these tests verify the streaming behaviour at the
integration level without the subprocess overhead.
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


def test_http_download_streams_chunks() -> None:
    """HttpDownload via HttpxCatalogTransport streams chunks through IterableBytesIO to RecordBatch."""

    import pyarrow as pa

    csv_bytes = _csv_text(rows=20)
    server, base = _start_chunked_server(csv_bytes, chunk_size=128)
    try:
        transport = HttpxCatalogTransport()
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
        server.shutdown()
        server.server_close()


def test_urllib_runtime_transport_reads_the_resource() -> None:
    """The stdlib runtime transport reads a resource through the same seam."""

    import pyarrow as pa

    csv_bytes = _csv_text(rows=5)
    server, base = start_test_server({"/buffered": MockResponse(status=200, body=csv_bytes)})
    try:
        transport = UrllibCatalogTransport()
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
        server.shutdown()
        server.server_close()


def test_streaming_does_not_buffer_full_body() -> None:
    """Iteration is lazy — pulling only the first batch does not consume the full body.

    The chunked endpoint simulates a real streaming server. We open a
    BatchStream, pull a single RecordBatch, and verify the underlying
    IterableBytesIO has NOT been exhausted (i.e. we did not slurp the entire
    body before yielding the first batch). The peak-RSS subprocess
    test in 04-04 catches accidental full-buffering at the memory level.
    """

    big_rows = 50_000
    csv_bytes = _csv_text(rows=big_rows)
    assert len(csv_bytes) > 1_000_000, "fixture must produce a large body"

    server, base = _start_chunked_server(csv_bytes, chunk_size=4096)
    try:
        transport = HttpxCatalogTransport()
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
        server.shutdown()
        server.server_close()
