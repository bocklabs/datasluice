"""Buffered-download correctness for the HttpDownload dispatch path.

Both runtime transports return the complete body inside ``RuntimeResponse.body``
today, so HTTP downloads are fully buffered in memory before the format reader
sees them. These tests verify chunked-transfer reassembly, parity between the
httpx and stdlib transports through the buffered ``send()`` seam, and complete
read correctness for large multi-megabyte bodies (all batches consumed; total
row count and id-column checksum checked against the fixture). They
intentionally do NOT verify bounded memory: no peak-RSS subprocess covers
``HttpDownload`` today — the peak-RSS coverage in ``test_peak_rss.py``
exercises local byte sources only. Bounded-memory HTTP behavior remains
unverified until a streaming transport seam exists.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyarrow")

from datasluice.data.access import DataPlaneResourceReader
from datasluice.domain import HttpDownload, Resource
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from tests.helpers.http_server import MockResponse, start_test_server


def _csv_text(rows: int) -> bytes:
    lines = [b"id,name,value"]
    for i in range(rows):
        lines.append(f"{i},item_{i},{i * 1.5:.1f}".encode())
    return b"\n".join(lines) + b"\n"


def _resource(base: str, path: str) -> Resource:
    url = f"{base}{path}"
    return Resource(id="r1", url=url, format="CSV", access=HttpDownload(url=url))


def _read_row_totals(reader: DataPlaneResourceReader, resource: Resource) -> tuple[int, int]:
    """Consume every batch and return the summed row count plus the id-column checksum."""
    import pyarrow as pa

    with reader.open(resource) as batch_stream:
        batches = list(batch_stream.iter_batches())
        schema = batch_stream.schema
    table = pa.Table.from_batches(batches, schema=schema)
    ids = table.column("id").to_pylist()
    return table.num_rows, sum(ids)


def test_http_download_reads_chunked_response_through_buffered_send_seam() -> None:
    """A chunked-transfer response is reassembled into one buffered RuntimeResponse body."""
    pytest.importorskip("httpx")

    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport

    server, base = start_test_server({"/stream": MockResponse(status=200, body=_csv_text(rows=20), chunk_size=128)})
    try:
        transport = HttpxCatalogTransport()
        try:
            rows, id_checksum = _read_row_totals(
                DataPlaneResourceReader(transport=transport), _resource(base, "/stream")
            )
        finally:
            transport.close()

        assert rows == 20
        assert id_checksum == sum(range(20))
    finally:
        server.shutdown()
        server.server_close()


def test_urllib_runtime_transport_reads_the_resource() -> None:
    """The stdlib runtime transport reads a resource through the same buffered seam."""
    server, base = start_test_server({"/buffered": MockResponse(status=200, body=_csv_text(rows=5))})
    try:
        transport = UrllibCatalogTransport()
        try:
            rows, id_checksum = _read_row_totals(
                DataPlaneResourceReader(transport=transport), _resource(base, "/buffered")
            )
        finally:
            transport.close()

        assert rows == 5
        assert id_checksum == sum(range(5))
    finally:
        server.shutdown()
        server.server_close()


def test_large_chunked_download_materializes_every_row_without_truncation() -> None:
    """A multi-megabyte chunked body is fully consumed: row count and checksum must match.

    The full body is consumed by the transport before the first batch is
    yielded; this test pins read correctness at scale while the streaming
    seam is absent. Any truncation or dropped chunk changes either the summed
    row count or the id-column checksum and fails below.
    """
    pytest.importorskip("httpx")

    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport

    big_rows = 50_000
    csv_bytes = _csv_text(rows=big_rows)
    assert len(csv_bytes) > 1_000_000, "fixture must produce a large body"

    server, base = start_test_server({"/stream": MockResponse(status=200, body=csv_bytes, chunk_size=4096)})
    try:
        transport = HttpxCatalogTransport()
        try:
            rows, id_checksum = _read_row_totals(
                DataPlaneResourceReader(transport=transport), _resource(base, "/stream")
            )
        finally:
            transport.close()

        assert rows == big_rows
        assert id_checksum == big_rows * (big_rows - 1) // 2
    finally:
        server.shutdown()
        server.server_close()
