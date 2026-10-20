from __future__ import annotations

import importlib

import pytest

pytest.importorskip("pyarrow")

try:
    _access_module = importlib.import_module("datasluice.data.access")
except ImportError:
    pytest.skip("datasluice.data.access not importable", allow_module_level=True)

DataPlaneResourceReader = _access_module.DataPlaneResourceReader


def _csv_text(rows: int = 10) -> bytes:
    lines = [b"id,name,value"]
    for i in range(rows):
        lines.append(f"{i},item_{i},{i * 1.5:.1f}".encode())
    return b"\n".join(lines) + b"\n"


def test_local_file_dispatch_yields_batch_stream(tmp_path) -> None:
    """LocalFile dispatch opens path 'rb' and yields RecordBatch via CSV reader."""
    import pyarrow as pa

    from datasluice.domain import LocalFile, Resource

    csv_path = tmp_path / "data.csv"
    csv_path.write_bytes(_csv_text(rows=10))

    resource = Resource(
        id="r1",
        url=f"file://{csv_path}",
        format="CSV",
        access=LocalFile(path=str(csv_path)),
    )
    reader = DataPlaneResourceReader(transport=None)
    with reader.open(resource) as bs:
        schema = bs.schema
        assert schema is not None
        batches = list(bs.iter_batches())
    table = pa.Table.from_batches(batches, schema=schema)
    assert table.num_rows == 10
    assert set(table.column_names) == {"id", "name", "value"}


def test_default_http_download_when_access_is_none() -> None:
    """resource.access=None defaults to HttpDownload(url=resource.url)."""
    from datasluice.domain import HttpDownload, Resource

    resource = Resource(id="r1", url="https://example.com/data.csv", format="CSV", access=None)
    reader = DataPlaneResourceReader(transport=None)
    resolved = reader._resolve_access(resource)
    assert isinstance(resolved, HttpDownload)
    assert resolved.kind == "http_download"
    assert resolved.url == "https://example.com/data.csv"


def test_query_access_raises_unsupported() -> None:
    """QueryAccess raises UnsupportedAccessError with message."""
    from datasluice.domain import QueryAccess, Resource
    from datasluice.exceptions import UnsupportedAccessError

    resource = Resource(
        id="r1",
        url="https://example.com",
        format="CSV",
        access=QueryAccess(endpoint="https://example.com/api", query_language="SQL"),
    )
    reader = DataPlaneResourceReader(transport=None)
    with pytest.raises(UnsupportedAccessError) as exc_info:
        reader.open(resource)
    msg = str(exc_info.value).lower()
    assert "query" in msg
    assert "not implemented" in msg


def test_stream_access_raises_unsupported() -> None:
    """StreamAccess raises UnsupportedAccessError (out of scope per REQUIREMENTS)."""
    from datasluice.domain import Resource, StreamAccess
    from datasluice.exceptions import UnsupportedAccessError

    resource = Resource(
        id="r1",
        url="wss://example.com/stream",
        format="CSV",
        access=StreamAccess(url="wss://example.com/stream"),
    )
    reader = DataPlaneResourceReader(transport=None)
    with pytest.raises(UnsupportedAccessError):
        reader.open(resource)


def test_object_storage_dispatch_via_memory_fs() -> None:
    """ObjectStorage dispatch resolves via open_filesystem().open() (seekable BinaryIO)."""
    pytest.importorskip("fsspec")
    import pyarrow as pa
    from fsspec.implementations.memory import MemoryFileSystem

    from datasluice.domain import ObjectStorage, Resource

    csv_bytes = _csv_text(rows=5)
    mem = MemoryFileSystem()
    with mem.open("/bucket/data.csv", "wb") as f:
        f.write(csv_bytes)

    resource = Resource(
        id="r1",
        url="memory://bucket/data.csv",
        format="CSV",
        access=ObjectStorage(uri="memory://bucket/data.csv"),
    )
    reader = DataPlaneResourceReader(transport=None)
    with reader.open(resource) as bs:
        batches = list(bs.iter_batches())
    table = pa.Table.from_batches(batches, schema=bs.schema)
    assert table.num_rows == 5


def test_http_download_with_buffered_transport() -> None:
    """HttpDownload consumes a buffered CatalogTransport response."""
    import pyarrow as pa

    from tests.helpers.http_server import MockResponse, start_test_server

    pytest.importorskip("httpx")
    from datasluice.domain import HttpDownload, Resource
    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport

    csv_bytes = _csv_text(rows=8)
    server, base = start_test_server({"/data.csv": MockResponse(status=200, body=csv_bytes)})
    try:
        transport = HttpxCatalogTransport()
        resource = Resource(
            id="r1",
            url=f"{base}/data.csv",
            format="CSV",
            access=HttpDownload(url=f"{base}/data.csv"),
        )
        reader = DataPlaneResourceReader(transport=transport, allow_private_http=True)
        with reader.open(resource) as bs:
            batches = list(bs.iter_batches())
        table = pa.Table.from_batches(batches, schema=bs.schema)
        assert table.num_rows == 8
    finally:
        server.shutdown()
        server.server_close()


def test_full_pipeline_csv_with_gzip_local_file(tmp_path) -> None:
    """Full pipeline: gzip-compressed CSV → decompress → format reader → BatchStream."""
    import gzip

    import pyarrow as pa

    from datasluice.domain import LocalFile, Resource

    raw_csv = _csv_text(rows=12)
    compressed = gzip.compress(raw_csv)
    csv_path = tmp_path / "data.csv.gz"
    csv_path.write_bytes(compressed)

    resource = Resource(
        id="r1",
        url=f"file://{csv_path}",
        format="CSV",
        access=LocalFile(path=str(csv_path)),
    )
    reader = DataPlaneResourceReader(transport=None)
    with reader.open(resource) as bs:
        batches = list(bs.iter_batches())
    table = pa.Table.from_batches(batches, schema=bs.schema)
    assert table.num_rows == 12


def test_undeclared_container_formats_raise_unless_declared(tmp_path) -> None:
    """Reject ZIP and Parquet payloads without a declared format.

    Read declared CSV from a ZIP archive.
    """
    import zipfile

    import pyarrow as pa
    import pyarrow.parquet as pq

    from datasluice.domain import LocalFile, Resource
    from datasluice.exceptions import UnsupportedAccessError

    csv_bytes = _csv_text(rows=4)
    zip_path = tmp_path / "data.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("data.csv", csv_bytes)

    parquet_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2]}), parquet_path)

    reader = DataPlaneResourceReader(transport=None)
    for path, hint in ((zip_path, "CSV"), (parquet_path, "PARQUET")):
        undeclared = Resource(id="r1", url=f"file://{path}", format=None, access=LocalFile(path=str(path)))
        with pytest.raises(UnsupportedAccessError) as exc_info:
            reader.open(undeclared)
        assert "Pass format=" in str(exc_info.value)
        assert hint in str(exc_info.value)
    undeclared = Resource(id="r1", url=f"file://{zip_path}", format=None, access=LocalFile(path=str(zip_path)))
    with pytest.raises(UnsupportedAccessError) as exc_info:
        reader.open(undeclared)
    assert "XLSX" not in str(exc_info.value)

    declared = Resource(id="r1", url=f"file://{zip_path}", format="CSV", access=LocalFile(path=str(zip_path)))
    with reader.open(declared) as bs:
        table = pa.Table.from_batches(list(bs.iter_batches()), schema=bs.schema)
    assert table.num_rows == 4


@pytest.mark.parametrize("format_name", ["CSV", "JSONL"])
def test_streaming_formats_exceed_the_document_limit(format_name, monkeypatch) -> None:
    import datasluice.data.readers.base as readers_base
    from datasluice.domain import Resource
    from tests.helpers.httpx_probe import SyncProbe, fixed

    monkeypatch.setattr(readers_base, "MAX_DOCUMENT_BYTES", 4)
    payload = b"id\n1\n2\n" if format_name == "CSV" else b'{"id":1}\n{"id":2}\n'
    resource = Resource(id="large", url="https://catalog.example.test/data", format=format_name)
    with (
        SyncProbe(fixed(200, content=payload)) as transport,
        DataPlaneResourceReader(transport).open(resource) as stream,
    ):
        assert sum(batch.num_rows for batch in stream.iter_batches()) == 2


@pytest.mark.parametrize("format_name", ["JSON", "GEOJSON"])
def test_document_formats_keep_the_http_limit(format_name, monkeypatch) -> None:
    import datasluice.data.readers.base as readers_base
    from datasluice.domain import Resource
    from datasluice.runtime.transport.base import TransportError
    from tests.helpers.httpx_probe import SyncProbe, fixed

    monkeypatch.setattr(readers_base, "MAX_DOCUMENT_BYTES", 4)
    resource = Resource(id="large", url="https://catalog.example.test/data", format=format_name)
    with SyncProbe(fixed(200, content=b'[{"id":1}]')) as transport:
        reader = DataPlaneResourceReader(transport)
        with pytest.raises(TransportError, match="limit"):
            reader.open(resource)
