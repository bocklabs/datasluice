from __future__ import annotations

import bz2
import gzip
import importlib
import io

import pytest

pytest.importorskip("pyarrow")

try:
    _compression_module = importlib.import_module("datasluice.data.compression")
except ImportError:
    pytest.skip("datasluice.data.compression not importable", allow_module_level=True)

apply_compression = _compression_module.apply_compression
PeekableReader = _compression_module.PeekableReader


def _csv_bytes(rows: int = 20) -> bytes:
    lines = [b"id,name,value"]
    for i in range(rows):
        lines.append(f"{i},item_{i},{i * 1.5:.1f}".encode())
    return b"\n".join(lines) + b"\n"


def test_gzip_round_trip() -> None:
    """GZIP-compressed input decompresses through apply_compression."""
    raw = _csv_bytes()
    compressed = gzip.compress(raw)
    source = io.BytesIO(compressed)
    decompressed = apply_compression(source)
    assert decompressed.read() == raw
    decompressed.close()
    assert source.closed


def test_bzip2_round_trip() -> None:
    """BZIP2-compressed input decompresses through apply_compression."""
    raw = _csv_bytes()
    compressed = bz2.compress(raw)
    source = io.BytesIO(compressed)
    decompressed = apply_compression(source)
    assert decompressed.read() == raw


def test_zstd_round_trip() -> None:
    """ZSTD-compressed input decompresses through apply_compression.

    Skips gracefully when ``zstandard`` is not installed (RESEARCH L621).
    """
    zstandard = pytest.importorskip("zstandard")
    raw = _csv_bytes()
    cctx = zstandard.ZstdCompressor()
    compressed = cctx.compress(raw)
    source = io.BytesIO(compressed)
    decompressed = apply_compression(source)
    assert decompressed.read() == raw


def test_zip_extracts_largest_member() -> None:
    """ZIP spools to BytesIO then extracts the LARGEST member."""
    import zipfile

    large_csv = _csv_bytes(rows=100)
    small_readme = b"# README\nThis is a tiny metadata file.\n"

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.md", small_readme)
        zf.writestr("data.csv", large_csv)

    source = io.BytesIO(buffer.getvalue())
    decompressed = apply_compression(source)
    assert decompressed.read() == large_csv


def test_uncompressed_passthrough() -> None:
    """Plain bytes pass through unchanged (PeekableReader prepends peeked bytes)."""
    raw = _csv_bytes()
    source = io.BytesIO(raw)
    decompressed = apply_compression(source)
    assert decompressed.read() == raw


def test_magic_byte_detection() -> None:
    """Each format is detected by magic bytes alone (no content_encoding hint)."""
    raw = _csv_bytes(rows=5)

    gzipped = io.BytesIO(gzip.compress(raw))
    assert apply_compression(gzipped).read() == raw

    bzzed = io.BytesIO(bz2.compress(raw))
    assert apply_compression(bzzed).read() == raw

    zstd = pytest.importorskip("zstandard")
    zcompressed = io.BytesIO(zstd.ZstdCompressor().compress(raw))
    assert apply_compression(zcompressed).read() == raw


def test_truncated_gzip_raises_decompression_error() -> None:
    """A truncated GZIP frame raises DecompressionError."""
    from datasluice.exceptions import DecompressionError

    raw = _csv_bytes(rows=10)
    truncated = gzip.compress(raw)[:-8]
    source = io.BytesIO(truncated)
    apply_compression_2 = apply_compression(source)
    with pytest.raises(DecompressionError):
        apply_compression_2.read()


def test_content_encoding_hint_gzip() -> None:
    """An HTTP ``Content-Encoding: gzip`` hint selects GZIP even without magic-byte inspection."""
    raw = _csv_bytes(rows=5)
    compressed = gzip.compress(raw)
    source = io.BytesIO(compressed)
    decompressed = apply_compression(source, content_encoding="gzip")
    assert decompressed.read() == raw


def test_peekable_reader_prepends_peeked_bytes() -> None:
    """PeekableReader.peek() does not consume bytes; read() yields peeked + rest."""
    underlying = io.BytesIO(b"hello world")
    peekable = PeekableReader(underlying)
    peeked = peekable.peek(5)
    assert peeked == b"hello"
    assert peekable.read() == b"hello world"


def test_peekable_reader_streams_non_seekable() -> None:
    """PeekableReader works on a non-seekable IterableBytesIO source."""
    from datasluice.data._byte_source import IterableBytesIO

    underlying = IterableBytesIO([b"abc", b"def", b"gh"])
    peekable = PeekableReader(underlying)
    peeked = peekable.peek(4)
    assert peeked == b"abcd"
    assert peekable.read(6) == b"abcdef"
    assert peekable.read() == b"gh"


@pytest.mark.parametrize("kind", ["gzip", "zip"])
def test_expanded_payload_is_bounded(monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    import zipfile

    from datasluice.exceptions import DecompressionError

    monkeypatch.setattr(_compression_module, "_MAX_DECOMPRESSED_BYTES", 16, raising=False)
    raw = b"x" * 17
    buffer = io.BytesIO()
    if kind == "zip":
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("data.csv", raw)
        data = buffer.getvalue()
    else:
        data = gzip.compress(raw)
    with (
        pytest.raises(DecompressionError, match="decompressed byte limit"),
        apply_compression(io.BytesIO(data)) as source,
    ):
        source.read()
