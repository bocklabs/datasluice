"""Transparent decompression decorator pipeline (DATA-06, D-P4-12).

Sits BETWEEN access acquisition (``access.py``) and the format reader
(``data/readers/``). The pipeline peeks the first few bytes of the byte source,
detects compression by magic bytes (or an HTTP ``Content-Encoding`` hint), wraps
the source in the appropriate decompressor, and returns a new ``BinaryIO``.

GZIP / BZIP2 / ZSTD stream on non-seekable input (RESEARCH Pattern 3) — they
need no seek and honour the bounded-memory contract. ZIP requires seekable
input (the central directory lives at EOF — RESEARCH Pitfall 2); the pipeline
spools the full body to :class:`io.BytesIO` before ``zipfile.ZipFile`` and
extracts the LARGEST member (RESEARCH Open Question 5 — open-data ZIPs often
bundle a small README alongside the data file; largest avoids picking the
README).
"""

from __future__ import annotations

import bz2
import gzip
import io
import zipfile
from typing import TYPE_CHECKING, Any

from datasluice.exceptions import DecompressionError
from datasluice.logging import get_logger

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

logger = get_logger("data.compression")


class _ErrorTranslatingReader(io.RawIOBase):
    """Wrap a decompressed stream, translating stdlib decode errors to DecompressionError.

    GZIP/BZIP2/ZSTD/ZIP all surface truncated-frame errors only on ``read`` (the
    decompressor is constructed lazily and decodes incrementally). Wrapping the
    stdlib decompressor here keeps that translation in one place so callers see
    only :class:`DecompressionError` (D-P4-21).
    """

    def __init__(self, inner: Any, exc_types: tuple[type[BaseException], ...], label: str) -> None:
        self._inner = inner
        self._exc_types = exc_types
        self._label = label

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return getattr(self._inner, "seekable", lambda: False)()

    def writable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return getattr(self._inner, "closed", False)

    def read(self, n: int = -1) -> bytes:
        try:
            return self._inner.read(n)
        except self._exc_types as exc:
            raise DecompressionError(f"{self._label} decompression failed: {exc}") from exc

    def readinto(self, b: WriteableBuffer) -> int:
        mv = memoryview(b)
        try:
            chunk = self._inner.read(len(mv))
        except self._exc_types as exc:
            raise DecompressionError(f"{self._label} decompression failed: {exc}") from exc
        if not chunk:
            return 0
        n = min(len(mv), len(chunk))
        mv[:n] = chunk[:n]
        return n

    def close(self) -> None:
        if hasattr(self._inner, "close"):
            self._inner.close()


_GZIP_MAGIC = b"\x1f\x8b"
_BZIP2_MAGIC = b"BZh"
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
_ZIP_MAGIC = b"PK\x03\x04"

_PEEK_SIZE = 6


class PeekableReader(io.RawIOBase):
    """Wrap a ``BinaryIO`` byte source with one-chunk lookahead.

    Buffers peeked bytes (via :meth:`peek`) and prepends them to subsequent
    reads so downstream consumers see the full stream from offset 0. Works on
    both seekable and non-seekable sources (the only operations used are
    ``read`` and iteration).
    """

    def __init__(self, source: Any) -> None:
        self._source = source
        self._buffer = bytearray()
        self._closed = False

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return getattr(self._source, "seekable", lambda: False)()

    def writable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def peek(self, n: int = _PEEK_SIZE) -> bytes:
        """Read up to *n* bytes into the buffer WITHOUT consuming them.

        The peeked bytes are returned and also retained so the next
        :meth:`read` sees them. If fewer than *n* bytes remain, returns
        whatever is available (possibly empty).
        """

        if self._closed:
            raise ValueError("I/O operation on closed file")
        needed = n - len(self._buffer)
        while needed > 0:
            chunk = self._source.read(needed)
            if not chunk:
                break
            self._buffer.extend(chunk)
            needed = n - len(self._buffer)
        return bytes(self._buffer[:n])

    def readinto(self, b: WriteableBuffer) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        mv = memoryview(b)
        if not self._buffer:
            chunk = self._source.read(len(mv))
            if not chunk:
                return 0
            n = min(len(mv), len(chunk))
            mv[:n] = chunk[:n]
            if n < len(chunk):
                self._buffer.extend(chunk[n:])
            return n
        n = min(len(mv), len(self._buffer))
        mv[:n] = self._buffer[:n]
        del self._buffer[:n]
        return n

    def read(self, n: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if n is None or n < 0:
            chunks = [bytes(self._buffer)]
            self._buffer.clear()
            rest = self._source.read()
            if rest:
                chunks.append(rest)
            return b"".join(chunks)
        out = bytearray()
        if self._buffer:
            take = self._buffer[:n]
            out.extend(take)
            del self._buffer[: len(take)]
            n -= len(take)
        while n > 0:
            chunk = self._source.read(n)
            if not chunk:
                break
            out.extend(chunk)
            n -= len(chunk)
        return bytes(out)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if hasattr(self._source, "close"):
            self._source.close()


def _detect_format(magic: bytes, content_encoding: str | None) -> str:
    """Return the compression format key from magic bytes or content-encoding hint.

    Args:
        magic: Peeked leading bytes (may be shorter than 6 if EOF).
        content_encoding: Lowercased HTTP ``Content-Encoding`` value or ``None``.

    Returns:
        One of ``"gzip"``, ``"bzip2"``, ``"zstd"``, ``"zip"`` or ``"none"``.
    """

    ce = (content_encoding or "").lower().strip()
    if magic.startswith(_GZIP_MAGIC) or ce in {"gzip", "x-gzip"}:
        return "gzip"
    if magic.startswith(_BZIP2_MAGIC) or ce in {"bzip2", "bz2", "x-bzip2"}:
        return "bzip2"
    if magic.startswith(_ZSTD_MAGIC) or ce == "zstd":
        return "zstd"
    if magic.startswith(_ZIP_MAGIC) or ce in {"zip", "x-zip"}:
        return "zip"
    return "none"


def _wrap_gzip(source: Any) -> Any:
    try:
        decompressor = gzip.GzipFile(fileobj=source)
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
        raise DecompressionError(f"GZIP decompression failed: {exc}") from exc
    return _ErrorTranslatingReader(decompressor, (gzip.BadGzipFile, EOFError, OSError), "GZIP")


def _wrap_bzip2(source: Any) -> Any:
    try:
        decompressor = bz2.BZ2File(source)
    except (OSError, ValueError, EOFError) as exc:
        raise DecompressionError(f"BZIP2 decompression failed: {exc}") from exc
    return _ErrorTranslatingReader(decompressor, (OSError, ValueError, EOFError), "BZIP2")


def _wrap_zstd(source: Any) -> Any | None:
    try:
        import zstandard
    except ImportError:
        logger.debug("zstandard not installed; skipping ZSTD detection")
        return None
    try:
        decompressor = zstandard.ZstdDecompressor()
        reader = decompressor.stream_reader(source, read_across_frames=True)
    except (zstandard.ZstdError, OSError, ValueError) as exc:
        raise DecompressionError(f"ZSTD decompression failed: {exc}") from exc
    return _ErrorTranslatingReader(
        reader,
        (zstandard.ZstdError, OSError, ValueError, EOFError),
        "ZSTD",
    )


def _zip_largest_member(source: Any) -> Any:
    """Spool ZIP body to BytesIO, extract the largest member (RESEARCH Pitfall 2 + OQ5)."""

    body = source.read()
    spooled = io.BytesIO(body)
    try:
        zf = zipfile.ZipFile(spooled)
    except zipfile.BadZipFile as exc:
        raise DecompressionError(f"ZIP archive could not be read: {exc}") from exc

    members = [info for info in zf.infolist() if not info.is_dir()]
    if not members:
        zf.close()
        raise DecompressionError("ZIP archive contains no file members")
    largest = max(members, key=lambda info: info.file_size)
    if len(members) > 1:
        skipped = [m.filename for m in members if m is not largest]
        logger.warning(
            "ZIP archive contained multiple members; selected %s (%d bytes). Skipped: %s",
            largest.filename,
            largest.file_size,
            skipped,
        )
    try:
        member_bytes = zf.read(largest)
    except (zipfile.BadZipFile, RuntimeError, OSError) as exc:
        zf.close()
        raise DecompressionError(f"ZIP member {largest.filename!r} could not be extracted: {exc}") from exc
    zf.close()
    return io.BytesIO(member_bytes)


def apply_compression(source: Any, content_encoding: str | None = None) -> Any:
    """Wrap *source* so compressed bytes are transparently decompressed (DATA-06).

    Probes the leading magic bytes (with an optional HTTP ``Content-Encoding``
    hint) and returns a new byte source yielding the decompressed stream.

    GZIP / BZIP2 / ZSTD wrap the source directly (true streaming on
    non-seekable input). ZIP requires seekable input (central directory at
    EOF), so the body is spooled to :class:`io.BytesIO` first; the LARGEST
    member is selected (open-data archives commonly bundle a small README
    alongside the data file).

    If ``zstandard`` is not installed, ZSTD detection is skipped gracefully —
    GZIP / BZIP2 / ZIP still work (RESEARCH L621).

    Args:
        source: A binary file-like (seekable or non-seekable).
        content_encoding: Optional lowercased HTTP ``Content-Encoding`` value
            used as a hint when magic bytes are inconclusive.

    Returns:
        A binary file-like yielding decompressed bytes. Uncompressed sources
        are returned wrapped in a :class:`PeekableReader` so the peeked magic
        bytes are not lost.

    Raises:
        DecompressionError: If the compressed frame is truncated or malformed.
    """

    peekable = source if isinstance(source, PeekableReader) else PeekableReader(source)
    magic = peekable.peek(_PEEK_SIZE)
    fmt = _detect_format(magic, content_encoding)

    if fmt == "gzip":
        return _wrap_gzip(peekable)
    if fmt == "bzip2":
        return _wrap_bzip2(peekable)
    if fmt == "zstd":
        wrapped = _wrap_zstd(peekable)
        if wrapped is not None:
            return wrapped
        # zstandard missing → fall through to passthrough (the bytes are likely
        # genuine ZSTD; reading them as raw bytes will surface a downstream
        # FormatError from the format reader, which is the user-visible failure
        # surface for "compression extra not installed").
        return peekable
    if fmt == "zip":
        return _zip_largest_member(peekable)
    return peekable
