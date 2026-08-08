"""IterableBytesIO — adapt an ``Iterable[bytes]`` into a non-seekable BinaryIO (D-P4-08).

Subclasses ``io.RawIOBase`` so pyarrow's probes (``.closed``, ``.readable()``,
``.seekable()``, ``.readinto()``) succeed. Verified against pyarrow 24.0.0:
``pa.csv.open_csv`` and ``pa.json.read_json`` accept this adapter on chunked
input (RESEARCH Pattern 1).
"""

from __future__ import annotations

from collections.abc import Iterable
from io import RawIOBase
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer


class IterableBytesIO(RawIOBase):
    """Wrap an ``Iterable[bytes]`` into a non-seekable ``BinaryIO``.

    The canonical use case is adapting ``StreamResponse`` (the Phase 3
    ``httpx.iter_raw()`` iterable) into a file-like that pyarrow readers can
    consume without buffering the entire stream.

    Read-only and non-seekable by design — the underlying iterable is a
    forward-only byte source (HTTP chunked transfer). A small peeked-byte
    buffer provides one-chunk lookahead needed by compression magic-byte
    peeking in plan 04-03.
    """

    def __init__(self, source: Iterable[bytes]) -> None:
        self._it = iter(source)
        self._peeked: bytes = b""
        self._closed = False

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def writable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def readinto(self, b: WriteableBuffer) -> int:
        """Read up to ``len(b)`` bytes into the provided buffer.

        Args:
            b: A writable bytes-like object (bytearray, memoryview, array.array).

        Returns:
            Number of bytes read (0 at EOF).
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")
        mv = memoryview(b)
        while not self._peeked:
            try:
                self._peeked = next(self._it)
            except StopIteration:
                return 0
        n = min(len(mv), len(self._peeked))
        mv[:n] = self._peeked[:n]
        self._peeked = self._peeked[n:]
        return n

    def read(self, n: int = -1) -> bytes:
        """Read up to ``n`` bytes; ``n=-1`` reads all remaining bytes.

        Args:
            n: Maximum number of bytes to read. ``-1`` or ``None`` reads all.

        Returns:
            Bytes read from the wrapped iterable.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if n is None or n < 0:
            chunks = [self._peeked]
            self._peeked = b""
            chunks.extend(self._it)
            return b"".join(chunks)
        out = bytearray()
        if self._peeked:
            take = self._peeked[:n]
            self._peeked = self._peeked[len(take) :]
            out.extend(take)
            n -= len(take)
        while n > 0:
            try:
                chunk = next(self._it)
            except StopIteration:
                break
            take = chunk[:n]
            self._peeked = chunk[len(take) :]
            out.extend(take)
            n -= len(take)
        return bytes(out)

    def close(self) -> None:
        """Mark the stream closed and exhaust the iterator."""
        self._closed = True
        self._it = iter(())
