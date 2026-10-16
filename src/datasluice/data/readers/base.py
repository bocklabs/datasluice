"""Abstract base class for streaming format readers.

Each reader receives a ``BinaryIO`` byte source and yields Arrow ``RecordBatch`` objects. This replaces the v0.1.0
``read(...) -> list[dict]`` contract with a streaming contract: callers wrap the iterator in a ``BatchStream`` (plan 01)
for context-managed consumption.

The reader does not own byte acquisition (transport), decompression (plan 04-03), or terminal conversion — it only
decodes bytes into ``RecordBatch`` chunks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from datasluice.exceptions import FormatError

if TYPE_CHECKING:
    from collections.abc import Iterator

MAX_DOCUMENT_BYTES = 64 * 1024 * 1024


def read_document(source: Any) -> bytes:
    """Read a whole-document format under a fixed 64 MiB input ceiling."""
    data = bytearray()
    while chunk := source.read(min(65536, MAX_DOCUMENT_BYTES + 1 - len(data))):
        data.extend(chunk)
        if len(data) > MAX_DOCUMENT_BYTES:
            raise FormatError("Whole-document JSON and GeoJSON reads are limited to 64 MiB; use JSONL for larger data")
    return bytes(data)


class BaseFormatReader(ABC):
    """Protocol for decoding a single file format into Arrow ``RecordBatch`` objects.

    Subclasses set ``format_name`` and implement :meth:`read_batches`. The method receives a binary file-like (seekable
    or non-seekable) and yields ``RecordBatch`` instances in roughly ``batch_size``-row chunks.

    The ``source`` parameter is typed ``Any`` rather than ``typing.BinaryIO`` so that ``io.RawIOBase`` subclasses
    (notably :class:`IterableBytesIO` from plan 04-01) are admitted at static-type level. ``typing.BinaryIO`` is a
    structural Protocol that ``RawIOBase`` does not formally satisfy, even though the runtime duck-typed contract
    (``.read()``, ``.seekable()``, ``.close()``) is met.
    """

    format_name: str = "base"

    @abstractmethod
    def read_batches(self, source: Any, *, batch_size: int = 65536) -> Iterator[Any]:
        """Read *source* and yield Arrow ``RecordBatch`` objects.

        Args:
            source: A binary file-like object (seekable or non-seekable).
                Accepts ``io.BytesIO``, ``io.RawIOBase`` subclasses, or any
                object exposing ``.read()`` returning ``bytes``.
            batch_size: Target row count per yielded batch. Readers may
                yield larger or smaller batches when the underlying decoder
                chunking does not align with this hint.

        Yields:
            ``pyarrow.RecordBatch`` instances.
        """
