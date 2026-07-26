"""Abstract base class for streaming format readers (D-P4-10).

Each reader receives a ``BinaryIO`` byte source and yields Arrow
``RecordBatch`` objects. This replaces the v0.1.0 ``read(...) -> list[dict]``
contract with a streaming contract: callers wrap the iterator in a
``BatchStream`` (Phase 4 plan 01) for context-managed consumption.

The reader does not own byte acquisition (transport), decompression
(plan 04-03), or terminal conversion (Phase 6) — it only decodes bytes
into ``RecordBatch`` chunks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any


class BaseFormatReader(ABC):
    """Protocol for decoding a single file format into Arrow ``RecordBatch`` objects.

    Subclasses set ``format_name`` and implement :meth:`read_batches`. The
    method receives a binary file-like (seekable or non-seekable) and yields
    ``RecordBatch`` instances in roughly ``batch_size``-row chunks.

    The ``source`` parameter is typed ``Any`` rather than ``typing.BinaryIO``
    so that ``io.RawIOBase`` subclasses (notably :class:`IterableBytesIO`
    from plan 04-01) are admitted at static-type level. ``typing.BinaryIO``
    is a structural Protocol that ``RawIOBase`` does not formally satisfy,
    even though the runtime duck-typed contract (``.read()``, ``.seekable()``,
    ``.close()``) is met.
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
