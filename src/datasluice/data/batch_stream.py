"""BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02, D-P4-17).

Placeholder class declaration so the :class:`ResourceReader` port
(``ports/resource_reader.py``) can narrow its return type to ``BatchStream``
in Task 1. The full implementation lands in Task 2.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator


class BatchStream:
    """Context-managed Arrow RecordBatch stream.

    Wraps a ``pa.RecordBatchReader`` or bare ``Iterator[RecordBatch]`` and
    exposes ``.schema`` + ``.iter_batches()`` with context-manager cleanup.
    Full implementation in Task 2.
    """

    def __init__(self, source: Any, schema: Any) -> None:
        self._source = source
        self._schema = schema
        self._closed = False

    @property
    def schema(self) -> Any:
        """The pa.Schema for batches yielded by this stream."""
        return self._schema

    def iter_batches(self) -> Iterator[Any]:
        """Yield Arrow RecordBatch objects; raises if closed (Task 2)."""
        raise NotImplementedError

    def close(self) -> None:
        """Idempotent close (Task 2)."""
        raise NotImplementedError

    def __enter__(self) -> BatchStream:
        return self

    def __exit__(self, *exc: Any) -> None:
        raise NotImplementedError
