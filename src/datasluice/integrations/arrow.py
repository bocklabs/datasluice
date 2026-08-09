"""to_arrow terminal: materialize a BatchStream into a pa.Table.

The shared substrate the other terminals (to_pandas/to_polars/to_duckdb)
delegate through for single-substrate consistency. pyarrow is
lazy-imported inside the function body to keep ``import datasluice.integrations``
light on bare installs (AGENTS.md lazy-dep discipline).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datasluice.data.batch_stream import BatchStream


def to_arrow(stream: BatchStream) -> Any:
    """Materialize *stream* into a ``pa.Table``.

    The shared substrate the other terminals (to_pandas/to_polars/to_duckdb)
    delegate through for single-substrate consistency.

    Args:
        stream: The :class:`~datasluice.data.BatchStream` to materialize.

    Returns:
        A ``pyarrow.Table`` built from the stream's batches with the stream's
        schema.
    """
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise ImportError("to_arrow requires 'pyarrow'. Install with: pip install datasluice[streaming]") from exc

    return pa.Table.from_batches(stream.iter_batches(), schema=stream.schema)
