"""to_polars terminal: convert a BatchStream to a polars DataFrame (INTG-03, D-P6-01).

Lazy-imports polars; zero-copy Arrow interop via the :func:`to_arrow` substrate
(single-substrate consistency, QUAL-10). The v0.1.0 polars read path was fully
removed per D-P4-18; Phase 6 rebuilds it over the shared
:class:`datasluice.data.BatchStream`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datasluice.data.batch_stream import BatchStream


def to_polars(stream: BatchStream) -> Any:
    """Convert *stream* to a polars ``DataFrame`` (INTG-03).

    Delegates through :func:`~datasluice.integrations.arrow.to_arrow` for
    single-substrate consistency (QUAL-10), then zero-copy converts the Arrow
    Table to a DataFrame via ``polars.from_arrow``.

    Args:
        stream: The :class:`~datasluice.data.BatchStream` to convert.

    Returns:
        A ``polars.DataFrame`` built via Arrow zero-copy interop.

    Raises:
        ImportError: If ``polars`` is not installed. Install with
            ``pip install datasluice[polars]``.
    """
    try:
        import polars as pl  # noqa: F401 — lazy import gate
    except ImportError as exc:
        raise ImportError("to_polars requires 'polars'. Install with: pip install datasluice[polars]") from exc

    from datasluice.integrations.arrow import to_arrow

    return pl.from_arrow(to_arrow(stream))
