"""to_pandas terminal: convert a BatchStream to a pandas DataFrame.

Lazy-imports pandas; zero-copy Arrow interop via the :func:`to_arrow` substrate
. The v0.1.0
``resource_to_dataframe`` / ``dataset_to_dataframes`` helpers were removed per
 (they relied on the deleted ``datasluice.formats`` read path);
rebuilds the terminal over the shared :class:`datasluice.data.BatchStream`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datasluice.data.batch_stream import BatchStream


def to_pandas(stream: BatchStream) -> Any:
    """Convert *stream* to a pandas ``DataFrame``.

    Delegates through :func:`~datasluice.integrations.arrow.to_arrow` for
    single-substrate consistency, then zero-copy converts the Arrow
    Table to a DataFrame.

    Args:
        stream: The :class:`~datasluice.data.BatchStream` to convert.

    Returns:
        A ``pandas.DataFrame`` built via Arrow zero-copy interop.

    Raises:
        ImportError: If ``pandas`` is not installed. Install with
            ``pip install datasluice[pandas]``.
    """
    try:
        import pandas as pd  # noqa: F401 — lazy import gate
    except ImportError as exc:
        raise ImportError("to_pandas requires 'pandas'. Install with: pip install datasluice[pandas]") from exc

    from datasluice.integrations.arrow import to_arrow

    return to_arrow(stream).to_pandas()
