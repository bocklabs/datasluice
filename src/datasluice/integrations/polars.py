"""Polars integration: load resources into LazyFrames / DataFrames.

Requires ``polars``: install with ``pip install datasluice[polars]``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

    from datasluice.domain.resource import Resource


def resource_to_dataframe(resource: Resource, **kwargs: Any) -> pl.DataFrame:
    """Read a :class:`Resource` into a Polars :class:`~polars.DataFrame`.

    Args:
        resource: The resource to read.
        **kwargs: Extra keyword arguments passed to ``polars``.
    """
    import polars as pl

    fmt = (resource.format or "CSV").upper()
    url = resource.url or ""
    if fmt == "CSV":
        return pl.read_csv(url, **kwargs)
    if fmt in ("JSON", "JSONL"):
        return pl.read_json(url, **kwargs)
    if fmt == "PARQUET":
        return pl.read_parquet(url, **kwargs)
    return pl.read_csv(url, **kwargs)
