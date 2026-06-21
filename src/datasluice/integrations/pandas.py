"""Pandas integration: load resources into DataFrames.

Requires ``pandas``: install with ``pip install datasluice[pandas]``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from datasluice.domain.resource import Resource


def resource_to_dataframe(resource: Resource, **kwargs: Any) -> pd.DataFrame:
    """Read a :class:`Resource` into a pandas :class:`~pandas.DataFrame`.

    Args:
        resource: The resource to read.
        **kwargs: Extra keyword arguments passed to the format reader or
            ``pandas`` reader.
    """
    import pandas as pd

    from datasluice.formats import get_reader

    fmt = (resource.format or "CSV").upper()
    if fmt in ("CSV", "JSON", "JSONL", "PARQUET", "XLSX", "GEOJSON"):
        reader = get_reader(fmt)
        records = reader.read(resource.url or "")
        return pd.DataFrame(records, **kwargs)

    return pd.read_csv(resource.url or "", **kwargs)  # type: ignore[no-any-return]


def dataset_to_dataframes(
    dataset_id: str,
    portal_url: str,
    **kwargs: Any,
) -> dict[str, pd.DataFrame]:
    """Return a ``{resource_name: DataFrame}`` mapping for a dataset."""
    from datasluice import DataSluice

    ds = DataSluice(portal_url)
    dataset = ds.get_dataset(dataset_id)
    return {(r.name or r.id): resource_to_dataframe(r, **kwargs) for r in dataset.resources if r.url}
