"""Shared CLI parsing and one-resource resolution through the public facade."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from datasluice.application import DirectResourceLocator
from datasluice.domain import Resource
from datasluice.exceptions import DataSluiceError


def parse_locator(
    locator: str | None,
    *,
    portal: str | None,
    dataset: str | None,
    resource: str | None,
) -> DirectResourceLocator:
    """Parse either a direct locator or catalog options without I/O."""
    if locator is not None:
        if portal is not None or dataset is not None or resource is not None:
            raise DataSluiceError("A direct locator cannot be combined with --portal, --dataset, or --resource")
        if not locator:
            raise DataSluiceError("A direct locator cannot be empty")
        path = urlsplit(locator).path
        format_name = Path(path).suffix.removeprefix(".").upper() or None
        return DirectResourceLocator(uri=locator, format=format_name)

    raise DataSluiceError("Catalog references are no longer supported; provide a direct locator")


def resolve_one_resource(data_sluice: Any, locator: DirectResourceLocator) -> tuple[DirectResourceLocator, Resource]:
    """Resolve exactly one direct Resource."""
    return locator, data_sluice.resolve(locator)
