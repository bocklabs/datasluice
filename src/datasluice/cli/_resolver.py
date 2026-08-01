"""Shared CLI parsing and one-resource resolution through the public facade."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from datasluice._uri import sanitize_uri
from datasluice.application import CatalogResourceLocator, DirectResourceLocator, ResourceLocator
from datasluice.domain import Resource
from datasluice.exceptions import DataSluiceError, ResourceResolutionError


@dataclass(frozen=True)
class _CatalogReference:
    """Catalog input which still needs a unique resource selector."""

    portal_url: str
    dataset_id: str


type ParsedLocator = DirectResourceLocator | CatalogResourceLocator | _CatalogReference


def parse_locator(
    locator: str | None,
    *,
    portal: str | None,
    dataset: str | None,
    resource: str | None,
) -> ParsedLocator:
    """Parse either a direct locator or catalog options without I/O."""
    if locator is not None:
        if portal is not None or dataset is not None or resource is not None:
            raise DataSluiceError("A direct locator cannot be combined with --portal, --dataset, or --resource")
        if not locator:
            raise DataSluiceError("A direct locator cannot be empty")
        path = urlsplit(locator).path
        format_name = Path(path).suffix.removeprefix(".").upper() or None
        return DirectResourceLocator(uri=locator, format=format_name)

    if portal is None and dataset is None and resource is None:
        raise DataSluiceError("Provide a direct locator or both --portal and --dataset")
    if portal is None or dataset is None:
        raise DataSluiceError("Catalog references require both --portal and --dataset")
    if not portal or not dataset:
        raise DataSluiceError("Catalog portal and dataset values cannot be empty")
    if resource is None:
        return _CatalogReference(portal_url=portal, dataset_id=dataset)
    if not resource:
        raise DataSluiceError("Catalog resource selector cannot be empty")
    return CatalogResourceLocator(portal_url=portal, dataset_id=dataset, resource_id=resource)


def resolve_one_resource(data_sluice: Any, locator: ParsedLocator) -> tuple[ResourceLocator, Resource]:
    """Resolve exactly one Resource and reject ambiguous catalog datasets."""
    if isinstance(locator, (DirectResourceLocator, CatalogResourceLocator)):
        return locator, data_sluice.resolve(locator)

    dataset = data_sluice.portal(locator.portal_url).get_dataset(locator.dataset_id)
    resources = list(dataset.resources)
    selectors = ", ".join(sorted(sanitize_uri(resource.id) for resource in resources)) or "(none)"
    if len(resources) != 1:
        raise ResourceResolutionError(
            f"Dataset {sanitize_uri(locator.dataset_id)!r} requires --resource. Valid selectors: {selectors}"
        )
    selected = CatalogResourceLocator(
        portal_url=locator.portal_url,
        dataset_id=locator.dataset_id,
        resource_id=resources[0].id,
    )
    return selected, data_sluice.resolve(selected)
