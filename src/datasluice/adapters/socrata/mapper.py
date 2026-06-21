"""Mapping functions to convert Socrata-native JSON into domain models."""

from __future__ import annotations

from typing import Any

from datasluice.domain import Dataset, License, Organization, Resource


def map_resource(view: dict[str, Any]) -> Resource:
    """Convert a Socrata view/resource dict into a :class:`Resource`."""
    fourfour = view.get("id", "")
    return Resource(
        id=str(fourfour),
        name=view.get("name"),
        url=f"https://api.us.socrata.com/api/catalog/views/{fourfour}.json" if fourfour else None,
        format=Resource.normalize_format(view.get("resource", {}).get("type")),
        description=view.get("description"),
        created=view.get("createdAt"),
        modified=view.get("updatedAt"),
        extra=view,
    )


def map_organization(owner: dict[str, Any] | None) -> Organization | None:
    """Convert a Socrata customer/owner dict into an :class:`Organization`."""
    if not owner:
        return None
    return Organization(
        id=str(owner.get("id", owner.get("displayName", ""))),
        name=owner.get("displayName") or owner.get("screenName"),
        title=owner.get("displayName"),
        url=owner.get("url"),
        extra=owner,
    )


def map_dataset(result: dict[str, Any]) -> Dataset:
    """Convert a Socrata catalog search result into a :class:`Dataset`."""
    resource = result.get("resource", {})
    classification = result.get("classification", {})
    owner = result.get("owner") or resource.get("owner")
    return Dataset(
        id=str(resource.get("id", result.get("permalink", ""))),
        title=resource.get("name"),
        name=resource.get("id"),
        description=resource.get("description"),
        resources=[map_resource(resource)],
        organization=map_organization(owner),
        license=License(id=classification.get("license", {}).get("id", "unknown"))
        if classification.get("license")
        else None,
        tags=classification.get("domain_tags", []),
        themes=classification.get("domain_category", [])
        if isinstance(classification.get("domain_category"), list)
        else [classification.get("domain_category")]
        if classification.get("domain_category")
        else [],
        created=resource.get("createdAt"),
        modified=resource.get("updatedAt"),
        url=result.get("permalink") or resource.get("permalink"),
        extra=result,
    )
