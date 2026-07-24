"""Mapping functions to convert CKAN-native JSON into domain models."""

from __future__ import annotations

from typing import Any

from datasluice.domain import Dataset, License, Organization, Resource


def map_license(raw: dict[str, Any] | None) -> License | None:
    """Convert a CKAN license dict into a :class:`License`."""
    if not raw:
        return None
    return License(
        id=raw.get("id", ""),
        title=raw.get("title"),
        url=raw.get("url"),
    )


def map_resource(raw: dict[str, Any]) -> Resource:
    """Convert a CKAN resource dict into a :class:`Resource`."""
    return Resource(
        id=str(raw.get("id", "")),
        name=raw.get("name"),
        url=raw.get("url"),
        format=Resource.normalize_format(raw.get("format")),
        media_type=raw.get("mimetype") or raw.get("mimetype_inner"),
        description=raw.get("description"),
        size=raw.get("size"),
        created=raw.get("created"),
        modified=raw.get("last_modified"),
        extra=raw,
    )


def map_organization(raw: dict[str, Any] | None) -> Organization | None:
    """Convert a CKAN organization/group dict into an :class:`Organization`."""
    if not raw:
        return None
    return Organization(
        id=str(raw.get("id", raw.get("name", ""))),
        name=raw.get("name"),
        title=raw.get("title"),
        description=raw.get("description"),
        url=raw.get("url") or raw.get("site_url"),
        logo_url=raw.get("image_url"),
        created=raw.get("created"),
        extra=raw,
    )


def map_dataset(raw: dict[str, Any]) -> Dataset:
    """Convert a CKAN package dict into a :class:`Dataset`."""
    return Dataset(
        id=str(raw.get("id", "")),
        title=raw.get("title"),
        name=raw.get("name"),
        description=raw.get("notes"),
        resources=[map_resource(r) for r in raw.get("resources", [])],
        organization=map_organization(raw.get("organization")),
        license=map_license(
            {"id": raw.get("license_id"), "title": raw.get("license_title"), "url": raw.get("license_url")}
        ),
        tags=[t.get("name", "") for t in raw.get("tags", []) if t.get("name")],
        themes=[g.get("name", "") for g in raw.get("groups", []) if g.get("name")],
        created=raw.get("metadata_created"),
        modified=raw.get("metadata_modified"),
        url=raw.get("url"),
        extra=raw,
    )
