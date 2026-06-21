"""Mapping functions to convert data.gouv.fr (udata) JSON into domain models."""

from __future__ import annotations

from typing import Any

from datasluice.domain import Dataset, License, Organization, Resource


def map_license(raw: dict[str, Any] | None) -> License | None:
    """Convert a udata license dict into a :class:`License`."""
    if not raw:
        return None
    return License(
        id=raw.get("id", ""),
        title=raw.get("title"),
        url=raw.get("url"),
    )


def map_resource(raw: dict[str, Any]) -> Resource:
    """Convert a udata resource dict into a :class:`Resource`."""
    return Resource(
        id=str(raw.get("id", "")),
        name=raw.get("title"),
        url=raw.get("url") or raw.get("latest"),
        format=Resource.normalize_format(raw.get("format")),
        media_type=raw.get("mime"),
        description=raw.get("description"),
        size=raw.get("filesize"),
        created=raw.get("created_at"),
        modified=raw.get("last_modified"),
        extra=raw,
    )


def map_organization(raw: dict[str, Any] | None) -> Organization | None:
    """Convert a udata organization dict into an :class:`Organization`."""
    if not raw:
        return None
    return Organization(
        id=str(raw.get("id", raw.get("slug", ""))),
        name=raw.get("slug"),
        title=raw.get("name"),
        description=raw.get("description"),
        url=raw.get("url") or raw.get("website"),
        logo_url=(raw.get("logo") or {}).get("url") if isinstance(raw.get("logo"), dict) else raw.get("logo_thumbnail"),
        created=raw.get("created_at"),
        extra=raw,
    )


def map_dataset(raw: dict[str, Any]) -> Dataset:
    """Convert a udata dataset dict into a :class:`Dataset`."""
    return Dataset(
        id=str(raw.get("id", "")),
        title=raw.get("title"),
        name=raw.get("slug"),
        description=raw.get("description"),
        resources=[map_resource(r) for r in raw.get("resources", [])],
        organization=map_organization(raw.get("organization")),
        license=map_license(
            {"id": raw.get("license"), "title": raw.get("license")}
            if isinstance(raw.get("license"), str)
            else raw.get("license")
        ),
        tags=raw.get("tags", []) if isinstance(raw.get("tags"), list) else [],
        themes=[t if isinstance(t, str) else t.get("label", "") for t in raw.get("theme", [])],
        language=raw.get("language", [])
        if isinstance(raw.get("language"), list)
        else [raw["language"]]
        if raw.get("language")
        else [],
        created=raw.get("created_at"),
        modified=raw.get("last_modified"),
        url=raw.get("page") or raw.get("uri"),
        extra=raw,
    )
