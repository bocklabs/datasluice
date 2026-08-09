"""Mapping functions to convert CKAN-native JSON into domain models."""

from __future__ import annotations

from typing import Any

from datasluice.domain import (
    Dataset,
    HttpDownload,
    License,
    Organization,
    QueryAccess,
    Resource,
    ResourceAccess,
    Schema,
)


def _coerce_int(value: Any) -> int | None:
    """Best-effort coerce *value* to ``int``; return ``None`` on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def map_license(raw: dict[str, Any] | None) -> License | None:
    """Convert a CKAN license dict into a :class:`License`."""
    if not raw:
        return None
    return License(
        id=raw.get("id") or "",
        title=raw.get("title"),
        url=raw.get("url"),
    )


def _resolve_access(raw: dict[str, Any], base_url: str | None) -> ResourceAccess | None:
    """Resolve the resource access descriptor.

    HttpDownload wins when ``url`` is truthy; otherwise a CKAN datastore-backed
    QueryAccess is emitted when ``datastore_active`` is truthy; else ``None``.
    The endpoint URL is advisory — when ``base_url`` is supplied by
    the adapter it points at the standard CKAN ``datastore_search`` action,
    otherwise a stable placeholder is emitted.
    """
    url = raw.get("url")
    if url:
        return HttpDownload(url=str(url))
    if bool(raw.get("datastore_active", False)):
        if base_url:
            endpoint = f"{base_url}/api/3/action/datastore_search"
        else:
            endpoint = "ckan://api/3/action/datastore_search"
        return QueryAccess(
            endpoint=endpoint,
            query_language="ckan-datastore",
            extra={"resource_id": str(raw.get("id", ""))},
        )
    return None


def _resolve_schema(raw: dict[str, Any]) -> Schema | None:
    """Best-effort schema extraction.

    Reads ``datastore_fields`` first (CKAN DataPusher output), then falls back
    to ``schema.fields`` (Frictionless Tabular Data Package). Returns ``None``
    when the portal is silent so callers can infer the schema from real bytes.
    """
    fields = raw.get("datastore_fields") or (raw.get("schema") or {}).get("fields")
    if not fields:
        return None
    return Schema(name=str(raw.get("id", "ckan-resource")), columns=list(fields))


def map_resource(raw: dict[str, Any], *, base_url: str | None = None) -> Resource:
    """Convert a CKAN resource dict into a :class:`Resource`.

    Populates advisory ``access`` and ``schema`` descriptors
    so downstream readers can pick the right transport without re-probing the
    portal. ``base_url`` is forwarded by the adapter for QueryAccess endpoints.
    """
    return Resource(
        id=str(raw.get("id", "")),
        name=raw.get("name"),
        url=raw.get("url"),
        format=Resource.normalize_format(raw.get("format")),
        media_type=raw.get("mimetype") or raw.get("mimetype_inner"),
        description=raw.get("description"),
        size=_coerce_int(raw.get("size")),
        created=raw.get("created"),
        modified=raw.get("last_modified"),
        access=_resolve_access(raw, base_url),
        schema=_resolve_schema(raw),
        extra=raw,
    )


def map_organization(raw: dict[str, Any] | None) -> Organization | None:
    """Convert a CKAN organization/group dict into an :class:`Organization`."""
    if not raw:
        return None
    return Organization(
        id=str(raw.get("id") or raw.get("name") or ""),
        name=raw.get("name"),
        title=raw.get("title"),
        description=raw.get("description"),
        url=raw.get("url") or raw.get("site_url"),
        logo_url=raw.get("image_url"),
        created=raw.get("created"),
        extra=raw,
    )


def map_dataset(raw: dict[str, Any], *, base_url: str | None = None) -> Dataset:
    """Convert a CKAN package dict into a :class:`Dataset`.

    ``base_url`` is forwarded to :func:`map_resource` so QueryAccess endpoints
    resolve against the right portal instance.
    """
    return Dataset(
        id=str(raw.get("id", "")),
        title=raw.get("title"),
        name=raw.get("name"),
        description=raw.get("notes"),
        resources=[map_resource(r, base_url=base_url) for r in raw.get("resources") or []],
        organization=map_organization(raw.get("organization")),
        license=map_license(
            {"id": raw.get("license_id"), "title": raw.get("license_title"), "url": raw.get("license_url")}
        ),
        tags=[t.get("name") for t in raw.get("tags") or [] if t.get("name")],
        themes=[g.get("name") for g in raw.get("groups") or [] if g.get("name")],
        created=raw.get("metadata_created"),
        modified=raw.get("metadata_modified"),
        url=raw.get("url"),
        extra=raw,
    )
