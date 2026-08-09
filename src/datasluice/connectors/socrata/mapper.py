"""Mapping functions to convert Socrata-native JSON into domain models."""

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


def _resolve_access(resource: dict[str, Any], base_url: str | None) -> ResourceAccess | None:
    """Resolve the resource access descriptor for Socrata views.

    Socrata catalog responses nest resource metadata under the catalog result's
    ``resource`` key. File-typed views expose a direct ``url`` for HTTP download;
    purely-queryable views (no attachment URL) yield a Socrata SoQL
    :class:`QueryAccess` descriptor against the SODA2 ``/resource/{4x4}.json``
    endpoint. Returns ``None`` only when neither signal is present.
    """
    url = resource.get("url")
    if url:
        return HttpDownload(url=str(url))
    fourfour = resource.get("id")
    if fourfour:
        root = base_url or "https://data.socrata.example"
        return QueryAccess(
            endpoint=f"{root}/resource/{fourfour}.json",
            query_language="socrata-soql",
            extra={"four_by_four": str(fourfour)},
        )
    return None


def _resolve_schema(resource: dict[str, Any]) -> Schema | None:
    """Best-effort schema extraction for Socrata views.

    Socrata's catalog payload exposes typed column metadata as parallel arrays
    (``columns_field_name`` / ``columns_name`` / ``columns_datatype``, zipped
    positionally). ``columns_field_name`` is the stable SoQL column identifier
    and is preferred as the column ``id``; ``columns_name`` is the display
    label and serves as fallback. Returns ``None`` when the arrays are absent
    or empty so readers can infer from real bytes.
    """
    names = resource.get("columns_field_name") or resource.get("columns_name") or []
    types = resource.get("columns_datatype") or []
    if not names:
        return None
    columns = [{"id": str(n), "type": str(t)} for n, t in zip(names, types, strict=False)]
    return Schema(name=str(resource.get("id", "socrata-view")), columns=columns)


def map_resource(view: dict[str, Any], *, base_url: str | None = None) -> Resource:
    """Convert a Socrata view/resource dict into a :class:`Resource`.

    Populates advisory ``access`` and ``schema`` descriptors
    so downstream readers can pick the right transport without re-probing the
    portal. ``base_url`` is forwarded by the adapter for the SoQL
    :class:`QueryAccess` endpoint on purely-queryable views.
    """
    fourfour = view.get("id", "")
    return Resource(
        id=str(fourfour),
        name=view.get("name"),
        url=view.get("url") or f"https://api.us.socrata.com/api/catalog/views/{fourfour}.json" if fourfour else None,
        format=Resource.normalize_format(view.get("type")),
        description=view.get("description"),
        created=view.get("createdAt"),
        modified=view.get("updatedAt"),
        access=_resolve_access(view, base_url),
        schema=_resolve_schema(view),
        extra=view,
    )


def map_organization(owner: dict[str, Any] | None) -> Organization | None:
    """Convert a Socrata customer/owner dict into an :class:`Organization`."""
    if not owner:
        return None
    return Organization(
        id=str(owner.get("id", owner.get("display_name", ""))),
        name=owner.get("displayName") or owner.get("display_name") or owner.get("screenName"),
        title=owner.get("displayName") or owner.get("display_name"),
        url=owner.get("url"),
        extra=owner,
    )


def map_dataset(result: dict[str, Any], *, base_url: str | None = None) -> Dataset:
    """Convert a Socrata catalog search result into a :class:`Dataset`.

    ``base_url`` is forwarded to :func:`map_resource` so the SoQL
    :class:`QueryAccess` endpoint resolves against the right portal instance.
    """
    resource = result.get("resource", {})
    classification = result.get("classification", {})
    owner = result.get("owner") or resource.get("owner")
    return Dataset(
        id=str(resource.get("id", result.get("permalink", ""))),
        title=resource.get("name"),
        name=resource.get("id"),
        description=resource.get("description"),
        resources=[map_resource(resource, base_url=base_url)],
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
