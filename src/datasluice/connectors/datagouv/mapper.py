"""Mapping functions to convert data.gouv.fr (udata) JSON into domain models."""

from __future__ import annotations

from typing import Any

from datasluice.domain import (
    Dataset,
    HttpDownload,
    License,
    Organization,
    Resource,
    ResourceAccess,
    Schema,
)


def map_license(raw: dict[str, Any] | None) -> License | None:
    """Convert a udata license dict into a :class:`License`."""
    if not raw:
        return None
    return License(
        id=raw.get("id", ""),
        title=raw.get("title"),
        url=raw.get("url"),
    )


def _resolve_access(raw: dict[str, Any]) -> ResourceAccess | None:
    """Resolve the resource access descriptor for udata resources.

    udata resources carry a direct ``url`` for HTTP download; udata does not
    expose a queryable-resource signal the way CKAN exposes
    ``datastore_active``, so the QueryAccess branch is omitted and the choice
    is binary: ``HttpDownload`` when ``url`` is truthy, ``None`` otherwise.
    """
    url = raw.get("url") or raw.get("latest")
    if url:
        return HttpDownload(url=str(url))
    return None


def _resolve_schema(raw: dict[str, Any]) -> Schema | None:
    """Best-effort schema extraction for udata resources.

    Reads the Frictionless-style ``schema.fields`` list when udata exposes it;
    returns ``None`` when the portal is silent so readers can infer from bytes.
    """
    schema = raw.get("schema")
    fields = schema.get("fields") if isinstance(schema, dict) else None
    if not fields:
        return None
    return Schema(name=str(raw.get("id") or "udata-resource"), columns=list(fields))


def map_resource(raw: dict[str, Any], *, base_url: str | None = None) -> Resource:
    """Convert a udata resource dict into a :class:`Resource`.

    Populates advisory ``access`` and ``schema`` descriptors
    so downstream readers can pick the right transport without re-probing the
    portal. ``base_url`` is accepted for parity with the CKAN mapper; udata
    resources do not require it for access-descriptor resolution.
    """
    del base_url
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
        access=_resolve_access(raw),
        schema=_resolve_schema(raw),
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


def map_dataset(raw: dict[str, Any], *, base_url: str | None = None) -> Dataset:
    """Convert a udata dataset dict into a :class:`Dataset`.

    ``base_url`` is forwarded to :func:`map_resource` for parity with the CKAN
    mapper (udata resources do not currently require it, but the contract is
    uniform across connectors).
    """
    return Dataset(
        id=str(raw.get("id", "")),
        title=raw.get("title"),
        name=raw.get("slug"),
        description=raw.get("description"),
        resources=[map_resource(r, base_url=base_url) for r in raw.get("resources", [])],
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
