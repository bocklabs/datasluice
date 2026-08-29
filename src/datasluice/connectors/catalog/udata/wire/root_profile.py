"""Exact uData root-profile request builders and bounded response decoders."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

from datasluice.connectors.catalog.udata.models.root_profile import (
    ROOT_OPERATION,
    SiteCatalogQuery,
    SiteDocument,
    SitePatchInput,
    SiteProfile,
)
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError

ROOT_OPERATIONS = {
    "get_site": ROOT_OPERATION,
    "set_site": "udata/api-v1.set-site",
    "data_portal": ROOT_OPERATION,
    "site_data_portal": ROOT_OPERATION,
    "rdf_catalog": ROOT_OPERATION,
    "site_rdf_catalog": ROOT_OPERATION,
    "rdf_catalog_format": ROOT_OPERATION,
    "site_rdf_catalog_format": ROOT_OPERATION,
    "datasets_csv": ROOT_OPERATION,
    "site_datasets_csv": ROOT_OPERATION,
    "resources_csv": ROOT_OPERATION,
    "site_resources_csv": ROOT_OPERATION,
    "organizations_csv": ROOT_OPERATION,
    "site_organizations_csv": ROOT_OPERATION,
    "reuses_csv": ROOT_OPERATION,
    "site_reuses_csv": ROOT_OPERATION,
    "dataservices_csv": ROOT_OPERATION,
    "site_dataservices_csv": ROOT_OPERATION,
    "harvests_csv": ROOT_OPERATION,
    "site_harvests_csv": ROOT_OPERATION,
    "tags_csv": ROOT_OPERATION,
    "site_tags_csv": ROOT_OPERATION,
    "jsonld_context": ROOT_OPERATION,
    "site_jsonld_context": ROOT_OPERATION,
}

_SITE_PATH = "/api/1/site/"
_FORMAT_MEDIA_TYPES = {
    "xml": "application/rdf+xml",
    "n3": "text/n3",
    "ttl": "application/x-turtle",
    "turtle": "application/x-turtle",
    "nt": "application/n-triples",
    "json": "application/ld+json",
    "jsonld": "application/ld+json",
    "trig": "application/trig",
}
_ACCEPTED_MEDIA_TYPES = {
    "application/rdf+xml",
    "application/xml",
    "text/n3",
    "application/x-turtle",
    "text/turtle",
    "application/n-triples",
    "application/ld+json",
    "application/json",
    "application/trig",
    "text/xml",
}
_CSV_PATHS = {
    "datasets": "/api/1/site/datasets.csv",
    "resources": "/api/1/site/resources.csv",
    "organizations": "/api/1/site/organizations.csv",
    "reuses": "/api/1/site/reuses.csv",
    "dataservices": "/api/1/site/dataservices.csv",
    "harvests": "/api/1/site/harvests.csv",
    "tags": "/api/1/site/tags.csv",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FORMAT = re.compile(r"^[A-Za-z0-9-]{1,16}$")
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


def _format_extension(value: object) -> str:
    if not isinstance(value, str) or _FORMAT.fullmatch(value) is None:
        raise CatalogValidationError(
            "The uData site document format must be a short ASCII extension.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Use one of the documented RDF format extensions.",
        )
    extension = value.lower()
    if extension not in _FORMAT_MEDIA_TYPES:
        raise CatalogValidationError(
            "The uData site document format is unsupported.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Use xml, n3, ttl, nt, json, jsonld, or trig.",
        )
    return extension


def media_type_for_format(value: str) -> str:
    """Return the documented media type for one root RDF extension."""
    return _FORMAT_MEDIA_TYPES[_format_extension(value)]


def _query(query: SiteCatalogQuery | None) -> str:
    return urlencode(query.query_params()) if query is not None else ""


def _with_query(path: str, query: SiteCatalogQuery | None) -> str:
    encoded = _query(query)
    return f"{path}?{encoded}" if encoded else path


def _accept_header(accept: str | None) -> dict[str, str]:
    if accept is None:
        return {}
    if not isinstance(accept, str) or not accept:
        raise CatalogValidationError(
            "The uData RDF Accept value must be a non-empty media type.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Pass a documented RDF media type or omit Accept.",
        )
    media_type = accept.split(";", 1)[0].strip().lower()
    if media_type not in _ACCEPTED_MEDIA_TYPES:
        raise CatalogValidationError(
            "The uData RDF Accept value is not supported by the pinned profile.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Pass a documented uData RDF media type.",
        )
    return {"Accept": accept}


def _request(
    method: str, path: str, *, headers: Mapping[str, str] | None = None, body: object = None
) -> tuple[str, str, dict[str, str], Any]:
    return method, path, dict(headers or {}), body


def get_site_request() -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/."""
    return _request("GET", _SITE_PATH)


def set_site_request(client_input: SitePatchInput) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Build PATCH /api/1/site/ with exact presence semantics."""
    if not isinstance(client_input, SitePatchInput):
        raise TypeError("uData site PATCH requires SitePatchInput.")
    return _request("PATCH", _SITE_PATH, body=client_input.payload())


def data_portal_request(fmt: str) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/data.<format>."""
    extension = _format_extension(fmt)
    return _request("GET", f"{_SITE_PATH}data.{quote(extension, safe='')}")


def rdf_catalog_request(
    query: SiteCatalogQuery | None = None, *, accept: str | None = None
) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/catalog with optional content negotiation."""
    return _request("GET", _with_query(f"{_SITE_PATH}catalog", query), headers=_accept_header(accept))


def rdf_catalog_format_request(
    fmt: str, query: SiteCatalogQuery | None = None
) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/catalog.<format>."""
    extension = _format_extension(fmt)
    return _request("GET", _with_query(f"{_SITE_PATH}catalog.{quote(extension, safe='')}", query))


def csv_request(name: str, query: SiteCatalogQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build one of the stock site CSV export requests."""
    path = _CSV_PATHS.get(name)
    if path is None:
        raise CatalogValidationError(
            "The uData site CSV export is not declared.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Use a declared site CSV export name.",
        )
    return _request("GET", _with_query(path, query))


def datasets_csv_request(query: SiteCatalogQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/datasets.csv."""
    return csv_request("datasets", query)


def resources_csv_request(query: SiteCatalogQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/resources.csv."""
    return csv_request("resources", query)


def organizations_csv_request(query: SiteCatalogQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/organizations.csv."""
    return csv_request("organizations", query)


def reuses_csv_request(query: SiteCatalogQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/reuses.csv."""
    return csv_request("reuses", query)


def dataservices_csv_request(query: SiteCatalogQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/dataservices.csv."""
    return csv_request("dataservices", query)


def harvests_csv_request() -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/harvests.csv."""
    return csv_request("harvests")


def tags_csv_request() -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/tags.csv."""
    return csv_request("tags")


def jsonld_context_request() -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/context.jsonld."""
    return _request("GET", f"{_SITE_PATH}context.jsonld")


def parse_site_profile(payload: object, *, operation: str = ROOT_OPERATION) -> SiteProfile:
    """Decode a typed site document while preserving unknown fields."""
    try:
        return SiteProfile.from_payload(payload, operation=operation)
    except ValueError as error:
        raise CatalogValidationError(
            "The uData site response contains an invalid documented field.",
            operation=operation,
            platform="udata",
            safe_action="Verify the response against the pinned uData site schema.",
        ) from error


def _media_type(value: str | None, fallback: str) -> str:
    negotiated = (value or fallback).split(";", 1)[0].strip().lower()
    return negotiated


def parse_document(
    body: bytes,
    *,
    endpoint: str,
    expected_media_type: str,
    response_media_type: str | None = None,
    status_code: int = 200,
    fmt: str | None = None,
    data: Mapping[str, object] | None = None,
    operation: str = ROOT_OPERATION,
) -> SiteDocument:
    """Decode a bounded root document without retaining its bytes."""
    if not isinstance(body, bytes):
        raise NativeCatalogError(
            "The uData site document body must be buffered bytes.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    try:
        body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise NativeCatalogError(
            "The uData site document is not valid UTF-8.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        ) from error
    media_type = _media_type(response_media_type, expected_media_type)
    expected = expected_media_type.split(";", 1)[0].strip().lower()
    if media_type != expected:
        raise NativeCatalogError(
            "The uData site document media type differs from its route contract.",
            operation=operation,
            platform="udata",
            status_code=status_code,
            metadata={"media_type": media_type},
        )
    digest = hashlib.sha256(body).hexdigest()
    metadata = {"media_type": media_type, "size_bytes": len(body), "sha256": digest}
    return SiteDocument(
        endpoint=endpoint,
        media_type=media_type,
        status_code=status_code,
        size_bytes=len(body),
        sha256=digest,
        metadata=metadata,
        data=data,
        format=fmt,
    )


def parse_jsonld_context(
    body: bytes,
    *,
    endpoint: str,
    response_media_type: str | None = None,
    status_code: int = 200,
    operation: str = ROOT_OPERATION,
) -> SiteDocument:
    """Decode the JSON-LD context into an immutable JSON-safe mapping."""
    try:
        payload = json.loads(body)
    except (TypeError, ValueError) as error:
        raise NativeCatalogError(
            "The uData JSON-LD context is not valid JSON.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        ) from error
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData JSON-LD context must be a JSON object.",
            operation=operation,
            platform="udata",
            safe_action="Verify the deployment serves the stock context.jsonld document.",
        )
    return parse_document(
        body,
        endpoint=endpoint,
        expected_media_type="application/ld+json",
        response_media_type=response_media_type,
        status_code=status_code,
        data=payload,
        operation=operation,
    )


def parse_redirect(
    *,
    status_code: int,
    headers: Mapping[str, str],
    endpoint: str,
    origin: str,
    expected_path: str | None = None,
    operation: str = ROOT_OPERATION,
) -> SiteDocument:
    """Decode a same-origin uData redirect into bounded metadata."""
    if status_code not in _REDIRECT_STATUSES:
        raise NativeCatalogError(
            "The uData root response is not a redirect.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    location = next((value for key, value in headers.items() if key.lower() == "location"), None)
    if not location:
        raise NativeCatalogError(
            "The uData root redirect omits its Location header.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    target = urlsplit(location)
    configured = urlsplit(origin)
    if target.username or target.password or target.fragment:
        raise NativeCatalogError(
            "The uData root redirect contains unsafe URL components.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    if target.scheme and target.scheme != configured.scheme or target.netloc and target.netloc != configured.netloc:
        raise NativeCatalogError(
            "The uData root redirect points outside the configured deployment origin.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    if expected_path is not None and target.path != expected_path:
        raise NativeCatalogError(
            "The uData root redirect target does not match its route contract.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    digest = hashlib.sha256(b"").hexdigest()
    metadata = {"status_code": status_code, "location": location}
    return SiteDocument(
        endpoint=endpoint,
        media_type="text/plain",
        status_code=status_code,
        size_bytes=0,
        sha256=digest,
        metadata=metadata,
        location=location,
    )


site_request = get_site_request
site_data_portal_request = data_portal_request
site_rdf_catalog_request = rdf_catalog_request
site_rdf_catalog_format_request = rdf_catalog_format_request
site_datasets_csv_request = datasets_csv_request
site_resources_csv_request = resources_csv_request
site_organizations_csv_request = organizations_csv_request
site_reuses_csv_request = reuses_csv_request
site_dataservices_csv_request = dataservices_csv_request
site_harvests_csv_request = harvests_csv_request
site_tags_csv_request = tags_csv_request
site_jsonld_context_request = jsonld_context_request
parse_site = parse_site_profile

__all__ = [
    "ROOT_OPERATION",
    "ROOT_OPERATIONS",
    "csv_request",
    "data_portal_request",
    "datasets_csv_request",
    "dataservices_csv_request",
    "get_site_request",
    "harvests_csv_request",
    "jsonld_context_request",
    "media_type_for_format",
    "organizations_csv_request",
    "parse_document",
    "parse_jsonld_context",
    "parse_redirect",
    "parse_site_profile",
    "rdf_catalog_format_request",
    "rdf_catalog_request",
    "resources_csv_request",
    "reuses_csv_request",
    "set_site_request",
    "site_data_portal_request",
    "site_datasets_csv_request",
    "site_dataservices_csv_request",
    "site_harvests_csv_request",
    "site_jsonld_context_request",
    "site_organizations_csv_request",
    "site_rdf_catalog_format_request",
    "site_rdf_catalog_request",
    "site_request",
    "site_resources_csv_request",
    "site_reuses_csv_request",
    "site_tags_csv_request",
    "tags_csv_request",
]
