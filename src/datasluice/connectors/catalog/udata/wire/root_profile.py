"""Exact uData root-profile request builders and bounded response decoders."""

from __future__ import annotations

import hashlib
import json
import math
import re
from codecs import getincrementaldecoder
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit

from datasluice.domain.catalog.udata import (
    ROOT_OPERATION,
    SET_SITE_OPERATION,
    SiteDataserviceCsvQuery,
    SiteDatasetCatalogQuery,
    SiteDatasetCsvQuery,
    SiteDocument,
    SiteOrganizationCsvQuery,
    SitePatchInput,
    SiteProfile,
    SiteReuseCsvQuery,
)
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError
from datasluice.runtime.transport.base import AsyncRuntimeStreamResponse, RuntimeStreamResponse

ROOT_OPERATIONS = {
    "get_site": ROOT_OPERATION,
    "set_site": SET_SITE_OPERATION,
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
    "rdf": "application/rdf+xml",
    "owl": "application/rdf+xml",
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
_MEDIA_TYPE = re.compile(r"^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$")
_SENSITIVE_QUERY_PARTS = frozenset({"api_key", "apikey", "token", "secret", "password", "credential", "signature"})

type SiteRootQuery = (
    SiteDatasetCatalogQuery
    | SiteDatasetCsvQuery
    | SiteDataserviceCsvQuery
    | SiteOrganizationCsvQuery
    | SiteReuseCsvQuery
)


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


def _query(query: SiteRootQuery | None) -> str:
    return urlencode(query.query_params()) if query is not None else ""


def _with_query(path: str, query: SiteRootQuery | None) -> str:
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
    query: SiteDatasetCatalogQuery | None = None, *, accept: str | None = None
) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/catalog with optional content negotiation."""
    return _request("GET", _with_query(f"{_SITE_PATH}catalog", query), headers=_accept_header(accept))


def rdf_catalog_format_request(
    fmt: str, query: SiteDatasetCatalogQuery | None = None
) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/catalog.<format>."""
    extension = _format_extension(fmt)
    return _request("GET", _with_query(f"{_SITE_PATH}catalog.{quote(extension, safe='')}", query))


def csv_request(name: str, query: SiteRootQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build one of the stock site CSV export requests."""
    path = _CSV_PATHS.get(name)
    if path is None:
        raise CatalogValidationError(
            "The uData site CSV export is not declared.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Use a declared site CSV export name.",
        )
    if name in {"datasets", "resources"} and query is not None and not isinstance(query, SiteDatasetCsvQuery):
        raise CatalogValidationError(
            "The uData dataset CSV routes require SiteDatasetCsvQuery.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Use the route-specific dataset query model.",
        )
    if name == "organizations" and query is not None and not isinstance(query, SiteOrganizationCsvQuery):
        raise CatalogValidationError(
            "The uData organization CSV route requires SiteOrganizationCsvQuery.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Use the route-specific organization query model.",
        )
    if name == "reuses" and query is not None and not isinstance(query, SiteReuseCsvQuery):
        raise CatalogValidationError(
            "The uData reuse CSV route requires SiteReuseCsvQuery.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Use the route-specific reuse query model.",
        )
    if name == "dataservices" and query is not None and not isinstance(query, SiteDataserviceCsvQuery):
        raise CatalogValidationError(
            "The uData dataservice CSV route requires SiteDataserviceCsvQuery.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Use the route-specific dataservice query model.",
        )
    if name not in {"datasets", "resources", "organizations", "reuses", "dataservices"} and query is not None:
        raise CatalogValidationError(
            "This uData CSV route does not accept query parameters in the pinned parser.",
            operation=ROOT_OPERATION,
            platform="udata",
            safe_action="Omit the query argument for this export route.",
        )
    return _request("GET", _with_query(path, query))


def datasets_csv_request(query: SiteDatasetCsvQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/datasets.csv."""
    return csv_request("datasets", query)


def resources_csv_request(query: SiteDatasetCsvQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/resources.csv."""
    return csv_request("resources", query)


def organizations_csv_request(query: SiteOrganizationCsvQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/organizations.csv."""
    return csv_request("organizations", query)


def reuses_csv_request(query: SiteReuseCsvQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    """Build GET /api/1/site/reuses.csv."""
    return csv_request("reuses", query)


def dataservices_csv_request(query: SiteDataserviceCsvQuery | None = None) -> tuple[str, str, dict[str, str], None]:
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
        _validate_json_value(payload)
        return SiteProfile.from_payload(payload, operation=operation)
    except ValueError as error:
        raise CatalogValidationError(
            "The uData site response contains an invalid documented field.",
            operation=operation,
            platform="udata",
            safe_action="Verify the response against the pinned uData site schema.",
        ) from error


def _validate_json_value(value: object) -> None:
    """Reject JSON values that stdlib parsing can represent as non-finite numbers."""
    if isinstance(value, Mapping):
        for item in value.values():
            _validate_json_value(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _validate_json_value(item)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError("uData JSON values must be finite numbers.")


def response_media_type(
    headers: Mapping[str, str],
    *,
    operation: str,
    status_code: int,
    expected_media_type: str | None = None,
) -> str:
    """Read exactly one non-blank Content-Type value without inventing metadata."""
    values = [value for key, value in headers.items() if key.lower() == "content-type"]
    if len(values) != 1 or not values[0].strip() or "," in values[0]:
        raise NativeCatalogError(
            "The uData response must supply exactly one non-blank Content-Type header.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    if expected_media_type is None:
        return values[0]
    return _media_type(values[0], expected_media_type, operation=operation, status_code=status_code)


def _media_type(value: str | None, expected: str, *, operation: str, status_code: int) -> str:
    if value is None:
        raise NativeCatalogError(
            "The uData response omits its Content-Type header.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    parts = [part.strip() for part in value.split(";")]
    media_type = parts[0].lower()
    if not media_type or _MEDIA_TYPE.fullmatch(media_type) is None or "," in media_type:
        raise NativeCatalogError(
            "The uData response contains an invalid Content-Type header.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    parameters: set[str] = set()
    for parameter in parts[1:]:
        key, separator, parameter_value = parameter.partition("=")
        key = key.strip().lower()
        if not separator or key != "charset" or parameter_value.strip().lower() != "utf-8" or key in parameters:
            raise NativeCatalogError(
                "The uData response contains unsupported or duplicate Content-Type parameters.",
                operation=operation,
                platform="udata",
                status_code=status_code,
            )
        parameters.add(key)
    expected_type = expected.split(";", 1)[0].strip().lower()
    if media_type != expected_type:
        raise NativeCatalogError(
            "The uData site document media type differs from its route contract.",
            operation=operation,
            platform="udata",
            status_code=status_code,
            metadata={"media_type": media_type},
        )
    return media_type


def _query_multimap(value: str) -> dict[str, tuple[str, ...]]:
    """Normalize query pairs without making repeated-key order significant."""
    values: dict[str, list[str]] = {}
    for key, item in parse_qsl(value, keep_blank_values=True):
        values.setdefault(key, []).append(item)
    return {key: tuple(sorted(items)) for key, items in values.items()}


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
    except UnicodeDecodeError:
        raise NativeCatalogError(
            "The uData site document is not valid UTF-8.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        ) from None
    media_type = _media_type(
        response_media_type,
        expected_media_type,
        operation=operation,
        status_code=status_code,
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


def digest_stream_document(
    response: RuntimeStreamResponse,
    *,
    endpoint: str,
    expected_media_type: str,
    max_bytes: int,
    sink: Callable[[bytes], None] | None = None,
    fmt: str | None = None,
    operation: str = ROOT_OPERATION,
) -> SiteDocument:
    """Digest a bounded synchronous export while forwarding each verified chunk to a sink."""
    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError("uData root export limits must be positive integers.")
    digest = hashlib.sha256()
    decoder = getincrementaldecoder("utf-8")()
    size_bytes = 0
    complete_ready = False
    decode_failure: NativeCatalogError | None = None
    try:
        media_type = response_media_type(
            response.headers,
            operation=operation,
            status_code=response.status_code,
            expected_media_type=expected_media_type,
        )
        for chunk in response:
            next_size = size_bytes + len(chunk)
            if next_size > max_bytes:
                raise NativeCatalogError(
                    "The uData site export exceeds its configured byte limit.",
                    operation=operation,
                    platform="udata",
                    status_code=response.status_code,
                )
            decoder.decode(chunk)
            digest.update(chunk)
            if sink is not None:
                sink(chunk)
            size_bytes = next_size
        decoder.decode(b"", final=True)
        complete_ready = True
    except UnicodeDecodeError:
        decode_failure = NativeCatalogError(
            "The uData site document is not valid UTF-8.",
            operation=operation,
            platform="udata",
            status_code=response.status_code,
        )
        response.fail(decode_failure)
    except BaseException as error:
        response.fail(error)
        raise
    finally:
        response.close()
    if decode_failure is not None:
        raise decode_failure
    if complete_ready:
        try:
            response.complete()
        except BaseException as error:
            response.fail(error)
            raise
    metadata = {"media_type": media_type, "size_bytes": size_bytes, "sha256": digest.hexdigest(), "streamed": True}
    return SiteDocument(
        endpoint=endpoint,
        media_type=media_type,
        status_code=response.status_code,
        size_bytes=size_bytes,
        sha256=digest.hexdigest(),
        metadata=metadata,
        format=fmt,
    )


async def digest_stream_document_async(
    response: AsyncRuntimeStreamResponse,
    *,
    endpoint: str,
    expected_media_type: str,
    max_bytes: int,
    sink: Callable[[bytes], Awaitable[None] | None] | None = None,
    fmt: str | None = None,
    operation: str = ROOT_OPERATION,
) -> SiteDocument:
    """Digest a bounded asynchronous export while forwarding each verified chunk to a sink."""
    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError("uData root export limits must be positive integers.")
    digest = hashlib.sha256()
    decoder = getincrementaldecoder("utf-8")()
    size_bytes = 0
    complete_ready = False
    decode_failure: NativeCatalogError | None = None
    try:
        media_type = response_media_type(
            response.headers,
            operation=operation,
            status_code=response.status_code,
            expected_media_type=expected_media_type,
        )
        async for chunk in response:
            next_size = size_bytes + len(chunk)
            if next_size > max_bytes:
                raise NativeCatalogError(
                    "The uData site export exceeds its configured byte limit.",
                    operation=operation,
                    platform="udata",
                    status_code=response.status_code,
                )
            decoder.decode(chunk)
            digest.update(chunk)
            if sink is not None:
                result = sink(chunk)
                if result is not None:
                    await result
            size_bytes = next_size
        decoder.decode(b"", final=True)
        complete_ready = True
    except UnicodeDecodeError:
        decode_failure = NativeCatalogError(
            "The uData site document is not valid UTF-8.",
            operation=operation,
            platform="udata",
            status_code=response.status_code,
        )
        await response.fail(decode_failure)
    except BaseException as error:
        await response.fail(error)
        raise
    finally:
        await response.aclose()
    if decode_failure is not None:
        raise decode_failure
    if complete_ready:
        try:
            await response.complete()
        except BaseException as error:
            await response.fail(error)
            raise
    metadata = {"media_type": media_type, "size_bytes": size_bytes, "sha256": digest.hexdigest(), "streamed": True}
    return SiteDocument(
        endpoint=endpoint,
        media_type=media_type,
        status_code=response.status_code,
        size_bytes=size_bytes,
        sha256=digest.hexdigest(),
        metadata=metadata,
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

    def reject_constant(value: str) -> None:
        raise ValueError(f"The uData JSON-LD context contains the non-JSON constant {value!r}.")

    try:
        payload = json.loads(body, parse_constant=reject_constant)
    except (TypeError, ValueError):
        raise NativeCatalogError(
            "The uData JSON-LD context is not valid JSON.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        ) from None
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
    expected_path_prefix: str | None = None,
    expected_query: str | None = None,
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
    try:
        target = urlsplit(urljoin(origin + "/", location))
    except ValueError as error:
        raise NativeCatalogError(
            "The uData root redirect contains an invalid target URL.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        ) from error
    configured = urlsplit(origin)
    if target.username or target.password or target.fragment:
        raise NativeCatalogError(
            "The uData root redirect contains unsafe URL components.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    if target.scheme.lower() != configured.scheme.lower() or target.netloc.lower() != configured.netloc.lower():
        raise NativeCatalogError(
            "The uData root redirect points outside the configured deployment origin.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    route = urlsplit(endpoint)
    required_path = route.path if expected_path is None else expected_path
    required_query = route.query if expected_query is None else expected_query
    if target.path != required_path and (
        expected_path_prefix is None or not target.path.startswith(expected_path_prefix)
    ):
        raise NativeCatalogError(
            "The uData root redirect target does not match its route contract.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    target_query = _query_multimap(target.query)
    required_query_map = _query_multimap(required_query)
    default_catalog_query = {"page": ("1",), "page_size": ("100",)}
    query_matches = target_query == required_query_map
    if route.path == f"{_SITE_PATH}catalog" and not required_query:
        query_matches = target_query == default_catalog_query
    if not query_matches:
        raise NativeCatalogError(
            "The uData root redirect query does not match its route contract.",
            operation=operation,
            platform="udata",
            status_code=status_code,
        )
    if any(
        any(part in key.lower() for part in _SENSITIVE_QUERY_PARTS)
        for key, _ in parse_qsl(target.query, keep_blank_values=True)
    ):
        raise NativeCatalogError(
            "The uData root redirect contains a sensitive query component.",
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
    "digest_stream_document",
    "digest_stream_document_async",
    "SiteDatasetCsvQuery",
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
    "response_media_type",
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
