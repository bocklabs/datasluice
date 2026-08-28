"""Exact dataset wire encoders and decoders for the pinned uData contract."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import quote, urlencode

from datasluice.connectors.catalog.udata.mapping import _DATASETS_OPERATION_ID, PLATFORM
from datasluice.connectors.catalog.udata.models.datasets import (
    DatasetCreateInput,
    DatasetDeleteOptions,
    DatasetExtrasDelete,
    DatasetExtrasUpdate,
    DatasetListQuery,
    DatasetSearchQuery,
    DatasetSuggestQuery,
    DatasetUpdateInput,
)
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import NativeRecord
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError

DATASET_OPERATIONS = {
    "list": "udata/api-v1.list-datasets",
    "create": "udata/api-v1.create-dataset",
    "atom": "udata/api-v1.recent-datasets-atom",
    "get": "udata/api-v1.get-dataset",
    "update": "udata/api-v1.update-dataset",
    "delete": "udata/api-v1.delete-dataset",
    "feature": "udata/api-v1.feature-dataset",
    "unfeature": "udata/api-v1.unfeature-dataset",
    "rdf": "udata/api-v1.rdf-dataset",
    "rdf_format": "udata/api-v1.rdf-dataset-format",
    "suggest": "udata/api-v1.suggest-datasets",
    "v2_search": "udata/api-v2.search-datasets",
    "v2_list": "udata/api-v2.list-datasets",
    "v2_get": "udata/api-v2.get-dataset",
    "v2_get_extras": "udata/api-v2.get-dataset-extras",
    "v2_update_extras": "udata/api-v2.update-dataset-extras",
    "v2_delete_extras": "udata/api-v2.delete-dataset-extras",
}

_DATASET_PATH = "/api/1/datasets/"
_DATASET_V2_PATH = "/api/2/datasets/"


def _native_dataset(payload: Mapping[str, object], *, operation: str) -> NativeRecord:
    identifier = payload.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise CatalogValidationError(
            "The uData dataset document requires a non-empty string id.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned source oracle.",
        )
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(platform=PLATFORM, resource_kind=ResourceKind.DATASET, value=identifier),
        payload=dict(payload),
    )


def create_request(
    client_input: DatasetCreateInput,
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Encode POST /api/1/datasets/."""
    return "POST", _DATASET_PATH, {}, client_input.payload()


def update_request(
    dataset_id: str, client_input: DatasetUpdateInput
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Encode PUT /api/1/datasets/<id>/."""
    identifier = _required_id(dataset_id)
    return "PUT", f"{_DATASET_PATH}{identifier}/", {}, client_input.payload()


def delete_request(dataset_id: str, options: DatasetDeleteOptions) -> tuple[str, str, dict[str, str], None]:
    """Encode DELETE /api/1/datasets/<id>/."""
    identifier = _required_id(dataset_id)
    query = _query(options.query_params())
    path = f"{_DATASET_PATH}{identifier}/" + (f"?{query}" if query else "")
    return "DELETE", path, {}, None


def featured_request(dataset_id: str, featured: bool) -> tuple[str, str, dict[str, str], None]:
    """Encode POST/DELETE /api/1/datasets/<id>/featured/."""
    identifier = _required_id(dataset_id)
    return ("POST" if featured else "DELETE"), f"{_DATASET_PATH}{identifier}/featured/", {}, None


def suggest_request(query: DatasetSuggestQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/suggest/."""
    return "GET", f"{_DATASET_PATH}suggest/?{_query(query.query_params())}", {}, None


def atom_request(query: DatasetListQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/recent.atom."""
    return "GET", f"{_DATASET_PATH}recent.atom?{_query(query.query_params())}", {}, None


def rdf_request(dataset_id: str, fmt: str | None) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/<id>[/rdf|/rdf.<format>]."""
    identifier = _required_id(dataset_id)
    if fmt is None:
        return "GET", f"{_DATASET_PATH}{identifier}/rdf", {}, None
    if not isinstance(fmt, str) or not fmt or "/" in fmt or "." in fmt:
        raise CatalogValidationError(
            "The uData RDF format must be a short extension such as rdf, ttl, or jsonld.",
            operation=_DATASETS_OPERATION_ID,
            platform=PLATFORM.value,
            safe_action="Pass a short RDF extension without slashes.",
        )
    return "GET", f"{_DATASET_PATH}{identifier}/rdf.{quote(fmt, safe='')}", {}, None


def list_request(query: DatasetListQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/ with the stock query string."""
    return "GET", f"{_DATASET_PATH}?{_query(query.query_params())}", {}, None


def v2_search_request(query: DatasetSearchQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/2/datasets/search/."""
    return "GET", f"{_DATASET_V2_PATH}search/?{_query(query.query_params())}", {}, None


def v2_list_request(query: DatasetListQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/2/datasets/."""
    return "GET", f"{_DATASET_V2_PATH}?{_query(query.query_params())}", {}, None


def v2_get_request(dataset_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/2/datasets/<id>/."""
    return "GET", f"{_DATASET_V2_PATH}{_required_id(dataset_id)}/", {}, None


def v2_extras_get_request(dataset_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/2/datasets/<id>/extras/."""
    return "GET", f"{_DATASET_V2_PATH}{_required_id(dataset_id)}/extras/", {}, None


def v2_extras_put_request(
    dataset_id: str, client_input: DatasetExtrasUpdate
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Encode PUT /api/2/datasets/<id>/extras/."""
    return (
        "PUT",
        f"{_DATASET_V2_PATH}{_required_id(dataset_id)}/extras/",
        {},
        client_input.payload(),
    )


def v2_extras_delete_request(
    dataset_id: str, client_input: DatasetExtrasDelete
) -> tuple[str, str, dict[str, str], list[str]]:
    """Encode DELETE /api/2/datasets/<id>/extras/."""
    return (
        "DELETE",
        f"{_DATASET_V2_PATH}{_required_id(dataset_id)}/extras/",
        {},
        client_input.payload(),
    )


def parse_dataset_detail(payload: object, *, operation: str = _DATASETS_OPERATION_ID) -> NativeRecord:
    """Decode one full v1/v2 dataset document into a native record.

    The documented core fields (``id``, ``title``, ``slug``) are type-checked;
    unknown portal fields pass through losslessly into the native payload.
    """
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData dataset document must be a JSON object.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned source oracle.",
        )
    for field in ("id", "title", "slug"):
        value = payload.get(field)
        if value is not None and not isinstance(value, str):
            raise CatalogValidationError(
                f"The uData dataset document field {field!r} must be a string.",
                operation=operation,
                platform=PLATFORM.value,
                safe_action="Verify the response against the pinned source oracle.",
            )
    return _native_dataset(payload, operation=operation)


def parse_suggestions(payload: object, *, operation: str = _DATASETS_OPERATION_ID) -> tuple[NativeRecord, ...]:
    """Decode the suggest response into bounded native suggestion records."""
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData suggest response must be a JSON array of objects.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned source oracle.",
        )
    records = []
    for item in payload:
        title = item.get("title")
        if title is not None and not isinstance(title, str):
            raise CatalogValidationError(
                "The uData suggestion field 'title' must be a string.",
                operation=operation,
                platform=PLATFORM.value,
                safe_action="Verify the response against the pinned source oracle.",
            )
        records.append(
            NativeRecord(
                platform=PLATFORM,
                resource_kind=ResourceKind.DATASET,
                id=CatalogId(
                    platform=PLATFORM,
                    resource_kind=ResourceKind.DATASET,
                    value=_required_id(item.get("id"), operation=operation),
                ),
                payload=dict(item),
            )
        )
    return tuple(records)


def parse_extras(payload: object, *, operation: str = _DATASETS_OPERATION_ID) -> dict[str, object]:
    """Decode a v2 extras document."""
    if not isinstance(payload, dict):
        raise CatalogValidationError(
            "The uData extras response must be a JSON object.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned source oracle.",
        )
    return dict(payload)


RDF_FORMAT_MEDIA_TYPES = {
    "rdf": "application/rdf+xml",
    "xml": "application/rdf+xml",
    "ttl": "text/turtle",
    "nt": "application/n-triples",
    "n3": "text/n3",
    "jsonld": "application/ld+json",
}


def media_type_for_format(fmt: str) -> str:
    """Return the stock media type for one RDF format extension."""
    media_type = RDF_FORMAT_MEDIA_TYPES.get(fmt)
    if media_type is None:
        raise CatalogValidationError(
            f"The uData RDF format {fmt!r} has no stock media type.",
            operation=_DATASETS_OPERATION_ID,
            platform=PLATFORM.value,
            safe_action=f"Use one of the stock extensions: {sorted(RDF_FORMAT_MEDIA_TYPES)}.",
        )
    return media_type


def parse_text_document(body: bytes, media_type: str, *, operation: str = _DATASETS_OPERATION_ID) -> NativeRecord:
    """Bound a non-JSON document (atom/rdf) into a typed native record.

    The document body is the requested operation result, retained with its
    validated media type, byte size, and digest for caller-side verification.
    """
    import hashlib

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NativeCatalogError(
            "The uData text document is not valid UTF-8.",
            operation=operation,
            platform=PLATFORM.value,
        ) from exc
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(platform=PLATFORM, resource_kind=ResourceKind.DATASET, value=f"text:{media_type}"),
        payload={
            "media_type": media_type,
            "body": text,
            "size_bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
        },
    )


def _required_id(value: object, *, operation: str = _DATASETS_OPERATION_ID) -> str:
    if not isinstance(value, str) or not value:
        raise CatalogValidationError(
            "The uData dataset identifier must be a non-empty string.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Pass the dataset id or slug from a prior read.",
        )
    if any(character in value for character in "/?#"):
        raise CatalogValidationError(
            "The uData dataset identifier must be a single unescaped path segment.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Pass the raw dataset id or slug; separators are encoded automatically.",
        )
    return quote(value, safe="")


def _query(pairs: list[tuple[str, str]]) -> str:
    """Encode query pairs exactly, preserving repeated keys in order."""
    return urlencode(pairs, doseq=True) if any(isinstance(v, list) for _, v in pairs) else urlencode(pairs)


__all__ = [
    "atom_request",
    "list_request",
    "create_request",
    "delete_request",
    "featured_request",
    "parse_dataset_detail",
    "parse_extras",
    "parse_suggestions",
    "parse_text_document",
    "rdf_request",
    "suggest_request",
    "update_request",
    "v2_extras_delete_request",
    "v2_extras_get_request",
    "v2_extras_put_request",
    "v2_get_request",
    "v2_list_request",
    "v2_search_request",
]
