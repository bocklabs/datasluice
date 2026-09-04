"""Exact dataset wire encoders and decoders for the pinned uData contract."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import cast
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
from datasluice.domain.catalog.models import NativeRecord, _freeze_json
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
_DATASET_STRING_FIELDS = frozenset(
    {
        "id",
        "title",
        "acronym",
        "slug",
        "description",
        "description_short",
        "created",
        "created_at",
        "last_modified",
        "last_update",
        "deleted",
        "archived",
        "frequency",
        "frequency_date",
        "license",
        "access_type",
        "authorization_request_url",
        "access_type_reason_category",
        "access_type_reason",
        "uri",
        "page",
    }
)
_DATASET_BOOLEAN_FIELDS = frozenset({"private", "featured"})
_DATASET_MAPPING_FIELDS = frozenset(
    {
        "organization",
        "owner",
        "metrics",
        "extras",
        "harvest",
        "temporal_coverage",
        "spatial",
        "schema",
        "quality",
        "internal",
        "permissions",
    }
)
_DATASET_LINK_FIELDS = frozenset({"resources", "community_resources"})
_DATASET_LIST_FIELDS = frozenset({"tags", "badges", "access_audiences", "contact_points"})
_DATASET_DETAIL_FIELDS = frozenset(
    {
        "id",
        "title",
        "acronym",
        "slug",
        "description",
        "description_short",
        "created",
        "created_at",
        "last_modified",
        "last_update",
        "private",
        "featured",
        "deleted",
        "archived",
        "frequency",
        "frequency_date",
        "license",
        "access_type",
        "access_audiences",
        "authorization_request_url",
        "access_type_reason_category",
        "access_type_reason",
        "uri",
        "page",
        "organization",
        "owner",
        "metrics",
        "extras",
        "harvest",
        "temporal_coverage",
        "spatial",
        "schema",
        "quality",
        "internal",
        "permissions",
        "tags",
        "resources",
        "community_resources",
        "badges",
        "contact_points",
    }
)


def _invalid_field(field: str, operation: str, expected: str) -> CatalogValidationError:
    return CatalogValidationError(
        f"The uData dataset field {field!r} must be {expected}.",
        operation=operation,
        platform=PLATFORM.value,
        safe_action="Verify the response against the pinned source oracle.",
    )


def _validate_dataset_link(value: object, *, field: str, operation: str) -> None:
    if not isinstance(value, Mapping):
        raise _invalid_field(field, operation, "a list of objects or a link object")
    for key in ("href", "rel", "type"):
        nested = value.get(key)
        if not isinstance(nested, str) or not nested:
            raise _invalid_field(f"{field}.{key}", operation, "a non-empty string")
    total = value.get("total")
    if type(total) is not int or total < 0:
        raise _invalid_field(f"{field}.total", operation, "a non-negative integer")


def _validate_json_value(value: object, *, operation: str, path: str) -> None:
    if value is None or type(value) is bool or type(value) is int or isinstance(value, str):
        return
    if isinstance(value, float):
        import math

        if math.isfinite(value):
            return
        raise _invalid_field(path, operation, "a finite JSON value")
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if not isinstance(key, str) or not key:
                raise _invalid_field(path, operation, "an object with string keys")
            _validate_json_value(nested, operation=operation, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, nested in enumerate(value):
            _validate_json_value(nested, operation=operation, path=f"{path}[{index}]")
        return
    raise _invalid_field(path, operation, "a JSON value")


def _validate_nested_string_fields(
    value: Mapping[str, object], fields: frozenset[str], *, operation: str, path: str
) -> None:
    for field in fields:
        nested = value.get(field)
        if nested is not None and (not isinstance(nested, str) or not nested):
            raise _invalid_field(f"{path}.{field}", operation, "a non-empty string or null")


def _validate_dataset_nested_fields(payload: Mapping[str, object], *, operation: str) -> None:
    for field in ("organization", "owner"):
        value = payload.get(field)
        if isinstance(value, Mapping):
            _validate_nested_string_fields(value, frozenset({"id", "name", "slug"}), operation=operation, path=field)
    for field in ("schema", "temporal_coverage"):
        value = payload.get(field)
        if isinstance(value, Mapping):
            _validate_nested_string_fields(
                value,
                frozenset({"name", "version", "url", "start", "end"}),
                operation=operation,
                path=field,
            )
    for field in ("resources", "badges", "community_resources", "access_audiences", "contact_points"):
        value = payload.get(field)
        if isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, Mapping):
                    _validate_nested_string_fields(
                        item,
                        frozenset({"id", "title", "name", "slug", "url"}),
                        operation=operation,
                        path=f"{field}[{index}]",
                    )


def _validate_dataset_fields(payload: Mapping[str, object], *, operation: str, detail: bool) -> None:
    required = ("id", "title", "slug") if detail else ("id",)
    for field in required:
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            raise _invalid_field(field, operation, "a non-empty string")
    for field, value in payload.items():
        if field in _DATASET_STRING_FIELDS:
            if value is not None and (not isinstance(value, str) or not value):
                raise _invalid_field(field, operation, "a non-empty string or null")
        elif field in _DATASET_BOOLEAN_FIELDS:
            if type(value) is not bool:
                raise _invalid_field(field, operation, "a boolean")
        elif field in _DATASET_MAPPING_FIELDS:
            if value is not None and not isinstance(value, Mapping):
                raise _invalid_field(field, operation, "an object or null")
        elif field in _DATASET_LINK_FIELDS and operation.startswith("udata/api-v2."):
            _validate_dataset_link(value, field=field, operation=operation)
        elif field in _DATASET_LIST_FIELDS or field in _DATASET_LINK_FIELDS:
            if not isinstance(value, list):
                raise _invalid_field(field, operation, "a list")
            if field == "tags" and not all(isinstance(tag, str) and tag for tag in value):
                raise _invalid_field(field, operation, "a list of non-empty strings")
            if field != "tags" and not all(isinstance(item, Mapping) for item in value):
                raise _invalid_field(field, operation, "a list of objects")
        _validate_json_value(value, operation=operation, path=field)
    _validate_dataset_nested_fields(payload, operation=operation)


def _native_dataset(payload: Mapping[str, object], *, operation: str) -> NativeRecord:
    identifier = cast(str, payload["id"])
    known = {key: value for key, value in payload.items() if key in _DATASET_DETAIL_FIELDS}
    extensions = {"udata.dataset": {key: value for key, value in payload.items() if key not in _DATASET_DETAIL_FIELDS}}
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(platform=PLATFORM, resource_kind=ResourceKind.DATASET, value=identifier),
        payload=known,
        extensions=extensions,
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
    identifier = _path_segment(_required_id(dataset_id))
    return "PUT", f"{_DATASET_PATH}{identifier}/", {}, client_input.payload()


def delete_request(dataset_id: str, options: DatasetDeleteOptions) -> tuple[str, str, dict[str, str], None]:
    """Encode DELETE /api/1/datasets/<id>/."""
    identifier = _path_segment(_required_id(dataset_id))
    query = _query(options.query_params())
    path = f"{_DATASET_PATH}{identifier}/" + (f"?{query}" if query else "")
    return "DELETE", path, {}, None


def featured_request(dataset_id: str, featured: bool) -> tuple[str, str, dict[str, str], None]:
    """Encode POST/DELETE /api/1/datasets/<id>/featured/."""
    identifier = _path_segment(_required_id(dataset_id))
    return ("POST" if featured else "DELETE"), f"{_DATASET_PATH}{identifier}/featured/", {}, None


def suggest_request(query: DatasetSuggestQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/suggest/."""
    return "GET", f"{_DATASET_PATH}suggest/?{_query(query.query_params())}", {}, None


def atom_request(query: DatasetListQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/recent.atom."""
    return "GET", f"{_DATASET_PATH}recent.atom?{_query(query.query_params())}", {}, None


def rdf_request(dataset_id: str, fmt: str | None) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/<id>[/rdf|/rdf.<format>]; validates the format allowlist."""
    identifier = _path_segment(_required_id(dataset_id))
    if fmt is None:
        return "GET", f"{_DATASET_PATH}{identifier}/rdf", {}, None
    extension = _format_extension(fmt)
    media_type_for_format(extension)
    return "GET", f"{_DATASET_PATH}{identifier}/rdf.{_path_segment(extension)}", {}, None


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
    return "GET", f"{_DATASET_V2_PATH}{_path_segment(_required_id(dataset_id))}/", {}, None


def v2_extras_get_request(dataset_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/2/datasets/<id>/extras/."""
    return "GET", f"{_DATASET_V2_PATH}{_path_segment(_required_id(dataset_id))}/extras/", {}, None


def v2_extras_put_request(
    dataset_id: str, client_input: DatasetExtrasUpdate
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Encode PUT /api/2/datasets/<id>/extras/."""
    return (
        "PUT",
        f"{_DATASET_V2_PATH}{_path_segment(_required_id(dataset_id))}/extras/",
        {},
        client_input.payload(),
    )


def v2_extras_delete_request(
    dataset_id: str, client_input: DatasetExtrasDelete
) -> tuple[str, str, dict[str, str], list[str]]:
    """Encode DELETE /api/2/datasets/<id>/extras/."""
    return (
        "DELETE",
        f"{_DATASET_V2_PATH}{_path_segment(_required_id(dataset_id))}/extras/",
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
    _validate_dataset_fields(payload, operation=operation, detail=True)
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
        _validate_dataset_fields(item, operation=operation, detail=False)
        title = item.get("title")
        if not isinstance(title, str) or not title:
            raise CatalogValidationError(
                "The uData suggestion requires a non-empty string title.",
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
                payload={key: value for key, value in item.items() if key in _DATASET_DETAIL_FIELDS},
                extensions={
                    "udata.dataset": {key: value for key, value in item.items() if key not in _DATASET_DETAIL_FIELDS}
                },
            )
        )
    return tuple(records)


def parse_extras(payload: object, *, operation: str = _DATASETS_OPERATION_ID) -> Mapping[str, object]:
    """Decode a v2 extras document with typed value validation."""
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData extras response must be a JSON object.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned source oracle.",
        )
    for key, value in payload.items():
        _validate_json_value(value, operation=operation, path=key)
    frozen = _freeze_json(payload, "udata.dataset_extras")
    if not isinstance(frozen, Mapping):
        raise CatalogValidationError(
            "The uData extras response must be a JSON object.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned source oracle.",
        )
    return frozen


RDF_FORMAT_MEDIA_TYPES = {
    "rdf": "application/rdf+xml",
    "xml": "application/rdf+xml",
    "ttl": "application/x-turtle",
    "turtle": "application/x-turtle",
    "nt": "application/n-triples",
    "n3": "text/n3",
    "json": "application/ld+json",
    "jsonld": "application/ld+json",
    "trig": "application/trig",
}

_APPROVED_TEXT_MEDIA_TYPES = frozenset(
    {
        "application/atom+xml",
        "application/json",
        "application/ld+json",
        "application/n-triples",
        "application/rdf+xml",
        "application/trig",
        "application/x-turtle",
        "text/n3",
        "text/turtle",
        "text/xml",
    }
)


def media_type_for_format(fmt: str) -> str:
    """Return the stock media type for one RDF format extension."""
    extension = _format_extension(fmt)
    media_type = RDF_FORMAT_MEDIA_TYPES.get(extension)
    if media_type is None:
        raise CatalogValidationError(
            "The uData RDF format is unsupported.",
            operation=_DATASETS_OPERATION_ID,
            platform=PLATFORM.value,
            safe_action="Use a stock uData RDF extension.",
        )
    return media_type


def _format_extension(fmt: object) -> str:
    if not isinstance(fmt, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,16}", fmt) is None:
        raise CatalogValidationError(
            "The uData RDF format is invalid.",
            operation=_DATASETS_OPERATION_ID,
            platform=PLATFORM.value,
            safe_action="Use a short ASCII uData RDF extension.",
        )
    return fmt.lower()


def parse_text_document(
    body: bytes, media_type: str, *, response_media_type: str | None = None, operation: str = _DATASETS_OPERATION_ID
) -> NativeRecord:
    """Bound a non-JSON document (atom/rdf) into a typed native record.

    The no-raw-body contract is preserved: only the approved media type, byte
    count, and SHA-256 digest are retained — never the document content.
    """
    if not isinstance(body, bytes):
        raise NativeCatalogError(
            "The uData document body must be buffered bytes.",
            operation=operation,
            platform=PLATFORM.value,
        )
    try:
        body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NativeCatalogError(
            "The uData text document is not valid UTF-8.",
            operation=operation,
            platform=PLATFORM.value,
        ) from exc
    negotiated = (response_media_type or media_type).split(";", 1)[0].strip().lower()
    if negotiated not in _APPROVED_TEXT_MEDIA_TYPES:
        raise NativeCatalogError(
            f"The uData document media type {negotiated!r} is not an approved text contract.",
            operation=operation,
            platform=PLATFORM.value,
            metadata={"safe_action": f"Request one of the approved media types: {media_type}."},
        )
    digest = hashlib.sha256(body).hexdigest()
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(platform=PLATFORM, resource_kind=ResourceKind.DATASET, value=f"text:{operation}:{digest}"),
        payload={"media_type": negotiated, "size_bytes": len(body), "sha256": digest},
    )


def _required_id(value: object, *, operation: str = _DATASETS_OPERATION_ID) -> str:
    """Validate one raw dataset identifier without altering its spelling."""
    if not isinstance(value, str) or not value:
        raise CatalogValidationError(
            "The uData dataset identifier must be a non-empty string.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Pass the dataset id or slug from a prior read.",
        )
    if any(character in value for character in "/?#") or value in {".", ".."}:
        raise CatalogValidationError(
            "The uData dataset identifier must be a single non-dot path segment.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Pass the raw dataset id or slug; separators are encoded automatically.",
        )
    return value


def _path_segment(identifier: str) -> str:
    """Encode one validated identifier at the request boundary."""
    return quote(identifier, safe="")


def _query(pairs: list[tuple[str, str]]) -> str:
    """Encode query pairs exactly, preserving repeated keys in order."""
    return urlencode(pairs)


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
