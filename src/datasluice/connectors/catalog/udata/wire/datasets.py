"""Exact dataset wire encoders and decoders for the pinned uData contract."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, cast
from urllib.parse import quote, urlencode

from datasluice.connectors.catalog.udata.mapping import _DATASETS_OPERATION_ID, PLATFORM, parse_native_page
from datasluice.connectors.catalog.udata.models._segment import path_segment
from datasluice.connectors.catalog.udata.wire._rdf import RDF_FORMAT_MEDIA_TYPES, validate_format_extension
from datasluice.connectors.catalog.udata.wire._text_document import (
    APPROVED_TEXT_MEDIA_TYPES,
    bound_text_document,
)
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, _freeze_json, redact_record_payload
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.models.datasets import (
        DatasetCreateInput,
        DatasetDeleteOptions,
        DatasetExtrasDelete,
        DatasetExtrasUpdate,
        DatasetFollowersQuery,
        DatasetListQuery,
        DatasetSearchQuery,
        DatasetSuggestQuery,
        DatasetUpdateInput,
    )

_PINNED_SOURCE_ORACLE_ACTION = "Verify the response against the pinned source oracle."

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
    "list_followers": "udata/api-v1.list-dataset-followers",
    "follow": "udata/api-v1.follow-dataset",
    "unfollow": "udata/api-v1.unfollow-dataset",
}

_UDATA_DATASET_LABEL = "uData dataset"

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
        safe_action=_PINNED_SOURCE_ORACLE_ACTION,
    )


def _validate_dataset_link(value: object, *, field: str, operation: str) -> None:
    if not isinstance(value, Mapping):
        raise _invalid_field(field, operation, "a pagination link object")
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
        _validate_json_object(value, operation=operation, path=path)
        return
    if isinstance(value, list):
        _validate_json_array(value, operation=operation, path=path)
        return
    raise _invalid_field(path, operation, "a JSON value")


def _validate_json_object(value: Mapping[object, object], *, operation: str, path: str) -> None:
    """Validate the string keys and nested values of one JSON object."""
    for key, nested in value.items():
        if not isinstance(key, str) or not key:
            raise _invalid_field(path, operation, "an object with string keys")
        _validate_json_value(nested, operation=operation, path=path)


def _validate_json_array(value: list[object], *, operation: str, path: str) -> None:
    """Validate the nested values of one JSON array."""
    for nested in value:
        _validate_json_value(nested, operation=operation, path=path)


def _validate_nested_string_fields(
    value: Mapping[str, object], fields: frozenset[str], *, operation: str, path: str
) -> None:
    for field in fields:
        nested = value.get(field)
        if nested is not None and (not isinstance(nested, str) or not nested):
            raise _invalid_field(f"{path}.{field}", operation, "a non-empty string or null")


def _validate_dataset_nested_fields(payload: Mapping[str, object], *, operation: str) -> None:
    _validate_nested_mapping_fields(
        payload, ("organization", "owner"), frozenset({"id", "name", "slug"}), operation=operation
    )
    _validate_nested_mapping_fields(
        payload,
        ("schema", "temporal_coverage"),
        frozenset({"name", "version", "url", "start", "end"}),
        operation=operation,
    )
    _validate_nested_list_fields(
        payload,
        ("resources", "badges", "community_resources", "access_audiences", "contact_points"),
        frozenset({"id", "title", "name", "slug", "url"}),
        operation=operation,
    )


def _validate_nested_mapping_fields(
    payload: Mapping[str, object], fields: tuple[str, ...], nested_fields: frozenset[str], *, operation: str
) -> None:
    for field in fields:
        value = payload.get(field)
        if isinstance(value, Mapping):
            _validate_nested_string_fields(value, nested_fields, operation=operation, path=field)


def _validate_nested_list_fields(
    payload: Mapping[str, object], fields: tuple[str, ...], nested_fields: frozenset[str], *, operation: str
) -> None:
    for field in fields:
        value = payload.get(field)
        if isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, Mapping):
                    _validate_nested_string_fields(
                        item,
                        nested_fields,
                        operation=operation,
                        path=f"{field}[{index}]",
                    )


def _dataset_nesting_error(operation: str, status_code: int | None) -> NativeCatalogError:
    return NativeCatalogError(
        "The uData dataset response exceeds the supported JSON nesting.",
        operation=operation,
        platform=PLATFORM.value,
        status_code=status_code,
    )


def _validate_dataset_fields(
    payload: Mapping[str, object], *, operation: str, detail: bool, status_code: int | None = None
) -> None:
    try:
        _validate_required_dataset_fields(payload, operation=operation, detail=detail)
        for field, value in payload.items():
            _validate_dataset_field(field, value, operation=operation)
            _validate_json_value(
                value, operation=operation, path=field if field in _DATASET_DETAIL_FIELDS else "extension"
            )
        _validate_dataset_nested_fields(payload, operation=operation)
        return
    except RecursionError:
        pass
    raise _dataset_nesting_error(operation, status_code)


def _validate_required_dataset_fields(payload: Mapping[str, object], *, operation: str, detail: bool) -> None:
    required = ("id", "title", "slug") if detail else ("id",)
    for field in required:
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            raise _invalid_field(field, operation, "a non-empty string")


def _validate_dataset_field(field: str, value: object, *, operation: str) -> None:
    if field in _DATASET_STRING_FIELDS:
        _validate_dataset_optional_string(field, value, operation=operation)
    elif field in _DATASET_BOOLEAN_FIELDS:
        _validate_dataset_boolean(field, value, operation=operation)
    elif field in _DATASET_MAPPING_FIELDS:
        _validate_dataset_optional_mapping(field, value, operation=operation)
    elif field in _DATASET_LINK_FIELDS and operation.startswith("udata/api-v2."):
        _validate_dataset_link(value, field=field, operation=operation)
    elif field in _DATASET_LIST_FIELDS or field in _DATASET_LINK_FIELDS:
        _validate_dataset_list(field, value, operation=operation)


def _validate_dataset_optional_string(field: str, value: object, *, operation: str) -> None:
    if value is not None and (not isinstance(value, str) or not value):
        raise _invalid_field(field, operation, "a non-empty string or null")
    if field in {"created_at", "last_modified"} and isinstance(value, str):
        try:
            datetime.fromisoformat(value)
            return
        except ValueError:
            pass
        raise _invalid_field(field, operation, "an ISO date-time string or null")


def _validate_dataset_boolean(field: str, value: object, *, operation: str) -> None:
    if type(value) is not bool:
        raise _invalid_field(field, operation, "a boolean")


def _validate_dataset_optional_mapping(field: str, value: object, *, operation: str) -> None:
    if value is not None and not isinstance(value, Mapping):
        raise _invalid_field(field, operation, "an object or null")


def _validate_dataset_list(field: str, value: object, *, operation: str) -> None:
    if not isinstance(value, list):
        raise _invalid_field(field, operation, "a list")
    if field == "tags" and not all(isinstance(tag, str) and tag for tag in value):
        raise _invalid_field(field, operation, "a list of non-empty strings")
    if field != "tags" and not all(isinstance(item, Mapping) for item in value):
        raise _invalid_field(field, operation, "a list of objects")


def _native_dataset(payload: Mapping[str, object]) -> NativeRecord:
    identifier = cast("str", payload["id"])
    known = {key: value for key, value in payload.items() if key in _DATASET_DETAIL_FIELDS}
    unknown = {key: value for key, value in payload.items() if key not in _DATASET_DETAIL_FIELDS}
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(platform=PLATFORM, resource_kind=ResourceKind.DATASET, value=identifier),
        payload=redact_record_payload(known),
        extensions={"udata.dataset": redact_record_payload(unknown)},
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


def rdf_request(
    dataset_id: str, fmt: str | None, *, operation: str = _DATASETS_OPERATION_ID
) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/<id>[/rdf|/rdf.<format>]; validates the format allowlist."""
    identifier = _path_segment(_required_id(dataset_id, operation=operation))
    if fmt is None:
        return "GET", f"{_DATASET_PATH}{identifier}/rdf", {}, None
    extension = _format_extension(fmt, operation=operation)
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


def parse_dataset_detail(
    payload: object, *, operation: str = _DATASETS_OPERATION_ID, status_code: int | None = None
) -> NativeRecord:
    """Decode one full v1/v2 dataset document into a native record.

    The documented core fields (``id``, ``title``, ``slug``) are type-checked; unknown portal fields are retained in the
    ``udata.dataset`` native extension only after shared credential redaction, with nesting depth and text length
    bounded.
    """
    try:
        if not isinstance(payload, Mapping):
            raise CatalogValidationError(
                "The uData dataset document must be a JSON object.",
                operation=operation,
                platform=PLATFORM.value,
                safe_action=_PINNED_SOURCE_ORACLE_ACTION,
            )
        _validate_dataset_fields(payload, operation=operation, detail=True, status_code=status_code)
        return _native_dataset(payload)
    except RecursionError:
        pass
    raise _dataset_nesting_error(operation, status_code)


def parse_suggestions(
    payload: object, *, operation: str = _DATASETS_OPERATION_ID, status_code: int | None = None
) -> tuple[NativeRecord, ...]:
    """Decode the suggest response into bounded native suggestion records."""
    try:
        if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
            raise CatalogValidationError(
                "The uData suggest response must be a JSON array of objects.",
                operation=operation,
                platform=PLATFORM.value,
                safe_action=_PINNED_SOURCE_ORACLE_ACTION,
            )
        records = []
        for item in payload:
            _validate_dataset_fields(item, operation=operation, detail=False, status_code=status_code)
            title = item.get("title")
            if not isinstance(title, str) or not title:
                raise CatalogValidationError(
                    "The uData suggestion requires a non-empty string title.",
                    operation=operation,
                    platform=PLATFORM.value,
                    safe_action=_PINNED_SOURCE_ORACLE_ACTION,
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
                    payload=redact_record_payload(
                        {key: value for key, value in item.items() if key in _DATASET_DETAIL_FIELDS}
                    ),
                    extensions={
                        "udata.dataset": redact_record_payload(
                            {key: value for key, value in item.items() if key not in _DATASET_DETAIL_FIELDS}
                        )
                    },
                )
            )
        return tuple(records)
    except RecursionError:
        pass
    raise _dataset_nesting_error(operation, status_code)


def parse_extras(
    payload: object, *, operation: str = _DATASETS_OPERATION_ID, status_code: int | None = None
) -> Mapping[str, object]:
    """Decode a v2 extras document with typed validation and redaction.

    Extras are caller-authored free-form JSON, so they are the widest credential surface in the dataset family.
    Validation runs first so a malformed document still fails with its typed error; the retained mapping is then
    redacted, keeping every non-credential extra while credential-shaped keys and credential-shaped string content are
    replaced by the shared marker.
    """
    try:
        if not isinstance(payload, Mapping):
            raise CatalogValidationError(
                "The uData extras response must be a JSON object.",
                operation=operation,
                platform=PLATFORM.value,
                safe_action=_PINNED_SOURCE_ORACLE_ACTION,
            )
        for key, value in payload.items():
            if not isinstance(key, str) or not key:
                raise _invalid_field("extras", operation, "an object with string keys")
            _validate_json_value(value, operation=operation, path="extras")
        frozen = _freeze_json(redact_record_payload(payload), "udata.dataset_extras")
        if not isinstance(frozen, Mapping):
            raise CatalogValidationError(
                "The uData extras response must be a JSON object.",
                operation=operation,
                platform=PLATFORM.value,
                safe_action=_PINNED_SOURCE_ORACLE_ACTION,
            )
        return frozen
    except RecursionError:
        pass
    raise _dataset_nesting_error(operation, status_code)


def media_type_for_format(fmt: str, *, operation: str = _DATASETS_OPERATION_ID) -> str:
    """Return the stock media type for one RDF format extension."""
    return RDF_FORMAT_MEDIA_TYPES[validate_format_extension(fmt, operation=operation, platform=PLATFORM.value)]


def _format_extension(fmt: object, *, operation: str = _DATASETS_OPERATION_ID) -> str:
    return validate_format_extension(fmt, operation=operation, platform=PLATFORM.value)


def parse_text_document(
    body: bytes, media_type: str, *, response_media_type: str | None = None, operation: str = _DATASETS_OPERATION_ID
) -> NativeRecord:
    """Bound a non-JSON document (atom/rdf) into a typed native record.

    The no-raw-body contract is preserved: only the approved media type, byte
    count, and SHA-256 digest are retained — never the document content.
    """
    document = bound_text_document(
        body,
        media_type,
        response_media_type=response_media_type,
        operation=operation,
        platform=PLATFORM.value,
        approved_media_types=APPROVED_TEXT_MEDIA_TYPES,
        list_approved_action=True,
    )
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(
            platform=PLATFORM, resource_kind=ResourceKind.DATASET, value=f"text:{operation}:{document.sha256}"
        ),
        payload=document.payload(),
    )


def list_dataset_followers_request(
    dataset_id: str, query: DatasetFollowersQuery
) -> tuple[str, str, dict[str, str], None]:
    operation = DATASET_OPERATIONS["list_followers"]
    identifier = path_segment(dataset_id, operation, _UDATA_DATASET_LABEL)
    return "GET", f"{_DATASET_PATH}{identifier}/followers/?{_query(query.query_params())}", {}, None


def follow_dataset_request(dataset_id: str) -> tuple[str, str, dict[str, str], None]:
    identifier = path_segment(dataset_id, DATASET_OPERATIONS["follow"], _UDATA_DATASET_LABEL)
    return "POST", f"{_DATASET_PATH}{identifier}/followers/", {}, None


def unfollow_dataset_request(dataset_id: str) -> tuple[str, str, dict[str, str], None]:
    identifier = path_segment(dataset_id, DATASET_OPERATIONS["unfollow"], _UDATA_DATASET_LABEL)
    return "DELETE", f"{_DATASET_PATH}{identifier}/followers/", {}, None


def parse_dataset_followers(payload: object, *, status_code: int | None = None) -> MappingRecord:
    operation = DATASET_OPERATIONS["list_followers"]
    if not isinstance(payload, Mapping):
        raise _invalid_field("followers page", operation, "a JSON object")
    page = parse_native_page(payload, operation=operation)
    for item in page.items:
        for field in ("id", "since"):
            if field in item and item[field] is not None and not isinstance(item[field], str):
                raise _invalid_field(field, operation, "a string or null")
        if "follower" in item and item["follower"] is not None and not isinstance(item["follower"], Mapping):
            raise _invalid_field("follower", operation, "an object or null")
    try:
        return MappingRecord(payload)
    except RecursionError:
        pass
    raise _dataset_nesting_error(operation, status_code)


def parse_dataset_follower_count(payload: object, *, operation: str) -> Mapping[str, object]:
    if not isinstance(payload, Mapping) or type(payload.get("followers")) is not int or payload["followers"] < 0:
        raise _invalid_field("followers", operation, "a non-negative integer")
    return parse_extras(payload, operation=operation)


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
    "list_dataset_followers_request",
    "follow_dataset_request",
    "unfollow_dataset_request",
    "parse_dataset_followers",
    "parse_dataset_follower_count",
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
    "v2_extras_put_request",
    "v2_list_request",
    "v2_search_request",
]
