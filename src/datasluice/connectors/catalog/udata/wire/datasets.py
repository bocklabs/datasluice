"""Exact dataset wire encoders and decoders for the pinned uData contract."""

from __future__ import annotations

from collections.abc import Mapping

from datasluice.connectors.catalog.udata.mapping import _DATASETS_OPERATION_ID, PLATFORM
from datasluice.connectors.catalog.udata.models.datasets import (
    DatasetCreateInput,
    DatasetDeleteOptions,
    DatasetExtrasDelete,
    DatasetExtrasUpdate,
    DatasetListQuery,
    DatasetSuggestQuery,
    DatasetUpdateInput,
)
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import NativeRecord
from datasluice.errors.catalog import CatalogValidationError

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
    query = "&".join(f"{key}={value}" for key, value in sorted(options.query_params().items()))
    path = f"{_DATASET_PATH}{identifier}/" + (f"?{query}" if query else "")
    return "DELETE", path, {}, None


def featured_request(dataset_id: str, featured: bool) -> tuple[str, str, dict[str, str], None]:
    """Encode POST/DELETE /api/1/datasets/<id>/featured/."""
    identifier = _required_id(dataset_id)
    return ("POST" if featured else "DELETE"), f"{_DATASET_PATH}{identifier}/featured/", {}, None


def suggest_request(query: DatasetSuggestQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/suggest/."""
    pairs = "&".join(f"{key}={value}" for key, value in sorted(query.query_params().items()))
    return "GET", f"{_DATASET_PATH}suggest/?{pairs}", {}, None


def atom_request(query: DatasetListQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/recent.atom."""
    pairs = "&".join(f"{key}={value}" for key, value in sorted(query.query_params().items()))
    return "GET", f"{_DATASET_PATH}recent.atom?{pairs}", {}, None


def rdf_request(dataset_id: str, fmt: str | None) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/<id>[/rdf|/rdf.<format>]."""
    identifier = _required_id(dataset_id)
    if fmt is None:
        return "GET", f"{_DATASET_PATH}{identifier}/rdf", {}, None
    if not isinstance(fmt, str) or not fmt or "/" in fmt:
        raise CatalogValidationError(
            "The uData RDF format must be a short extension such as rdf, ttl, or jsonld.",
            operation=_DATASETS_OPERATION_ID,
            platform=PLATFORM.value,
            safe_action="Pass a short RDF extension without slashes.",
        )
    return "GET", f"{_DATASET_PATH}{identifier}/rdf.{fmt}", {}, None


def list_request(query: DatasetListQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/datasets/ with the stock query string."""
    pairs = "&".join(f"{key}={value}" for key, value in sorted(query.query_params().items()))
    return "GET", f"{_DATASET_PATH}?{pairs}", {}, None


def v2_search_request(query: DatasetListQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/2/datasets/search/."""
    pairs = "&".join(f"{key}={value}" for key, value in sorted(query.query_params().items()))
    return "GET", f"{_DATASET_V2_PATH}search/?{pairs}", {}, None


def v2_list_request(query: DatasetListQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/2/datasets/."""
    pairs = "&".join(f"{key}={value}" for key, value in sorted(query.query_params().items()))
    return "GET", f"{_DATASET_V2_PATH}?{pairs}", {}, None


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
    """Decode one full v1/v2 dataset document into a native record."""
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData dataset document must be a JSON object.",
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
    return tuple(
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
        for item in payload
    )


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


def parse_text_document(body: bytes, content_type: str, *, operation: str = _DATASETS_OPERATION_ID) -> NativeRecord:
    """Bound a non-JSON document (atom/rdf) into a native record without losing content."""
    text = body.decode("utf-8")
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(platform=PLATFORM, resource_kind=ResourceKind.DATASET, value=f"text:{content_type}"),
        payload={"content_type": content_type, "body": text},
    )


def _required_id(value: object, *, operation: str = _DATASETS_OPERATION_ID) -> str:
    if not isinstance(value, str) or not value:
        raise CatalogValidationError(
            "The uData dataset identifier must be a non-empty string.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Pass the dataset id or slug from a prior read.",
        )
    return value


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
