"""Exact taxonomy request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Mapping

from datasluice.connectors.catalog.udata.models.taxonomies import BadgeCreateInput, SuggestQuery, segment
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = "udata"
TAXONOMIES_OPERATION = "udata/api-v1.taxonomies-licenses-frequencies-formats-badges-and-schemas"
AVAILABLE_BADGES_OPERATION = "udata/api-v1.available-dataset-badges"
ADD_BADGE_OPERATION = "udata/api-v1.add-dataset-badge"
DELETE_BADGE_OPERATION = "udata/api-v1.delete-dataset-badge"
SUGGEST_FORMATS_OPERATION = "udata/api-v1.suggest-formats"
SUGGEST_MIME_OPERATION = "udata/api-v1.suggest-mime"
LICENSES_OPERATION = "udata/api-v1.list-licenses"
FREQUENCIES_OPERATION = "udata/api-v1.list-frequencies"
EXTENSIONS_OPERATION = "udata/api-v1.allowed-extensions"
SCHEMAS_OPERATION = "udata/api-v1.list-dataset-schemas"
DATASET_SCHEMAS_OPERATION = "udata/api-v2.get-dataset-schemas"
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."


def _request(method: str, path: str, body: object = None) -> tuple[str, str, dict[str, str], object | None]:
    return method, path, {}, body


def available_badges_request() -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", "/api/1/datasets/badges/")


def add_badge_request(dataset_id: str, client_input: BadgeCreateInput) -> tuple[str, str, dict[str, str], object]:
    return _request(
        "POST",
        f"/api/1/datasets/{segment(dataset_id, ADD_BADGE_OPERATION)}/badges/",
        client_input.payload(),
    )


def delete_badge_request(dataset_id: str, kind: str) -> tuple[str, str, dict[str, str], object | None]:
    return _request(
        "DELETE",
        f"/api/1/datasets/{segment(dataset_id, DELETE_BADGE_OPERATION)}"
        f"/badges/{segment(kind, DELETE_BADGE_OPERATION)}/",
    )


def suggest_request(kind: str, query: SuggestQuery) -> tuple[str, str, dict[str, str], object | None]:
    if kind not in {"formats", "mime"}:
        raise CatalogValidationError(
            "The uData taxonomy suggestion kind is not documented.",
            operation=TAXONOMIES_OPERATION,
            platform=PLATFORM,
            safe_action="Use suggest_formats or suggest_mime.",
        )
    return _request("GET", f"/api/1/datasets/suggest/{kind}/?{query.query()}")


def licenses_request() -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", "/api/1/datasets/licenses/")


def frequencies_request() -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", "/api/1/datasets/frequencies/")


def extensions_request() -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", "/api/1/datasets/extensions/")


def schemas_request() -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", "/api/1/datasets/schemas/")


def dataset_schemas_request(dataset_id: str) -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", f"/api/2/datasets/{segment(dataset_id, DATASET_SCHEMAS_OPERATION)}/schemas/")


def parse_mapping(payload: object, operation: str) -> MappingRecord:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData taxonomy badges response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_objects(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData taxonomy response must be a list of objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(MappingRecord(item) for item in payload)


def parse_strings(payload: object, operation: str) -> tuple[str, ...]:
    if not isinstance(payload, list) or not all(isinstance(item, str) and item for item in payload):
        raise CatalogValidationError(
            "The uData taxonomy response must be a list of non-empty strings.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(payload)
