"""Exact reuse and reuse-follower request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.reuses import (
    ReuseCreateInput,
    ReuseFollowersQuery,
    ReuseListQuery,
    ReuseSearchQuery,
    ReuseSuggestQuery,
    ReuseUpdateInput,
    segment,
)
from datasluice.connectors.catalog.udata.wire._text_document import bound_text_document
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = "udata"
LIST_REUSES_OPERATION = "udata/api-v1.list-reuses"
CREATE_REUSE_OPERATION = "udata/api-v1.create-reuse"
RECENT_REUSES_ATOM_FEED_OPERATION = "udata/api-v1.recent-reuses-atom"
GET_REUSE_OPERATION = "udata/api-v1.get-reuse"
UPDATE_REUSE_OPERATION = "udata/api-v1.update-reuse"
DELETE_REUSE_OPERATION = "udata/api-v1.delete-reuse"
REUSE_ADD_DATASET_OPERATION = "udata/api-v1.reuse-add-dataset"
REUSE_ADD_DATASERVICE_OPERATION = "udata/api-v1.reuse-add-dataservice"
AVAILABLE_REUSE_BADGES_OPERATION = "udata/api-v1.available-reuse-badges"
ADD_REUSE_BADGE_OPERATION = "udata/api-v1.add-reuse-badge"
DELETE_REUSE_BADGE_OPERATION = "udata/api-v1.delete-reuse-badge"
FEATURE_REUSE_OPERATION = "udata/api-v1.feature-reuse"
UNFEATURE_REUSE_OPERATION = "udata/api-v1.unfeature-reuse"
SUGGEST_REUSES_OPERATION = "udata/api-v1.suggest-reuses"
REUSE_IMAGE_OPERATION = "udata/api-v1.reuse-image"
REUSE_TYPES_OPERATION = "udata/api-v1.reuse-types"
REUSE_TOPICS_OPERATION = "udata/api-v1.reuse-topics"
SEARCH_REUSES_V2_OPERATION = "udata/api-v2.search-reuses"
LIST_REUSES_V2_OPERATION = "udata/api-v2.list-reuses"
LIST_REUSE_FOLLOWERS_OPERATION = "udata/api-v1.list-reuse-followers"
FOLLOW_REUSE_OPERATION = "udata/api-v1.follow-reuse"
UNFOLLOW_REUSE_OPERATION = "udata/api-v1.unfollow-reuse"
_REUSE_PATH = "/api/1/reuses/"
_REUSE_V2_PATH = "/api/2/reuses/"
_ATOM_MEDIA_TYPE = "application/atom+xml"
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."

type Request = tuple[str, str, dict[str, str], object | None]


def _request(method: str, path: str, body: object = None) -> Request:
    return method, path, {}, body


def _query(pairs: list[tuple[str, str]]) -> str:
    return urlencode(pairs)


def list_reuses_request(query: ReuseListQuery) -> Request:
    return _request("GET", f"{_REUSE_PATH}?{_query(query.query_params())}")


def create_reuse_request(client_input: ReuseCreateInput) -> Request:
    return _request("POST", _REUSE_PATH, client_input.payload())


def recent_reuses_atom_feed_request(query: ReuseListQuery) -> Request:
    return _request("GET", f"{_REUSE_PATH}recent.atom?{_query(query.query_params())}")


def get_reuse_request(reuse_id: str) -> Request:
    return _request("GET", f"{_REUSE_PATH}{segment(reuse_id, GET_REUSE_OPERATION)}/")


def update_reuse_request(reuse_id: str, client_input: ReuseUpdateInput) -> Request:
    return _request(
        "PUT",
        f"{_REUSE_PATH}{segment(reuse_id, UPDATE_REUSE_OPERATION)}/",
        client_input.payload(),
    )


def delete_reuse_request(reuse_id: str) -> Request:
    return _request("DELETE", f"{_REUSE_PATH}{segment(reuse_id, DELETE_REUSE_OPERATION)}/")


def reuse_add_dataset_request(reuse_id: str, dataset_id: str) -> Request:
    return _request(
        "POST",
        f"{_REUSE_PATH}{segment(reuse_id, REUSE_ADD_DATASET_OPERATION)}/datasets/",
        {"id": dataset_id},
    )


def reuse_add_dataservice_request(reuse_id: str, dataservice_id: str) -> Request:
    return _request(
        "POST",
        f"{_REUSE_PATH}{segment(reuse_id, REUSE_ADD_DATASERVICE_OPERATION)}/dataservices/",
        {"id": dataservice_id},
    )


def available_reuse_badges_request() -> Request:
    return _request("GET", f"{_REUSE_PATH}badges/")


def add_reuse_badge_request(reuse_id: str, badge_kind: str) -> Request:
    return _request(
        "POST",
        f"{_REUSE_PATH}{segment(reuse_id, ADD_REUSE_BADGE_OPERATION)}/badges/",
        {"kind": badge_kind},
    )


def delete_reuse_badge_request(reuse_id: str, badge_kind: str) -> Request:
    return _request(
        "DELETE",
        f"{_REUSE_PATH}{segment(reuse_id, DELETE_REUSE_BADGE_OPERATION)}/badges/"
        f"{segment(badge_kind, DELETE_REUSE_BADGE_OPERATION)}/",
    )


def feature_reuse_request(reuse_id: str) -> Request:
    return _request("POST", f"{_REUSE_PATH}{segment(reuse_id, FEATURE_REUSE_OPERATION)}/featured/")


def unfeature_reuse_request(reuse_id: str) -> Request:
    return _request("DELETE", f"{_REUSE_PATH}{segment(reuse_id, UNFEATURE_REUSE_OPERATION)}/featured/")


def suggest_reuses_request(query: ReuseSuggestQuery) -> Request:
    return _request("GET", f"{_REUSE_PATH}suggest/?{_query(query.query_params())}")


def reuse_image_request(reuse_id: str) -> tuple[str, str, dict[str, str]]:
    return "POST", f"{_REUSE_PATH}{segment(reuse_id, REUSE_IMAGE_OPERATION)}/image/", {}


def reuse_types_request() -> Request:
    return _request("GET", f"{_REUSE_PATH}types/")


def reuse_topics_request() -> Request:
    return _request("GET", f"{_REUSE_PATH}topics/")


def search_reuses_v2_request(query: ReuseSearchQuery) -> Request:
    return _request("GET", f"{_REUSE_V2_PATH}search/?{_query(query.query_params())}")


def list_reuses_v2_request(query: ReuseListQuery) -> Request:
    return _request("GET", f"{_REUSE_V2_PATH}?{_query(query.query_params())}")


def list_reuse_followers_request(reuse_id: str, query: ReuseFollowersQuery) -> Request:
    return _request(
        "GET",
        f"{_REUSE_PATH}{segment(reuse_id, LIST_REUSE_FOLLOWERS_OPERATION)}/followers/?{_query(query.query_params())}",
    )


def follow_reuse_request(reuse_id: str) -> Request:
    return _request("POST", f"{_REUSE_PATH}{segment(reuse_id, FOLLOW_REUSE_OPERATION)}/followers/")


def unfollow_reuse_request(reuse_id: str) -> Request:
    return _request("DELETE", f"{_REUSE_PATH}{segment(reuse_id, UNFOLLOW_REUSE_OPERATION)}/followers/")


def parse_mapping(payload: object, operation: str) -> MappingRecord:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData reuse response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_mapping_sequence(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData reuse response must be a list of objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(MappingRecord(item) for item in payload)


def parse_text_document(
    body: bytes,
    media_type: str,
    *,
    response_media_type: str | None = None,
    operation: str = RECENT_REUSES_ATOM_FEED_OPERATION,
) -> MappingRecord:
    """Bound a non-JSON atom document, retaining only media type, size, and digest."""
    document = bound_text_document(
        body,
        media_type,
        response_media_type=response_media_type,
        operation=operation,
        platform=PLATFORM,
        approved_media_types=frozenset({_ATOM_MEDIA_TYPE}),
        subject="reuse",
    )
    return MappingRecord(document.payload())
