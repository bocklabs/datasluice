"""Exact topic, topic-element, and feature request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.topics import (
    TopicCreateInput,
    TopicElementInput,
    TopicElementsCreateInput,
    TopicElementsQuery,
    TopicListQuery,
    TopicSearchQuery,
    TopicUpdateInput,
    segment,
)
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = "udata"
SEARCH_TOPICS_OPERATION = "udata/api-v2.search-topics"
LIST_TOPICS_OPERATION = "udata/api-v2.list-topics"
CREATE_TOPIC_OPERATION = "udata/api-v2.create-topic"
GET_TOPIC_OPERATION = "udata/api-v2.get-topic"
UPDATE_TOPIC_OPERATION = "udata/api-v2.update-topic"
DELETE_TOPIC_OPERATION = "udata/api-v2.delete-topic"
TOPIC_ELEMENTS_OPERATION = "udata/api-v2.topic-elements"
TOPIC_ELEMENTS_CREATE_OPERATION = "udata/api-v2.topic-elements-create"
TOPIC_ELEMENTS_DELETE_OPERATION = "udata/api-v2.topic-elements-delete"
TOPIC_ELEMENT_UPDATE_OPERATION = "udata/api-v2.topic-element-update"
TOPIC_ELEMENT_DELETE_OPERATION = "udata/api-v2.topic-element-delete"
FEATURE_TOPIC_OPERATION = "udata/api-v2.feature-topic"
UNFEATURE_TOPIC_OPERATION = "udata/api-v2.unfeature-topic"
_TOPICS_PATH = "/api/2/topics/"
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."

type Request = tuple[str, str, dict[str, str], object | None]


def _request(method: str, path: str, body: object = None) -> Request:
    return method, path, {}, body


def _query(pairs: list[tuple[str, str]]) -> str:
    return urlencode(pairs)


def _topic_path(topic_id: str, operation: str, suffix: str = "") -> str:
    return f"{_TOPICS_PATH}{segment(topic_id, operation)}/{suffix}"


def search_topics_request(query: TopicSearchQuery) -> Request:
    return _request("GET", f"{_TOPICS_PATH}search/?{_query(query.query_params())}")


def list_topics_request(query: TopicListQuery) -> Request:
    return _request("GET", f"{_TOPICS_PATH}?{_query(query.query_params())}")


def create_topic_request(client_input: TopicCreateInput) -> Request:
    return _request("POST", _TOPICS_PATH, client_input.payload())


def get_topic_request(topic_id: str) -> Request:
    return _request("GET", _topic_path(topic_id, GET_TOPIC_OPERATION))


def update_topic_request(topic_id: str, client_input: TopicUpdateInput) -> Request:
    return _request("PUT", _topic_path(topic_id, UPDATE_TOPIC_OPERATION), client_input.payload())


def delete_topic_request(topic_id: str) -> Request:
    return _request("DELETE", _topic_path(topic_id, DELETE_TOPIC_OPERATION))


def topic_elements_request(topic_id: str, query: TopicElementsQuery) -> Request:
    return _request(
        "GET",
        f"{_topic_path(topic_id, TOPIC_ELEMENTS_OPERATION)}elements/?{_query(query.query_params())}",
    )


def topic_elements_create_request(topic_id: str, client_input: TopicElementsCreateInput) -> Request:
    return _request(
        "POST", f"{_topic_path(topic_id, TOPIC_ELEMENTS_CREATE_OPERATION)}elements/", client_input.payload()
    )


def topic_elements_delete_request(topic_id: str) -> Request:
    return _request("DELETE", f"{_topic_path(topic_id, TOPIC_ELEMENTS_DELETE_OPERATION)}elements/")


def topic_element_request(topic_id: str, element_id: str, operation: str) -> str:
    return f"{_topic_path(topic_id, operation)}elements/{segment(element_id, operation)}/"


def topic_element_update_request(topic_id: str, element_id: str, client_input: TopicElementInput) -> Request:
    return _request(
        "PUT",
        topic_element_request(topic_id, element_id, TOPIC_ELEMENT_UPDATE_OPERATION),
        client_input.payload(),
    )


def topic_element_delete_request(topic_id: str, element_id: str) -> Request:
    return _request("DELETE", topic_element_request(topic_id, element_id, TOPIC_ELEMENT_DELETE_OPERATION))


def feature_topic_request(topic_id: str, *, featured: bool) -> Request:
    operation = FEATURE_TOPIC_OPERATION if featured else UNFEATURE_TOPIC_OPERATION
    return _request("POST" if featured else "DELETE", f"{_topic_path(topic_id, operation)}featured/")


def parse_mapping(payload: object, operation: str) -> MappingRecord:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData topic response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_mapping_sequence(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData topic response must be a list of objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(MappingRecord(item) for item in payload)
