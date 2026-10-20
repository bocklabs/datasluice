"""Exact topic, topic-element, and feature request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from typing import TYPE_CHECKING

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
from datasluice.connectors.catalog.udata.wire._common import (
    Request,
    build_request,
    parse_mapping_payload,
    parse_mapping_sequence_payload,
)

if TYPE_CHECKING:
    from datasluice.domain.catalog.models import MappingRecord

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


def _topic_path(topic_id: str, operation: str, suffix: str = "") -> str:
    return f"{_TOPICS_PATH}{segment(topic_id, operation)}/{suffix}"


def search_topics_request(query: TopicSearchQuery) -> Request:
    return build_request("GET", f"{_TOPICS_PATH}search/", query=query.query_params())


def list_topics_request(query: TopicListQuery) -> Request:
    return build_request("GET", _TOPICS_PATH, query=query.query_params())


def create_topic_request(client_input: TopicCreateInput) -> Request:
    return build_request("POST", _TOPICS_PATH, client_input.payload())


def get_topic_request(topic_id: str) -> Request:
    return build_request("GET", _topic_path(topic_id, GET_TOPIC_OPERATION))


def update_topic_request(topic_id: str, client_input: TopicUpdateInput) -> Request:
    return build_request("PUT", _topic_path(topic_id, UPDATE_TOPIC_OPERATION), client_input.payload())


def delete_topic_request(topic_id: str) -> Request:
    return build_request("DELETE", _topic_path(topic_id, DELETE_TOPIC_OPERATION))


def topic_elements_request(topic_id: str, query: TopicElementsQuery) -> Request:
    return build_request(
        "GET",
        f"{_topic_path(topic_id, TOPIC_ELEMENTS_OPERATION)}elements/",
        query=query.query_params(),
    )


def topic_elements_create_request(topic_id: str, client_input: TopicElementsCreateInput) -> Request:
    return build_request(
        "POST", f"{_topic_path(topic_id, TOPIC_ELEMENTS_CREATE_OPERATION)}elements/", client_input.payload()
    )


def topic_elements_delete_request(topic_id: str) -> Request:
    return build_request("DELETE", f"{_topic_path(topic_id, TOPIC_ELEMENTS_DELETE_OPERATION)}elements/")


def topic_element_request(topic_id: str, element_id: str, operation: str) -> str:
    return f"{_topic_path(topic_id, operation)}elements/{segment(element_id, operation)}/"


def topic_element_update_request(topic_id: str, element_id: str, client_input: TopicElementInput) -> Request:
    return build_request(
        "PUT",
        topic_element_request(topic_id, element_id, TOPIC_ELEMENT_UPDATE_OPERATION),
        client_input.payload(),
    )


def topic_element_delete_request(topic_id: str, element_id: str) -> Request:
    return build_request("DELETE", topic_element_request(topic_id, element_id, TOPIC_ELEMENT_DELETE_OPERATION))


def feature_topic_request(topic_id: str, *, featured: bool) -> Request:
    operation = FEATURE_TOPIC_OPERATION if featured else UNFEATURE_TOPIC_OPERATION
    return build_request("POST" if featured else "DELETE", f"{_topic_path(topic_id, operation)}featured/")


def parse_mapping(payload: object, operation: str) -> MappingRecord:
    return parse_mapping_payload(payload, operation, "topic")


def parse_mapping_sequence(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    return parse_mapping_sequence_payload(payload, operation, "topic")
