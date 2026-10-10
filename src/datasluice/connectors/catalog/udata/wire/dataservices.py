"""Exact dataservice, relationship, and follower request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.dataservices import (
    DataserviceCreateInput,
    DataserviceDatasetLinkInput,
    DataserviceDeleteOptions,
    DataserviceFollowersQuery,
    DataserviceListQuery,
    DataserviceSearchQuery,
    DataserviceUpdateInput,
    segment,
)
from datasluice.connectors.catalog.udata.wire._text_document import bound_text_document
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = "udata"
LIST_DATASERVICES_OPERATION = "udata/api-v1.list-dataservices"
CREATE_DATASERVICE_OPERATION = "udata/api-v1.create-dataservice"
RECENT_DATASERVICES_ATOM_FEED_OPERATION = "udata/api-v1.recent-dataservices-atom"
GET_DATASERVICE_OPERATION = "udata/api-v1.get-dataservice"
UPDATE_DATASERVICE_OPERATION = "udata/api-v1.update-dataservice"
DELETE_DATASERVICE_OPERATION = "udata/api-v1.delete-dataservice"
FEATURE_DATASERVICE_OPERATION = "udata/api-v1.feature-dataservice"
UNFEATURE_DATASERVICE_OPERATION = "udata/api-v1.unfeature-dataservice"
DATASERVICE_DATASETS_ADD_OPERATION = "udata/api-v1.dataservice-datasets-create"
DATASERVICE_DATASET_REMOVE_OPERATION = "udata/api-v1.dataservice-dataset-delete"
RDF_DATASERVICE_OPERATION = "udata/api-v1.rdf-dataservice"
RDF_DATASERVICE_FORMAT_OPERATION = "udata/api-v1.rdf-dataservice-format"
SEARCH_DATASERVICES_OPERATION = "udata/api-v2.search-dataservices"
LIST_DATASERVICE_FOLLOWERS_OPERATION = "udata/api-v1.list-dataservice-followers"
FOLLOW_DATASERVICE_OPERATION = "udata/api-v1.follow-dataservice"
UNFOLLOW_DATASERVICE_OPERATION = "udata/api-v1.unfollow-dataservice"
_DATASERVICE_PATH = "/api/1/dataservices/"
_DATASERVICE_V2_PATH = "/api/2/dataservices/"
_ATOM_MEDIA_TYPE = "application/atom+xml"
_RDF_MEDIA_TYPE = "application/rdf+xml"
RDF_FORMAT_MEDIA_TYPES = {
    "rdf": _RDF_MEDIA_TYPE,
    "xml": _RDF_MEDIA_TYPE,
    "ttl": "text/turtle",
    "turtle": "text/turtle",
    "n3": "text/n3",
    "nt": "application/n-triples",
    "trig": "application/trig",
    "json": "application/ld+json",
    "json-ld": "application/ld+json",
}
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."

type Request = tuple[str, str, dict[str, str], object | None]


def _request(method: str, path: str, body: object = None) -> Request:
    return method, path, {}, body


def _query(pairs: Iterable[tuple[str, str]]) -> str:
    return urlencode(list(pairs))


def _format_extension(value: str) -> str:
    """Validate one stock RDF format extension and return it unchanged."""
    if not isinstance(value, str) or not value:
        raise CatalogValidationError(
            "The uData RDF format is invalid.",
            operation=RDF_DATASERVICE_FORMAT_OPERATION,
            platform=PLATFORM,
            safe_action="Use a short ASCII uData RDF extension.",
        )
    if value not in RDF_FORMAT_MEDIA_TYPES:
        raise CatalogValidationError(
            "The uData RDF format is unsupported.",
            operation=RDF_DATASERVICE_FORMAT_OPERATION,
            platform=PLATFORM,
            safe_action="Use a stock uData RDF extension.",
        )
    return value


def media_type_for_format(extension: str) -> str:
    """Return the stock media type for one RDF format extension.

    Args:
        extension: The RDF format extension, which must be a stock uData value.

    Returns:
        The media type uData serializes that format as.

    Raises:
        CatalogValidationError: If the extension is not a stock uData RDF format.
    """
    return RDF_FORMAT_MEDIA_TYPES[_format_extension(extension)]


def list_dataservices_request(query: DataserviceListQuery) -> Request:
    return _request("GET", f"{_DATASERVICE_PATH}?{_query(query.query_params())}")


def create_dataservice_request(client_input: DataserviceCreateInput) -> Request:
    return _request("POST", _DATASERVICE_PATH, client_input.payload())


def recent_dataservices_atom_feed_request(query: DataserviceListQuery) -> Request:
    return _request("GET", f"{_DATASERVICE_PATH}recent.atom?{_query(query.query_params())}")


def get_dataservice_request(dataservice_id: str) -> Request:
    return _request("GET", f"{_DATASERVICE_PATH}{segment(dataservice_id, GET_DATASERVICE_OPERATION)}/")


def update_dataservice_request(dataservice_id: str, client_input: DataserviceUpdateInput) -> Request:
    return _request(
        "PATCH",
        f"{_DATASERVICE_PATH}{segment(dataservice_id, UPDATE_DATASERVICE_OPERATION)}/",
        client_input.payload(),
    )


def delete_dataservice_request(dataservice_id: str, options: DataserviceDeleteOptions) -> Request:
    path = f"{_DATASERVICE_PATH}{segment(dataservice_id, DELETE_DATASERVICE_OPERATION)}/"
    return _request("DELETE", f"{path}?{_query(options.query_params())}" if options.query_params() else path)


def feature_dataservice_request(dataservice_id: str, *, featured: bool) -> Request:
    return _request(
        "POST" if featured else "DELETE",
        f"{_DATASERVICE_PATH}{segment(dataservice_id, FEATURE_DATASERVICE_OPERATION)}/featured/",
    )


def dataservice_datasets_add_request(dataservice_id: str, client_input: DataserviceDatasetLinkInput) -> Request:
    return _request(
        "POST",
        f"{_DATASERVICE_PATH}{segment(dataservice_id, DATASERVICE_DATASETS_ADD_OPERATION)}/datasets/",
        client_input.payload(),
    )


def dataservice_dataset_remove_request(dataservice_id: str, dataset_id: str) -> Request:
    return _request(
        "DELETE",
        f"{_DATASERVICE_PATH}{segment(dataservice_id, DATASERVICE_DATASET_REMOVE_OPERATION)}"
        f"/datasets/{segment(dataset_id, DATASERVICE_DATASET_REMOVE_OPERATION)}/",
    )


def rdf_dataservice_request(dataservice_id: str, fmt: str | None = None) -> Request:
    """Encode GET /api/1/dataservices/<id>[/rdf|/rdf.<format>] with a validated format allowlist."""
    identifier = segment(dataservice_id, RDF_DATASERVICE_OPERATION)
    if fmt is None:
        return _request("GET", f"{_DATASERVICE_PATH}{identifier}/rdf")
    return _request(
        "GET",
        f"{_DATASERVICE_PATH}{identifier}/rdf.{segment(_format_extension(fmt), RDF_DATASERVICE_FORMAT_OPERATION)}",
    )


def search_dataservices_request(query: DataserviceSearchQuery) -> Request:
    return _request("GET", f"{_DATASERVICE_V2_PATH}search/?{_query(query.query_params())}")


def list_dataservice_followers_request(dataservice_id: str, query: DataserviceFollowersQuery) -> Request:
    return _request(
        "GET",
        f"{_DATASERVICE_PATH}{segment(dataservice_id, LIST_DATASERVICE_FOLLOWERS_OPERATION)}"
        f"/followers/?{_query(query.query_params())}",
    )


def follow_dataservice_request(dataservice_id: str) -> Request:
    return _request("POST", f"{_DATASERVICE_PATH}{segment(dataservice_id, FOLLOW_DATASERVICE_OPERATION)}/followers/")


def unfollow_dataservice_request(dataservice_id: str) -> Request:
    return _request(
        "DELETE", f"{_DATASERVICE_PATH}{segment(dataservice_id, UNFOLLOW_DATASERVICE_OPERATION)}/followers/"
    )


def parse_mapping(payload: object, operation: str) -> MappingRecord:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData dataservice response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_mapping_sequence(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData dataservice response must be a list of objects.",
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
    operation: str = RECENT_DATASERVICES_ATOM_FEED_OPERATION,
) -> MappingRecord:
    """Bound a non-JSON atom or RDF document, retaining only media type, size, and digest."""
    document = bound_text_document(
        body,
        media_type,
        response_media_type=response_media_type,
        operation=operation,
        platform=PLATFORM,
        subject="dataservice",
    )
    return MappingRecord(document.payload())
