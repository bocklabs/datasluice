"""Exact transfer-request request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.transfers import (
    TRANSFER_STATUSES,
    TransferListQuery,
    TransferRequestInput,
    TransferResponseInput,
    linked_identifier,
    segment,
)
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = "udata"
LIST_TRANSFERS_OPERATION = "udata/api-v1.list-transfers"
REQUEST_TRANSFER_OPERATION = "udata/api-v1.request-transfer"
GET_TRANSFER_OPERATION = "udata/api-v1.get-transfer"
RESPOND_TO_TRANSFER_OPERATION = "udata/api-v1.respond-to-transfer"
TRANSFER_OPERATIONS = (
    LIST_TRANSFERS_OPERATION,
    REQUEST_TRANSFER_OPERATION,
    GET_TRANSFER_OPERATION,
    RESPOND_TO_TRANSFER_OPERATION,
)
_TRANSFER_PATH = "/api/1/transfer/"
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."

type Request = tuple[str, str, dict[str, str], object | None]


def _request(method: str, path: str, body: object = None) -> Request:
    return method, path, {}, body


def _subject_identifier(value: str, operation: str) -> str:
    return linked_identifier(value, operation, "uData transfer subject")


def _recipient_identifier(value: str, operation: str) -> str:
    return linked_identifier(value, operation, "uData transfer recipient")


def list_transfers_request(query: TransferListQuery) -> Request:
    """Build GET /api/1/transfer/ with the documented filter arguments."""
    if query.subject is not None:
        _subject_identifier(query.subject, LIST_TRANSFERS_OPERATION)
    if query.recipient is not None:
        _recipient_identifier(query.recipient, LIST_TRANSFERS_OPERATION)
    return _request("GET", f"{_TRANSFER_PATH}?{urlencode(query.query_params())}")


def request_transfer_request(client_input: TransferRequestInput) -> Request:
    """Build POST /api/1/transfer/ with the documented subject and recipient references."""
    _subject_identifier(client_input.subject_id, REQUEST_TRANSFER_OPERATION)
    _recipient_identifier(client_input.recipient_id, REQUEST_TRANSFER_OPERATION)
    return _request("POST", _TRANSFER_PATH, client_input.payload())


def get_transfer_request(transfer_id: str) -> Request:
    """Build GET /api/1/transfer/<id>/."""
    return _request("GET", f"{_TRANSFER_PATH}{segment(transfer_id, GET_TRANSFER_OPERATION)}/")


def respond_to_transfer_request(transfer_id: str, client_input: TransferResponseInput) -> Request:
    """Build POST /api/1/transfer/<id>/ with the documented response body."""
    return _request(
        "POST",
        f"{_TRANSFER_PATH}{segment(transfer_id, RESPOND_TO_TRANSFER_OPERATION)}/",
        client_input.payload(),
    )


def parse_transfer(payload: object, operation: str) -> MappingRecord:
    """Decode one marshalled transfer, requiring the documented invariants.

    Args:
        payload: The decoded JSON body of one transfer response.
        operation: The owning operation name, used to build the error details.

    Returns:
        The bounded immutable transfer record.

    Raises:
        CatalogValidationError: If the body is not one documented transfer object.
    """
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData transfer response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    identifier = payload.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise CatalogValidationError(
            "The uData transfer response must carry its identifier.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    if payload.get("status") not in TRANSFER_STATUSES:
        raise CatalogValidationError(
            "The uData transfer response status is not a documented transfer status.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_transfers(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    """Decode the transfer collection, requiring the documented list of transfers.

    Args:
        payload: The decoded JSON body of the transfer collection response.
        operation: The owning operation name, used to build the error details.

    Returns:
        The bounded immutable transfer records.

    Raises:
        CatalogValidationError: If the body is not a list of documented transfers.
    """
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData transfer response must be a list of objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(parse_transfer(item, operation) for item in payload)
