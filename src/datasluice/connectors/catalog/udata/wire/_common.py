from collections.abc import Mapping
from urllib.parse import urlencode

from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

type Request = tuple[str, str, dict[str, str], object | None]


def build_request(
    method: str, path: str, body: object = None, *, query: list[tuple[str, str]] | None = None
) -> Request:
    return method, f"{path}?{urlencode(query)}" if query is not None else path, {}, body


def parse_mapping_payload(payload: object, operation: str, subject: str) -> MappingRecord:
    if not isinstance(payload, Mapping):
        raise _schema_error(operation, subject, "an object")
    return MappingRecord(payload)


def parse_mapping_sequence_payload(payload: object, operation: str, subject: str) -> tuple[MappingRecord, ...]:
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise _schema_error(operation, subject, "a list of objects")
    return tuple(MappingRecord(item) for item in payload)


def _schema_error(operation: str, subject: str, expected: str) -> CatalogValidationError:
    return CatalogValidationError(
        f"The uData {subject} response must be {expected}.",
        operation=operation,
        platform="udata",
        safe_action="Verify the response against the pinned uData 17.6 schema.",
    )
