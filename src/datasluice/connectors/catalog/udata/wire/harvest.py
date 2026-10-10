"""Exact uData harvest request builders and bounded response decoders."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.mapping import parse_native_page
from datasluice.connectors.catalog.udata.models.harvest import (
    HARVEST_JOB_STATUSES,
    HARVEST_OPERATION,
    PLATFORM,
    HarvestJobItemsQuery,
    HarvestJobQuery,
    HarvestPage,
    HarvestScheduleInput,
    HarvestSourceInput,
    HarvestSourceQuery,
    HarvestValidationInput,
    crawl_endpoint,
    job_segment,
    source_segment,
)
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

SOURCES_PATH = "/api/1/harvest/sources/"
SOURCE_PATH = "/api/1/harvest/source/"
SOURCE_PREVIEW_PATH = "/api/1/harvest/source/preview/"
JOB_PATH = "/api/1/harvest/job/"
BACKENDS_PATH = "/api/1/harvest/backends/"

BULK_MUTATIONS = {"delete-source": "deleted", "unschedule-source": "unscheduled", "run-source": "ran"}

_PINNED_HARVEST_ACTION = "Verify the response against the pinned uData 17.6 harvest schema."


def _write_body(client_input: HarvestSourceInput) -> dict[str, object]:
    """Return the exact write body after re-validating the deployment crawl target.

    The deployment, not this client, fetches a harvest source URL, so the
    endpoint policy is re-applied at the request boundary rather than trusted
    from the model alone. A refusal here is raised before any dispatch.
    """
    crawl_endpoint(client_input.url, allow_private=client_input.allow_private_endpoint)
    return client_input.payload()


def _query(params: list[tuple[str, str]]) -> str:
    return urlencode(params)


def _source_path(source_id: str) -> str:
    return f"{SOURCE_PATH}{source_segment(source_id, HARVEST_OPERATION)}/"


def list_sources_request(query: HarvestSourceQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/harvest/sources/ with the documented index query."""
    return "GET", f"{SOURCES_PATH}?{_query(query.query_params())}", {}, None


def create_source_request(
    client_input: HarvestSourceInput,
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Encode POST /api/1/harvest/sources/ with the exact write body."""
    return "POST", SOURCES_PATH, {}, _write_body(client_input)


def get_source_request(source_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/harvest/source/<id>/."""
    return "GET", _source_path(source_id), {}, None


def update_source_request(
    source_id: str, client_input: HarvestSourceInput
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Encode PUT /api/1/harvest/source/<id>/ with the exact write body."""
    return "PUT", _source_path(source_id), {}, _write_body(client_input)


def delete_source_request(source_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode DELETE /api/1/harvest/source/<id>/."""
    return "DELETE", _source_path(source_id), {}, None


def validate_source_request(
    source_id: str, client_input: HarvestValidationInput
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Encode POST /api/1/harvest/source/<id>/validate/ with the exact validation body."""
    return "POST", f"{_source_path(source_id)}validate/", {}, client_input.payload()


def run_source_request(source_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode POST /api/1/harvest/source/<id>/run/."""
    return "POST", f"{_source_path(source_id)}run/", {}, None


def schedule_source_request(source_id: str, client_input: HarvestScheduleInput) -> tuple[str, str, dict[str, str], str]:
    """Encode POST /api/1/harvest/source/<id>/schedule/ with the exact cron string body."""
    return "POST", f"{_source_path(source_id)}schedule/", {}, client_input.payload()


def unschedule_source_request(source_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode DELETE /api/1/harvest/source/<id>/schedule/."""
    return "DELETE", f"{_source_path(source_id)}schedule/", {}, None


def preview_source_config_request(
    client_input: HarvestSourceInput,
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Encode POST /api/1/harvest/source/preview/ with the exact write body."""
    return "POST", SOURCE_PREVIEW_PATH, {}, _write_body(client_input)


def preview_source_request(source_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/harvest/source/<id>/preview/."""
    return "GET", f"{_source_path(source_id)}preview/", {}, None


def list_jobs_request(source_id: str, query: HarvestJobQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/harvest/source/<id>/jobs/ with the pager query."""
    return "GET", f"{_source_path(source_id)}jobs/?{_query(query.query_params())}", {}, None


def get_job_request(job_id: str) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/harvest/job/<ident>/."""
    return "GET", f"{JOB_PATH}{job_segment(job_id)}/", {}, None


def list_job_items_request(job_id: str, query: HarvestJobItemsQuery) -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/harvest/job/<ident>/items/ with the pager and status query."""
    return "GET", f"{JOB_PATH}{job_segment(job_id)}/items/?{_query(query.query_params())}", {}, None


def backends_request() -> tuple[str, str, dict[str, str], None]:
    """Encode GET /api/1/harvest/backends/."""
    return "GET", BACKENDS_PATH, {}, None


def parse_record(payload: object, operation: str) -> MappingRecord:
    """Decode one harvest JSON object into a bounded mapping record."""
    if not isinstance(payload, Mapping) or not payload:
        raise CatalogValidationError(
            "The uData harvest response must be a non-empty JSON object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_HARVEST_ACTION,
        )
    if "id" in payload and (not isinstance(payload["id"], str) or not payload["id"]):
        raise CatalogValidationError(
            "The uData harvest record identifier must be a non-empty string.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_HARVEST_ACTION,
        )
    return MappingRecord(payload)


def parse_optional_record(payload: object, operation: str) -> MappingRecord | None:
    """Decode one harvest object, treating the documented empty body as absent."""
    if payload is None or (isinstance(payload, str) and not payload) or (isinstance(payload, Mapping) and not payload):
        return None
    return parse_record(payload, operation)


def parse_page(payload: object, operation: str) -> HarvestPage:
    """Decode one harvest pager envelope preserving native pager field presence."""
    page = parse_native_page(payload, operation=operation)
    return HarvestPage(
        records=tuple(parse_record(item, operation) for item in page.items),
        page=page.page,
        page_size=page.page_size,
        previous_page=page.previous_page,
        next_page=page.next_page,
        total=page.total,
        present_fields=page.present_fields,
    )


def parse_backends(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    """Decode the harvest backend list into bounded mapping records."""
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData harvest backend response must be a list of JSON objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_HARVEST_ACTION,
        )
    return tuple(MappingRecord(item) for item in payload)


def parse_job_identifier(payload: object) -> str | None:
    """Return the queued job identifier one dispatch response refers to, if any."""
    if not isinstance(payload, Mapping):
        return None
    last_job = payload.get("last_job")
    if isinstance(last_job, Mapping):
        identifier = last_job.get("id")
        if isinstance(identifier, str) and identifier:
            return identifier
    return None


def parse_job_status(payload: object, operation: str) -> str:
    """Return the validated harvest job status one job document declares.

    A queued job is only ever awaited under a caller lease, so an unknown or
    missing status is refused instead of being coerced into a terminal state.

    Args:
        payload: The decoded harvest job document.
        operation: The owning operation used to build the error details.

    Returns:
        One member of the pinned harvest job status vocabulary.

    Raises:
        CatalogValidationError: If the document omits a known harvest job status.
    """
    status = payload.get("status") if isinstance(payload, Mapping) else None
    if not isinstance(status, str) or status not in HARVEST_JOB_STATUSES:
        raise CatalogValidationError(
            "The uData harvest job response does not declare a known job status.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_HARVEST_ACTION,
        )
    return status
