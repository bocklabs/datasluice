"""Exact uData worker job, task, and schedulable request builders and bounded decoders."""

from __future__ import annotations

from collections.abc import Mapping

from datasluice.connectors.catalog.udata.models.jobs import (
    PLATFORM,
    WORKER_TASK_STATES,
    JobScheduleInput,
    job_segment,
    task_segment,
)
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

JOBS_PATH = "/api/1/workers/jobs/"
SCHEDULABLES_PATH = "/api/1/workers/jobs/schedulables/"
TASKS_PATH = "/api/1/workers/tasks/"

LIST_JOBS_OPERATION = "udata/api-v1.list-jobs"
CREATE_JOB_OPERATION = "udata/api-v1.create-job"
GET_JOB_OPERATION = "udata/api-v1.get-job"
UPDATE_JOB_OPERATION = "udata/api-v1.update-job"
DELETE_JOB_OPERATION = "udata/api-v1.delete-job"
GET_TASK_OPERATION = "udata/api-v1.get-task"
LIST_SCHEDULABLES_OPERATION = "udata/api-v1.list-schedulable-jobs"

_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."

type Request = tuple[str, str, dict[str, str], object | None]


def _request(method: str, path: str, body: object = None) -> Request:
    return method, path, {}, body


def _job_path(job_id: str, operation: str) -> str:
    return f"{JOBS_PATH}{job_segment(job_id, operation)}/"


def list_jobs_request() -> Request:
    return _request("GET", JOBS_PATH)


def create_job_request(client_input: JobScheduleInput) -> Request:
    return _request("POST", JOBS_PATH, client_input.payload())


def get_job_request(job_id: str) -> Request:
    return _request("GET", _job_path(job_id, GET_JOB_OPERATION))


def update_job_request(job_id: str, client_input: JobScheduleInput) -> Request:
    return _request("PUT", _job_path(job_id, UPDATE_JOB_OPERATION), client_input.payload())


def delete_job_request(job_id: str) -> Request:
    return _request("DELETE", _job_path(job_id, DELETE_JOB_OPERATION))


def get_task_request(task_id: str) -> Request:
    return _request("GET", f"{TASKS_PATH}{task_segment(task_id, GET_TASK_OPERATION)}/")


def list_schedulables_request() -> Request:
    return _request("GET", SCHEDULABLES_PATH)


def parse_record(payload: object, operation: str) -> MappingRecord:
    """Decode one worker job or task document into a bounded mapping record."""
    noun = "worker task" if operation == GET_TASK_OPERATION else "worker job"
    if not isinstance(payload, Mapping) or not payload:
        raise CatalogValidationError(
            f"The uData {noun} response must be a non-empty JSON object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    if not isinstance(payload.get("id"), str) or not payload["id"]:
        raise CatalogValidationError(
            f"The uData {noun} response must carry a non-empty string identifier.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_job_sequence(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    """Decode the unpaginated job collection into bounded mapping records."""
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData worker job collection must be a list of JSON objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(parse_record(item, operation) for item in payload)


def parse_schedulables(payload: object, operation: str) -> tuple[str, ...]:
    """Decode the schedulable task-name reference into validated strings."""
    if not isinstance(payload, list) or not all(isinstance(item, str) and item for item in payload):
        raise CatalogValidationError(
            "The uData schedulable reference must be a list of non-empty strings.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(payload)


def parse_task_status(payload: object, operation: str) -> str:
    """Return the validated celery task status one task document declares.

    A queued task is only ever awaited under a caller lease, so an unknown or
    missing status is refused instead of being coerced into a terminal state.

    Args:
        payload: The decoded worker task document.
        operation: The owning operation used to build the error details.

    Returns:
        One member of the pinned celery state vocabulary.

    Raises:
        CatalogValidationError: If the document omits a known task status.
    """
    status = payload.get("status") if isinstance(payload, Mapping) else None
    if not isinstance(status, str) or status not in WORKER_TASK_STATES:
        raise CatalogValidationError(
            "The uData worker task response does not declare a known task status.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return status
