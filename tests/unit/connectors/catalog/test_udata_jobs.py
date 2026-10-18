from __future__ import annotations

import asyncio
import dataclasses
import json

import pytest

from datasluice.connectors.catalog.udata.clients import declared_udata_profile
from datasluice.connectors.catalog.udata.models.jobs import (
    PERIODS,
    WORKER_TASK_STATES,
    CrontabSchedule,
    IntervalSchedule,
    JobScheduleInput,
    UDataJobHandle,
)
from datasluice.connectors.catalog.udata.services.jobs import AsyncWorkerJobsService, SyncWorkerJobsService
from datasluice.connectors.catalog.udata.wire import jobs as wire
from datasluice.contracts.catalog.native.udata import AsyncUDataJobsService, SyncUDataJobsService
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogError,
    CatalogNotFoundError,
    CatalogRateLimitError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
)
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    RouteTable,
    async_client,
    async_route_table,
    mutation_policy,
    sync_client,
    sync_route_table,
    with_site_route,
)

ORIGIN = UDATA_ORIGIN
JOBS = "/api/1/workers/jobs/"
TASKS = "/api/1/workers/tasks/"
ADMIN = UDATA_ADMIN_PERMISSIONS


def _job(job_id: str = "wheel-job") -> dict[str, object]:
    return {
        "id": job_id,
        "name": "wheel-job",
        "description": None,
        "task": "wheel-task",
        "crontab": {"minute": "0", "hour": "*", "day_of_week": "*", "day_of_month": "*", "month_of_year": "*"},
        "interval": None,
        "args": [],
        "kwargs": {},
        "schedule": "0 * * * *",
        "last_run_at": None,
        "last_run_id": None,
        "enabled": False,
    }


def _task(task_id: str = "wheel-task-run") -> dict[str, object]:
    return {"id": task_id, "status": "SUCCESS", "result": None}


def _routes() -> RouteTable:
    return with_site_route(
        {
            ("GET", f"{ORIGIN}{JOBS}"): (200, [_job()]),
            ("POST", f"{ORIGIN}{JOBS}"): (201, _job()),
            ("GET", f"{ORIGIN}{JOBS}wheel-job/"): (200, _job()),
            ("PUT", f"{ORIGIN}{JOBS}wheel-job/"): (200, _job()),
            ("DELETE", f"{ORIGIN}{JOBS}wheel-job/"): (204, None),
            ("GET", f"{ORIGIN}{TASKS}wheel-task-run/"): (200, _task()),
            ("GET", f"{ORIGIN}{JOBS}schedulables/"): (200, ["wheel-task"]),
        }
    )


def _cron_input(**overrides: object) -> JobScheduleInput:
    values: dict[str, object] = {
        "name": "wheel-job",
        "task": "wheel-task",
        "crontab": CrontabSchedule(minute="0", hour="*", day_of_week="*", day_of_month="*", month_of_year="*"),
    }
    values.update(overrides)
    return JobScheduleInput(**values)  # type: ignore[arg-type]


def _interval_input(**overrides: object) -> JobScheduleInput:
    values: dict[str, object] = {
        "name": "wheel-job",
        "task": "wheel-task",
        "interval": IntervalSchedule(every=4, period="hours"),
    }
    values.update(overrides)
    return JobScheduleInput(**values)  # type: ignore[arg-type]


def _receipt_from(error: BaseException) -> MutationReceipt:
    receipt = error.__dict__.get("mutation_receipt")
    assert isinstance(receipt, MutationReceipt), error.__dict__
    return receipt


_ASSIGNED_METHODS = {
    "create_job",
    "delete_job",
    "get_job",
    "get_task",
    "list_jobs",
    "list_schedulables",
    "update_job",
    "wait_for_task",
}


def test_worker_jobs_contract_exposes_every_assigned_method_in_both_modes() -> None:
    """The family is registered on both clients and projects one typed Protocol per mode."""
    sync_names = {
        name
        for name in dir(SyncWorkerJobsService)
        if not name.startswith("_") and callable(getattr(SyncWorkerJobsService, name))
    }
    async_names = {
        name
        for name in dir(AsyncWorkerJobsService)
        if not name.startswith("_") and callable(getattr(AsyncWorkerJobsService, name))
    }
    assert sync_names == async_names == _ASSIGNED_METHODS

    with sync_client(sync_route_table(with_site_route({})), None) as client:
        assert isinstance(client.worker_jobs, SyncUDataJobsService)
        assert client.worker_jobs.error_type is NativeCatalogError

    async def run() -> None:
        async with async_client(async_route_table(with_site_route({})), None) as client:
            assert isinstance(client.worker_jobs, AsyncUDataJobsService)
            assert client.worker_jobs.error_type is NativeCatalogError

    asyncio.run(run())


def test_every_worker_job_route_has_an_exact_wire_shape() -> None:
    """Each assigned row dispatches its pinned verb, full path, and exact body once."""
    router = sync_route_table(_routes())
    cron = _cron_input()
    with sync_client(router, UDATA_CREDENTIAL) as client:
        assert [record.payload["id"] for record in client.worker_jobs.list_jobs(ADMIN)] == ["wheel-job"]
        created = client.worker_jobs.create_job(cron, ADMIN, mutation_policy(wire.CREATE_JOB_OPERATION, "wheel-job"))
        assert created.record is not None
        assert created.record.payload["id"] == "wheel-job"
        assert client.worker_jobs.get_job("wheel-job", ADMIN).payload["task"] == "wheel-task"
        updated = client.worker_jobs.update_job(
            "wheel-job", cron, ADMIN, mutation_policy(wire.UPDATE_JOB_OPERATION, "wheel-job")
        )
        assert updated.receipt.audit_metadata["status_code"] == 200
        deleted = client.worker_jobs.delete_job(
            "wheel-job", ADMIN, mutation_policy(wire.DELETE_JOB_OPERATION, "wheel-job", destructive=True)
        )
        assert deleted.record is None
        assert client.worker_jobs.get_task("wheel-task-run", ADMIN).payload["status"] == "SUCCESS"
        assert client.worker_jobs.list_schedulables(ADMIN) == ("wheel-task",)

    dispatched = [(request.method, request.url) for request in router.requests if "/api/1/workers/" in request.url]
    assert dispatched == [
        ("GET", f"{ORIGIN}{JOBS}"),
        ("POST", f"{ORIGIN}{JOBS}"),
        ("GET", f"{ORIGIN}{JOBS}wheel-job/"),
        ("PUT", f"{ORIGIN}{JOBS}wheel-job/"),
        ("DELETE", f"{ORIGIN}{JOBS}wheel-job/"),
        ("GET", f"{ORIGIN}{TASKS}wheel-task-run/"),
        ("GET", f"{ORIGIN}{JOBS}schedulables/"),
    ]
    bodies = [
        json.loads(request.body) for request in router.requests if request.url == f"{ORIGIN}{JOBS}" and request.body
    ]
    assert bodies == [
        {
            "name": "wheel-job",
            "task": "wheel-task",
            "crontab": {"minute": "0", "hour": "*", "day_of_week": "*", "day_of_month": "*", "month_of_year": "*"},
        }
    ]


def test_worker_job_args_and_kwargs_reach_the_wire_as_plain_json() -> None:
    """A nested mapping is thawed before dispatch, so the request body is serializable."""
    router = sync_route_table(_routes())
    job = _cron_input(args=({"a": 1},), kwargs={"k": "v"})
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.worker_jobs.create_job(job, ADMIN, mutation_policy(wire.CREATE_JOB_OPERATION, "wheel-job"))

    body = json.loads(
        next(
            request.body or b"{}"
            for request in router.requests
            if request.url == f"{ORIGIN}{JOBS}" and request.method == "POST"
        )
    )
    assert body["args"] == [{"a": 1}]
    assert body["kwargs"] == {"k": "v"}


def test_create_and_update_bodies_carry_only_the_documented_fields() -> None:
    """Absent optional fields stay omitted and exactly one schedule key is ever sent."""
    minimal = _interval_input()
    assert minimal.payload() == {
        "name": "wheel-job",
        "task": "wheel-task",
        "interval": {"every": 4, "period": "hours"},
    }
    full = _cron_input(description="d", args=(1, "two"), kwargs={"k": "v"}, enabled=True)
    assert full.payload() == {
        "name": "wheel-job",
        "task": "wheel-task",
        "description": "d",
        "crontab": {"minute": "0", "hour": "*", "day_of_week": "*", "day_of_month": "*", "month_of_year": "*"},
        "args": [1, "two"],
        "kwargs": {"k": "v"},
        "enabled": True,
    }


def test_mutually_exclusive_schedules_are_refused_at_construction() -> None:
    """The pinned POST refuses both schedules at once and neither is invented."""
    with pytest.raises(ValueError, match="exactly one"):
        JobScheduleInput(
            name="j",
            task="t",
            crontab=CrontabSchedule(),
            interval=IntervalSchedule(every=1, period="days"),
        )
    with pytest.raises(ValueError, match="exactly one"):
        JobScheduleInput(name="j", task="t")


def test_interval_periods_are_the_pinned_vocabulary() -> None:
    """The interval period enum is the pinned celerybeat-mongo vocabulary, fail-closed."""
    assert frozenset({"days", "hours", "minutes", "seconds", "microseconds"}) == PERIODS
    with pytest.raises(ValueError, match="period"):
        IntervalSchedule(every=1, period="weeks")


def test_malformed_job_names_and_tasks_fail_before_dispatch() -> None:
    """Required write fields are validated at the typed boundary."""
    with pytest.raises(ValueError):
        _cron_input(name="")
    with pytest.raises(ValueError):
        _cron_input(task="")
    with pytest.raises(ValueError):
        _cron_input(description="x" * 256)


@pytest.mark.parametrize("identifier", ["..", "a/b", "a\nb", "", "a#b"])
def test_malformed_identifiers_never_reach_a_route(identifier: str) -> None:
    """Traversal-shaped path identifiers are stopped before any dispatch."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(CatalogValidationError):
            client.worker_jobs.get_job(identifier, ADMIN)
        with pytest.raises(CatalogValidationError):
            client.worker_jobs.get_task(identifier, ADMIN)
    assert [request for request in router.requests if "/api/1/workers/" in request.url] == []


def test_admin_reads_require_admin_role_evidence_in_both_modes() -> None:
    """Every stock workers route is admin-gated upstream, reads included."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(ForbiddenError) as raised:
            client.worker_jobs.list_jobs(UDATA_PERMISSIONS)
        assert raised.value.capability_state == "forbidden"
        with pytest.raises(ForbiddenError):
            client.worker_jobs.get_task("wheel-task-run", UDATA_PERMISSIONS)
    assert [request for request in router.requests if "/api/1/workers/" in request.url] == []

    async def run() -> None:
        router_async = async_route_table(_routes())
        async with async_client(router_async, UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError):
                await client.worker_jobs.list_jobs(UDATA_PERMISSIONS)
        assert [request for request in router_async.requests if "/api/1/workers/" in request.url] == []

    asyncio.run(run())


def test_anonymous_family_access_fails_closed_before_dispatch() -> None:
    """No credential means no family access and zero network activity."""
    router = sync_route_table(_routes())
    with sync_client(router, None) as client, pytest.raises(UnauthenticatedError):
        client.worker_jobs.list_jobs(ADMIN)
    assert [request for request in router.requests if "/api/1/workers/" in request.url] == []


def test_a_destructive_delete_requires_a_confirmed_destructive_policy() -> None:
    """A delete without the destructive flag is refused before dispatch."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as raised:
        client.worker_jobs.delete_job("wheel-job", ADMIN, mutation_policy(wire.DELETE_JOB_OPERATION, "wheel-job"))
    assert _receipt_from(raised.value).outcome == "rejected"
    assert [request for request in router.requests if request.method == "DELETE"] == []


def test_a_confirmation_bound_to_another_target_is_refused_before_dispatch() -> None:
    """The confirmation must name this operation and this exact target."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as raised:
        client.worker_jobs.delete_job(
            "wheel-job", ADMIN, mutation_policy(wire.DELETE_JOB_OPERATION, "other-job", destructive=True)
        )
    assert _receipt_from(raised.value).outcome == "rejected"
    assert [request for request in router.requests if request.method == "DELETE"] == []


_STATUS_OUTCOMES = (
    (400, CatalogValidationError, None),
    (401, UnauthenticatedError, "unauthorized"),
    (403, ForbiddenError, "forbidden"),
    (404, CatalogNotFoundError, None),
    (410, CatalogConflictError, "unavailable"),
    (423, CatalogUnavailableError, "deployment-disabled"),
    (429, CatalogRateLimitError, None),
    (500, CatalogUnavailableError, "unavailable"),
    (503, CatalogUnavailableError, "unavailable"),
)


@pytest.mark.parametrize(("status", "error_type", "state"), _STATUS_OUTCOMES)
def test_status_failures_decode_into_the_documented_error_shape(
    status: int, error_type: type[CatalogError], state: str | None
) -> None:
    """Every pinned failure status maps to its normalized error and never leaks the raw body."""
    router = sync_route_table(with_site_route({("GET", f"{ORIGIN}{JOBS}wheel-job/"): (status, {"id": "wheel-job"})}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(error_type) as raised:
        client.worker_jobs.get_job("wheel-job", ADMIN)
    assert raised.value.operation == wire.GET_JOB_OPERATION
    assert raised.value.__dict__["metadata"]["status_code"] == status
    if state is not None:
        assert raised.value.capability_state == state
    assert "mutation_receipt" not in raised.value.__dict__


def test_delete_failures_keep_the_exact_target_and_a_redacted_receipt() -> None:
    """A refused delete stays visible against its exact target with no credential echo."""
    router = sync_route_table(with_site_route({("DELETE", f"{ORIGIN}{JOBS}wheel-job/"): (403, {"message": "Denied"})}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as raised:
        client.worker_jobs.delete_job(
            "wheel-job", ADMIN, mutation_policy(wire.DELETE_JOB_OPERATION, "wheel-job", destructive=True)
        )
    receipt = _receipt_from(raised.value)
    assert receipt.outcome == "failed"
    assert receipt.target.value == "wheel-job"
    assert receipt.operation == wire.DELETE_JOB_OPERATION
    assert "secret-key" not in json.dumps(receipt.to_dict())


def test_create_without_a_server_identifier_receipts_the_collection_target() -> None:
    """A 201 body without an id still receipts an exact, bounded target."""
    router = sync_route_table(with_site_route({("POST", f"{ORIGIN}{JOBS}"): (201, {"name": "wheel-job"})}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        created = client.worker_jobs.create_job(
            _cron_input(), ADMIN, mutation_policy(wire.CREATE_JOB_OPERATION, "wheel-job")
        )
    assert created.receipt.outcome == "succeeded"
    assert created.receipt.target.value == "worker-jobs-collection"


def test_receipts_are_redacted_and_carry_no_raw_body_for_every_mutation() -> None:
    """Create, update, and delete receipts never retain bodies or credentials."""
    router = sync_route_table(_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        created = client.worker_jobs.create_job(
            _cron_input(), ADMIN, mutation_policy(wire.CREATE_JOB_OPERATION, "wheel-job")
        )
        updated = client.worker_jobs.update_job(
            "wheel-job", _cron_input(), ADMIN, mutation_policy(wire.UPDATE_JOB_OPERATION, "wheel-job")
        )
        deleted = client.worker_jobs.delete_job(
            "wheel-job", ADMIN, mutation_policy(wire.DELETE_JOB_OPERATION, "wheel-job", destructive=True)
        )
    for receipt, target in (
        (created.receipt, "wheel-job"),
        (updated.receipt, "wheel-job"),
        (deleted.receipt, "wheel-job"),
    ):
        serialized = json.dumps(receipt.to_dict())
        assert receipt.target.value == target
        assert receipt.target.resource_kind.value == "worker-job"
        assert "secret-key" not in serialized
        assert "wheel-task" not in serialized


def test_task_status_decoding_fails_closed_on_unknown_states() -> None:
    """The task status enum is the pinned celery state vocabulary, never coerced."""
    assert frozenset({"PENDING", "RECEIVED", "STARTED", "SUCCESS", "FAILURE", "RETRY", "REVOKED"}) == WORKER_TASK_STATES
    assert wire.parse_task_status({"id": "t", "status": "SUCCESS"}, wire.GET_TASK_OPERATION) == "SUCCESS"
    for payload in ({"id": "t", "status": "finished"}, {"id": "t"}, "nope", None):
        with pytest.raises(NativeCatalogError):
            wire.parse_task_status(payload, wire.GET_TASK_OPERATION)


def test_schedulables_decode_into_bounded_strings() -> None:
    """The schedulable reference decodes to validated task names only."""
    assert wire.parse_schedulables(["a", "b"], wire.LIST_SCHEDULABLES_OPERATION) == ("a", "b")
    for payload in (["a", 4], "a", {"a": 1}, None):
        with pytest.raises(CatalogValidationError):
            wire.parse_schedulables(payload, wire.LIST_SCHEDULABLES_OPERATION)


def test_job_list_decoding_fails_closed_on_malformed_documents() -> None:
    """The job collection is a list of objects with non-empty identifiers."""
    assert [record.payload["id"] for record in wire.parse_job_sequence([_job()], wire.LIST_JOBS_OPERATION)] == [
        "wheel-job"
    ]
    for payload in (["nope"], {"id": "x"}, None, [_job(), {"no-id": True}]):
        with pytest.raises(CatalogValidationError):
            wire.parse_job_sequence(payload, wire.LIST_JOBS_OPERATION)


@pytest.mark.parametrize("literal", (b"NaN", b"Infinity", b"-Infinity"))
def test_non_finite_responses_fail_typed_without_echoing_the_raw_body(literal: bytes) -> None:
    """NaN and infinite literals never survive decoding in either read shape."""
    router = sync_route_table(with_site_route({("GET", f"{ORIGIN}{JOBS}wheel-job/"): (200, literal)}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(NativeCatalogError):
        client.worker_jobs.get_job("wheel-job", ADMIN)
    router = sync_route_table(with_site_route({("GET", f"{ORIGIN}{TASKS}wheel-task-run/"): (200, literal)}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(NativeCatalogError):
        client.worker_jobs.get_task("wheel-task-run", ADMIN)


def test_typed_inputs_and_results_are_immutable() -> None:
    """Every family record refuses mutation in place."""
    schedule = _cron_input()
    with pytest.raises(dataclasses.FrozenInstanceError):
        schedule.name = "other"  # type: ignore[misc]
    handle = UDataJobHandle(task_id="wheel-task-run")
    with pytest.raises(dataclasses.FrozenInstanceError):
        handle.status = "SUCCESS"  # type: ignore[misc]


def test_async_family_matches_sync_wire_exactly() -> None:
    """The async surface dispatches the same verbs, paths, and bodies."""

    async def run() -> list[tuple[str, str]]:
        router = async_route_table(_routes())
        async with async_client(router, UDATA_CREDENTIAL) as client:
            await client.worker_jobs.list_jobs(ADMIN)
            await client.worker_jobs.get_job("wheel-job", ADMIN)
            await client.worker_jobs.get_task("wheel-task-run", ADMIN)
            await client.worker_jobs.list_schedulables(ADMIN)
            created = await client.worker_jobs.create_job(
                _cron_input(), ADMIN, mutation_policy(wire.CREATE_JOB_OPERATION, "wheel-job")
            )
            await client.worker_jobs.update_job(
                "wheel-job", _cron_input(), ADMIN, mutation_policy(wire.UPDATE_JOB_OPERATION, "wheel-job")
            )
            await client.worker_jobs.delete_job(
                "wheel-job", ADMIN, mutation_policy(wire.DELETE_JOB_OPERATION, "wheel-job", destructive=True)
            )
            assert created.receipt.audit_metadata["status_code"] == 201
        return [(request.method, request.url) for request in router.requests if "/api/1/workers/" in request.url]

    dispatched = asyncio.run(run())
    assert dispatched == [
        ("GET", f"{ORIGIN}{JOBS}"),
        ("GET", f"{ORIGIN}{JOBS}wheel-job/"),
        ("GET", f"{ORIGIN}{TASKS}wheel-task-run/"),
        ("GET", f"{ORIGIN}{JOBS}schedulables/"),
        ("POST", f"{ORIGIN}{JOBS}"),
        ("PUT", f"{ORIGIN}{JOBS}wheel-job/"),
        ("DELETE", f"{ORIGIN}{JOBS}wheel-job/"),
    ]


def test_declared_profile_covers_every_assigned_operation() -> None:
    """Each pinned route dispatches under a declared admin operation identity."""
    declared = {
        f"{operation.platform}/{operation.service}.{operation.method}"
        for operation in declared_udata_profile().operations
    }
    expected = {
        wire.LIST_JOBS_OPERATION,
        wire.CREATE_JOB_OPERATION,
        wire.GET_JOB_OPERATION,
        wire.UPDATE_JOB_OPERATION,
        wire.DELETE_JOB_OPERATION,
        wire.GET_TASK_OPERATION,
        wire.LIST_SCHEDULABLES_OPERATION,
    }
    assert expected <= declared
