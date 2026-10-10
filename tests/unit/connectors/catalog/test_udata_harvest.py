"""Exact wire, SSRF, receipt, and budget coverage for the uData harvest family."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from threading import Event
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.connectors.catalog.udata.models.harvest import (
    HARVEST_OPERATION,
    HarvestBulkTargets,
    HarvestJobItemsQuery,
    HarvestJobQuery,
    HarvestJobWaitResult,
    HarvestMutationResult,
    HarvestPreviewResult,
    HarvestScheduleInput,
    HarvestSourceInput,
    HarvestSourceQuery,
    HarvestValidationInput,
    UDataJobHandle,
    bulk_target,
    crawl_endpoint,
)
from datasluice.connectors.catalog.udata.services.harvest import (
    AsyncHarvestService,
    SyncHarvestService,
)
from datasluice.connectors.catalog.udata.wire import harvest as wire
from datasluice.contracts.catalog.native.udata import (
    AsyncUDataHarvestService,
    SyncUDataHarvestService,
)
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import BulkExecutionPolicy, MutationPolicy
from datasluice.errors.catalog import (
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
)
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    AsyncRouteRouter,
    RouteTable,
    SyncRouteRouter,
    async_client,
    async_route_table,
    mutation_policy,
    sync_client,
    sync_route_table,
    thawed,
    with_site_route,
)

if TYPE_CHECKING:
    from datasluice.domain.catalog.receipts import BulkCheckpoint

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_ADMIN_PERMISSIONS
_SECRET = "secret-key"

_SOURCE = {"id": "source-1", "name": "opendata", "url": "https://example.org/feed", "backend": "csv"}
_METADATA_INSTANCE = "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
_LOOPBACK_FEED = "http://127.0.0.1:8000/feed"
_BUDGET = TimeBudget(connect=1.0, read=1.0, write=1.0, total=5.0)

_ASSIGNED_METHODS = {
    "backends",
    "create_source",
    "delete_source",
    "get_job",
    "get_source",
    "list_job_items",
    "list_jobs",
    "list_sources",
    "preview_bulk",
    "preview_source",
    "preview_source_config",
    "run_bulk",
    "run_source",
    "schedule_source",
    "unschedule_source",
    "update_source",
    "validate_source",
    "wait_for_job",
}


def _url(path: str) -> str:
    return f"{ORIGIN}{path}"


def _policy(target: str, *, destructive: bool = False) -> MutationPolicy:
    return mutation_policy(HARVEST_OPERATION, target, destructive=destructive)


def _writes() -> RouteTable:
    return {
        ("POST", _url("/api/1/harvest/sources/")): (201, _SOURCE),
        ("PUT", _url("/api/1/harvest/source/source-1/")): (200, _SOURCE),
        ("DELETE", _url("/api/1/harvest/source/source-1/")): (204, None),
        ("POST", _url("/api/1/harvest/source/source-1/validate/")): (200, {**_SOURCE, "state": "accepted"}),
        ("POST", _url("/api/1/harvest/source/source-1/run/")): (202, {**_SOURCE, "last_job": {"id": "job-9"}}),
        ("POST", _url("/api/1/harvest/source/source-1/schedule/")): (200, {**_SOURCE, "schedule": "0 6 * * *"}),
        ("DELETE", _url("/api/1/harvest/source/source-1/schedule/")): (204, None),
        ("POST", _url("/api/1/harvest/source/preview/")): (200, {"valid": True}),
    }


def _dispatches(router: SyncRouteRouter | AsyncRouteRouter) -> list[tuple[str, str]]:
    return [(request.method, request.url) for request in router.requests if "/api/1/harvest/" in request.url]


def test_harvest_contract_exposes_every_assigned_method_in_both_modes() -> None:
    """The sync and async surfaces are name-for-name identical and Protocol-bound."""
    sync_names = {
        name
        for name in dir(SyncHarvestService)
        if not name.startswith("_") and callable(getattr(SyncHarvestService, name))
    }
    async_names = {
        name
        for name in dir(AsyncHarvestService)
        if not name.startswith("_") and callable(getattr(AsyncHarvestService, name))
    }
    assert sync_names == async_names
    assert sync_names == _ASSIGNED_METHODS

    with sync_client(sync_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
        assert isinstance(client.harvest_moderation_admin, SyncUDataHarvestService)

    async def run() -> None:
        async with async_client(async_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            assert isinstance(client.harvest_moderation_admin, AsyncUDataHarvestService)

    asyncio.run(run())


def test_every_harvest_route_has_an_exact_wire_shape() -> None:
    """Verb, path, and query are pinned per row; path identifiers stay one segment."""
    source = HarvestSourceInput(name="opendata", url="https://example.org/feed", backend="csv")
    actual = [
        wire.list_sources_request(HarvestSourceQuery())[0:2],
        wire.create_source_request(source)[0:2],
        wire.get_source_request("source-1")[0:2],
        wire.update_source_request("source-1", source)[0:2],
        wire.delete_source_request("source-1")[0:2],
        wire.validate_source_request("source-1", HarvestValidationInput("accepted"))[0:2],
        wire.run_source_request("source-1")[0:2],
        wire.schedule_source_request("source-1", HarvestScheduleInput("0 6 * * *"))[0:2],
        wire.unschedule_source_request("source-1")[0:2],
        wire.preview_source_config_request(source)[0:2],
        wire.preview_source_request("source-1")[0:2],
        wire.list_jobs_request("source-1", HarvestJobQuery())[0:2],
        wire.get_job_request("job-9")[0:2],
        wire.list_job_items_request("job-9", HarvestJobItemsQuery())[0:2],
        wire.backends_request()[0:2],
    ]
    assert actual == [
        ("GET", "/api/1/harvest/sources/?page=1&page_size=20"),
        ("POST", "/api/1/harvest/sources/"),
        ("GET", "/api/1/harvest/source/source-1/"),
        ("PUT", "/api/1/harvest/source/source-1/"),
        ("DELETE", "/api/1/harvest/source/source-1/"),
        ("POST", "/api/1/harvest/source/source-1/validate/"),
        ("POST", "/api/1/harvest/source/source-1/run/"),
        ("POST", "/api/1/harvest/source/source-1/schedule/"),
        ("DELETE", "/api/1/harvest/source/source-1/schedule/"),
        ("POST", "/api/1/harvest/source/preview/"),
        ("GET", "/api/1/harvest/source/source-1/preview/"),
        ("GET", "/api/1/harvest/source/source-1/jobs/?page=1&page_size=20"),
        ("GET", "/api/1/harvest/job/job-9/"),
        ("GET", "/api/1/harvest/job/job-9/items/?page=1&page_size=20"),
        ("GET", "/api/1/harvest/backends/"),
    ]

    filtered = HarvestSourceQuery(q="opendata", page=2, page_size=50, owner_id="owner-1", deleted=True)
    assert wire.list_sources_request(filtered)[1] == (
        "/api/1/harvest/sources/?page=2&page_size=50&q=opendata&owner=owner-1&deleted=true"
    )
    assert wire.list_job_items_request("job-9", HarvestJobItemsQuery(status="done"))[1] == (
        "/api/1/harvest/job/job-9/items/?page=1&page_size=20&status=done"
    )
    assert wire.create_source_request(source) == (
        "POST",
        "/api/1/harvest/sources/",
        {},
        {"name": "opendata", "url": "https://example.org/feed", "backend": "csv"},
    )
    assert wire.schedule_source_request("source-1", HarvestScheduleInput("0 6 * * *"))[3] == "0 6 * * *"
    assert wire.validate_source_request("source-1", HarvestValidationInput("refused", "bad csv"))[3] == {
        "state": "refused",
        "comment": "bad csv",
    }


@pytest.mark.parametrize("identifier", [".", "..", "a/b", "a?b", "a#b", "", "a\x00b", 4, None])
def test_malformed_harvest_segments_never_reach_a_route(identifier: object) -> None:
    """Dot segments and separators are refused, never percent-quoted into a new route."""
    for builder in (wire.get_source_request, wire.delete_source_request, wire.run_source_request):
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            builder(cast("str", identifier))
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        wire.get_job_request(cast("str", identifier))


@pytest.mark.parametrize("page_size", [0, -1, 101, True, False, 1.0, 10.5, "10", None])
def test_harvest_page_sizes_are_bounded_before_dispatch(page_size: object) -> None:
    """A pager is an amplification lever, so every ceiling is validated pre-dispatch."""
    for factory in (HarvestSourceQuery, HarvestJobQuery, HarvestJobItemsQuery):
        with pytest.raises(ValueError, match="page_size"):
            factory(page_size=cast("int", page_size))
    assert HarvestSourceQuery(page_size=100).query_params()[1] == ("page_size", "100")


# --- T-04-ADM-01: server-side request forgery through the harvest endpoint -----------


def _link_local_inputs(url: str) -> tuple[HarvestSourceInput | None, Exception | None]:
    try:
        return HarvestSourceInput(name="probe", url=url, backend="csv"), None
    except CatalogValidationError as error:
        return None, error


def test_link_local_and_loopback_harvest_endpoints_are_refused_before_any_dispatch() -> None:
    """The deployment, not this client, fetches the endpoint, so a private target is SSRF."""
    for url in (_METADATA_INSTANCE, _LOOPBACK_FEED):
        client_input, refusal = _link_local_inputs(url)
        assert client_input is None
        assert isinstance(refusal, CatalogValidationError)
        assert refusal.capability_state == "forbidden"
        assert url not in str(refusal) + repr(refusal.__dict__)
        assert refusal.metadata["endpoint_classification"] == "locality"


@pytest.mark.parametrize("url", [_METADATA_INSTANCE, _LOOPBACK_FEED])
def test_private_harvest_endpoints_dispatch_nothing_through_every_write_route(url: str) -> None:
    """Every write that can carry a crawl target re-validates it before the transport runs."""
    for builder in (wire.create_source_request, wire.preview_source_config_request):
        tampered = HarvestSourceInput(name="probe", url="https://example.org/feed", backend="csv")
        object.__setattr__(tampered, "url", url)
        with pytest.raises(CatalogValidationError, match="permitted crawl target"):
            builder(tampered)

    router = sync_route_table(with_site_route(_writes()))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        tampered = HarvestSourceInput(name="probe", url="https://example.org/feed", backend="csv")
        object.__setattr__(tampered, "url", url)
        with pytest.raises(CatalogValidationError, match="permitted crawl target") as refused:
            client.harvest_moderation_admin.create_source(tampered, PERMISSIONS, _policy("harvest-source:create"))
    assert _dispatches(router) == []
    receipt = refused.value.__dict__["mutation_receipt"]
    assert receipt.outcome == "rejected"
    assert url not in json.dumps(receipt.to_dict())


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "gopher://example.org/",
        "ftp://example.org/feed",
        "https://user:pass@example.org/feed",
        "https://example.org:0/feed",
        "https://example.org:70000/feed",
        "https://169.254.169.254/",
        "http://[::1]/feed",
        "http://localhost/feed",
        "http://metadata.internal/feed",
        "http://10.0.0.5/feed",
        "http://192.168.1.4/feed",
        "https://example.org/ feed",
        "https://example.org/\nfeed",
        "x" * 4096,
    ],
)
def test_non_public_or_unsupported_harvest_endpoints_are_refused(url: str) -> None:
    """Scheme, userinfo, port, and locality rules are all enforced pre-dispatch."""
    with pytest.raises(CatalogValidationError, match="permitted crawl target"):
        crawl_endpoint(url)


@pytest.mark.parametrize("url", ["https://example.org/feed", "http://example.org:8080/feed.csv"])
def test_public_harvest_endpoints_are_accepted_unchanged(url: str) -> None:
    """A genuinely public crawl target passes the policy untouched."""
    assert crawl_endpoint(url) == url


def test_private_endpoint_allowance_is_per_input_never_global() -> None:
    """The non-public rule relaxes only through explicit per-input consent."""
    allowed = HarvestSourceInput(name="internal", url=_LOOPBACK_FEED, backend="csv", allow_private_endpoint=True)
    assert allowed.url == _LOOPBACK_FEED
    assert allowed.endpoint_scope == "private-allowed"
    assert "allow_private_endpoint" not in allowed.payload()
    assert crawl_endpoint(_LOOPBACK_FEED, allow_private=True) == _LOOPBACK_FEED
    with pytest.raises(CatalogValidationError):
        crawl_endpoint(_LOOPBACK_FEED)


@pytest.mark.parametrize("url", [_METADATA_INSTANCE, _LOOPBACK_FEED])
def test_nested_config_endpoints_are_validated_too(url: str) -> None:
    """A crawler config can carry its own target, so it is screened as well."""
    with pytest.raises(CatalogValidationError, match="permitted crawl target"):
        HarvestSourceInput(
            name="nested", url="https://example.org/feed", backend="csv", config={"parser": {"url": url}}
        )


def test_harvest_writes_require_admin_evidence_and_a_confirmed_target() -> None:
    """Elevation of privilege: no admin role and no bound confirmation both dispatch nothing."""
    router = sync_route_table(with_site_route(_writes()))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        with pytest.raises(ForbiddenError) as unprivileged:
            service.create_source(
                HarvestSourceInput(name="opendata", url="https://example.org/feed", backend="csv"),
                UDATA_PERMISSIONS,
                _policy("harvest-source:create"),
            )
        assert unprivileged.value.operation == HARVEST_OPERATION
        assert unprivileged.value.capability_state == "forbidden"
        assert _dispatches(router) == []

        with pytest.raises(ForbiddenError):
            service.delete_source("source-1", PERMISSIONS, _policy("source-1"))
        assert _dispatches(router) == []

        with pytest.raises(ForbiddenError):
            service.delete_source("source-1", PERMISSIONS, _policy("source-2", destructive=True))
        assert _dispatches(router) == []


# --- T-04-ADM-02: repudiation, per-item receipts, and exact targets ---------------------


def test_every_harvest_mutation_returns_a_redacted_receipt_with_its_exact_target() -> None:
    """Each outcome, success or refusal, carries an immutable receipt naming the target."""
    router = sync_route_table(with_site_route(_writes()))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        created = service.create_source(
            HarvestSourceInput(name="opendata", url="https://example.org/feed", backend="csv"),
            PERMISSIONS,
            _policy("harvest-source:create"),
        )
        updated = service.update_source(
            "source-1",
            HarvestSourceInput(name="opendata", url="https://example.org/feed", backend="csv"),
            PERMISSIONS,
            _policy("source-1"),
        )
        validated = service.validate_source(
            "source-1", HarvestValidationInput("accepted"), PERMISSIONS, _policy("source-1")
        )
        scheduled = service.schedule_source(
            "source-1", HarvestScheduleInput("0 6 * * *"), PERMISSIONS, _policy("source-1")
        )
        run = service.run_source("source-1", PERMISSIONS, _policy("source-1"))
        previewed = service.preview_source_config(
            HarvestSourceInput(name="opendata", url="https://example.org/feed", backend="csv"),
            PERMISSIONS,
            _policy("harvest-source:preview"),
        )
        deleted = service.delete_source("source-1", PERMISSIONS, _policy("source-1", destructive=True))
        unscheduled = service.unschedule_source("source-1", PERMISSIONS, _policy("source-1", destructive=True))

    assert isinstance(created, HarvestMutationResult)
    assert created.receipt.target.value == "source-1"
    assert deleted.record is None
    assert unscheduled.record is None
    assert isinstance(previewed, HarvestPreviewResult)
    assert previewed.to_dict()["dry_run"] is True
    assert cast("dict[str, object]", updated.receipt.audit_metadata)["mutation"] == "updated"
    assert cast("dict[str, object]", validated.receipt.audit_metadata)["mutation"] == "validated"
    assert cast("dict[str, object]", scheduled.receipt.audit_metadata)["mutation"] == "scheduled"
    assert cast("dict[str, object]", run.receipt.audit_metadata)["mutation"] == "run"
    assert cast("dict[str, object]", previewed.receipt.audit_metadata)["mutation"] == "previewed"
    assert cast("dict[str, object]", deleted.receipt.audit_metadata)["mutation"] == "deleted"
    assert cast("dict[str, object]", unscheduled.receipt.audit_metadata)["mutation"] == "unscheduled"
    for receipt in (created, updated, validated, scheduled, run, previewed, deleted, unscheduled):
        assert receipt.receipt.outcome == "succeeded"
        assert receipt.receipt.operation == HARVEST_OPERATION
        assert _SECRET not in json.dumps(receipt.to_dict())
        assert "example.org" not in json.dumps(receipt.receipt.to_dict())

    assert _dispatches(router) == [
        ("POST", _url("/api/1/harvest/sources/")),
        ("PUT", _url("/api/1/harvest/source/source-1/")),
        ("POST", _url("/api/1/harvest/source/source-1/validate/")),
        ("POST", _url("/api/1/harvest/source/source-1/schedule/")),
        ("POST", _url("/api/1/harvest/source/source-1/run/")),
        ("POST", _url("/api/1/harvest/source/preview/")),
        ("DELETE", _url("/api/1/harvest/source/source-1/")),
        ("DELETE", _url("/api/1/harvest/source/source-1/schedule/")),
    ]


def test_bulk_preview_is_local_and_binds_confirmation_to_the_exact_ordered_set() -> None:
    """A plan is previewed without dispatch, and its target cannot drift from its set."""
    router = sync_route_table(with_site_route(_writes()))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        targets = HarvestBulkTargets(("source-1", "source-2"))
        preview = service.preview_bulk("delete-source", targets)
    assert _dispatches(router) == []
    assert preview.destructive is True
    assert preview.plan.preview is True
    assert preview.plan.to_dict()["preview"] is True
    assert preview.confirmation_target == bulk_target("delete-source", ("source-1", "source-2"))
    assert preview.confirmation_target != bulk_target("delete-source", ("source-2", "source-1"))
    assert preview.confirmation_target != bulk_target("run-source", ("source-1", "source-2"))
    assert "source-1" not in preview.confirmation_target
    assert service.preview_bulk("run-source", targets).destructive is False
    assert service.preview_bulk("unschedule-source", targets).destructive is True


@pytest.mark.parametrize("mutation", ["source-1", "other"])
def test_bulk_plan_refusal_dispatches_nothing_and_receipts_the_plan_target(mutation: str) -> None:
    """A plan confirmed against the wrong target never reaches one item."""
    router = sync_route_table(with_site_route(_writes()))
    targets = HarvestBulkTargets(("source-1", "source-2"))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        preview = service.preview_bulk("delete-source", targets)
        with pytest.raises(ForbiddenError) as refused:
            service.run_bulk(
                "delete-source",
                targets,
                PERMISSIONS,
                _policy(mutation, destructive=True),
                checkpoint_sink=lambda checkpoint: None,
            )
    assert _dispatches(router) == []
    receipt = refused.value.__dict__["mutation_receipt"]
    assert receipt.outcome == "rejected"
    assert receipt.target.value == preview.confirmation_target
    assert cast("dict[str, object]", receipt.audit_metadata)["mutation"] == "deleted"


def test_bulk_run_is_bounded_checkpointed_and_receipts_every_item() -> None:
    """Each item settles into its own receipt and each boundary persists a checkpoint."""
    routes = with_site_route(
        {
            ("DELETE", _url("/api/1/harvest/source/source-1/")): (204, None),
            ("DELETE", _url("/api/1/harvest/source/source-2/")): (404, {"message": "not found"}),
            ("DELETE", _url("/api/1/harvest/source/source-3/")): (204, None),
        }
    )
    router = sync_route_table(routes)
    targets = HarvestBulkTargets(("source-1", "source-2", "source-3"))
    checkpoints: list[BulkCheckpoint] = []
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        preview = service.preview_bulk("delete-source", targets)
        result = service.run_bulk(
            "delete-source",
            targets,
            PERMISSIONS,
            _policy(preview.confirmation_target, destructive=True),
            execution_policy=BulkExecutionPolicy(max_parallelism=1),
            checkpoint_sink=checkpoints.append,
        )

    assert [item.receipt.target.value for item in result.receipts] == ["source-1", "source-2", "source-3"]
    assert [item.receipt.outcome for item in result.receipts] == ["succeeded", "failed", "succeeded"]
    assert result.summary.total == 3
    assert result.summary.settled == 3
    assert result.summary.outstanding == 0
    assert result.summary.state == "completed"
    assert len(checkpoints) == 3
    assert [entry["settled"] for entry in result.checkpoints] == [1, 2, 3]
    assert result.checkpoints[-1]["completed_indexes"] == [0, 1, 2]
    assert result.resume is not None
    assert result.resume.resumption_cursor == result.plan.resumption_cursor
    assert _SECRET not in json.dumps(result.to_dict())


def test_bulk_run_resumes_from_a_checkpoint_without_redispatching_settled_items() -> None:
    """A resumed plan re-dispatches only the outstanding items."""
    routes = with_site_route(
        {
            ("DELETE", _url("/api/1/harvest/source/source-1/")): (204, None),
            ("DELETE", _url("/api/1/harvest/source/source-2/")): (204, None),
        }
    )
    targets = HarvestBulkTargets(("source-1", "source-2"))
    with sync_client(sync_route_table(routes), UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        preview = service.preview_bulk("delete-source", targets)
        policy = _policy(preview.confirmation_target, destructive=True)
        first = service.run_bulk("delete-source", targets, PERMISSIONS, policy, checkpoint_sink=lambda checkpoint: None)
        resumed_router = sync_route_table(routes)
    with sync_client(resumed_router, UDATA_CREDENTIAL) as client:
        resumed = client.harvest_moderation_admin.run_bulk(
            "delete-source",
            targets,
            PERMISSIONS,
            policy,
            checkpoint=first.resume,
            checkpoint_sink=lambda checkpoint: None,
        )
    assert _dispatches(resumed_router) == []
    assert [item.receipt.outcome for item in resumed.receipts] == ["succeeded", "succeeded"]


def test_bulk_cancellation_drains_in_flight_work_and_persists_a_terminal_checkpoint() -> None:
    """A cancelled plan still settles what it dispatched and records what is outstanding."""
    cancel = Event()
    routes = with_site_route(
        {
            ("DELETE", _url("/api/1/harvest/source/source-1/")): (204, None),
            ("DELETE", _url("/api/1/harvest/source/source-2/")): (204, None),
            ("DELETE", _url("/api/1/harvest/source/source-3/")): (204, None),
        }
    )
    router = sync_route_table(routes)
    targets = HarvestBulkTargets(("source-1", "source-2", "source-3"))
    checkpoints: list[BulkCheckpoint] = []
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        preview = service.preview_bulk("delete-source", targets)

        def sink(checkpoint: BulkCheckpoint) -> None:
            checkpoints.append(checkpoint)
            cancel.set()

        result = service.run_bulk(
            "delete-source",
            targets,
            PERMISSIONS,
            _policy(preview.confirmation_target, destructive=True),
            execution_policy=BulkExecutionPolicy(max_parallelism=1),
            checkpoint_sink=sink,
            cancel_event=cancel,
        )

    assert result.summary.cancelled is True
    assert result.summary.state == "cancelled"
    assert result.summary.outstanding is not None
    assert result.summary.outstanding >= 1
    assert checkpoints[-1].cancellation_requested is True
    assert result.checkpoints[-1]["cancellation_requested"] is True


def test_bulk_run_budget_exhaustion_is_reported_not_hidden() -> None:
    """A whole-run budget stops the plan and states why it stopped."""
    routes = with_site_route(
        {
            ("DELETE", _url("/api/1/harvest/source/source-1/")): (204, None),
            ("DELETE", _url("/api/1/harvest/source/source-2/")): (204, None),
        }
    )
    router = sync_route_table(routes)
    targets = HarvestBulkTargets(("source-1", "source-2"))
    ticks: list[float] = [0.0, 100.0]
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        preview = service.preview_bulk("delete-source", targets)
        result = service.run_bulk(
            "delete-source",
            targets,
            PERMISSIONS,
            _policy(preview.confirmation_target, destructive=True),
            checkpoint_sink=lambda checkpoint: None,
            whole_run_budget=_BUDGET,
            clock=lambda: ticks.pop() if ticks else 100.0,
        )
    assert result.summary.budget_exhausted is True
    assert result.summary.state == "budget_exhausted"
    assert result.summary.dispatches <= 1


# --- T-04-ADM-03: bounded job waits and abandonment ---------------------------------------


def test_queued_harvest_work_returns_a_handle_and_a_bounded_wait() -> None:
    """Dispatching a run returns a typed handle; waiting is explicit and stops on terminal."""
    routes = with_site_route(
        {
            ("POST", _url("/api/1/harvest/source/source-1/run/")): (202, {**_SOURCE, "last_job": {"id": "job-9"}}),
            ("GET", _url("/api/1/harvest/job/job-9/")): (200, {"id": "job-9", "status": "done"}),
        }
    )
    router = sync_route_table(routes)
    waited_sleeps: list[float] = []
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        run = service.run_source("source-1", PERMISSIONS, _policy("source-1"))
        assert isinstance(run.handle, UDataJobHandle)
        assert run.handle.job_id == "job-9"
        assert run.handle.terminal is False
        result = service.wait_for_job(run.handle, PERMISSIONS, budget=_BUDGET, sleeper=waited_sleeps.append)

    assert isinstance(result, HarvestJobWaitResult)
    assert result.handle.status == "done"
    assert result.handle.terminal is True
    assert result.handle.polls == 1
    assert result.handle.abandoned is False
    assert waited_sleeps == [1.0]
    handle = result.to_dict()["handle"]
    assert isinstance(handle, Mapping)
    assert handle["terminal"] is True


def test_a_job_that_never_finishes_is_abandoned_at_its_poll_ceiling() -> None:
    """The lease is finite: a still-running job is left abandoned and detectable."""
    routes = with_site_route(
        {
            ("POST", _url("/api/1/harvest/source/source-1/run/")): (202, {**_SOURCE, "last_job": {"id": "job-9"}}),
            ("GET", _url("/api/1/harvest/job/job-9/")): (200, {"id": "job-9", "status": "processing"}),
        }
    )
    router = sync_route_table(routes)
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        run = service.run_source("source-1", PERMISSIONS, _policy("source-1"))
        handle = run.handle
        assert handle is not None
        bounded = UDataJobHandle(
            source_id=handle.source_id,
            job_id=handle.job_id,
            poll_interval=0.0,
            max_polls=3,
            budget=_BUDGET,
        )
        result = service.wait_for_job(bounded, PERMISSIONS, sleeper=lambda delay: None)

    polls = [request for request in router.requests if "/api/1/harvest/job/" in request.url]
    assert len(polls) == 3
    assert result.handle.polls == 3
    assert result.handle.abandoned is True
    assert result.handle.exhausted is False
    assert result.handle.terminal is False


def test_a_job_wait_stops_at_its_budget_and_reports_exhaustion() -> None:
    """Budget exhaustion ends the lease rather than looping the deployment."""
    routes = with_site_route(
        {
            ("POST", _url("/api/1/harvest/source/source-1/run/")): (202, {**_SOURCE, "last_job": {"id": "job-9"}}),
            ("GET", _url("/api/1/harvest/job/job-9/")): (200, {"id": "job-9", "status": "processing"}),
        }
    )
    router = sync_route_table(routes)
    with sync_client(router, UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        run = service.run_source("source-1", PERMISSIONS, _policy("source-1"))
        handle = run.handle
        assert handle is not None
        bounded = UDataJobHandle(
            source_id=handle.source_id, job_id=handle.job_id, poll_interval=0.0, max_polls=64, budget=_BUDGET
        )
        result = service.wait_for_job(
            bounded, PERMISSIONS, clock=iter((0.0, 100.0, 100.0, 100.0)).__next__, sleeper=lambda delay: None
        )

    assert result.handle.exhausted is True
    assert result.handle.abandoned is True
    assert result.handle.polls == 0
    assert not [request for request in router.requests if "/api/1/harvest/job/" in request.url]


def test_waiting_without_a_queued_job_is_refused_before_any_dispatch() -> None:
    """A handle with no job identifier cannot poll, and says so instead of guessing."""
    router = sync_route_table(with_site_route(_writes()))
    with (
        sync_client(router, UDATA_CREDENTIAL) as client,
        pytest.raises(CatalogValidationError, match="queued job identifier"),
    ):
        client.harvest_moderation_admin.wait_for_job(
            UDataJobHandle(source_id="source-1"), PERMISSIONS, sleeper=lambda delay: None
        )
    assert _dispatches(router) == []


# --- reading, decoding, and dispatch parity ------------------------------------------------


def test_harvest_reads_decode_into_bounded_records_in_both_modes() -> None:
    """Listings keep native pager presence; records stay frozen mapping envelopes."""
    page = {
        "data": [_SOURCE],
        "page": 1,
        "page_size": 20,
        "total": 1,
        "next_page": None,
        "previous_page": None,
    }
    routes = with_site_route(
        {
            ("GET", _url("/api/1/harvest/sources/?page=1&page_size=20")): (200, page),
            ("GET", _url("/api/1/harvest/source/source-1/")): (200, _SOURCE),
            ("GET", _url("/api/1/harvest/source/source-1/preview/")): (200, {"valid": True}),
            ("GET", _url("/api/1/harvest/source/source-1/jobs/?page=1&page_size=20")): (200, page),
            ("GET", _url("/api/1/harvest/job/job-9/")): (200, {"id": "job-9", "status": "processing"}),
            ("GET", _url("/api/1/harvest/job/job-9/items/?page=1&page_size=20")): (200, page),
            ("GET", _url("/api/1/harvest/backends/")): (200, [{"id": "csv", "title": "CSV"}, {"id": "json"}]),
        }
    )
    with sync_client(sync_route_table(routes), UDATA_CREDENTIAL) as client:
        service = client.harvest_moderation_admin
        sources = service.list_sources(HarvestSourceQuery(), PERMISSIONS)
        assert sources.page == 1
        assert sources.total == 1
        assert sources.present_fields == frozenset({"data", "page", "page_size", "total", "next_page", "previous_page"})
        assert thawed([record.payload for record in sources.records]) == [_SOURCE]
        assert service.get_source("source-1", PERMISSIONS).payload == _SOURCE
        assert service.preview_source("source-1", PERMISSIONS).payload == {"valid": True}
        assert service.list_jobs("source-1", HarvestJobQuery(), PERMISSIONS).records
        assert service.get_job("job-9", PERMISSIONS).payload["status"] == "processing"
        assert service.list_job_items("job-9", HarvestJobItemsQuery(), PERMISSIONS).records
        backends = service.backends(PERMISSIONS)
        assert thawed([record.payload for record in backends]) == [{"id": "csv", "title": "CSV"}, {"id": "json"}]

    async def run() -> None:
        async with async_client(async_route_table(routes), UDATA_CREDENTIAL) as client:
            service = client.harvest_moderation_admin
            assert (await service.list_sources(HarvestSourceQuery(), PERMISSIONS)).total == 1
            assert (await service.get_source("source-1", PERMISSIONS)).payload == _SOURCE
            assert (await service.get_job("job-9", PERMISSIONS)).payload["status"] == "processing"
            assert len(await service.backends(PERMISSIONS)) == 2

    asyncio.run(run())


@pytest.mark.parametrize(
    ("payload", "reader"),
    [
        ({}, "get_source"),
        ([], "get_source"),
        ("source", "get_source"),
        ({"data": "not-a-list"}, "list_sources"),
        ({"data": [{"id": 4}]}, "list_sources"),
        ([{"id": "csv"}, "json"], "backends"),
        ({}, "backends"),
    ],
)
def test_malformed_harvest_responses_fail_typed_without_a_raw_body(payload: object, reader: str) -> None:
    """Every decoder refuses a non-conforming body rather than coercing it."""
    routes: RouteTable = {
        ("GET", _url("/api/1/harvest/source/source-1/")): (200, payload),
        ("GET", _url("/api/1/harvest/sources/?page=1&page_size=20")): (200, payload),
        ("GET", _url("/api/1/harvest/backends/")): (200, payload),
    }
    with (
        sync_client(sync_route_table(with_site_route(routes)), UDATA_CREDENTIAL) as client,
        pytest.raises(CatalogValidationError) as raised,
    ):
        arguments: tuple[object, ...] = (
            (HarvestSourceQuery(), PERMISSIONS) if reader == "list_sources" else ("source-1", PERMISSIONS)
        )
        if reader == "backends":
            arguments = (PERMISSIONS,)
        getattr(client.harvest_moderation_admin, reader)(*arguments)
    rendered = repr(raised.value) + str(raised.value.__dict__)
    assert _SECRET not in rendered


@pytest.mark.parametrize("literal", [b"NaN", b"Infinity", b"-Infinity"])
def test_non_finite_harvest_responses_fail_typed_without_a_raw_body(literal: bytes) -> None:
    """NaN and the infinities are not JSON and never reach a native record."""
    routes: RouteTable = {
        ("GET", _url("/api/1/harvest/source/source-1/")): (200, b'{"id": "source-1", "title": ' + literal + b"}"),
        ("GET", _url("/api/1/site/")): (
            200,
            b'{"version": "17.6.0", "id": "site", "title": "uData", '
            b'"feed_size": 0, "keywords": [], "metrics": {"widgets": NaN}}',
        ),
    }
    with (
        sync_client(sync_route_table(routes), UDATA_CREDENTIAL) as client,
        pytest.raises(NativeCatalogError) as raised,
    ):
        client.harvest_moderation_admin.get_source("source-1", PERMISSIONS)
    rendered = repr(raised.value) + str(raised.value.__dict__)
    assert literal.decode() not in rendered
    assert _SECRET not in rendered


def test_harvest_job_status_is_validated_before_it_can_be_awaited() -> None:
    """An unknown job status is refused rather than coerced into a terminal state."""
    routes = with_site_route(
        {
            ("GET", _url("/api/1/harvest/job/job-9/")): (200, {"id": "job-9", "status": "exploded"}),
        }
    )
    with sync_client(sync_route_table(routes), UDATA_CREDENTIAL) as client:
        with pytest.raises(CatalogValidationError, match="known job status"):
            wire.parse_job_status({"status": "exploded"}, HARVEST_OPERATION)
        with pytest.raises(CatalogValidationError, match="known job status"):
            wire.parse_job_status({}, HARVEST_OPERATION)
        with pytest.raises(CatalogValidationError):
            client.harvest_moderation_admin.wait_for_job(
                UDataJobHandle(source_id="source-1", job_id="job-9", poll_interval=0.0),
                PERMISSIONS,
                sleeper=lambda delay: None,
            )


@pytest.mark.parametrize(
    "mutation",
    [
        {"frequency": "hourly"},
        {"name": ""},
        {"url": "https://example.org/feed", "backend": "csv"},
        {"name": "n", "url": "https://example.org/feed"},
        {"name": "n", "url": "https://example.org/feed", "backend": "csv", "owner_id": "a/b"},
        {"name": "n", "url": "https://example.org/feed", "backend": "csv", "active": "yes"},
        {"name": "n", "url": "https://example.org/feed", "backend": "csv", "config": ["not", "a", "map"]},
        {"name": "n", "url": "https://example.org/feed", "backend": "csv", "config": {"x": object()}},
    ],
)
def test_malformed_harvest_write_inputs_are_rejected_before_dispatch(mutation: Mapping[str, object]) -> None:
    """Presence-aware write bodies never coerce a caller value into a deployment payload."""
    with pytest.raises((ValueError, CatalogValidationError)):
        HarvestSourceInput(
            name=cast("str", mutation.get("name", "")),
            url=cast("str", mutation.get("url", "")),
            backend=cast("str", mutation.get("backend", "")),
            owner_id=cast("str | None", mutation.get("owner_id")),
            active=cast("bool | None", mutation.get("active")),
            config=cast("Mapping[str, object] | None", mutation.get("config")),
        )


@pytest.mark.parametrize("cron", ["", "0 6 * *", "0 6 * * * *", "@daily", "0 6 * * MON", "x" * 40, 5])
def test_malformed_harvest_cron_expressions_are_rejected(cron: object) -> None:
    """A cron string is passed to the deployment verbatim, so it is screened first."""
    with pytest.raises(ValueError, match="cron"):
        HarvestScheduleInput(cast("str", cron))


@pytest.mark.parametrize("state", ["approved", "", None, 4])
def test_unknown_harvest_validation_states_are_rejected(state: object) -> None:
    """Validation state is a pinned vocabulary the deployment enforces."""
    with pytest.raises(ValueError, match="validation state"):
        HarvestValidationInput(cast("str", state))


def test_refused_harvest_validation_requires_a_comment() -> None:
    """A rejection without a reason is refused before the deployment sees it."""
    with pytest.raises(ValueError, match="comment"):
        HarvestValidationInput("refused")


def test_harvest_mutations_dispatch_their_exact_operation_in_both_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every row is guarded and receipted under the pinned harvest operation."""
    captured: list[dict[str, object]] = []

    def capture(**kwargs: object) -> tuple[int, object, object]:
        captured.append(kwargs)
        return 204, None, object()

    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        monkeypatch.setattr(client, "_dataset_call", capture)
        client.harvest_moderation_admin.delete_source("source-1", PERMISSIONS, _policy("source-1", destructive=True))
    assert captured[0]["owning_operation"] == HARVEST_OPERATION
    assert captured[0]["method"] == "DELETE"
    assert captured[0]["path"] == "/api/1/harvest/source/source-1/"

    async def run() -> None:
        seen: list[dict[str, object]] = []

        async def async_capture(**kwargs: object) -> tuple[int, object, object]:
            seen.append(kwargs)
            return 204, None, object()

        async with async_client(async_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            monkeypatch.setattr(client, "_dataset_call_async", async_capture)
            deleted = await client.harvest_moderation_admin.delete_source(
                "source-1", PERMISSIONS, _policy("source-1", destructive=True)
            )
        assert seen[0]["owning_operation"] == HARVEST_OPERATION
        assert deleted.receipt.operation == HARVEST_OPERATION

    asyncio.run(run())
