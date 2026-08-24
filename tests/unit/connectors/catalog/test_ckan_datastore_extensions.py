"""Deterministic loopback coverage for the datastore and extension surfaces.

Covers the deployment-gated sql-search id (D-02), destructive scoping of
``datastore_delete`` versus record-scoped deletes, sysadmin config options,
and the job/task-status family — plus the per-id classification agreement
with the 03-05 probe mapper (Pitfall 5).
"""

from __future__ import annotations

import asyncio
import json

import pytest

from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.connectors.catalog.ckan.probes import (
    DATASTORE_CRUD_OPERATION_ID,
    SQL_SEARCH_OPERATION_ID,
    classify_probe_response,
)
from datasluice.connectors.catalog.ckan.results import CKANMutationResult
from datasluice.connectors.catalog.ckan.services.datastore import AsyncDatastoreService, SyncDatastoreService
from datasluice.connectors.catalog.ckan.services.extensions import AsyncExtensionsService, SyncExtensionsService
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ValueRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError, UnsupportedCapabilityError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
SQL_ID = "ckan/datastore-extension.sql-search"

JOB_RESULT: dict[str, object] = {"id": "job-1", "title": "bulk_enqueue", "state": "queued"}
TASK_RESULT: dict[str, object] = {"id": "task-1", "entity_id": "dataset-a", "task_type": "archiver", "key": "celery"}


def _success_body(result: object) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


def _failure_body(error: dict[str, object]) -> bytes:
    return json.dumps({"success": False, "error": dict(error)}).encode("utf-8")


class SyncCaptureTransport:
    """A deterministic loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    def close(self) -> None:
        return None


class AsyncCaptureTransport:
    """A deterministic async loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    async def aclose(self) -> None:
        return None


class SeededProbeRunner:
    """A bounded stub runner answering seeded response classes per OperationId."""

    def __init__(self, *, deployment_disabled: frozenset[str] = frozenset()) -> None:
        self.deployment_disabled = deployment_disabled
        self.probed: list[OperationId] = []

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.probed.append(operation_id)
        response_class = (
            ProbeResponseClass.DEPLOYMENT_DISABLED
            if str(operation_id) in self.deployment_disabled
            else ProbeResponseClass.SUCCESS
        )
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url="https://demo.ckan.org/api/3/action/status_show",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=response_class,
        )


class AsyncSeededProbeRunner:
    """The async twin of the seeded stub runner."""

    def __init__(self, *, deployment_disabled: frozenset[str] = frozenset()) -> None:
        self.deployment_disabled = deployment_disabled

    async def probe(self, operation_id: OperationId) -> ProbeEvidence:
        response_class = (
            ProbeResponseClass.DEPLOYMENT_DISABLED
            if str(operation_id) in self.deployment_disabled
            else ProbeResponseClass.SUCCESS
        )
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url="https://demo.ckan.org/api/3/action/status_show",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=response_class,
        )


def _confirmed_destructive_policy() -> MutationPolicy:
    return MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _client(transport: SyncCaptureTransport, runner: SeededProbeRunner | None = None) -> SyncCKANClient:
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
        probe_policy="auto",
        probe_runner=runner if runner is not None else SeededProbeRunner(),
    )


def _async_client(transport: AsyncCaptureTransport, runner: AsyncSeededProbeRunner | None = None) -> AsyncCKANClient:
    return AsyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
        probe_policy="auto",
        probe_runner=runner if runner is not None else AsyncSeededProbeRunner(),
    )


def _names_for_group(group: str) -> set[str]:
    return {entry.name for entry in CKAN_ACTIONS.entries if entry.group == group}


def test_datastore_and_extension_actions_expose_typed_methods_on_both_mode_services() -> None:
    """Each manifest-registered datastore or extension action has both typed methods."""
    sync_datastore = {name for name in dir(SyncDatastoreService) if not name.startswith("_")}
    async_datastore = {name for name in dir(AsyncDatastoreService) if not name.startswith("_")}
    datastore_names = _names_for_group("datastore")
    assert len(datastore_names) == 10
    for action in datastore_names:
        assert action in sync_datastore, f"sync surface misses {action}"
        assert action in async_datastore, f"async surface misses {action}"

    sync_extensions = {name for name in dir(SyncExtensionsService) if not name.startswith("_")}
    async_extensions = {name for name in dir(AsyncExtensionsService) if not name.startswith("_")}
    extension_names = _names_for_group("extensions")
    assert len(extension_names) == 11
    for action in extension_names:
        assert action in sync_extensions, f"sync surface misses {action}"
        assert action in async_extensions, f"async surface misses {action}"


def test_sqlsearch_deployment_disabled_blocks_before_dispatch_naming_the_gate() -> None:
    """DEPLOYMENT_DISABLED sqlsearch evidence refuses pre-dispatch at zero I/O."""
    transport = SyncCaptureTransport(body=_success_body({"records": []}))
    client = _client(transport, SeededProbeRunner(deployment_disabled=frozenset({SQL_ID})))

    with pytest.raises(UnsupportedCapabilityError) as excinfo:
        client.datastore.datastore_search_sql(sql='SELECT * FROM "res-1"')

    assert transport.requests == []
    assert excinfo.value.capability_state == "deployment-disabled"
    assert "Enable the capability" in excinfo.value.safe_action


def test_not_found_envelopes_classify_deployment_disabled_on_exactly_the_sqlsearch_id() -> None:
    """Pitfall 5 agreement: not-found is DEPLOYMENT_DISABLED only on the sql id."""
    not_found = {"__type": "Not Found Error", "message": "Not found"}

    assert classify_probe_response(SQL_SEARCH_OPERATION_ID, not_found) is ProbeResponseClass.DEPLOYMENT_DISABLED
    assert classify_probe_response(DATASTORE_CRUD_OPERATION_ID, not_found) is ProbeResponseClass.UNAVAILABLE


def test_datastore_delete_is_destructive_and_refuses_unconfirmed_policies() -> None:
    """The table-dropping delete gates on a confirmed destructive policy at zero I/O."""
    transport = SyncCaptureTransport(body=_success_body({}))
    client = _client(transport)

    with pytest.raises(CatalogValidationError) as excinfo:
        client.datastore.datastore_delete(resource_id="res-1")

    assert transport.requests == []
    assert "destructive" in str(excinfo.value)

    confirmed_transport = SyncCaptureTransport(body=_success_body({}))
    confirmed_client = _client(confirmed_transport)
    deleted = confirmed_client.datastore.datastore_delete(resource_id="res-1", policy=_confirmed_destructive_policy())
    assert isinstance(deleted, CKANMutationResult)
    assert deleted.receipt.outcome == "succeeded"
    assert json.loads(confirmed_transport.requests[0].body or b"{}") == {"resource_id": "res-1"}


def test_datastore_records_delete_never_engages_the_destructive_gate() -> None:
    """The record-scoped delete dispatches on the standard tier without a policy."""
    transport = SyncCaptureTransport(body=_success_body({"deleted": 2}))
    client = _client(transport)

    result = client.datastore.datastore_records_delete(resource_id="res-1", filters={"id": "1"})

    assert isinstance(result, CKANMutationResult)
    assert len(transport.requests) == 1
    body = json.loads(transport.requests[0].body or b"{}")
    assert body == {"resource_id": "res-1", "filters": {"id": "1"}}


def test_datastore_search_decodes_records_and_totals_verbatim() -> None:
    """Search results keep their records and total count through the shaper."""
    transport = SyncCaptureTransport(body=_success_body({"records": [{"id": "1"}, {"id": "2"}], "count": 2}))
    client = _client(transport)

    envelope = client.datastore.datastore_search(resource_id="res-1", q="health", limit=5)

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/datastore_search")
    assert json.loads(request.body or b"{}") == {"resource_id": "res-1", "q": "health", "limit": 5}
    mapping = envelope.items[0]
    assert isinstance(mapping, MappingRecord)
    assert dict(mapping.payload)["count"] == 2
    assert envelope.page is not None
    assert envelope.page.total_items == 2


def test_datastore_function_create_and_delete_carry_receipts() -> None:
    """Function lifecycle mutations are receipt-bearing standard-tier calls."""
    create_transport = SyncCaptureTransport(body=_success_body({"name": "lower_title"}))
    create_client = _client(create_transport)

    created = create_client.datastore.datastore_function_create(
        name="lower_title", language="python", handler="lower_title", source="def lower_title(x): return x.lower()"
    )

    assert isinstance(created, CKANMutationResult)
    assert json.loads(create_transport.requests[0].body or b"{}") == {
        "name": "lower_title",
        "language": "python",
        "handler": "lower_title",
        "source": "def lower_title(x): return x.lower()",
    }

    delete_transport = SyncCaptureTransport(body=_success_body(True))
    delete_client = _client(delete_transport)
    deleted = delete_client.datastore.datastore_function_delete(name="lower_title")
    assert isinstance(deleted, CKANMutationResult)
    assert isinstance(deleted.result.items[0], ValueRecord)


def test_config_options_dispatch_under_the_admin_id_and_map_forbidden_envelopes() -> None:
    """Config options ride their sysadmin id; forbidden envelopes map with safe actions."""
    show_transport = SyncCaptureTransport(body=_success_body("demo.ckan.org"))
    show_client = _client(show_transport)

    shown = show_client.extensions.config_option_show(key="ckan.site_url")

    assert isinstance(shown.items[0], ValueRecord)
    assert shown.items[0].value == "demo.ckan.org"
    assert json.loads(show_transport.requests[0].body or b"{}") == {"key": "ckan.site_url"}

    list_transport = SyncCaptureTransport(body=_success_body(["ckan.site_url", "ckan.site_title"]))
    list_client = _client(list_transport)
    listed = list_client.extensions.config_option_list()
    assert all(isinstance(item, ValueRecord) for item in listed.items)

    update_transport = SyncCaptureTransport(body=_success_body({"ckan.site_title": "New"}))
    update_client = _client(update_transport)
    updated = update_client.extensions.config_option_update(values={"ckan.site_title": "New"})
    assert isinstance(updated, CKANMutationResult)
    assert json.loads(update_transport.requests[0].body or b"{}") == {"ckan.site_title": "New"}

    forbidden_transport = SyncCaptureTransport(
        body=_failure_body({"__type": "Authorization Error", "message": "not authorized to update config"})
    )
    forbidden_client = _client(forbidden_transport)
    with pytest.raises(ForbiddenError) as excinfo:
        forbidden_client.extensions.config_option_update(values={"ckan.site_title": "Nope"})
    assert excinfo.value.safe_action


def test_job_family_reads_decode_and_destructive_members_gate() -> None:
    """Jobs decode their record kind while job_clear/task_status_delete gate destructively."""
    list_transport = SyncCaptureTransport(body=_success_body([JOB_RESULT]))
    list_client = _client(list_transport)

    listed = list_client.extensions.job_list()

    record = listed.items[0]
    assert isinstance(record, NativeRecord)
    assert record.resource_kind.value == "job"
    assert record.id.value == "job-1"

    show_transport = SyncCaptureTransport(body=_success_body(JOB_RESULT))
    show_client = _client(show_transport)
    shown = show_client.extensions.job_show(id="job-1")
    assert isinstance(shown.items[0], NativeRecord)

    cancel_transport = SyncCaptureTransport(body=_success_body(True))
    cancel_client = _client(cancel_transport)
    cancelled = cancel_client.extensions.job_cancel(id="job-1")
    assert isinstance(cancelled, CKANMutationResult)

    clear_transport = SyncCaptureTransport(body=_success_body(True))
    clear_client = _client(clear_transport)
    with pytest.raises(CatalogValidationError):
        clear_client.extensions.job_clear()
    assert clear_transport.requests == []
    cleared = clear_client.extensions.job_clear(queues=["bulk"], policy=_confirmed_destructive_policy())
    assert isinstance(cleared, CKANMutationResult)
    assert json.loads(clear_transport.requests[0].body or b"{}") == {"queues": ["bulk"]}


def test_task_status_family_decodes_and_gates_the_destructive_delete() -> None:
    """Task-status records decode their own kind; deletes refuse unconfirmed policies."""
    show_transport = SyncCaptureTransport(body=_success_body(TASK_RESULT))
    show_client = _client(show_transport)

    shown = show_client.extensions.task_status_show(id="task-1")

    record = shown.items[0]
    assert isinstance(record, NativeRecord)
    assert record.resource_kind.value == "task"

    update_transport = SyncCaptureTransport(body=_success_body(TASK_RESULT))
    update_client = _client(update_transport)
    updated = update_client.extensions.task_status_update(
        entity_id="dataset-a", task_type="archiver", key="celery", value="done"
    )
    assert isinstance(updated, CKANMutationResult)
    assert json.loads(update_transport.requests[0].body or b"{}") == {
        "entity_id": "dataset-a",
        "task_type": "archiver",
        "key": "celery",
        "value": "done",
    }

    delete_transport = SyncCaptureTransport(body=_success_body({}))
    delete_client = _client(delete_transport)
    with pytest.raises(CatalogValidationError):
        delete_client.extensions.task_status_delete(id="task-1")
    assert delete_transport.requests == []
    deleted = delete_client.extensions.task_status_delete(id="task-1", policy=_confirmed_destructive_policy())
    assert isinstance(deleted, CKANMutationResult)


def test_async_sqlsearch_blocks_and_datastore_dispatches_from_their_own_evidence() -> None:
    """The async twin keeps the per-id split: sqlsearch blocked, crud dispatching."""
    blocked_transport = AsyncCaptureTransport(body=_success_body({"records": []}))
    blocked_client = _async_client(blocked_transport, AsyncSeededProbeRunner(deployment_disabled=frozenset({SQL_ID})))

    with pytest.raises(UnsupportedCapabilityError):
        asyncio.run(blocked_client.datastore.datastore_search_sql(sql='SELECT * FROM "res-1"'))
    assert blocked_transport.requests == []

    allowed_transport = AsyncCaptureTransport(body=_success_body({"records": [{"id": "1"}], "count": 1}))
    allowed_client = _async_client(allowed_transport)
    envelope = asyncio.run(allowed_client.datastore.datastore_search(resource_id="res-1"))
    assert isinstance(envelope.items[0], MappingRecord)
    assert len(allowed_transport.requests) == 1

    destructive_transport = AsyncCaptureTransport(body=_success_body({}))
    destructive_client = _async_client(destructive_transport)
    with pytest.raises(CatalogValidationError):
        asyncio.run(destructive_client.datastore.datastore_delete(resource_id="res-1"))
    assert destructive_transport.requests == []

    forbidden_transport = AsyncCaptureTransport(
        body=_failure_body({"__type": "Authorization Error", "message": "not authorized"})
    )
    forbidden_client = _async_client(forbidden_transport)
    with pytest.raises(ForbiddenError):
        asyncio.run(forbidden_client.extensions.config_option_update(values={"ckan.site_title": "Changed"}))
    assert len(forbidden_transport.requests) == 1
