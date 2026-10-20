from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from datasluice.connectors.catalog.udata.models.harvest import UDataJobHandle as HarvestHandle
from datasluice.connectors.catalog.udata.models.jobs import UDataJobHandle as TaskHandle
from datasluice.connectors.catalog.udata.models.transfers import TransferResponseInput
from datasluice.connectors.catalog.udata.wire.transfers import RESPOND_TO_TRANSFER_OPERATION
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import BudgetExhaustedError
from datasluice.runtime.resilience import DeadlineMonitor
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    async_client,
    async_route_table,
    sync_client,
    sync_route_table,
    with_site_route,
)

if TYPE_CHECKING:
    from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("family", ["harvest", "task"])
@pytest.mark.parametrize("transport_raises", [False, True])
def test_cold_probe_shares_wait_deadline(
    asynchronous: bool, family: str, transport_raises: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [0.0]
    requests: list[RuntimeRequest] = []
    budget = TimeBudget(1, 1, 1, 1)

    def respond(request: RuntimeRequest) -> RuntimeResponse:
        requests.append(request)
        assert request.deadline is not None
        assert request.deadline.budget is budget
        now[0] = 2.0
        if transport_raises:
            request.deadline.assert_dispatchable()
        return sync_route_table(with_site_route({})).response_for(request)

    def wait(client: Any) -> Any:
        if family == "harvest":
            return client.harvest_moderation_admin.wait_for_job(
                HarvestHandle("source", "job", poll_interval=0),
                UDATA_ADMIN_PERMISSIONS,
                budget=budget,
                clock=lambda: now[0],
            )
        return client.worker_jobs.wait_for_task(
            TaskHandle("task", poll_interval=0), UDATA_ADMIN_PERMISSIONS, budget=budget, clock=lambda: now[0]
        )

    async def run() -> Any:
        async def send(request: RuntimeRequest) -> RuntimeResponse:
            return respond(request)

        router = async_route_table(with_site_route({}))
        monkeypatch.setattr(router, "send", send)
        async with async_client(router) as client:
            return await wait(client)

    if asynchronous:
        result = asyncio.run(run())
    else:
        router = sync_route_table(with_site_route({}))
        monkeypatch.setattr(router, "send", respond)
        with sync_client(router) as client:
            result = wait(client)
    assert result.handle.exhausted and result.handle.abandoned
    assert len(requests) == 1
    assert requests[0].url.endswith("/api/1/site/")


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("phase", ["preflight", "mutation", "read"])
@pytest.mark.parametrize("transport_raises", [False, True])
def test_deadline_settlement_preserves_dispatch_evidence(
    asynchronous: bool, phase: str, transport_raises: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [0.0]
    requests: list[RuntimeRequest] = []
    routes = with_site_route(
        {
            ("POST", f"{UDATA_ORIGIN}/api/1/transfer/transfer-1/"): (200, {"id": "transfer-1", "status": "accepted"}),
            ("GET", f"{UDATA_ORIGIN}/api/1/transfer/transfer-1/"): (200, {"id": "transfer-1"}),
        }
    )
    policy = MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True, operation=RESPOND_TO_TRANSFER_OPERATION, target="transfer-1"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )

    def respond(request: RuntimeRequest) -> RuntimeResponse:
        requests.append(request)
        if request.url.endswith("/api/1/site/") == (phase == "preflight"):
            now[0] = 2.0
            if transport_raises:
                assert request.deadline is not None
                request.deadline.assert_dispatchable()
        return sync_route_table(routes).response_for(request)

    def invoke(client: Any) -> Any:
        monkeypatch.setattr(client, "_clock", lambda: now[0])
        monkeypatch.setattr(client, "_budget", TimeBudget(1, 1, 1, 1))
        if phase == "read":
            return client.transfers.get_transfer("transfer-1", UDATA_PERMISSIONS)
        return client.transfers.respond_to_transfer(
            "transfer-1", TransferResponseInput("accept"), UDATA_PERMISSIONS, policy
        )

    async def run() -> None:
        async def send(request: RuntimeRequest) -> RuntimeResponse:
            return respond(request)

        router = async_route_table(routes)
        monkeypatch.setattr(router, "send", send)
        async with async_client(router) as client:
            await invoke(client)

    with pytest.raises(BudgetExhaustedError) as raised:
        if asynchronous:
            asyncio.run(run())
        else:
            router = sync_route_table(routes)
            monkeypatch.setattr(router, "send", respond)
            with sync_client(router) as client:
                invoke(client)
    if phase == "read":
        assert not hasattr(raised.value, "mutation_receipt")
    else:
        receipt = getattr(raised.value, "mutation_receipt", None)
        assert isinstance(receipt, MutationReceipt)
        assert receipt.outcome == ("ambiguous" if phase == "mutation" else "failed")
    assert len(requests) == (1 if phase == "preflight" else 2)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_expired_caller_deadline_prevents_cold_probe(asynchronous: bool) -> None:
    now = [0.0]
    deadline = DeadlineMonitor(TimeBudget(1, 1, 1, 1), clock=lambda: now[0])
    now[0] = 2.0
    kwargs: dict[str, Any] = {
        "method": "POST",
        "path": "/api/1/transfer/transfer-1/",
        "owning_operation": RESPOND_TO_TRANSFER_OPERATION,
        "deadline": deadline,
    }

    async def run() -> None:
        router = async_route_table(with_site_route({}))
        async with async_client(router) as client:
            with pytest.raises(BudgetExhaustedError):
                await client._dataset_call_async(**kwargs)
        assert not router.requests

    if asynchronous:
        asyncio.run(run())
    else:
        router = sync_route_table(with_site_route({}))
        with sync_client(router) as client, pytest.raises(BudgetExhaustedError):
            client._dataset_call(**kwargs)
        assert not router.requests
