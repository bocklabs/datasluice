from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.compatibility import UDataRouteGate, UDataRouteSurface, udata_version_policy
from datasluice.errors.catalog import (
    CatalogError,
    CatalogUnavailableError,
    CatalogValidationError,
    UnsupportedCapabilityError,
)
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.unit.connectors.catalog.test_udata_tracer import (
    RecordingAsyncTransport,
    RecordingTransport,
    _datasets_operation,
)


def test_17_16_deployment_completes_dataset_read() -> None:
    transport = RecordingTransport(site_version="17.16.0")
    client = SyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")
    outcome = "completed"
    try:
        envelope = client.datasets_list(*_datasets_operation())
    except CatalogError as error:
        outcome = type(error).__name__
    assert outcome == "completed"
    assert len(envelope.items) == 1


def test_async_client_admits_contending_native_reads_in_successive_loops() -> None:
    class PendingTransport(AsyncSurfaceTransport):
        started: asyncio.Event
        release: asyncio.Event

        async def send(self, request: RuntimeRequest) -> RuntimeResponse:
            if request.url.endswith("/swagger.json"):
                self.started.set()
                await self.release.wait()
            if request.url.endswith("/datasets/abc/"):
                return RuntimeResponse(200, {}, b'{"id":"abc","title":"Dataset","slug":"dataset"}')
            return await super().send(request)

    surface = RuntimeResponse(
        200,
        {},
        json.dumps({"swagger": "2.0", "basePath": "/api/1", "paths": {"/datasets/{id}/": {"get": {}}}}).encode(),
    )
    transport = PendingTransport(surface)
    client = AsyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")

    async def run(*, close: bool) -> None:
        transport.started = asyncio.Event()
        transport.release = asyncio.Event()
        first = asyncio.create_task(client.datasets.get("abc"))
        await asyncio.wait_for(transport.started.wait(), timeout=2)
        second = asyncio.create_task(client.datasets.get("abc"))
        await asyncio.sleep(0)
        transport.release.set()
        try:
            records = await asyncio.gather(first, second)
            assert [record.id.value for record in records] == ["abc", "abc"]
        finally:
            if close:
                await client.aclose()

    asyncio.run(run(close=False))
    client.invalidate()
    asyncio.run(run(close=True))


def test_sync_gate_probes_independent_api_versions_concurrently() -> None:
    entered = {api: threading.Event() for api in ("1", "2")}
    release = threading.Event()

    class PendingTransport(RecordingTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            api = request.url.split("/")[-2]
            self.requests.append(request)
            entered[api].set()
            assert release.wait(timeout=3)
            return RuntimeResponse(
                200,
                {},
                json.dumps(
                    {"swagger": "2.0", "basePath": f"/api/{api}", "paths": {"/datasets/": {"get": {}}}}
                ).encode(),
            )

    transport = PendingTransport()
    gate = UDataRouteGate("http://127.0.0.1:5640", transport, 60)
    with ThreadPoolExecutor(max_workers=2) as workers:
        pending = [
            workers.submit(gate.require, "GET", f"/api/{api}/datasets/", "udata/read", "17.6.0") for api in entered
        ]
        try:
            assert all(event.wait(timeout=2) for event in entered.values())
            gate.invalidate()
        finally:
            release.set()
        for future in pending:
            future.result(timeout=3)
    for api in entered:
        gate.require("GET", f"/api/{api}/datasets/", "udata/read", "17.6.0")
    assert len(transport.requests) == 4


def test_unsupported_typed_path_is_refused_without_dispatch_or_caller_data() -> None:
    transport = RecordingTransport()
    gate = UDataRouteGate("http://127.0.0.1:5640", transport, 60)
    with pytest.raises(CatalogValidationError) as raised:
        gate.require("GET", "/api/3/private-id/?token=caller-secret", "udata/read", "17.6.0")
    assert raised.value.safe_action
    assert raised.value.platform == "udata"
    assert raised.value.operation == "udata/route-admission"
    assert raised.value.metadata == {"route_state": "unsupported"}
    assert not transport.requests


@pytest.mark.parametrize("version", ["17.0.0", "17.6.0", "17.16.0", "17.999.0"])
def test_profile_version_range_accepts_the_complete_major_line(version: str) -> None:
    assert udata_version_policy().accepts(version)
    assert declared_udata_profile().profile_version == "17.6.0"


@pytest.mark.parametrize("version", ["16.99.99", "18.0.0", "17.6", "017.6.0", "17.6.0-rc1", "１７.6.0", ""])
def test_profile_version_range_rejects_other_or_invalid_versions(version: str) -> None:
    assert not udata_version_policy().accepts(version)


def test_route_templates_prefer_literals_and_ignore_query_values() -> None:
    surface = UDataRouteSurface.parse(
        {
            "swagger": "2.0",
            "basePath": "/api/1",
            "paths": {
                "/datasets/{dataset_id}/": {"get": {}},
                "/datasets/search/": {"post": {}},
            },
        },
        "1",
    )
    assert surface.allows("GET", "/api/1/datasets/abc/?key=private")
    assert not surface.allows("GET", "/api/1/datasets/search/")
    assert surface.allows("POST", "/api/1/datasets/search/")


def test_route_parser_rejects_ambiguous_parameter_segments() -> None:
    with pytest.raises(ValueError, match="at most one parameter"):
        UDataRouteSurface.parse(
            {
                "swagger": "2.0",
                "basePath": "/api/1",
                "paths": {"/{first}{second}/": {"get": {}}},
            },
            "1",
        )


def test_route_parser_limits_the_deployment_surface() -> None:
    with pytest.raises(ValueError, match="bounded"):
        UDataRouteSurface.parse(
            {
                "swagger": "2.0",
                "basePath": "/api/1",
                "paths": {f"/route-{index}/": {"get": {}} for index in range(4097)},
            },
            "1",
        )


class SurfaceTransport(RecordingTransport):
    def __init__(self, surface: RuntimeResponse | Exception) -> None:
        super().__init__(site_version="17.16.0")
        self.surface = surface

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.url.endswith("/swagger.json"):
            self.requests.append(request)
            assert not request.headers
            if isinstance(self.surface, Exception):
                raise self.surface
            return self.surface
        return super().send(request)


@pytest.mark.parametrize("methods", [{"post": {}}, {}])
def test_removed_dataset_method_blocks_dispatch(methods: dict[str, object]) -> None:
    response = RuntimeResponse(
        200,
        {},
        json.dumps(
            {
                "swagger": "2.0",
                "basePath": "/api/1",
                "paths": {"/datasets/": methods},
            }
        ).encode(),
    )
    transport = SurfaceTransport(response)
    client = SyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")
    with pytest.raises(UnsupportedCapabilityError) as raised:
        client.datasets_list(*_datasets_operation())
    assert len(transport.requests) == 2
    assert raised.value.metadata["route_state"] == "absent"


@pytest.mark.parametrize(
    "response",
    [
        RuntimeResponse(200, {}, b"not-json"),
        RuntimeResponse(200, {}, b"{}"),
        RuntimeResponse(200, {}, b" " * (4 * 1024 * 1024 + 1)),
        RuntimeResponse(503, {}, b""),
        RuntimeResponse(302, {"Location": "https://other.example"}, b""),
        TimeoutError("private"),
    ],
)
def test_unverifiable_surface_fails_closed(response: RuntimeResponse | Exception) -> None:
    transport = SurfaceTransport(response)
    client = SyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")
    with pytest.raises(CatalogUnavailableError) as raised:
        client.datasets_list(*_datasets_operation())
    assert len(transport.requests) == 2
    assert raised.value.metadata["route_state"] == "unavailable"
    assert "private" not in str(raised.value)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_swagger_gate_preserves_original_typed_catalog_error(asynchronous: bool) -> None:
    original = CatalogValidationError(
        "The request exceeds the transport bound.",
        operation="udata/api-v1.list-datasets",
        platform="udata",
        safe_action="Reduce the request size.",
    )
    transport = SurfaceTransport(original)

    class AsyncTransport(RecordingAsyncTransport):
        async def send(self, request: RuntimeRequest) -> RuntimeResponse:
            return transport.send(request)

    with pytest.raises(CatalogValidationError) as raised:
        if asynchronous:
            gate = UDataRouteGate(transport=AsyncTransport(), origin="https://example.test", ttl_seconds=60)
            asyncio.run(gate.require_async("GET", "/api/1/datasets/", "udata/api-v1.list-datasets", "17.6.0"))
        else:
            gate = UDataRouteGate(transport=transport, origin="https://example.test", ttl_seconds=60)
            gate.require("GET", "/api/1/datasets/", "udata/api-v1.list-datasets", "17.6.0")
    assert raised.value is original


def test_async_17_16_deployment_completes_dataset_read() -> None:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient
    from tests.unit.connectors.catalog.test_udata_tracer import RecordingAsyncTransport

    transport = RecordingAsyncTransport(site_version="17.16.0")
    client = AsyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")

    async def run() -> None:
        assert len((await client.datasets_list(*_datasets_operation())).items) == 1
        await client.aclose()

    asyncio.run(run())


def test_removed_native_route_blocks_before_dispatch() -> None:
    class NativeTransport(SurfaceTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            if request.url.endswith("/datasets/private-id/"):
                self.requests.append(request)
                return RuntimeResponse(200, {}, b'{"id":"private-id","title":"Dataset","slug":"dataset"}')
            return super().send(request)

    transport = NativeTransport(
        RuntimeResponse(
            200,
            {},
            json.dumps(
                {
                    "swagger": "2.0",
                    "basePath": "/api/1",
                    "paths": {"/datasets/": {"get": {}}},
                }
            ).encode(),
        )
    )
    client = SyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")
    outcome = "dispatched"
    try:
        client.datasets.get("private-id")
    except UnsupportedCapabilityError:
        outcome = "unsupported"
    assert outcome == "unsupported"
    assert len(transport.requests) == 2


_ABSENT_SURFACE = RuntimeResponse(
    200,
    {},
    json.dumps(
        {
            "swagger": "2.0",
            "basePath": "/api/1",
            "paths": {"/unrelated/": {"get": {}}},
        }
    ).encode(),
)


@pytest.mark.parametrize("entry", ["detail", "site", "stream", "redirect", "v2"])
def test_sync_native_entry_points_share_route_admission(entry: str) -> None:
    response = (
        _ABSENT_SURFACE
        if entry != "v2"
        else RuntimeResponse(200, {}, _ABSENT_SURFACE.body.replace(b"/api/1", b"/api/2"))
    )
    transport = SurfaceTransport(response)
    with SyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640") as client:
        calls = {
            "detail": lambda: client.datasets.get("private-id"),
            "site": client.root_profile.get,
            "v2": client.datasets.list_v2,
            "stream": lambda: client.root_profile.datasets_csv(sink=lambda _: None),
            "redirect": lambda: client.resources.redirect("private-id"),
        }
        with pytest.raises(UnsupportedCapabilityError) as raised:
            calls[entry]()
        assert "private-id" not in str(raised.value.metadata)
    assert len(transport.requests) == 2


class AsyncSurfaceTransport:
    def __init__(self, surface: RuntimeResponse | Exception) -> None:
        self.router = SurfaceTransport(surface)

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self.router.send(request)

    async def aclose(self) -> None:
        self.router.close()


@pytest.mark.parametrize("entry", ["detail", "site", "stream", "redirect", "v2"])
def test_async_native_entry_points_share_route_admission(entry: str) -> None:
    response = (
        _ABSENT_SURFACE
        if entry != "v2"
        else RuntimeResponse(200, {}, _ABSENT_SURFACE.body.replace(b"/api/1", b"/api/2"))
    )
    transport = AsyncSurfaceTransport(response)
    client = AsyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")

    async def run() -> None:
        calls = {
            "detail": lambda: client.datasets.get("private-id"),
            "site": client.root_profile.get,
            "v2": client.datasets.list_v2,
            "stream": lambda: client.root_profile.datasets_csv(sink=lambda _: None),
            "redirect": lambda: client.resources.redirect("private-id"),
        }
        with pytest.raises(UnsupportedCapabilityError):
            await calls[entry]()
        await client.aclose()

    asyncio.run(run())
    assert len(transport.router.requests) == 2


def test_surface_cache_expires_and_refreshes_without_caller_credentials() -> None:
    from datasluice.domain.catalog.auth import UDataCredential

    now = [0.0]
    transport = RecordingTransport(site_version="17.16.0")
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=UDataCredential(api_key="caller-secret"),
        capability_cache_ttl=10,
        clock=lambda: now[0],
    )
    for _ in range(2):
        client.datasets_list(*_datasets_operation())

    def probes() -> list[RuntimeRequest]:
        return [request for request in transport.requests if request.url.endswith("/swagger.json")]

    assert len(probes()) == 1
    now[0] = 10.0
    client.datasets_list(*_datasets_operation())
    assert len(probes()) == 2
    client.invalidate()
    client.datasets_list(*_datasets_operation())
    assert len(probes()) == 3
    assert all(not request.headers for request in probes())
    client.close()


def test_undeclared_operation_is_rejected_before_probes_or_credential_resolution() -> None:
    from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
    from datasluice.domain.catalog.operations import OperationId

    class CredentialProvider:
        def resolve(self) -> object:
            raise AssertionError("An undeclared operation must not resolve credentials.")

    transport = RecordingTransport(site_version="17.16.0")
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=CredentialProvider(),
    )
    unknown = OperationId("udata", "api-v1", "new-deployment-route")
    with pytest.raises(UnsupportedCapabilityError):
        client.datasets_list(CatalogOperationRequest(unknown, {}), CatalogOperationGuard(unknown))
    assert not transport.requests


def test_slow_swagger_probe_exhausts_the_operation_deadline_before_dispatch() -> None:
    from datasluice.domain.catalog.resilience import TimeBudget
    from datasluice.errors.catalog import BudgetExhaustedError

    now = [0.0]

    class SlowSurfaceTransport(RecordingTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            response = super().send(request)
            if request.url.endswith("/swagger.json"):
                now[0] = 2.0
            return response

    transport = SlowSurfaceTransport(site_version="17.16.0")
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        budget=TimeBudget(1, 1, 1, 1),
        clock=lambda: now[0],
    )
    with pytest.raises(BudgetExhaustedError):
        client.datasets_list(*_datasets_operation())
    assert len(transport.requests) == 2


@pytest.mark.parametrize("probe_path", ["/api/1/site/", "/api/1/swagger.json"])
def test_refresh_during_an_async_probe_does_not_restore_stale_evidence(probe_path: str) -> None:
    async def run() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        surface = RuntimeResponse(
            200,
            {},
            json.dumps(
                {
                    "swagger": "2.0",
                    "basePath": "/api/1",
                    "paths": {"/datasets/": {"get": {}}},
                }
            ).encode(),
        )

        class PendingSurfaceTransport(AsyncSurfaceTransport):
            probes = 0

            async def send(self, request: RuntimeRequest) -> RuntimeResponse:
                if request.url.endswith(probe_path):
                    self.probes += 1
                    if self.probes == 1:
                        started.set()
                        await release.wait()
                return await super().send(request)

        transport = PendingSurfaceTransport(surface)
        client = AsyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")
        pending = asyncio.create_task(client.datasets_list(*_datasets_operation()))
        await asyncio.wait_for(started.wait(), timeout=1)
        client.invalidate()
        release.set()
        await pending
        await client.datasets_list(*_datasets_operation())
        assert transport.probes == 2
        await client.aclose()

    asyncio.run(run())
