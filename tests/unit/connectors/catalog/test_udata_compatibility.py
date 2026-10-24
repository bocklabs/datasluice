from __future__ import annotations

import asyncio
import json

import pytest

from datasluice.connectors.catalog.udata.clients import SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.compatibility import UDataRouteSurface, udata_version_policy
from datasluice.errors.catalog import CatalogError, CatalogUnavailableError, UnsupportedCapabilityError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.unit.connectors.catalog.test_udata_tracer import RecordingTransport, _datasets_operation


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


def test_async_17_16_deployment_completes_dataset_read() -> None:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient
    from tests.unit.connectors.catalog.test_udata_tracer import RecordingAsyncTransport

    transport = RecordingAsyncTransport(site_version="17.16.0")
    client = AsyncUDataClient(transport, declared_udata_profile(), origin="http://127.0.0.1:5640")

    async def run() -> None:
        assert len((await client.datasets_list(*_datasets_operation())).items) == 1
        await client.aclose()

    asyncio.run(run())
