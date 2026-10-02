"""Exact wire and safety coverage for the uData spatial zones and coverage family.

Expectations are transcribed from the pinned uData 17.6.0 source
(`udata.core.spatial.api` and `udata.core.spatial.api_fields` at commit
0546582058d84706812a1c37387576efc4e5ad1f), not from the production request
builders.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.spatial import (
    SpatialDatasetQuery,
    SpatialSuggestQuery,
    segment,
)
from datasluice.connectors.catalog.udata.services.spatial import (
    AsyncSpatialService,
    SyncSpatialService,
)
from datasluice.connectors.catalog.udata.wire import spatial as wire
from datasluice.errors.catalog import CatalogNotFoundError, CatalogUnavailableError, CatalogValidationError
from datasluice.runtime.events import EventEmitter, ListSink
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure
from tests.helpers.udata_test_support import (
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_SITE,
    anonymous_sync_client,
    async_client,
    async_route_table,
    sync_client,
    sync_route_table,
    thawed,
    with_site_route,
)

ORIGIN = UDATA_ORIGIN
_SPATIAL = "/api/1/spatial"

_ASSIGNED_METHODS = {
    "suggest_zones",
    "spatial_zones",
    "spatial_zone_datasets",
    "spatial_zone",
    "spatial_levels",
    "spatial_granularities",
    "spatial_coverage",
}


def _feature(zone_id: str = "zone-1") -> dict[str, object]:
    return {
        "id": zone_id,
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [0, 0]]]},
        "properties": {"name": "A zone", "code": "FR-01", "level": "country:fr", "uri": "zone-1"},
    }


def _feature_collection() -> dict[str, object]:
    return {"type": "FeatureCollection", "features": [_feature()]}


def _dataset_ref() -> dict[str, object]:
    return {"id": "dataset-1", "title": "A dataset", "slug": "a-dataset", "uri": None, "page": None}


def _suggestion() -> dict[str, object]:
    return {"id": "zone-1", "name": "A zone", "code": "FR-01", "level": "country:fr", "uri": "zone-1"}


def test_spatial_contract_exposes_every_assigned_method_in_both_modes() -> None:
    sync_names = {
        name
        for name in dir(SyncSpatialService)
        if not name.startswith("_") and callable(getattr(SyncSpatialService, name))
    }
    async_names = {
        name
        for name in dir(AsyncSpatialService)
        if not name.startswith("_") and callable(getattr(AsyncSpatialService, name))
    }
    assert sync_names == async_names == _ASSIGNED_METHODS


def test_row3_suggest_zones_matches_exact_query_and_default_size() -> None:
    assert wire.suggest_zones_request(SpatialSuggestQuery(q="ab")) == (
        "GET",
        f"{_SPATIAL}/zones/suggest/?q=ab&size=10",
        {},
        None,
    )
    assert wire.suggest_zones_request(SpatialSuggestQuery(q="a b", size=3)) == (
        "GET",
        f"{_SPATIAL}/zones/suggest/?q=a+b&size=3",
        {},
        None,
    )


def test_row4_spatial_zones_encodes_the_comma_separated_id_list() -> None:
    assert wire.spatial_zones_request(("fr:1100001", "commune:75056")) == (
        "GET",
        f"{_SPATIAL}/zones/fr%3A1100001,commune%3A75056/",
        {},
        None,
    )


def test_rows5_6_7_8_9_cover_zone_datasets_zone_levels_granularities_and_coverage() -> None:
    assert wire.spatial_zone_datasets_request("fr:1100001", SpatialDatasetQuery()) == (
        "GET",
        f"{_SPATIAL}/zone/fr%3A1100001/datasets/?size=25",
        {},
        None,
    )
    assert wire.spatial_zone_datasets_request("fr:1100001", SpatialDatasetQuery(size=5)) == (
        "GET",
        f"{_SPATIAL}/zone/fr%3A1100001/datasets/?size=5",
        {},
        None,
    )
    assert wire.spatial_zone_request("fr:1100001") == ("GET", f"{_SPATIAL}/zone/fr%3A1100001/", {}, None)
    assert wire.spatial_levels_request() == ("GET", f"{_SPATIAL}/levels/", {}, None)
    assert wire.spatial_granularities_request() == ("GET", f"{_SPATIAL}/granularities/", {}, None)
    assert wire.spatial_coverage_request("country:fr") == ("GET", f"{_SPATIAL}/coverage/country%3Afr/", {}, None)


def test_every_spatial_operation_dispatches_under_its_own_operation_identity() -> None:
    router = sync_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}{_SPATIAL}/zones/suggest/?q=ab&size=10"): (200, [_suggestion()]),
                ("GET", f"{ORIGIN}{_SPATIAL}/zones/fr%3A1100001/"): (200, _feature_collection()),
                ("GET", f"{ORIGIN}{_SPATIAL}/zone/fr%3A1100001/datasets/?size=25"): (200, [_dataset_ref()]),
                ("GET", f"{ORIGIN}{_SPATIAL}/zone/fr%3A1100001/"): (200, _feature()),
                ("GET", f"{ORIGIN}{_SPATIAL}/levels/"): (200, [{"id": "country:fr", "name": "Pays"}]),
                ("GET", f"{ORIGIN}{_SPATIAL}/granularities/"): (200, [{"id": "country:fr", "name": "Pays"}]),
                ("GET", f"{ORIGIN}{_SPATIAL}/coverage/country%3Afr/"): (200, _feature_collection()),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        assert thawed(client.spatial.suggest_zones(SpatialSuggestQuery(q="ab"))[0].payload) == _suggestion()
        assert thawed(client.spatial.spatial_zones(("fr:1100001",)).payload) == _feature_collection()
        assert thawed(client.spatial.spatial_zone_datasets("fr:1100001")[0].payload) == _dataset_ref()
        assert thawed(client.spatial.spatial_zone("fr:1100001").payload) == _feature()
        assert thawed(client.spatial.spatial_levels()[0].payload) == {"id": "country:fr", "name": "Pays"}
        assert thawed(client.spatial.spatial_granularities()[0].payload) == {"id": "country:fr", "name": "Pays"}
        assert thawed(client.spatial.spatial_coverage("country:fr").payload) == _feature_collection()
    spatial_requests = [r for r in router.requests if "/spatial/" in r.url]
    assert [r.method for r in spatial_requests] == ["GET"] * 7
    assert spatial_requests[0].url == f"{ORIGIN}{_SPATIAL}/zones/suggest/?q=ab&size=10"
    assert spatial_requests[4].url == f"{ORIGIN}{_SPATIAL}/levels/"


def test_spatial_reads_succeed_without_any_credential() -> None:
    router = sync_route_table(
        with_site_route({("GET", f"{ORIGIN}{_SPATIAL}/levels/"): (200, [{"id": "country:fr", "name": "Pays"}])})
    )
    with anonymous_sync_client(router) as client:
        assert thawed(client.spatial.spatial_levels()[0].payload) == {"id": "country:fr", "name": "Pays"}
    spatial_request = next(r for r in router.requests if "/spatial/" in r.url)
    assert "X-API-KEY" not in dict(spatial_request.headers)


@pytest.mark.parametrize("identifier", [".", "..", "a/b", "a?b", ""])
def test_spatial_identifiers_reject_unsafe_path_segments(identifier: str) -> None:
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(identifier, wire.SPATIAL_ZONE_OPERATION)


def test_spatial_zone_identifier_encoding_happens_once() -> None:
    assert wire.spatial_zone_request("fr 1100001") == (
        "GET",
        f"{_SPATIAL}/zone/fr%201100001/",
        {},
        None,
    )


def test_spatial_queries_reject_invalid_input_before_dispatch() -> None:
    non_empty_string = cast(str, 1)
    non_integer = cast(int, "3")
    non_positive = 0
    with pytest.raises(ValueError):
        SpatialSuggestQuery(q="")
    with pytest.raises(ValueError):
        SpatialSuggestQuery(q=non_empty_string)
    with pytest.raises(ValueError):
        SpatialSuggestQuery(q="a", size=non_positive)
    with pytest.raises(ValueError):
        SpatialSuggestQuery(q="a", size=non_integer)
    with pytest.raises(ValueError):
        SpatialDatasetQuery(size=non_positive)
    with pytest.raises(ValueError):
        SpatialDatasetQuery(size=non_integer)


@pytest.mark.parametrize("oversized", [101, 1_000, 10**9])
def test_spatial_sizes_are_rejected_above_the_ceiling_before_dispatch(oversized: int) -> None:
    """An arbitrarily large size is refused at construction, so no request can ever carry it."""
    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(ValueError, match="1 through 100"):
            SpatialSuggestQuery(q="ab", size=oversized)
        with pytest.raises(ValueError, match="1 through 100"):
            SpatialDatasetQuery(size=oversized)
        with pytest.raises(ValueError, match="1 through 100"):
            client.spatial.suggest_zones(SpatialSuggestQuery(q="ab", size=oversized))
        with pytest.raises(ValueError, match="1 through 100"):
            client.spatial.spatial_zone_datasets("fr:1100001", SpatialDatasetQuery(size=oversized))
    assert [r.url for r in router.requests if "/spatial/" in r.url] == []


def test_spatial_accepts_the_exact_ceiling_boundary() -> None:
    """The bound is inclusive, so a caller asking for exactly the ceiling still dispatches."""
    router = sync_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}{_SPATIAL}/zone/fr%3A1100001/datasets/?size=100"): (200, [_dataset_ref()]),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        assert thawed(client.spatial.spatial_zone_datasets("fr:1100001", SpatialDatasetQuery(size=100))[0].payload) == (
            _dataset_ref()
        )
    assert [r.url for r in router.requests if "/spatial/" in r.url] == [
        f"{ORIGIN}{_SPATIAL}/zone/fr%3A1100001/datasets/?size=100"
    ]


def test_spatial_zone_collection_accepts_the_stock_geometry_less_feature() -> None:
    """A stock GeoZone carries id, type, and properties only; no geometry member is invented."""
    collection = wire.parse_feature_collection(
        {"type": "FeatureCollection", "features": [{"id": "zone-1", "type": "Feature", "properties": {}}]},
        wire.SPATIAL_ZONES_OPERATION,
    )
    assert collection.payload["type"] == "FeatureCollection"


def test_spatial_zone_list_rejects_an_empty_id_list() -> None:
    with pytest.raises(CatalogValidationError, match="one or more"):
        wire.spatial_zones_request(())


def test_spatial_responses_fail_typed_when_the_envelope_is_not_the_documented_shape() -> None:
    with pytest.raises(CatalogValidationError, match="must be an object"):
        wire.parse_feature_collection(["not-a-collection"], wire.SPATIAL_ZONES_OPERATION)
    with pytest.raises(CatalogValidationError, match="must be a list of objects"):
        wire.parse_mapping_sequence({"not": "a list"}, wire.SPATIAL_LEVELS_OPERATION)
    with pytest.raises(CatalogValidationError, match="must be a GeoJSON FeatureCollection"):
        wire.parse_feature_collection({"type": "FeatureCollection"}, wire.SPATIAL_ZONES_OPERATION)
    with pytest.raises(CatalogValidationError, match="must be a GeoJSON FeatureCollection"):
        wire.parse_feature_collection(
            {"type": "FeatureCollection", "features": ["not-a-feature"]}, wire.SPATIAL_ZONES_OPERATION
        )


def test_spatial_missing_zone_maps_to_a_typed_not_found_error() -> None:
    router = sync_route_table(
        with_site_route({("GET", f"{ORIGIN}{_SPATIAL}/zone/missing/"): (404, {"message": "Not found"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(CatalogNotFoundError):
            client.spatial.spatial_zone("missing")


def test_spatial_async_mode_matches_sync_exact_wire() -> None:
    router = async_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}{_SPATIAL}/zones/suggest/?q=ab&size=10"): (200, [_suggestion()]),
                ("GET", f"{ORIGIN}{_SPATIAL}/coverage/country%3Afr/"): (200, _feature_collection()),
            }
        )
    )

    async def run() -> None:
        async with async_client(router, UDATA_CREDENTIAL) as client:
            assert thawed((await client.spatial.suggest_zones(SpatialSuggestQuery(q="ab")))[0].payload) == _suggestion()
            coverage = await client.spatial.spatial_coverage("country:fr")
            assert cast(Mapping[str, object], thawed(coverage.payload))["type"] == "FeatureCollection"

    asyncio.run(run())
    spatial_requests = [r for r in router.requests if "/spatial/" in r.url]
    assert spatial_requests[0].url == f"{ORIGIN}{_SPATIAL}/zones/suggest/?q=ab&size=10"
    assert spatial_requests[1].url == f"{ORIGIN}{_SPATIAL}/coverage/country%3Afr/"


_LEVELS_URL = f"{ORIGIN}{_SPATIAL}/levels/"
_LEVELS_BODY: list[dict[str, object]] = [{"id": "country:fr", "name": "Pays"}]


class _InterruptingRoutes:
    """Answer the site probe and one family route, raising the configured interruption while armed."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, body: object) -> None:
        self._route = route
        self._interruption = interruption
        self._body = body
        self.armed = True
        self.requests: list[RuntimeRequest] = []

    def _respond(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if self.armed and (request.method, request.url) == self._route:
            raise self._interruption
        payload = UDATA_SITE if request.url.endswith("/api/1/site/") else self._body
        return RuntimeResponse(200, {"Content-Type": "application/json"}, json.dumps(payload).encode())


class _CancellingSyncTransport(_InterruptingRoutes):
    """Sync transport that counts `close` so close-once ownership is observable."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, body: object) -> None:
        super().__init__(route, interruption, body)
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._respond(request)

    def close(self) -> None:
        self.close_count += 1


class _CancellingAsyncTransport(_InterruptingRoutes):
    """Async transport that counts `aclose` so close-once ownership is observable."""

    def __init__(self, route: tuple[str, str], interruption: BaseException, body: object) -> None:
        super().__init__(route, interruption, body)
        self.aclose_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._respond(request)

    async def aclose(self) -> None:
        self.aclose_count += 1


def _spatial_interrupting_sync(
    interruption: BaseException, *, breaker_failure_threshold: int = 2, emitter: EventEmitter | None = None
) -> tuple[SyncUDataClient, _CancellingSyncTransport]:
    transport = _CancellingSyncTransport(("GET", _LEVELS_URL), interruption, _LEVELS_BODY)
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        emitter=emitter,
        breaker_failure_threshold=breaker_failure_threshold,
        retry_sleep=lambda _: None,
        owns_transport=True,
    )
    return client, transport


def _spatial_interrupting_async(
    interruption: BaseException, *, breaker_failure_threshold: int = 2, emitter: EventEmitter | None = None
) -> tuple[AsyncUDataClient, _CancellingAsyncTransport]:
    transport = _CancellingAsyncTransport(("GET", _LEVELS_URL), interruption, _LEVELS_BODY)
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        emitter=emitter,
        breaker_failure_threshold=breaker_failure_threshold,
        owns_transport=True,
    )
    return client, transport


def test_spatial_sync_cancellation_leaves_post_state_ready_and_closes_the_owned_transport_once() -> None:
    """An interrupted read propagates, leaves the client usable, and still honours close-once ownership."""
    client, transport = _spatial_interrupting_sync(KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        client.spatial.spatial_levels()
    assert transport.close_count == 0

    transport.armed = False
    assert thawed(client.spatial.spatial_levels()[0].payload) == _LEVELS_BODY[0]

    client.close()
    client.close()
    assert transport.close_count == 1


def test_spatial_sync_cancellation_never_closes_a_borrowed_transport() -> None:
    """A borrowed transport survives the same interrupted read and the context exit untouched."""
    transport = _CancellingSyncTransport(("GET", _LEVELS_URL), KeyboardInterrupt(), _LEVELS_BODY)
    with SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        owns_transport=False,
    ) as client:
        with pytest.raises(KeyboardInterrupt):
            client.spatial.spatial_levels()
    assert transport.close_count == 0


def test_spatial_sync_cancellation_is_never_recorded_as_a_circuit_breaker_failure() -> None:
    """Cancellations beyond the failure threshold still admit the next read, so a cancel is not an origin failure."""
    events = ListSink()
    client, transport = _spatial_interrupting_sync(KeyboardInterrupt(), emitter=EventEmitter(sinks=(events,)))

    for _ in range(4):
        with pytest.raises(KeyboardInterrupt):
            client.spatial.spatial_levels()

    transport.armed = False
    assert thawed(client.spatial.spatial_levels()[0].payload) == _LEVELS_BODY[0]
    assert [event.outcome for event in events.events if "breaker" in event.outcome] == []
    client.close()


def test_spatial_transport_failures_still_open_the_circuit_before_the_next_read() -> None:
    """The breaker is live for this family: genuine transport failures still fail the following read closed."""
    client, transport = _spatial_interrupting_sync(TransportFailure("no route"))

    with pytest.raises(TransportFailure):
        client.spatial.spatial_levels()

    transport.armed = False
    before = len(transport.requests)
    with pytest.raises(CatalogUnavailableError, match="circuit is open"):
        client.spatial.spatial_levels()
    assert len(transport.requests) == before
    client.close()


def test_spatial_async_cancellation_leaves_post_state_ready_and_closes_the_owned_transport_once() -> None:
    """The async mode matches sync: the cancellation propagates and ownership is still close-once."""
    client, transport = _spatial_interrupting_async(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError):
            await client.spatial.spatial_levels()
        assert transport.aclose_count == 0

        transport.armed = False
        assert thawed((await client.spatial.spatial_levels())[0].payload) == _LEVELS_BODY[0]

        await client.aclose()
        await client.aclose()
        assert transport.aclose_count == 1

    asyncio.run(run())


def test_spatial_async_cancellation_never_closes_a_borrowed_transport() -> None:
    """An interrupted async read against a borrowed transport leaves the caller's transport open."""
    transport = _CancellingAsyncTransport(("GET", _LEVELS_URL), asyncio.CancelledError(), _LEVELS_BODY)
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=UDATA_CREDENTIAL,
        owns_transport=False,
    )

    async def run() -> None:
        async with client:
            with pytest.raises(asyncio.CancelledError):
                await client.spatial.spatial_levels()
        assert transport.aclose_count == 0

    asyncio.run(run())


def test_spatial_async_cancellation_is_never_recorded_as_a_circuit_breaker_failure() -> None:
    """Async cancellations also leave the circuit closed, so the next read is admitted rather than refused."""
    events = ListSink()
    client, transport = _spatial_interrupting_async(asyncio.CancelledError(), emitter=EventEmitter(sinks=(events,)))

    async def run() -> None:
        for _ in range(4):
            with pytest.raises(asyncio.CancelledError):
                await client.spatial.spatial_levels()
        transport.armed = False
        assert thawed((await client.spatial.spatial_levels())[0].payload) == _LEVELS_BODY[0]
        await client.aclose()

    asyncio.run(run())
    assert [event.outcome for event in events.events if "breaker" in event.outcome] == []
