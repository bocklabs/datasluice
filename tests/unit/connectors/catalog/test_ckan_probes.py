"""Deterministic loopback coverage for the concrete CKAN capability probe runners."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from time import monotonic

import pytest

from datasluice.connectors.catalog.ckan.clients import SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.probes import (
    ACTIVITY_OPERATION_ID,
    ADVISORY_METADATA_KEYS,
    COLLABORATORS_OPERATION_ID,
    DATASTORE_CRUD_OPERATION_ID,
    DEFAULT_SNAPSHOT_TTL,
    EXTENSION_PROBES_OPERATION_ID,
    RESOURCE_VIEWS_OPERATION_ID,
    SQL_SEARCH_OPERATION_ID,
    CKANAsyncProbeRunner,
    CKANProbeRunner,
    LineState,
    classify_probe_response,
    map_extensions_to_operations,
    maybe_emit_line_advisory,
    version_line_state,
)
from datasluice.discovery.detector import detect
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import EvidenceProvenance, ProbeEvidence, ProbeResponseClass
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.capability import AsyncProbeRunner, EffectiveCapabilityCache, ProbeRunner
from datasluice.runtime.events import EventEmitter, ListSink
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure

ORIGIN = "https://127.0.0.1:8443"
DISCOVERY_OPERATION_ID = OperationId("ckan", "action-api-v3", "discovery-help-and-status")
FILESTORE_OPERATION_ID = OperationId("ckan", "filestore", "upload-and-resource-file-replacement")


def _success_body(result: dict[str, object]) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


def _failure_body(error: dict[str, object]) -> bytes:
    return json.dumps({"success": False, "error": error}).encode("utf-8")


def _status_body(*, extensions: list[str], version: str | None = "2.11.5") -> bytes:
    result: dict[str, object] = {"site_title": "Loopback CKAN", "extensions": extensions}
    if version is not None:
        result["ckan_version"] = version
    return _success_body(result)


class _CannedTransport:
    """A deterministic loopback transport serving one canned status_show body."""

    def __init__(self, body: bytes, *, status_code: int = 200) -> None:
        self.body = body
        self.status_code = status_code
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code, headers={"Content-Type": "application/json"}, body=self.body
        )

    def close(self) -> None:
        return None


class _AsyncCannedTransport:
    """The async twin of the deterministic loopback transport."""

    def __init__(self, body: bytes, *, status_code: int = 200) -> None:
        self.body = body
        self.status_code = status_code
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code, headers={"Content-Type": "application/json"}, body=self.body
        )

    async def aclose(self) -> None:
        return None


class _ScriptedTransport:
    """A loopback transport serving a queued body sequence for refresh transitions."""

    def __init__(self, bodies: list[bytes]) -> None:
        self.bodies = list(bodies)
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        body = self.bodies.pop(0) if len(self.bodies) > 1 else self.bodies[0]
        return RuntimeResponse(status_code=200, headers={"Content-Type": "application/json"}, body=body)

    def close(self) -> None:
        return None


class _Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


def _runner(
    transport: _CannedTransport | _ScriptedTransport,
    *,
    origin: str = ORIGIN,
    emitter: EventEmitter | None = None,
    clock: Callable[[], float] = monotonic,
    ttl_seconds: float = DEFAULT_SNAPSHOT_TTL,
) -> CKANProbeRunner:
    return CKANProbeRunner(
        transport,
        origin,
        declared_ckan_profile(),
        emitter=emitter,
        clock=clock,
        ttl_seconds=ttl_seconds,
    )


def test_runners_satisfy_the_public_probe_protocols() -> None:
    """Both concrete runners structurally satisfy the published runtime-checkable protocols."""
    assert isinstance(_runner(_CannedTransport(_status_body(extensions=[]))), ProbeRunner)
    async_runner = CKANAsyncProbeRunner(_AsyncCannedTransport(b"{}"), ORIGIN, declared_ckan_profile())
    assert isinstance(async_runner, AsyncProbeRunner)


def test_one_shared_read_answers_the_full_optional_sweep() -> None:
    """Every optional-family probe is answered by one canned status read with zero additional reads."""
    transport = _CannedTransport(_status_body(extensions=["datastore", "activity", "image_view"]))
    runner = _runner(transport)

    classes = {
        DATASTORE_CRUD_OPERATION_ID: runner.probe(DATASTORE_CRUD_OPERATION_ID).observed_response_class,
        SQL_SEARCH_OPERATION_ID: runner.probe(SQL_SEARCH_OPERATION_ID).observed_response_class,
        ACTIVITY_OPERATION_ID: runner.probe(ACTIVITY_OPERATION_ID).observed_response_class,
        RESOURCE_VIEWS_OPERATION_ID: runner.probe(RESOURCE_VIEWS_OPERATION_ID).observed_response_class,
    }

    assert classes[DATASTORE_CRUD_OPERATION_ID] is ProbeResponseClass.SUCCESS
    assert classes[SQL_SEARCH_OPERATION_ID] is ProbeResponseClass.DEPLOYMENT_DISABLED
    assert classes[ACTIVITY_OPERATION_ID] is ProbeResponseClass.SUCCESS
    assert classes[RESOURCE_VIEWS_OPERATION_ID] is ProbeResponseClass.SUCCESS
    assert len(transport.requests) == 1
    assert transport.requests[0].url == f"{ORIGIN}/api/3/action/status_show"


def test_sql_search_maps_disabled_while_datastore_crud_resolves_independently() -> None:
    """The v2 split keeps the sql-search gate scoped to exactly that OperationId."""
    transport = _CannedTransport(_status_body(extensions=["datastore"]))
    runner = _runner(transport)

    disabled = runner.probe(SQL_SEARCH_OPERATION_ID)
    control = runner.probe(DATASTORE_CRUD_OPERATION_ID)

    assert disabled.observed_response_class is ProbeResponseClass.DEPLOYMENT_DISABLED
    assert control.observed_response_class is ProbeResponseClass.SUCCESS
    assert len(transport.requests) == 1
    assert (
        classify_probe_response(SQL_SEARCH_OPERATION_ID, {"__type": "Not Found Error"})
        is ProbeResponseClass.DEPLOYMENT_DISABLED
    )
    assert (
        classify_probe_response(DATASTORE_CRUD_OPERATION_ID, {"__type": "Not Found Error"})
        is ProbeResponseClass.UNAVAILABLE
    )


def test_collaborators_classify_through_the_config_gate_family() -> None:
    """Collaborator gating stays scoped to its own id through the same classification core."""
    transport = _CannedTransport(_status_body(extensions=[]))
    runner = _runner(transport)

    evidence = runner.probe(COLLABORATORS_OPERATION_ID)

    assert evidence.observed_response_class is ProbeResponseClass.DEPLOYMENT_DISABLED
    assert (
        classify_probe_response(COLLABORATORS_OPERATION_ID, {"__type": "Not Found Error"})
        is ProbeResponseClass.DEPLOYMENT_DISABLED
    )
    assert classify_probe_response(COLLABORATORS_OPERATION_ID, {"records": []}) is ProbeResponseClass.SUCCESS


def test_absent_plugins_are_unsupported_only_on_their_own_families() -> None:
    """Absent plugin mapping degrades only the ids whose own family plugin is missing."""
    transport = _CannedTransport(_status_body(extensions=[]))
    runner = _runner(transport)

    classes = {
        operation_id: runner.probe(operation_id).observed_response_class
        for operation_id in (
            DATASTORE_CRUD_OPERATION_ID,
            SQL_SEARCH_OPERATION_ID,
            ACTIVITY_OPERATION_ID,
            RESOURCE_VIEWS_OPERATION_ID,
            EXTENSION_PROBES_OPERATION_ID,
            DISCOVERY_OPERATION_ID,
            FILESTORE_OPERATION_ID,
        )
    }

    for operation_id in (
        DATASTORE_CRUD_OPERATION_ID,
        SQL_SEARCH_OPERATION_ID,
        ACTIVITY_OPERATION_ID,
        RESOURCE_VIEWS_OPERATION_ID,
        EXTENSION_PROBES_OPERATION_ID,
    ):
        assert classes[operation_id] is ProbeResponseClass.UNSUPPORTED, operation_id
    assert classes[DISCOVERY_OPERATION_ID] is ProbeResponseClass.SUCCESS
    assert classes[FILESTORE_OPERATION_ID] is ProbeResponseClass.SUCCESS


def test_authorization_and_forbidden_envelopes_classify_through_the_shared_core() -> None:
    """Envelope-level authorization failures keep their bounded response classes."""
    unauthenticated = _CannedTransport(_failure_body({"__type": "Authorization Error", "message": "bad token"}))
    forbidden = _CannedTransport(_failure_body({"__type": "Authorization Error", "message": "unauthorized to edit"}))
    denied_status = _CannedTransport(b"{}", status_code=401)

    assert (
        _runner(unauthenticated).probe(DISCOVERY_OPERATION_ID).observed_response_class
        is ProbeResponseClass.UNAUTHORIZED
    )
    assert _runner(forbidden).probe(DISCOVERY_OPERATION_ID).observed_response_class is ProbeResponseClass.FORBIDDEN
    assert (
        _runner(denied_status).probe(DISCOVERY_OPERATION_ID).observed_response_class is ProbeResponseClass.UNAUTHORIZED
    )


def test_localhost_evidence_raises_the_sanitizer_error_through_the_cache_resolve_path() -> None:
    """The untouched HTTPS sanitizer rejects local-stack evidence routed through the cache."""
    transport = _CannedTransport(_status_body(extensions=[]))
    runner = _runner(transport, origin="http://localhost:5000")
    cache = EffectiveCapabilityCache(declared_ckan_profile(), runner)

    with pytest.raises(CatalogValidationError) as raised:
        cache.resolve(DISCOVERY_OPERATION_ID)

    assert raised.value.capability_state == "invalid-probe-evidence"
    assert "sanitized HTTPS" in str(raised.value.__cause__)


def test_map_extensions_to_operations_translates_plugin_names() -> None:
    """The module constant table maps plugin names onto their v2 optional OperationIds."""
    assert map_extensions_to_operations(["datastore"]) == frozenset({DATASTORE_CRUD_OPERATION_ID})
    assert map_extensions_to_operations(["image_view", "text_view"]) == frozenset({RESOURCE_VIEWS_OPERATION_ID})
    assert map_extensions_to_operations(["activity"]) == frozenset({ACTIVITY_OPERATION_ID})
    assert map_extensions_to_operations(["activity", "datastore"]) == frozenset(
        {ACTIVITY_OPERATION_ID, DATASTORE_CRUD_OPERATION_ID}
    )
    assert map_extensions_to_operations([]) == frozenset()
    assert map_extensions_to_operations(["mystery_plugin"]) == frozenset()


def test_async_runner_shares_the_classification_core() -> None:
    """The async twin classifies identically to the sync runner over the same canned status."""
    body = _status_body(extensions=["datastore", "activity", "image_view"])
    sync_transport = _CannedTransport(body)
    async_transport = _AsyncCannedTransport(body)
    sync_runner = _runner(sync_transport)
    async_runner = CKANAsyncProbeRunner(async_transport, ORIGIN, declared_ckan_profile())

    async def exercise() -> dict[OperationId, ProbeResponseClass]:
        return {
            operation_id: (await async_runner.probe(operation_id)).observed_response_class
            for operation_id in (
                DATASTORE_CRUD_OPERATION_ID,
                SQL_SEARCH_OPERATION_ID,
                ACTIVITY_OPERATION_ID,
                RESOURCE_VIEWS_OPERATION_ID,
            )
        }

    async_classes = asyncio.run(exercise())
    sync_classes = {
        operation_id: sync_runner.probe(operation_id).observed_response_class
        for operation_id in (
            DATASTORE_CRUD_OPERATION_ID,
            SQL_SEARCH_OPERATION_ID,
            ACTIVITY_OPERATION_ID,
            RESOURCE_VIEWS_OPERATION_ID,
        )
    }

    assert async_classes == sync_classes
    assert len(async_transport.requests) == 1
    assert len(sync_transport.requests) == 1


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        ("2.11.5", LineState.PINNED_LINE),
        ("2.11.4", LineState.IN_LINE_DRIFT),
        ("2.10.9", LineState.FOREIGN_LINE),
        (None, LineState.UNVERIFIED),
        ("not-a-version", LineState.UNVERIFIED),
        ("", LineState.UNVERIFIED),
    ],
)
def test_version_line_states_align_with_d08(observed: str | None, expected: LineState) -> None:
    """The four-state enum matches D-08 verbatim, including hidden-version degradation."""
    assert version_line_state(observed) is expected


def test_pinned_lines_never_advise_while_drift_states_do() -> None:
    """Advisories fire exactly for the three non-pinned states and never for the pinned line."""
    sink = ListSink()
    emitter = EventEmitter(sinks=(sink,))

    assert (
        maybe_emit_line_advisory(
            emitter,
            operation_id=DISCOVERY_OPERATION_ID,
            line_state=LineState.PINNED_LINE,
            observed_version_present=True,
        )
        is False
    )
    assert sink.events == ()
    for state in (LineState.IN_LINE_DRIFT, LineState.FOREIGN_LINE, LineState.UNVERIFIED):
        assert (
            maybe_emit_line_advisory(
                emitter,
                operation_id=DISCOVERY_OPERATION_ID,
                line_state=state,
                observed_version_present=False,
            )
            is True
        )

    assert len(sink.events) == 3


def test_in_line_drift_emits_the_d08_contradiction_fixing_advisory() -> None:
    """Drift inside the pinned major.minor line advises while keeping verified provenance."""
    sink = ListSink()
    transport = _CannedTransport(_status_body(extensions=[], version="2.11.4"))
    runner = _runner(transport, emitter=EventEmitter(sinks=(sink,)))

    evidence = runner.probe(DISCOVERY_OPERATION_ID)

    assert evidence.provenance is EvidenceProvenance.VERIFIED_LINE
    assert len(sink.events) == 1
    assert sink.events[0].metadata["line_state"] == "in-line-drift"
    assert sink.events[0].metadata["observed_version_present"] is True


def test_hidden_version_degrades_to_unverified_with_one_advisory_and_zero_exceptions() -> None:
    """A hide_version deployment yields UNVERIFIED provenance plus one advisory per window."""
    sink = ListSink()
    transport = _CannedTransport(_status_body(extensions=["datastore"], version=None))
    runner = _runner(transport, emitter=EventEmitter(sinks=(sink,)))

    first = runner.probe(DATASTORE_CRUD_OPERATION_ID)
    second = runner.probe(DATASTORE_CRUD_OPERATION_ID)

    assert first.observed_response_class is ProbeResponseClass.SUCCESS
    assert first.provenance is EvidenceProvenance.UNVERIFIED
    assert second.provenance is EvidenceProvenance.UNVERIFIED
    assert len(sink.events) == 1
    assert sink.events[0].outcome == "line_drift_advisory"
    assert sink.events[0].metadata["observed_version_present"] is False


def test_foreign_line_evidence_carries_unverified_provenance() -> None:
    """Foreign-line snapshots mark every produced evidence record as unverified."""
    transport = _CannedTransport(_status_body(extensions=[], version="2.10.9"))
    runner = _runner(transport)

    evidence = runner.probe(ACTIVITY_OPERATION_ID)

    assert evidence.provenance is EvidenceProvenance.UNVERIFIED


def test_advisory_metadata_is_bounded_to_two_keys() -> None:
    """Advisory envelopes carry only the two sanctioned metadata keys."""
    sink = ListSink()
    transport = _CannedTransport(_status_body(extensions=[], version=None))
    runner = _runner(transport, emitter=EventEmitter(sinks=(sink,)))

    runner.probe(DATASTORE_CRUD_OPERATION_ID)

    envelope = sink.events[0]
    assert set(envelope.metadata) == set(ADVISORY_METADATA_KEYS)
    assert set(envelope.metadata) == {"observed_version_present", "line_state"}


def test_advisories_deduplicate_per_state_transition_within_a_ttl_window() -> None:
    """One advisory fires per state transition or expired window, never more."""
    sink = ListSink()
    clock = _Clock()
    transport = _ScriptedTransport(
        [
            _status_body(extensions=[], version="2.11.5"),
            _status_body(extensions=[], version="2.10.9"),
            _status_body(extensions=[], version="2.10.9"),
            _status_body(extensions=[], version="2.10.9"),
        ]
    )
    runner = _runner(transport, emitter=EventEmitter(sinks=(sink,)), clock=clock)

    runner.probe(DATASTORE_CRUD_OPERATION_ID)
    assert len(sink.events) == 0

    clock.value += DEFAULT_SNAPSHOT_TTL + 1
    runner.probe(DATASTORE_CRUD_OPERATION_ID)
    assert len(sink.events) == 1

    runner.probe(DATASTORE_CRUD_OPERATION_ID)
    assert len(sink.events) == 1

    clock.value += DEFAULT_SNAPSHOT_TTL + 1
    runner.probe(DATASTORE_CRUD_OPERATION_ID)
    assert len(sink.events) == 2
    assert all(event.metadata["line_state"] == "foreign-line" for event in sink.events)


def test_advise_never_blocks_a_normal_client_read_on_foreign_line() -> None:
    """A client riding a foreign-line deployment still completes its documented read."""
    sink = ListSink()
    transport = _CannedTransport(_status_body(extensions=[], version="2.10.9"))
    runner = _runner(transport, emitter=EventEmitter(sinks=(sink,)))
    client = SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=ORIGIN,
        probe_runner=runner,
        owns_transport=False,
    )

    envelope = client.action_discovery.status_show()

    assert len(envelope.items) == 1
    assert any(event.metadata.get("line_state") == "foreign-line" for event in sink.events)


class _Registry:
    def __init__(self, connector_ids: tuple[str, ...]) -> None:
        self._connector_ids = connector_ids

    def list_connectors(self) -> list[str]:
        return list(self._connector_ids)


def test_detector_resolves_detection_rows_through_the_concrete_runner() -> None:
    """An engine wired with the concrete runner resolves every declared row from one status read."""
    sink = ListSink()
    transport = _CannedTransport(_status_body(extensions=["datastore", "activity", "image_view"]))
    runner = _runner(transport, emitter=EventEmitter(sinks=(sink,)))
    engine = EffectiveCapabilityCache(declared_ckan_profile(), runner)

    result = detect(ORIGIN, {"datasluice/ckan": engine}, _Registry(("datasluice/ckan",)))

    assert result.portal_type == "ckan"
    assert result.confidence == 1.0
    assert len(result.evidence) == len(declared_ckan_profile().operations)
    assert len(transport.requests) == 1
    assert sink.events == ()


def test_detector_fails_fast_when_the_engine_lacks_a_runner() -> None:
    """A runner-less engine raises the documented wiring error before any probing."""
    engine = EffectiveCapabilityCache(declared_ckan_profile())

    with pytest.raises(CatalogValidationError, match="no synchronous probe runner"):
        detect(ORIGIN, {"datasluice/ckan": engine}, _Registry(("datasluice/ckan",)))


def test_transport_failure_mid_sweep_is_contained_as_a_missed_row() -> None:
    """One failing probe produces a contained matched=False row while siblings still resolve."""
    failing = OperationId("ckan", "action-api-v3", "dataset-list-show-search")

    class _FailingRunner:
        def __init__(self, delegate: CKANProbeRunner) -> None:
            self._delegate = delegate

        def probe(self, operation_id: OperationId) -> ProbeEvidence:
            if operation_id == failing:
                raise TransportFailure("loopback connection reset")
            return self._delegate.probe(operation_id)

    transport = _CannedTransport(_status_body(extensions=[]))
    engine = EffectiveCapabilityCache(declared_ckan_profile(), _FailingRunner(_runner(transport)))

    result = detect(ORIGIN, {"datasluice/ckan": engine}, _Registry(("datasluice/ckan",)))

    failed_rows = [row for row in result.evidence if row.check == str(failing)]
    assert len(failed_rows) == 1
    assert failed_rows[0].matched is False
    assert failed_rows[0].detail.startswith("probe failed:")
    assert result.portal_type == "ckan"
    assert len(result.evidence) == len(declared_ckan_profile().operations)
