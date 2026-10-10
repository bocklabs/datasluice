"""Advisory-only, bounded, read-only evidence for public uData drift monitoring.

Every test runs against an injected in-process transport so no public request is
ever dispatched. The suite covers the empty reviewed allowlist fail-closed path,
exact-version and mismatched-version classification, outage and timeout
choreography, sync/async parity, bounded redacted fingerprints, immutable
advisory output, and the structural refusal of any mutation-class row inserted
into the drift read allowlist.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import operator
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.connectors.catalog.udata import drift
from datasluice.connectors.catalog.udata.clients import declared_udata_profile
from datasluice.connectors.catalog.udata.drift import (
    APPROVED_TARGETS,
    DRIFT_SCHEMA_VERSION,
    MAX_READS_PER_TARGET,
    MAX_TARGETS_PER_RUN,
    PINNED_UDATA_VERSION,
    READ_ALLOWLIST,
    DriftRecord,
    DriftReport,
    DriftTarget,
    ReadOperation,
    UDataDriftUnavailableError,
    run_udata_drift,
    run_udata_drift_async,
)
from datasluice.domain.catalog.operations import AuthClass, MutationClass, OperationId
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

ORIGIN = "https://www.data.gouv.fr"
_ALT_ORIGIN = "https://demo.data.gouv.fr"
_SITE_URL = f"{ORIGIN}/api/1/site/"
_DATASET_URL = f"{ORIGIN}/api/2/datasets/search/?page=1&page_size=1"
_TOPICS_URL = f"{ORIGIN}/api/2/topics/?page=1&page_size=1"
_FIXED_NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)


def _site_body(version: str) -> bytes:
    return json.dumps(
        {"feed_size": 0, "id": "site-id", "keywords": [], "metrics": {}, "title": "uData", "version": version}
    ).encode()


def _search_body(*, extra: Mapping[str, object] | None = None) -> bytes:
    payload: dict[str, object] = {
        "data": [{"id": "dataset-id", "title": "Dataset", "secret_payload": "Bearer sk-live-leaked-token"}],
        "next_page": None,
        "page": 1,
        "page_size": 1,
        "previous_page": None,
        "total": 1,
        "facets": {"tag": {"open": 1}},
    }
    if extra is not None:
        payload.update(extra)
    return json.dumps(payload).encode()


def _topics_body(*, extra: Mapping[str, object] | None = None) -> bytes:
    payload: dict[str, object] = {
        "data": [{"id": "topic-id", "name": "topic"}],
        "next_page": None,
        "page": 1,
        "page_size": 1,
        "previous_page": None,
        "total": 1,
    }
    if extra is not None:
        payload.update(extra)
    return json.dumps(payload).encode()


def _compatible_routes() -> dict[str, object]:
    return {
        _SITE_URL: _site_body(PINNED_UDATA_VERSION),
        _DATASET_URL: _search_body(),
        _TOPICS_URL: _topics_body(),
    }


class RecordingTransport:
    """A borrowed synchronous transport recording every dispatched request."""

    def __init__(self, routes: Mapping[str, object]) -> None:
        self._routes = dict(routes)
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        route = self._routes.get(request.url)
        if isinstance(route, BaseException):
            raise route
        if route is None:
            raise AssertionError(f"unexpected drift request {request.url}")
        if isinstance(route, RuntimeResponse):
            return route
        if isinstance(route, bytes):
            return RuntimeResponse(status_code=200, headers={}, body=route)
        if isinstance(route, tuple):
            status, body = cast("tuple[int, bytes]", route)
            return RuntimeResponse(status_code=status, headers={}, body=body)
        return RuntimeResponse(status_code=200, headers={}, body=cast("bytes", route))

    def close(self) -> None:
        self.close_count += 1

    @property
    def methods(self) -> list[str]:
        return [request.method for request in self.requests]

    @property
    def urls(self) -> list[str]:
        return [request.url for request in self.requests]


class RecordingAsyncTransport:
    """A borrowed asynchronous transport recording every dispatched request."""

    def __init__(self, routes: Mapping[str, object], *, hang: bool = False) -> None:
        self._routes = dict(routes)
        self._hang = hang
        self.requests: list[RuntimeRequest] = []
        self.aclose_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if self._hang:
            await asyncio.sleep(3600)
        route = self._routes.get(request.url)
        if route is None:
            raise AssertionError(f"unexpected drift request {request.url}")
        if isinstance(route, BaseException):
            raise route
        return _respond(route)

    async def aclose(self) -> None:
        self.aclose_count += 1

    @property
    def urls(self) -> list[str]:
        return [request.url for request in self.requests]


def _respond(route: object) -> RuntimeResponse:
    if isinstance(route, RuntimeResponse):
        return route
    if isinstance(route, bytes):
        return RuntimeResponse(status_code=200, headers={}, body=route)
    status, body = cast("tuple[int, bytes]", route)
    return RuntimeResponse(status_code=status, headers={}, body=body)


def _first_reads() -> tuple[str, ...]:
    return ("udata/api-v1.root-and-effective-profile-probe", "udata/api-v2.search-datasets", "udata/api-v2.list-topics")


def _target(*, origin: str = ORIGIN, alias: str = "primary", reads: Sequence[str] | None = None) -> DriftTarget:
    return DriftTarget(alias=alias, origin=origin, reads=tuple(reads) if reads is not None else _first_reads())


def _outcomes(report: DriftReport) -> list[str]:
    return [record.outcome for record in report.records]


def test_t01_reviewed_allowlist_is_empty_so_both_runners_fail_closed_before_dispatch() -> None:
    assert APPROVED_TARGETS == ()
    transport = RecordingTransport(_compatible_routes())

    with pytest.raises(UDataDriftUnavailableError) as raised:
        run_udata_drift(transport=transport)

    assert raised.value.approved_targets == 0
    assert raised.value.capability_state == "unavailable"
    assert raised.value.metadata["version_state"] == "unapproved-targets"
    assert transport.requests == []


def test_t01_async_runner_also_fails_closed_on_the_empty_allowlist() -> None:
    transport = RecordingAsyncTransport(_compatible_routes())

    async def run() -> UDataDriftUnavailableError:
        with pytest.raises(UDataDriftUnavailableError) as raised:
            await run_udata_drift_async(transport=transport)
        return raised.value

    error = asyncio.run(run())

    assert error.approved_targets == 0
    assert transport.requests == []


def test_t01_explicitly_empty_target_sequence_is_refused_before_dispatch() -> None:
    transport = RecordingTransport(_compatible_routes())

    with pytest.raises(UDataDriftUnavailableError):
        run_udata_drift([], transport=transport)

    assert transport.requests == []


def test_t03_exact_pinned_version_classifies_every_allowlisted_read_as_matched() -> None:
    transport = RecordingTransport(_compatible_routes())

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert _outcomes(report) == [drift.MATCHED] * 3
    assert report.schema_version == DRIFT_SCHEMA_VERSION
    assert report.profile_version == PINNED_UDATA_VERSION
    assert report.generated_at == _FIXED_NOW.isoformat()
    assert report.advisory_only is True
    assert report.targets == ("primary",)
    assert transport.methods == ["GET", "GET", "GET"]
    assert transport.urls == [_SITE_URL, _DATASET_URL, _TOPICS_URL]


def test_t03_mismatched_version_is_incompatible_and_skips_every_later_read() -> None:
    transport = RecordingTransport(
        {
            _SITE_URL: _site_body("17.8.0"),
            _DATASET_URL: _search_body(),
            _TOPICS_URL: _topics_body(),
        }
    )

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    site, dataset, topics = report.records
    assert site.outcome == drift.INCOMPATIBLE
    assert site.metadata["observed_version"] == "17.8.0"
    assert site.metadata["pinned_version"] == PINNED_UDATA_VERSION
    assert (dataset.outcome, topics.outcome) == (drift.SKIPPED, drift.SKIPPED)
    assert transport.urls == [_SITE_URL]


def test_t03_version_check_is_not_softened_to_a_compatible_prefix() -> None:
    transport = RecordingTransport({_SITE_URL: _site_body("17.60.0")})

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert report.records[0].outcome == drift.INCOMPATIBLE
    assert report.records[0].metadata["observed_version"] == "17.60.0"


def test_t03_missing_and_malformed_versions_are_incompatible_not_outage() -> None:
    for body in (json.dumps({"id": "s", "title": "uData"}).encode(), _site_body("not-a-version")):
        transport = RecordingTransport({_SITE_URL: body})
        report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)
        assert report.records[0].outcome == drift.INCOMPATIBLE


def test_t03_http_error_is_an_outage_that_skips_the_remaining_reads() -> None:
    transport = RecordingTransport(
        {
            _SITE_URL: (503, b"{}"),
            _DATASET_URL: _search_body(),
            _TOPICS_URL: _topics_body(),
        }
    )

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert report.records[0].outcome == drift.OUTAGE
    assert report.records[0].response_class == drift.HTTP_ERROR
    assert report.records[0].status_code == 503
    assert _outcomes(report) == [drift.OUTAGE, drift.SKIPPED, drift.SKIPPED]
    assert transport.urls == [_SITE_URL]


def test_t03_transport_failure_is_an_outage_without_an_aggressive_retry() -> None:
    transport = RecordingTransport({_SITE_URL: TimeoutError("drift read timed out")})

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert report.records[0].outcome == drift.OUTAGE
    assert report.records[0].metadata["error_type"] == "TimeoutError"
    assert transport.urls == [_SITE_URL]


def test_t03_bounded_deadline_exhaustion_is_classified_as_outage_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    class SlowTransport(RecordingTransport):
        """A transport whose first read consumes the whole operation budget."""

        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            time.sleep(0.05)
            return super().send(request)

    transport = SlowTransport(_compatible_routes())
    monkeypatch.setattr(
        drift, "READ_BUDGET", drift.READ_BUDGET.__class__(connect=0.001, read=0.001, write=0.001, total=0.01)
    )
    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert report.records[0].outcome == drift.MATCHED
    assert report.records[1].outcome == drift.OUTAGE
    assert report.records[1].metadata["error_type"] == "BudgetExhaustedError"
    assert report.records[2].outcome == drift.SKIPPED
    assert transport.urls == [_SITE_URL]


def test_t02_reports_retain_only_structural_fingerprints_and_never_bodies() -> None:
    transport = RecordingTransport(_compatible_routes())

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)
    rendered = report.to_json()

    assert "sk-live-leaked-token" not in rendered
    assert "Bearer" not in rendered
    assert "Dataset" not in rendered
    for record in report.records:
        assert set(record.to_dict()) == {
            "schema_version",
            "target",
            "operation",
            "response_class",
            "outcome",
            "fingerprint",
            "envelope_keys",
            "item_count",
            "status_code",
            "detail",
            "metadata",
        }
        assert len(record.fingerprint) == 32
        assert int(record.fingerprint, 16) >= 0
    site = report.records[0]
    assert site.envelope_keys == ("feed_size", "id", "keywords", "metrics", "title", "version")
    assert site.metadata["site_identity_present"] is True
    assert "site-id" not in rendered


def test_t02_credential_shaped_metadata_is_redacted_from_the_advisory_report() -> None:
    record = drift._record(
        target=_target(),
        operation_id="udata/api-v2.list-topics",
        response_class=drift.JSON_OBJECT,
        outcome=drift.MATCHED,
        detail="Bearer sk-live-leaked-token",
        metadata={"authorization": "Bearer sk-live-leaked-token", "nested": {"api_key": "sk-live-leaked-token"}},
    )

    rendered = json.dumps(record.to_dict(), sort_keys=True)

    assert "sk-live-leaked-token" not in rendered
    assert record.detail == "Bearer ***"
    assert record.metadata["authorization"] == "***"
    assert record.metadata["nested"] == {"api_key": "***"}


def test_t02_records_expose_no_mutation_or_write_surface() -> None:
    record = DriftRecord(
        schema_version=DRIFT_SCHEMA_VERSION,
        target="primary",
        operation="udata/api-v2.list-topics",
        response_class=drift.JSON_OBJECT,
        outcome=drift.OUTAGE,
        fingerprint="0" * 32,
        envelope_keys=(),
        item_count=None,
        status_code=None,
        detail="outage",
        metadata={"note": "ok"},
    )

    assert set(record.to_dict()) == {
        "schema_version",
        "target",
        "operation",
        "response_class",
        "outcome",
        "fingerprint",
        "envelope_keys",
        "item_count",
        "status_code",
        "detail",
        "metadata",
    }
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.metadata = {}  # type: ignore[misc]


def test_t01_advisory_report_is_immutable_and_has_no_write_surface() -> None:
    transport = RecordingTransport(_compatible_routes())
    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    with pytest.raises(dataclasses.FrozenInstanceError):
        operator.methodcaller("__setattr__", "outcome", "matched")(report.records[0])
    with pytest.raises(dataclasses.FrozenInstanceError):
        operator.methodcaller("__setattr__", "advisory_only", False)(report)
    assert not hasattr(report, "write_profile")
    assert not hasattr(report, "apply")
    assert set(vars(drift)) & {"write_profile", "apply_drift", "update_profile", "persist"} == set()


def test_t03_reports_are_json_serializable_with_finite_values_only() -> None:
    transport = RecordingTransport(_compatible_routes())

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert json.loads(report.to_json())["advisory_only"] is True


def test_t03_sync_and_async_runs_emit_equivalent_bounded_reports() -> None:
    sync_transport = RecordingTransport(_compatible_routes())
    async_transport = RecordingAsyncTransport(_compatible_routes())

    sync_report = run_udata_drift([_target()], transport=sync_transport, now=lambda: _FIXED_NOW)
    async_report = asyncio.run(run_udata_drift_async([_target()], transport=async_transport, now=lambda: _FIXED_NOW))

    assert sync_report.to_dict() == async_report.to_dict()
    assert sync_transport.urls == async_transport.urls
    assert sync_transport.methods == ["GET", "GET", "GET"]


def test_t03_async_timeout_is_an_outage_without_leaking_the_caller_task(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = RecordingAsyncTransport(_compatible_routes(), hang=True)
    monkeypatch.setattr(
        drift, "READ_BUDGET", drift.READ_BUDGET.__class__(connect=0.01, read=0.01, write=0.01, total=0.01)
    )
    report = asyncio.run(run_udata_drift_async([_target()], transport=transport, now=lambda: _FIXED_NOW))

    assert report.records[0].outcome == drift.OUTAGE
    assert transport.urls == [_SITE_URL]


def test_t03_async_cancellation_propagates_and_leaves_no_pending_task() -> None:
    transport = RecordingAsyncTransport(_compatible_routes(), hang=True)

    async def run() -> None:
        task = asyncio.ensure_future(run_udata_drift_async([_target()], transport=transport, now=lambda: _FIXED_NOW))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())

    assert transport.aclose_count == 0


def test_t01_borrowed_transports_are_never_closed_by_the_runner() -> None:
    transport = RecordingTransport(_compatible_routes())

    run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert transport.close_count == 0


class OwnedFailureTransport(RecordingTransport):
    """An owned transport whose every dispatch fails."""

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        raise TransportError("owned transport failed mid-run")


def test_t01_runner_owned_sync_transport_is_closed_even_when_a_read_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[RecordingTransport] = []

    def factory(**_: object) -> RecordingTransport:
        transport = OwnedFailureTransport({})
        created.append(transport)
        return transport

    monkeypatch.setattr(drift, "create_default_sync_transport", factory)
    report = run_udata_drift([_target()], now=lambda: _FIXED_NOW)

    assert report.records[0].outcome == drift.OUTAGE
    assert report.records[0].metadata["error_type"] == "TransportError"
    assert created[0].close_count == 1


def test_t01_owned_async_transport_is_closed_after_a_completed_run(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[RecordingAsyncTransport] = []

    def factory(**_: object) -> RecordingAsyncTransport:
        transport = RecordingAsyncTransport(_compatible_routes())
        created.append(transport)
        return transport

    monkeypatch.setattr(drift, "create_default_async_transport", factory)
    report = asyncio.run(run_udata_drift_async([_target()], now=lambda: _FIXED_NOW))

    assert _outcomes(report) == [drift.MATCHED] * 3
    assert created[0].aclose_count == 1


def test_t03_envelope_drift_is_advisory_and_never_changes_released_behavior() -> None:
    transport = RecordingTransport(
        {
            _SITE_URL: _site_body(PINNED_UDATA_VERSION),
            _DATASET_URL: _search_body(extra={"brand_new_key": ["value"]}),
            _TOPICS_URL: _topics_body(),
        }
    )

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert report.records[1].outcome == drift.DRIFTED
    assert report.records[1].metadata["envelope_key_count"] == 8
    assert report.records[2].outcome == drift.MATCHED
    assert report.advisory_only is True
    assert declared_udata_profile().profile_version == PINNED_UDATA_VERSION


def test_t03_non_json_and_json_list_payloads_are_advisory_not_errors() -> None:
    for body in (b"<html/>", b"[1, 2, 3]", b""):
        transport = RecordingTransport(
            {
                _SITE_URL: _site_body(PINNED_UDATA_VERSION),
                _DATASET_URL: body,
                _TOPICS_URL: _topics_body(),
            }
        )
        report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)
        assert report.records[1].outcome == drift.DRIFTED
        assert report.records[1].response_class in {drift.NON_JSON, drift.JSON_LIST}


def test_t03_item_counts_are_bounded_even_for_an_oversized_envelope() -> None:
    oversized = json.dumps(
        {
            "data": [{"id": str(index)} for index in range(50)],
            "next_page": None,
            "page": 1,
            "page_size": 1,
            "previous_page": None,
            "total": 50,
            "facets": {},
        }
    ).encode()
    transport = RecordingTransport(
        {
            _SITE_URL: _site_body(PINNED_UDATA_VERSION),
            _DATASET_URL: oversized,
            _TOPICS_URL: _topics_body(),
        }
    )

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert report.records[1].item_count == drift.MAX_FINGERPRINT_ITEMS
    assert drift.MAX_FINGERPRINT_ITEMS < 50


def test_t04_every_allowlisted_operation_is_anonymous_read_class_in_the_pinned_profile() -> None:
    profile = declared_udata_profile()

    assert set(READ_ALLOWLIST) == set(_first_reads())
    for operation_id, operation in READ_ALLOWLIST.items():
        platform, _, tail = operation_id.partition("/")
        service, _, method = tail.partition(".")
        spec = profile.operations[OperationId(platform=platform, service=service, method=method)]
        request = operation.build_request(ORIGIN)
        assert spec.mutation_class is MutationClass.READ
        assert spec.auth_class is AuthClass.PUBLIC
        assert request.method == "GET"
        assert request.body is None
        assert request.files == ()
        assert request.headers == {}
        assert request.url.startswith(f"{ORIGIN}/api/")
        assert drift.MAX_PAGE_SIZE == 1
        assert request.max_response_bytes == drift.MAX_RESPONSE_BYTES


def test_t04_allowlist_mapping_is_read_only_by_construction() -> None:
    with pytest.raises(TypeError):
        READ_ALLOWLIST["udata/api-v1.create-dataset"] = READ_ALLOWLIST[_first_reads()[0]]  # type: ignore[index]
    with pytest.raises(TypeError):
        del READ_ALLOWLIST[_first_reads()[0]]  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "mutation_operation",
    ["udata/api-v1.create-dataset", "udata/api-v2.delete-topic", "udata/api-v2.update-topic"],
)
def test_t04_a_mutation_class_row_cannot_even_be_constructed_as_a_drift_read(mutation_operation: str) -> None:
    with pytest.raises(ValueError) as raised:
        ReadOperation(
            operation_id=mutation_operation,
            method="GET",
            path="/api/1/datasets/",
            query=(),
            expected_envelope_keys=frozenset({"data"}),
        )

    assert "read-class" in str(raised.value)


def _forge_read_operation(**fields: object) -> ReadOperation:
    """Bypass ``__post_init__`` to simulate a hand-edited allowlist row."""
    forged = object.__new__(ReadOperation)
    for name, value in (
        ("operation_id", "udata/api-v2.list-topics"),
        ("method", "GET"),
        ("path", "/api/2/topics/"),
        ("query", ()),
        ("expected_envelope_keys", frozenset({"data"})),
    ):
        object.__setattr__(forged, name, fields.get(name, value))
    return forged


@pytest.mark.parametrize(
    "mutation_operation",
    ["udata/api-v1.create-dataset", "udata/api-v2.delete-topic", "udata/api-v2.update-topic"],
)
def test_t04_inserting_a_mutation_into_the_allowlist_is_refused_before_any_dispatch(
    monkeypatch: pytest.MonkeyPatch, mutation_operation: str
) -> None:
    forged = _forge_read_operation(
        operation_id=mutation_operation,
        path="/api/1/datasets/",
        expected_envelope_keys=frozenset({"data"}),
    )
    poisoned = dict(READ_ALLOWLIST)
    poisoned[mutation_operation] = forged
    monkeypatch.setattr(drift, "READ_ALLOWLIST", poisoned)
    transport = RecordingTransport(_compatible_routes())
    target = DriftTarget(
        alias="primary",
        origin=ORIGIN,
        reads=("udata/api-v1.root-and-effective-profile-probe", mutation_operation),
    )

    with pytest.raises(CatalogValidationError) as raised:
        run_udata_drift([target], transport=transport, now=lambda: _FIXED_NOW)

    assert "read-class" in str(raised.value)
    assert transport.urls == [_SITE_URL]
    assert not any("/datasets/" in url for url in transport.urls)


def test_t04_a_forged_non_get_allowlist_row_is_refused_before_any_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    forged = _forge_read_operation(
        operation_id="udata/api-v2.delete-topic",
        method="DELETE",
        path="/api/2/topics/topic-id/",
    )
    poisoned = dict(READ_ALLOWLIST)
    poisoned["udata/api-v2.delete-topic"] = forged
    monkeypatch.setattr(drift, "READ_ALLOWLIST", poisoned)
    transport = RecordingTransport(_compatible_routes())
    target = DriftTarget(
        alias="primary",
        origin=ORIGIN,
        reads=("udata/api-v1.root-and-effective-profile-probe", "udata/api-v2.delete-topic"),
    )

    with pytest.raises(CatalogValidationError) as raised:
        run_udata_drift([target], transport=transport, now=lambda: _FIXED_NOW)

    assert "anonymous GET" in str(raised.value)
    assert transport.methods == ["GET"]
    assert transport.urls == [_SITE_URL]


@pytest.mark.parametrize("mutation_operation", ["udata/api-v1.create-dataset", "udata/api-v2.delete-topic"])
def test_t04_a_drift_target_cannot_name_a_read_absent_from_the_allowlist(mutation_operation: str) -> None:
    with pytest.raises(CatalogValidationError) as raised:
        DriftTarget(
            alias="primary", origin=ORIGIN, reads=("udata/api-v1.root-and-effective-profile-probe", mutation_operation)
        )

    assert "read-only allowlist" in str(raised.value)


def test_t04_drift_targets_reject_non_https_and_unbounded_read_sets() -> None:
    with pytest.raises(CatalogValidationError, match="HTTPS"):
        DriftTarget(alias="primary", origin="http://127.0.0.1:5640", reads=_first_reads())
    with pytest.raises(ValueError):
        DriftTarget(alias="PRIMARY", origin=ORIGIN, reads=_first_reads())
    with pytest.raises(ValueError):
        DriftTarget(alias="primary", origin="", reads=_first_reads())
    with pytest.raises(CatalogValidationError):
        DriftTarget(alias="primary", origin=ORIGIN, reads=_first_reads() + ("udata/api-v2.list-topics",))
    with pytest.raises(CatalogValidationError):
        DriftTarget(alias="primary", origin=ORIGIN, reads=("udata/api-v2.list-topics",))
    with pytest.raises(CatalogValidationError):
        DriftTarget(alias="primary", origin=ORIGIN, reads=())


def test_t03_runs_are_bounded_to_the_reviewed_target_and_read_counts() -> None:
    assert MAX_TARGETS_PER_RUN == 2
    assert MAX_READS_PER_TARGET == 3
    assert drift.MAX_READS_PER_RUN == MAX_TARGETS_PER_RUN * MAX_READS_PER_TARGET

    targets = [
        DriftTarget(alias=f"t{index}", origin=ORIGIN, reads=_first_reads()) for index in range(MAX_TARGETS_PER_RUN + 1)
    ]
    with pytest.raises(CatalogValidationError, match="reviewed targets"):
        run_udata_drift(targets, transport=RecordingTransport(_compatible_routes()))


def test_t03_multiple_approved_targets_produce_one_bounded_report_each() -> None:
    primary = DriftTarget(alias="primary", origin=ORIGIN, reads=_first_reads())
    secondary = DriftTarget(alias="secondary", origin=_ALT_ORIGIN, reads=_first_reads())
    routes = {
        **_compatible_routes(),
        **{url.replace(ORIGIN, _ALT_ORIGIN): body for url, body in _compatible_routes().items()},
    }
    transport = RecordingTransport(routes)

    report = run_udata_drift([primary, secondary], transport=transport, now=lambda: _FIXED_NOW)

    assert report.targets == ("primary", "secondary")
    assert len(report.records) == 6
    assert {record.target for record in report.records} == {"primary", "secondary"}
    assert len(transport.requests) == 6
    assert set(transport.methods) == {"GET"}


def test_t02_fingerprints_are_stable_for_identical_structure_and_change_with_shape() -> None:
    def fingerprints(routes: Mapping[str, object]) -> list[str]:
        transport = RecordingTransport(routes)
        report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)
        return [record.fingerprint for record in report.records]

    baseline = fingerprints(_compatible_routes())
    assert baseline == fingerprints(_compatible_routes())
    changed = fingerprints({**_compatible_routes(), _TOPICS_URL: _topics_body(extra={"surprise": 1})})
    assert changed[:2] == baseline[:2]
    assert changed[2] != baseline[2]


def test_t02_envelope_key_fingerprints_are_bounded_to_the_reviewed_key_ceiling() -> None:
    wide = json.dumps({"data": [], **{f"key{index}": index for index in range(200)}}).encode()
    transport = RecordingTransport(
        {
            _SITE_URL: _site_body(PINNED_UDATA_VERSION),
            _DATASET_URL: wide,
            _TOPICS_URL: _topics_body(),
        }
    )

    report = run_udata_drift([_target()], transport=transport, now=lambda: _FIXED_NOW)

    assert len(report.records[1].envelope_keys) == drift.MAX_FINGERPRINT_KEYS
    assert drift.MAX_FINGERPRINT_KEYS < 200


def test_t03_renders_are_byte_identical_for_a_fixed_clock() -> None:
    first = run_udata_drift([_target()], transport=RecordingTransport(_compatible_routes()), now=lambda: _FIXED_NOW)
    second = run_udata_drift([_target()], transport=RecordingTransport(_compatible_routes()), now=lambda: _FIXED_NOW)

    assert first.to_json() == second.to_json()
    assert json.loads(first.to_json())["schema_version"] == DRIFT_SCHEMA_VERSION


def test_t01_runners_never_write_to_the_profile_oracle_or_fixtures() -> None:
    contracts = Path("src/datasluice/contracts/catalog")
    before = {path: path.read_bytes() for path in sorted(contracts.rglob("*.json"))}

    report = run_udata_drift([_target()], transport=RecordingTransport(_compatible_routes()), now=lambda: _FIXED_NOW)
    asyncio.run(
        run_udata_drift_async(
            [_target()], transport=RecordingAsyncTransport(_compatible_routes()), now=lambda: _FIXED_NOW
        )
    )

    after = {path: path.read_bytes() for path in sorted(contracts.rglob("*.json"))}
    assert after == before
    assert before
    assert report.advisory_only is True
