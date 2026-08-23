"""Offline loopback coverage for the runnable CKAN drift-read checker."""

from __future__ import annotations

import inspect
import json
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import pytest

from datasluice.connectors.catalog.ckan.clients import create_sync_client
from datasluice.connectors.catalog.ckan.drift import (
    DEFAULT_TARGETS,
    DRIFTED_DETAIL,
    MATCHED_DETAIL,
    AdvisoryRecord,
    DriftCheck,
    DriftClientFactory,
    DriftTarget,
    canonical_compare,
    main,
    run_drift_checks,
)
from datasluice.connectors.catalog.ckan.probes import LineState
from datasluice.connectors.catalog.ckan.settings import CKANClientSettings
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ResultEnvelope, ValueRecord
from datasluice.errors.catalog import ForbiddenError
from datasluice.runtime.redaction import redact_for_output

_STATUS_KEYS = frozenset(
    {"ckan_version", "error_emails_to", "extensions", "locale_default", "site_description", "site_title", "site_url"}
)
_SHOW_KEYS = frozenset({"id", "name", "title", "state"})
_ADVISORY_KEYS = frozenset({"target", "operation", "line_state", "outcome", "detail"})

DEMO_ORIGIN = "https://demo.ckan.org"
DGU_ORIGIN = "https://ckan.publishing.service.gov.uk"


def _mapping_envelope(payload: dict[str, object]) -> ResultEnvelope[object]:
    return ResultEnvelope(items=(MappingRecord(payload=payload),))


def _status_envelope(*, version: str | None = "2.11.5") -> ResultEnvelope[object]:
    payload: dict[str, object] = {
        "ckan_version": version,
        "error_emails_to": "mailto:ops@loopback.example",
        "extensions": ["datastore"],
        "locale_default": "en",
        "site_description": "Loopback CKAN",
        "site_title": "Loopback CKAN",
        "site_url": "https://loopback.example",
    }
    if version is None:
        del payload["ckan_version"]
    return _mapping_envelope(payload)


def _dataset_envelope(name: str, payload: dict[str, object]) -> ResultEnvelope[object]:
    return ResultEnvelope(
        items=(
            NativeRecord(
                platform=CatalogPlatform.CKAN,
                resource_kind=ResourceKind.DATASET,
                id=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, name),
                payload=payload,
            ),
        )
    )


def _name_list_envelope(names: list[str]) -> ResultEnvelope[object]:
    return ResultEnvelope(items=tuple(ValueRecord(value=name) for name in names))


class _FakeDiscovery:
    def __init__(self, outcome: ResultEnvelope[object] | Exception) -> None:
        self._outcome = outcome

    def status_show(self) -> ResultEnvelope[object]:
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


class _FakeDatasets:
    def __init__(self, *, shows: dict[str, ResultEnvelope[object]], names: list[str] | None = None) -> None:
        self.shows = shows
        self.names = names or []
        self.requested_ids: list[str] = []

    def package_list(self) -> ResultEnvelope[object]:
        return _name_list_envelope(self.names)

    def package_show(self, *, id: str) -> ResultEnvelope[object]:
        self.requested_ids.append(id)
        return self.shows[id]

    def current_package_list_with_resources(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> ResultEnvelope[object]:
        return next(iter(self.shows.values()))


class _FakeClient:
    def __init__(
        self,
        settings: CKANClientSettings,
        *,
        status: ResultEnvelope[object] | Exception,
        shows: dict[str, ResultEnvelope[object]],
        names: list[str] | None = None,
    ) -> None:
        self.settings = settings
        self.datasets = _FakeDatasets(shows=shows, names=names)
        self.action_discovery = _FakeDiscovery(status)
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _factory(
    clients: list[_FakeClient],
    *,
    status: ResultEnvelope[object] | Exception,
    shows: dict[str, ResultEnvelope[object]],
    names: list[str] | None = None,
):
    def construct(settings: CKANClientSettings) -> _FakeClient:
        client = _FakeClient(settings, status=status, shows=shows, names=names)
        clients.append(client)
        return client

    return construct


def _show_payload() -> dict[str, object]:
    return {"id": "ds-1", "name": "ds-1", "title": "Dataset One", "state": "active"}


def _two_check_target() -> DriftTarget:
    return DriftTarget(
        origin="https://loopback.example",
        checks=(
            DriftCheck(
                action="status_show",
                parameters={},
                ordering="canonicalized",
                rationale="pinned-line skeleton read",
                expected_keys=_STATUS_KEYS,
            ),
            DriftCheck(
                action="package_show",
                parameters={"id": "ds-1"},
                ordering="canonicalized",
                rationale="single bounded record",
                expected_keys=_SHOW_KEYS,
            ),
        ),
    )


def test_run_produces_one_matched_advisory_per_configured_check() -> None:
    target = _two_check_target()
    clients: list[_FakeClient] = []
    records = run_drift_checks(
        [target],
        client_factory=_factory(
            clients,
            status=_status_envelope(),
            shows={"ds-1": _dataset_envelope("ds-1", _show_payload())},
        ),
    )
    assert [record.outcome for record in records] == ["matched", "matched"]
    assert all(record.line_state == LineState.PINNED_LINE.value for record in records)
    assert all(record.target == "https://loopback.example" for record in records)
    assert [record.operation for record in records] == ["status_show", "package_show"]
    assert clients[0].closed is True


def test_nginx_html_403_surfaces_unavailable_while_siblings_execute_and_exit_stays_zero() -> None:
    target = _two_check_target()
    forbidden = ForbiddenError(
        "Catalog operation returned an unsuccessful HTTP status.",
        operation="ckan/action-api-v3.discovery-help-and-status",
        platform="ckan",
        safe_action="Treat endpoint-level gating as advisory unavailability.",
    )
    records = run_drift_checks(
        [target],
        client_factory=_factory([], status=forbidden, shows={"ds-1": _dataset_envelope("ds-1", _show_payload())}),
    )
    assert isinstance(records, list)
    assert records[0].outcome == "unavailable"
    assert records[1].outcome == "matched"
    assert records[0].line_state == LineState.UNVERIFIED.value
    assert records[0].detail == f"endpoint unavailable: {type(forbidden).__name__}"


def test_hidden_version_propagates_unverified_without_keyerror() -> None:
    target = DriftTarget(
        origin="https://loopback.example",
        checks=(
            DriftCheck(
                action="status_show",
                parameters={},
                ordering="canonicalized",
                rationale="hidden-version deployment",
                expected_keys=frozenset({"site_title"}),
            ),
        ),
    )
    records = run_drift_checks(
        [target],
        client_factory=_factory([], status=_mapping_envelope({"site_title": "Loopback CKAN"}), shows={}),
    )
    assert records[0].line_state == LineState.UNVERIFIED.value
    assert records[0].outcome == "matched"


def test_foreign_line_target_completes_with_advisories() -> None:
    target = DriftTarget(
        origin="https://loopback.example",
        checks=(
            DriftCheck(
                action="status_show",
                parameters={},
                ordering="canonicalized",
                rationale="foreign-line deployment",
                expected_keys=_STATUS_KEYS,
            ),
        ),
    )
    records = run_drift_checks(
        [target], client_factory=_factory([], status=_status_envelope(version="2.9.0"), shows={})
    )
    assert records[0].line_state == LineState.FOREIGN_LINE.value
    assert records[0].outcome == "matched"


def test_structural_mismatch_yields_drifted_detail() -> None:
    target = DriftTarget(
        origin="https://loopback.example",
        checks=(
            DriftCheck(
                action="package_show",
                parameters={"id": "ds-1"},
                ordering="canonicalized",
                rationale="schema skeleton",
                expected_keys=_SHOW_KEYS,
            ),
        ),
    )
    drifted_payload: dict[str, object] = {
        "id": "ds-1",
        "name": "ds-1",
        "title": "Dataset One",
        "state": "active",
        "surprise": True,
    }
    records = run_drift_checks(
        [target],
        client_factory=_factory(
            [], status=_status_envelope(), shows={"ds-1": _dataset_envelope("ds-1", drifted_payload)}
        ),
    )
    assert records[0].outcome == "drifted"
    assert records[0].detail == DRIFTED_DETAIL


def test_canonical_compare_matrix_covers_orderings_exact_sets_and_record_lists() -> None:
    skeleton = frozenset({"a", "b"})
    assert canonical_compare({"a": 1, "b": 2}, skeleton, "canonicalized") is True
    assert canonical_compare({"a": 1}, skeleton, "platform-deterministic") is False
    assert canonical_compare({"a": 1, "b": 2, "c": 3}, skeleton, "canonicalized") is False
    assert canonical_compare(["a", "b"], skeleton, "platform-deterministic") is True
    assert canonical_compare(["b", "a"], skeleton, "platform-deterministic") is False
    assert canonical_compare(("b", "a"), skeleton, "canonicalized") is True
    full_record = MappingRecord(payload={"a": 1, "b": 2})
    short_record = MappingRecord(payload={"b": 2})
    assert canonical_compare((full_record.payload,), skeleton, "platform-deterministic") is True
    assert canonical_compare((full_record.payload, short_record.payload), skeleton, "canonicalized") is False
    assert canonical_compare(7, skeleton, "canonicalized") is False
    assert canonical_compare(None, skeleton, "canonicalized") is False


def test_time_varying_keys_are_excluded_by_configuration_not_by_filtering() -> None:
    observed = {"site_title": "Portal", "last_poll": "2026-08-23T00:00:00Z"}
    stable_skeleton = frozenset({"site_title"})
    assert canonical_compare(observed, stable_skeleton, "canonicalized") is False
    assert canonical_compare({"site_title": "Portal"}, stable_skeleton, "canonicalized") is True


def test_per_target_parameterization_runs_each_target_against_its_own_payload() -> None:
    demo_target = DriftTarget(
        origin=DEMO_ORIGIN,
        checks=(
            DriftCheck(
                action="package_show",
                parameters={"id": "demo-sample"},
                ordering="canonicalized",
                rationale="primary pinned dataset",
                expected_keys=frozenset({"name", "title"}),
            ),
        ),
    )
    dgu_target = DriftTarget(
        origin=DGU_ORIGIN,
        checks=(
            DriftCheck(
                action="package_show",
                parameters={"id": "dgu-harvest-set"},
                ordering="canonicalized",
                rationale="secondary pinned dataset",
                expected_keys=frozenset({"name", "harvest"}),
            ),
        ),
    )
    constructed: list[_FakeClient] = []

    def construct(settings: CKANClientSettings) -> _FakeClient:
        show_id = "demo-sample" if settings.base_url == DEMO_ORIGIN else "dgu-harvest-set"
        payload: dict[str, object] = (
            {"name": "demo-sample", "title": "Demo"}
            if settings.base_url == DEMO_ORIGIN
            else {"name": "dgu-harvest-set", "harvest": {}}
        )
        client = _FakeClient(settings, status=_status_envelope(), shows={show_id: _dataset_envelope(show_id, payload)})
        constructed.append(client)
        return client

    records = run_drift_checks([demo_target, dgu_target], client_factory=cast(DriftClientFactory, construct))
    assert [record.outcome for record in records] == ["matched", "matched"]
    assert constructed[0].datasets.requested_ids == ["demo-sample"]
    assert constructed[1].datasets.requested_ids == ["dgu-harvest-set"]
    assert {client.settings.base_url for client in constructed} == {DEMO_ORIGIN, DGU_ORIGIN}


def test_bounded_current_package_list_dispatch_matches_against_the_record_schema() -> None:
    target = DriftTarget(
        origin="https://loopback.example",
        checks=(
            DriftCheck(
                action="current_package_list_with_resources",
                parameters={"limit": 1},
                ordering="canonicalized",
                rationale="bounded list-family read",
                expected_keys=_SHOW_KEYS,
            ),
        ),
    )
    records = run_drift_checks(
        [target],
        client_factory=_factory(
            [], status=_status_envelope(), shows={"ds-1": _dataset_envelope("ds-1", _show_payload())}
        ),
    )
    assert records[0].outcome == "matched"


def test_package_list_sequence_check_matches_names_canonically() -> None:
    target = DriftTarget(
        origin="https://loopback.example",
        checks=(
            DriftCheck(
                action="package_list",
                parameters={},
                ordering="platform-deterministic",
                rationale="bounded canned name sequence",
                expected_keys=frozenset({"alpha", "beta"}),
            ),
        ),
    )
    records = run_drift_checks(
        [target], client_factory=_factory([], status=_status_envelope(), shows={}, names=["alpha", "beta"])
    )
    assert records[0].outcome == "matched"
    assert records[0].line_state == LineState.UNVERIFIED.value


def test_serialized_records_carry_exactly_the_five_redacted_keys() -> None:
    record = AdvisoryRecord(
        target=DEMO_ORIGIN,
        operation="status_show",
        line_state=LineState.PINNED_LINE.value,
        outcome="matched",
        detail=MATCHED_DETAIL,
    )
    rendered = json.loads(json.dumps(record.to_dict()))
    assert set(rendered) == _ADVISORY_KEYS
    assert redact_for_output("token=abc123 secret") != "token=abc123 secret"


def test_default_targets_reflect_the_amended_demo_ckan_disposition() -> None:
    assert [target.origin for target in DEFAULT_TARGETS] == [DEMO_ORIGIN, DGU_ORIGIN]
    demo_checks = DEFAULT_TARGETS[0].checks
    dgu_checks = DEFAULT_TARGETS[1].checks
    assert [check.action for check in demo_checks] == ["status_show", "package_show"]
    assert demo_checks[1].parameters["id"] == "my-sample-dataset-001"
    assert dgu_checks[1].parameters["id"] == "0-1-annual-probability-extents14"
    assert "harvest" in dgu_checks[1].expected_keys
    assert all(check.rationale for check in (*demo_checks, *dgu_checks))


def test_default_client_factory_is_the_published_sync_factory_attaching_rate_policy() -> None:
    signature = inspect.signature(run_drift_checks)
    default_factory = signature.parameters["client_factory"].default
    assert default_factory is create_sync_client
    assert default_factory.__module__ == "datasluice.connectors.catalog.ckan.clients"


def test_drift_check_refuses_non_whitelisted_actions() -> None:
    with pytest.raises(ValueError, match="whitelisted typed read"):
        DriftCheck(
            action="package_search",
            parameters={"q": ""},
            ordering="canonicalized",
            rationale="unregistered action must be refused",
            expected_keys=frozenset({"count"}),
        )


def test_module_help_prints_usage_with_zero_network_io(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def bomb(*args: object, **kwargs: object) -> list[AdvisoryRecord]:
        raise AssertionError("--help must not dispatch any drift check")

    monkeypatch.setattr("datasluice.connectors.catalog.ckan.drift.run_drift_checks", bomb)
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])
    assert excinfo.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_main_emits_json_lines_and_honors_json_out(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    selected: list[DriftTarget] = []

    def fake_run(targets: Sequence[DriftTarget]) -> list[AdvisoryRecord]:
        selected.extend(targets)
        return [
            AdvisoryRecord(
                target=DEMO_ORIGIN,
                operation="status_show",
                line_state=LineState.PINNED_LINE.value,
                outcome="matched",
                detail=MATCHED_DETAIL,
            )
        ]

    monkeypatch.setattr("datasluice.connectors.catalog.ckan.drift.run_drift_checks", fake_run)
    out_path = tmp_path / "drift-demo.txt"
    exit_code = main(["--target", "demo.ckan.org", "--json-out", str(out_path)])
    assert exit_code == 0
    assert [target.origin for target in selected] == [DEMO_ORIGIN]
    stdout = capsys.readouterr().out
    decoded = json.loads(stdout.strip().splitlines()[0])
    assert set(decoded) == _ADVISORY_KEYS
    assert out_path.read_text(encoding="utf-8") == stdout


def test_real_client_construction_satisfies_the_checker_protocol_offline() -> None:
    settings = CKANClientSettings(base_url="https://127.0.0.1:8443", probe_policy="declared-baseline")
    client = create_sync_client(settings)
    try:
        assert callable(client.datasets.package_show)
        assert callable(client.action_discovery.status_show)
    finally:
        client.close()
