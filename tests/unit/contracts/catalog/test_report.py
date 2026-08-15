"""Tests for versioned catalog compliance reports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal, cast

import pytest

from datasluice.contracts.catalog.report import CaseOutcome, ComplianceReport


def _outcome(
    *, mode: Literal["sync", "async"] = "sync", state: Literal["passed", "failed", "blocked"] = "passed"
) -> CaseOutcome:
    return CaseOutcome(
        operation_id="ckan/datasets/get",
        mode=mode,
        capability="available",
        state=state,
        tier="core",
        evidence={"fixture_fingerprint": "fixture-sha256"},
        platform_metadata={"platform": "ckan", "authorization": "Bearer secret"},
    )


def test_report_round_trips_complete_version_bound_redacted_evidence() -> None:
    """Reports preserve contract evidence while dropping unsafe metadata."""
    report = ComplianceReport(
        connector_id="datasluice/ckan",
        manifest_version="1.0",
        profile_version="2.11.5",
        fixture_fingerprint="fixture-sha256",
        contract_schema_version="1",
        generated_at="2026-08-15T00:00:00Z",
        outcomes=(_outcome(), _outcome(mode="async")),
        expected_case_ids=("ckan/datasets/get[core][async]", "ckan/datasets/get[core][sync]"),
        platform_metadata={"platform": "ckan", "environment": "fixture", "api_key": "secret"},
    )

    payload = report.to_dict()

    assert ComplianceReport.from_dict(payload) == report
    assert report.is_compliant
    assert report.gaps == ()
    assert report.coverage_by_mode == {"async": 1, "sync": 1}
    assert report.coverage_by_tier == {"core": 2}
    assert report.platform_metadata == {"platform": "ckan", "environment": "fixture"}
    assert report.outcomes[0].platform_metadata == {"platform": "ckan"}


def test_report_derives_noncompliance_and_explicit_gaps_from_runner_owned_outcomes() -> None:
    """Connector input cannot supply a compliance summary or hide missing evidence."""
    report = ComplianceReport(
        outcomes=(_outcome(state="failed"),),
        expected_case_ids=("ckan/datasets/get[core][async]", "ckan/datasets/get[core][sync]"),
    )

    assert not report.is_compliant
    assert report.gaps == (
        "ckan/datasets/get[core][async]: missing",
        "ckan/datasets/get[core][sync]: failed",
    )
    assert report.coverage_by_state == {"failed": 1}
    assert "compliant" not in report.to_dict()


def test_reports_are_immutable_bounded_and_redact_diagnostics_by_default(tmp_path: Path) -> None:
    """Unsafe diagnostics do not reach the serialized report unless explicitly supported."""
    outcome = _outcome()
    report = ComplianceReport(
        outcomes=(outcome,),
        warnings=("Authorization: Bearer credential-value",),
        expected_case_ids=("ckan/datasets/get[core][sync]",),
    )
    path = tmp_path / "report.json"

    report.write_json(path)

    assert "credential-value" not in path.read_text(encoding="utf-8")
    assert json.loads(path.read_text(encoding="utf-8")) == report.to_dict()
    with pytest.raises(TypeError):
        cast(dict[str, object], report.platform_metadata)["platform"] = "other"
    with pytest.raises(ValueError):
        CaseOutcome(
            operation_id="ckan/datasets/get",
            mode="sync",
            capability="available",
            state="passed",
            warnings=("x" * 257,),
        )


def test_report_rejects_unversioned_or_non_strict_payloads() -> None:
    """Report decoding requires a complete schema-versioned envelope."""
    with pytest.raises(ValueError):
        ComplianceReport.from_dict({"schema_version": 1})
