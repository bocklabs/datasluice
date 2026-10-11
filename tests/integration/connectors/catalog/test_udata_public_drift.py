"""End-to-end proof that public uData drift fails closed while the reviewed allowlist is empty."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import sys
from pathlib import Path

import pytest

from datasluice.connectors.catalog.udata.drift import (
    APPROVED_TARGETS,
    DRIFT_SCHEMA_VERSION,
    MAX_PAGE_SIZE,
    MAX_READS_PER_RUN,
    MAX_TARGETS_PER_RUN,
    PINNED_UDATA_VERSION,
    READ_ALLOWLIST,
    UDataDriftUnavailableError,
    run_udata_drift,
    run_udata_drift_async,
)
from scripts.check_udata_drift import main

_ROOT = Path(__file__).parents[4]
_RECORD = _ROOT / ".planning/phases/04-udata-connector/04-DRIFT-TARGETS.md"
_WORKFLOW = _ROOT / ".github/workflows/catalog-drift.yml"

EXIT_OK = 0
EXIT_DISABLED = 3

_DISABLED_REPORT_KEYS = {
    "schema_version",
    "status",
    "pinned_version",
    "approved_targets",
    "max_targets_per_run",
    "max_reads_per_run",
    "max_page_size",
    "read_allowlist",
    "detail",
}


def _run_entry_point(*args: str) -> tuple[int, dict[str, object]]:
    """Run the shipped drift entry point with argv replaced and return its exit code and report."""
    buffer = io.StringIO()
    original = sys.argv
    sys.argv = ["check_udata_drift.py", *args]
    try:
        with contextlib.redirect_stdout(buffer):
            code = main()
    finally:
        sys.argv = original
    lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
    return code, json.loads(lines[-1])


def test_t01_the_reviewed_allowlist_is_empty() -> None:
    assert APPROVED_TARGETS == ()


def test_t01_the_entry_point_exits_disabled_and_names_no_target() -> None:
    code, report = _run_entry_point()

    assert code == EXIT_DISABLED
    assert report["status"] == "disabled"
    assert report["approved_targets"] == 0
    assert report["schema_version"] == DRIFT_SCHEMA_VERSION
    assert report["pinned_version"] == PINNED_UDATA_VERSION == "17.6.0"


def test_t02_the_disabled_report_publishes_only_bounds_and_no_content() -> None:
    _, report = _run_entry_point()

    assert set(report) == _DISABLED_REPORT_KEYS
    assert report["max_targets_per_run"] == MAX_TARGETS_PER_RUN
    assert report["max_reads_per_run"] == MAX_READS_PER_RUN
    assert report["max_page_size"] == MAX_PAGE_SIZE == 1
    assert report["read_allowlist"] == sorted(READ_ALLOWLIST)


def test_t01_allow_disabled_exits_zero_with_the_same_payload() -> None:
    disabled_code, disabled = _run_entry_point()
    allowed_code, allowed = _run_entry_point("--allow-disabled")

    assert disabled_code == EXIT_DISABLED
    assert allowed_code == EXIT_OK
    assert allowed == disabled


def test_t01_the_disabled_run_returns_no_report_because_nothing_was_dispatched() -> None:
    code, report = _run_entry_point()

    assert code == EXIT_DISABLED
    assert "records" not in report


def test_t01_both_runners_refuse_before_transport() -> None:
    with pytest.raises(UDataDriftUnavailableError):
        run_udata_drift()

    with pytest.raises(UDataDriftUnavailableError):
        asyncio.run(run_udata_drift_async())


def test_t04_every_allowlisted_read_is_an_anonymous_get() -> None:
    for operation_id, read in READ_ALLOWLIST.items():
        assert read.operation_id == operation_id
        assert read.method == "GET"
        if read.query:
            assert dict(read.query) == {"page": "1", "page_size": "1"}


def test_t04_no_public_origin_is_reachable_while_the_allowlist_is_empty() -> None:
    assert {target.origin for target in APPROVED_TARGETS} == set()


def test_t03_the_record_keeps_the_allowlist_empty_and_records_the_reprobe() -> None:
    text = _RECORD.read_text(encoding="utf-8")

    assert "**Empty.**" in text
    assert "17.12.1" in text


def test_t01_the_schedule_is_withheld_while_the_allowlist_is_empty() -> None:
    text = _WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in text
    assert "schedule:" not in text
    assert "cron:" not in text


def test_t01_the_workflow_runs_the_shipped_entry_point() -> None:
    text = _WORKFLOW.read_text(encoding="utf-8")

    assert "scripts/check_udata_drift.py" in text


def test_t01_a_restored_schedule_without_a_new_record_is_refused() -> None:
    text = _WORKFLOW.read_text(encoding="utf-8")

    assert r"grep -q '^\*\*Empty\.\*\*'" in text
