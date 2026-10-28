from __future__ import annotations

import asyncio
import contextlib
import io
import json
import sys

import pytest

from datasluice.connectors.catalog.udata.drift import (
    APPROVED_TARGETS,
    DRIFT_SCHEMA_VERSION,
    DRIFTED,
    INCOMPATIBLE,
    MAX_PAGE_SIZE,
    MAX_READS_PER_RUN,
    MAX_TARGETS_PER_RUN,
    OUTAGE,
    READ_ALLOWLIST,
    UDataDriftUnavailableError,
    run_udata_drift,
    run_udata_drift_async,
)
from scripts.check_udata_drift import (
    ADVISORY_OUTCOMES,
    EXIT_DISABLED,
    EXIT_DRIFTED,
    EXIT_ERROR,
    EXIT_OK,
    main,
)

_DISABLED_REPORT_KEYS = {
    "schema_version",
    "status",
    "supported_range",
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
    assert report["supported_range"] == ">=17.0.0,<18.0.0"


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


def test_t02_advisory_outcomes_are_the_drift_module_constants_not_literals() -> None:
    assert frozenset({DRIFTED, INCOMPATIBLE, OUTAGE}) == ADVISORY_OUTCOMES


def test_t01_a_malformed_invocation_does_not_masquerade_as_drift() -> None:
    with pytest.raises(SystemExit) as raised:
        _run_entry_point("--no-such-flag")

    assert raised.value.code == EXIT_ERROR
    assert EXIT_ERROR != EXIT_DRIFTED


def test_t01_the_disabled_run_returns_no_report_because_nothing_was_dispatched() -> None:
    code, report = _run_entry_point()

    assert code == EXIT_DISABLED
    assert "records" not in report


def test_t01_both_runners_refuse_before_transport() -> None:
    with pytest.raises(UDataDriftUnavailableError):
        run_udata_drift()

    operation = run_udata_drift_async()
    with pytest.raises(UDataDriftUnavailableError):
        asyncio.run(operation)


def test_t04_every_allowlisted_read_is_an_anonymous_get() -> None:
    for operation_id, read in READ_ALLOWLIST.items():
        assert read.operation_id == operation_id
        assert read.method == "GET"
        if read.query:
            assert dict(read.query) == {"page": "1", "page_size": "1"}


def test_t04_no_public_origin_is_reachable_while_the_allowlist_is_empty() -> None:
    assert {target.origin for target in APPROVED_TARGETS} == set()
