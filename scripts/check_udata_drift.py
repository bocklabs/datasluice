from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import NoReturn

from datasluice.connectors.catalog.udata.compatibility import udata_version_policy
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
    DriftReport,
    run_udata_drift,
)

EXIT_OK = 0
EXIT_DRIFTED = 2
EXIT_DISABLED = 3
EXIT_ERROR = 4

ADVISORY_OUTCOMES = frozenset({DRIFTED, INCOMPATIBLE, OUTAGE})


class _DriftParser(argparse.ArgumentParser):
    """Parser that keeps usage errors from colliding with the drifted exit status."""

    def exit(self, status: int = 0, message: str | None = None) -> NoReturn:  # noqa: A003
        super().exit(EXIT_ERROR if status else status, message)


def build_parser() -> argparse.ArgumentParser:
    """Build the single bounded drift invocation accepted by the scheduled workflow."""
    parser = _DriftParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="also write the redacted advisory report JSON to this path",
    )
    parser.add_argument(
        "--allow-disabled",
        action="store_true",
        help="exit 0 instead of 3 while the reviewed target allowlist is still empty",
    )
    return parser


def disabled_payload() -> dict[str, object]:
    """Return the redacted, dispatch-free state of an empty reviewed allowlist."""
    return {
        "schema_version": DRIFT_SCHEMA_VERSION,
        "status": "disabled",
        "supported_range": udata_version_policy().supported_range,
        "approved_targets": len(APPROVED_TARGETS),
        "max_targets_per_run": MAX_TARGETS_PER_RUN,
        "max_reads_per_run": MAX_READS_PER_RUN,
        "max_page_size": MAX_PAGE_SIZE,
        "read_allowlist": sorted(READ_ALLOWLIST),
        "detail": (
            "The reviewed uData public drift allowlist approves no target, so no public request was "
            "dispatched. Public drift monitoring stays disabled until two distinct deployments "
            "report a supported release."
        ),
    }


def report_exit_code(report: DriftReport) -> int:
    """Classify one completed advisory report as clean, drifted, or disabled."""
    if any(record.outcome in ADVISORY_OUTCOMES for record in report.records):
        return EXIT_DRIFTED
    return EXIT_OK


def main() -> int:
    """Emit one bounded advisory report and classify the run for the schedule.

    Returns:
        ``0`` when every reviewed read matched, ``2`` when any read drifted, was
        incompatible, or met an outage, ``3`` while the reviewed allowlist
        approves no target, and ``4`` when the invocation itself was malformed.
        An unexpected failure exits ``1`` via the interpreter's unhandled exception.
    """
    args = build_parser().parse_args()

    def emit(payload: str) -> None:
        print(payload)
        if args.out is not None:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(payload + "\n", encoding="utf-8")

    if not APPROVED_TARGETS:
        emit(json.dumps(disabled_payload(), sort_keys=True))
        return EXIT_OK if args.allow_disabled else EXIT_DISABLED
    report = run_udata_drift()
    emit(report.to_json())
    return report_exit_code(report)


if __name__ == "__main__":
    raise SystemExit(main())
