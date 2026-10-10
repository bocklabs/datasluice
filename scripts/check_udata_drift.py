"""Run the reviewed, bounded, advisory-only public uData drift check.

The reviewed record at ``.planning/phases/04-udata-connector/04-DRIFT-TARGETS.md`` approves
no public target, so the runtime allowlist is empty and the runner fails closed before any
transport dispatch. This entry point therefore exits ``3`` until a reviewed decision approves
two distinct deployments reporting the exact pinned release. It accepts no caller-supplied
target, route, or origin, and it writes only the redacted advisory report.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from datasluice.connectors.catalog.udata.drift import (
    APPROVED_TARGETS,
    DRIFT_SCHEMA_VERSION,
    MAX_PAGE_SIZE,
    MAX_READS_PER_RUN,
    MAX_TARGETS_PER_RUN,
    PINNED_UDATA_VERSION,
    READ_ALLOWLIST,
    DriftReport,
    UDataDriftUnavailableError,
    run_udata_drift,
)

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_DRIFTED = 2
EXIT_DISABLED = 3

ADVISORY_OUTCOMES = frozenset({"drifted", "incompatible", "outage"})


def build_parser() -> argparse.ArgumentParser:
    """Build the single bounded drift invocation accepted by the scheduled workflow."""
    parser = argparse.ArgumentParser(description=__doc__)
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
        "pinned_version": PINNED_UDATA_VERSION,
        "approved_targets": len(APPROVED_TARGETS),
        "max_targets_per_run": MAX_TARGETS_PER_RUN,
        "max_reads_per_run": MAX_READS_PER_RUN,
        "max_page_size": MAX_PAGE_SIZE,
        "read_allowlist": sorted(READ_ALLOWLIST),
        "detail": (
            "The reviewed uData public drift allowlist approves no target, so no public request was "
            "dispatched. Public drift monitoring stays disabled until two distinct deployments "
            "report the exact pinned release."
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
        incompatible, or met an outage, ``3`` while the reviewed allowlist approves
        no target, and ``1`` on an unexpected failure.
    """
    args = build_parser().parse_args()
    if not APPROVED_TARGETS:
        print(json.dumps(disabled_payload(), sort_keys=True))
        return EXIT_OK if args.allow_disabled else EXIT_DISABLED
    try:
        report = run_udata_drift()
    except UDataDriftUnavailableError:
        print(json.dumps(disabled_payload(), sort_keys=True))
        return EXIT_OK if args.allow_disabled else EXIT_DISABLED
    rendered = report.to_json()
    print(rendered)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered + "\n", encoding="utf-8")
    return report_exit_code(report)


if __name__ == "__main__":
    raise SystemExit(main())
