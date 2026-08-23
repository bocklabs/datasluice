"""Runnable CKAN drift-read checker producing advisory-shaped output (D-21).

The checker executes per-target bounded typed reads against representative
public deployments and classifies each read as ``matched``, ``drifted``, or
``unavailable``. ``AdvisoryRecord`` is a separate redacted schema, not the
RUN-05 event envelope: it carries exactly five operator-facing keys and never
serializes server-controlled payload content. Version position propagates from
the Plan 05 ``version_line_state`` helper through every record of a target;
foreign-line and unverified targets complete their run without blocking (D-08).
Scheduling lives in Phase 8 (QUAL-01); this module ships only the runnable
single-shot proof.

>>> python -m datasluice.connectors.catalog.ckan.drift --help
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Protocol

from datasluice.connectors.catalog.ckan.clients import create_sync_client
from datasluice.connectors.catalog.ckan.probes import version_line_state
from datasluice.connectors.catalog.ckan.settings import CKANClientSettings
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ResultEnvelope, ValueRecord
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.redaction import redact_for_output

type Ordering = Literal["platform-deterministic", "canonicalized"]

OUTCOME_MATCHED = "matched"
OUTCOME_DRIFTED = "drifted"
OUTCOME_UNAVAILABLE = "unavailable"

MATCHED_DETAIL = "matched the expected structural skeleton"
DRIFTED_DETAIL = "structural mismatch against the expected key set"

_TYPED_READ_ACTIONS = frozenset({"package_list", "package_show", "current_package_list_with_resources", "status_show"})

_ORDERINGS = frozenset({"platform-deterministic", "canonicalized"})


class _DriftDiscovery(Protocol):
    """The discovery surface the checker consumes from any constructed client."""

    def status_show(self) -> ResultEnvelope[CKANResultItem]:
        """Return the deployment status mapping."""
        ...


class _DriftDatasets(Protocol):
    """The dataset read surface the checker consumes from any constructed client."""

    def package_list(self) -> ResultEnvelope[CKANResultItem]:
        """List dataset names."""
        ...

    def package_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one dataset by id or name."""
        ...

    def current_package_list_with_resources(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List datasets with resources under native paging bounds."""
        ...


class _DriftClient(Protocol):
    """The structural client surface the checker consumes before closing."""

    @property
    def datasets(self) -> _DriftDatasets:
        """Return the dataset read projection."""

    @property
    def action_discovery(self) -> _DriftDiscovery:
        """Return the action-discovery projection."""

    def close(self) -> None:
        """Release the client and its owned transport exactly once."""


type DriftClientFactory = Callable[[CKANClientSettings], _DriftClient]

_STATUS_EXPECTED_KEYS = frozenset(
    {"ckan_version", "error_emails_to", "extensions", "locale_default", "site_description", "site_title", "site_url"}
)

_DEMO_DATASET_EXPECTED_KEYS = frozenset(
    {
        "author",
        "author_email",
        "creator_user_id",
        "extras",
        "groups",
        "id",
        "isopen",
        "license_id",
        "license_title",
        "maintainer",
        "maintainer_email",
        "metadata_created",
        "metadata_modified",
        "name",
        "notes",
        "num_resources",
        "num_tags",
        "organization",
        "owner_org",
        "private",
        "relationships_as_object",
        "relationships_as_subject",
        "resources",
        "state",
        "tags",
        "title",
        "type",
        "url",
        "version",
    }
)

_DGU_DATASET_EXPECTED_KEYS = frozenset(
    {
        "creator_user_id",
        "extras",
        "groups",
        "harvest",
        "id",
        "isopen",
        "license_id",
        "license_title",
        "metadata_created",
        "metadata_modified",
        "name",
        "notes",
        "num_resources",
        "num_tags",
        "organization",
        "owner_org",
        "private",
        "relationships_as_object",
        "relationships_as_subject",
        "resources",
        "state",
        "tags",
        "title",
        "type",
        "url",
        "version",
    }
)

_STATUS_RATIONALE = (
    "one anonymous status_show read; canonicalized exact-set comparison over the pinned-line "
    "skeleton whose ckan_version key drives line_state propagation"
)
_DEMO_SHOW_RATIONALE = (
    "bounded single-record package_show of a pinned sample dataset; stable default-schema key set "
    "snapshot 2026-08-23 on the verified 2.11.5 primary"
)
_DGU_SHOW_RATIONALE = (
    "bounded single-record package_show of a pinned harvest-source dataset; observed key set snapshot "
    "2026-08-23 including the ckanext-harvest key on the secondary"
)


@dataclass(frozen=True, slots=True)
class DriftCheck:
    """One per-target structural expectation executed as a typed read."""

    action: str
    parameters: Mapping[str, object]
    ordering: Ordering
    rationale: str
    expected_keys: frozenset[str]

    def __post_init__(self) -> None:
        if self.action not in _TYPED_READ_ACTIONS:
            raise ValueError(
                f"The drift check action {self.action!r} is not a whitelisted typed read; "
                "generic action invocation stays unavailable (D-22)."
            )
        if not isinstance(self.parameters, Mapping):
            raise ValueError("Drift check parameters must be a mapping.")
        if self.ordering not in _ORDERINGS:
            raise ValueError(f"The drift ordering {self.ordering!r} must be a documented ordering mode.")
        if not isinstance(self.rationale, str) or not self.rationale:
            raise ValueError("Every drift check documents its selection rationale.")
        if not isinstance(self.expected_keys, frozenset) or not all(isinstance(key, str) for key in self.expected_keys):
            raise ValueError("Drift expectations require a frozenset of string keys.")
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))


@dataclass(frozen=True, slots=True)
class DriftTarget:
    """One deployment origin carrying its own checks and parameters."""

    origin: str
    checks: tuple[DriftCheck, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.origin, str) or not self.origin.startswith("https://"):
            raise ValueError("Drift targets are sanitized HTTPS origins.")
        if not isinstance(self.checks, tuple) or not all(isinstance(check, DriftCheck) for check in self.checks):
            raise ValueError("Drift targets carry a tuple of DriftCheck records.")


@dataclass(frozen=True, slots=True)
class AdvisoryRecord:
    """One redacted five-key advisory row; deliberately not the RUN-05 event envelope."""

    target: str
    operation: str
    line_state: str
    outcome: str
    detail: str

    def to_dict(self) -> dict[str, str]:
        """Return the exact five-key JSON-safe advisory mapping."""
        return {
            "target": self.target,
            "operation": self.operation,
            "line_state": self.line_state,
            "outcome": self.outcome,
            "detail": self.detail,
        }


def canonical_compare(observed: object, expected_keys: frozenset[str], ordering: Ordering) -> bool:
    """Compare one observed payload against the expected structural skeleton.

    Args:
        observed: A payload mapping, a sequence of names, or a sequence of
            record mappings reduced from one typed read.
        expected_keys: The configured structural skeleton; time-varying keys
            are excluded here by configuration, never filtered ad hoc.
        ordering: ``platform-deterministic`` compares sequences positionally
            against the sorted expectation; ``canonicalized`` sorts both sides
            before comparing. Mappings always compare as exact key sets.

    Returns:
        Whether the observation matches the skeleton under the ordering mode.
    """
    if isinstance(observed, Mapping):
        return {str(key) for key in observed} == set(expected_keys)
    if isinstance(observed, Sequence) and not isinstance(observed, str | bytes | bytearray):
        if observed and all(isinstance(item, Mapping) for item in observed):
            return all(canonical_compare(item, expected_keys, ordering) for item in observed)
        names = [str(item) for item in observed]
        expected = sorted(expected_keys)
        if ordering == "platform-deterministic":
            return names == expected
        return sorted(names) == expected
    return False


def _invoke_typed_read(client: _DriftClient, check: DriftCheck) -> ResultEnvelope[CKANResultItem]:
    """Dispatch one whitelisted check through its owning typed method."""
    parameters = check.parameters
    if check.action == "status_show":
        envelope = client.action_discovery.status_show()
    elif check.action == "package_list":
        envelope = client.datasets.package_list()
    elif check.action == "package_show":
        envelope = client.datasets.package_show(id=str(parameters["id"]))
    elif check.action == "current_package_list_with_resources":
        limit = parameters.get("limit")
        offset = parameters.get("offset")
        envelope = client.datasets.current_package_list_with_resources(
            limit=limit if isinstance(limit, int) else None,
            offset=offset if isinstance(offset, int) else None,
        )
    else:
        raise CatalogValidationError(
            f"The drift action {check.action!r} has no typed read dispatch.",
            operation=f"ckan/{check.action}",
            platform="ckan",
            safe_action="Configure only whitelisted typed reads on the drift check.",
        )
    return envelope


def _observed_from(action: str, envelope: ResultEnvelope[CKANResultItem]) -> object:
    """Reduce one typed result envelope into the comparable observation."""
    if action == "package_list":
        values = []
        for item in envelope.items:
            if not isinstance(item, ValueRecord):
                raise CatalogValidationError(
                    "The package_list drift read decoded a non-value record.",
                    operation="ckan/package_list",
                    platform="ckan",
                    safe_action="Keep drift expectations aligned with the typed result shapes.",
                )
            values.append(item.value)
        return tuple(values)
    payloads = []
    for item in envelope.items:
        if not isinstance(item, MappingRecord | NativeRecord):
            raise CatalogValidationError(
                f"The {action} drift read decoded a non-mapping record.",
                operation=f"ckan/{action}",
                platform="ckan",
                safe_action="Keep drift expectations aligned with the typed result shapes.",
            )
        payloads.append(item.payload)
    if action in {"status_show", "package_show"}:
        return payloads[0] if payloads else None
    return tuple(payloads)


def _unavailable_detail(exc: Exception) -> str:
    """Bound an endpoint failure into a content-free advisory detail."""
    return f"endpoint unavailable: {type(exc).__name__}"


def _run_target(target: DriftTarget, client_factory: DriftClientFactory) -> list[AdvisoryRecord]:
    """Run one target's checks and classify them under the propagated line state."""
    client = client_factory(CKANClientSettings(base_url=target.origin))
    try:
        observations: list[tuple[DriftCheck, object]] = []
        for check in target.checks:
            try:
                observations.append((check, _observed_from(check.action, _invoke_typed_read(client, check))))
            except Exception as exc:
                observations.append((check, exc))
        version = None
        for check, observed in observations:
            if check.action == "status_show" and isinstance(observed, Mapping):
                candidate = observed.get("ckan_version")
                if isinstance(candidate, str):
                    version = candidate
        line_state = version_line_state(version)
        records: list[AdvisoryRecord] = []
        for check, observed in observations:
            if isinstance(observed, Exception):
                outcome = OUTCOME_UNAVAILABLE
                detail = _unavailable_detail(observed)
            elif canonical_compare(observed, check.expected_keys, check.ordering):
                outcome = OUTCOME_MATCHED
                detail = MATCHED_DETAIL
            else:
                outcome = OUTCOME_DRIFTED
                detail = DRIFTED_DETAIL
            redacted = redact_for_output(detail)
            records.append(
                AdvisoryRecord(
                    target=target.origin,
                    operation=check.action,
                    line_state=line_state.value,
                    outcome=outcome,
                    detail=redacted if isinstance(redacted, str) else str(redacted),
                )
            )
        return records
    finally:
        client.close()


def run_drift_checks(
    targets: Sequence[DriftTarget], *, client_factory: DriftClientFactory = create_sync_client
) -> list[AdvisoryRecord]:
    """Run every check of every target sequentially and collect advisory records.

    Args:
        targets: The per-target check configurations to exercise.
        client_factory: The injected client constructor; defaults to the
            published ``create_sync_client`` so the D-15 rate policy attaches
            automatically to every constructed client.

    Returns:
        One advisory record per configured check, in configuration order.
    """
    records: list[AdvisoryRecord] = []
    for target in targets:
        records.extend(_run_target(target, client_factory))
    return records


def serialize_records(records: Sequence[AdvisoryRecord]) -> str:
    """Render advisory records as redacted JSON lines."""
    return "".join(json.dumps(record.to_dict(), sort_keys=False) + "\n" for record in records)


DEFAULT_TARGETS: tuple[DriftTarget, ...] = (
    DriftTarget(
        origin="https://demo.ckan.org",
        checks=(
            DriftCheck(
                action="status_show",
                parameters={},
                ordering="canonicalized",
                rationale=_STATUS_RATIONALE,
                expected_keys=_STATUS_EXPECTED_KEYS,
            ),
            DriftCheck(
                action="package_show",
                parameters={"id": "my-sample-dataset-001"},
                ordering="canonicalized",
                rationale=_DEMO_SHOW_RATIONALE,
                expected_keys=_DEMO_DATASET_EXPECTED_KEYS,
            ),
        ),
    ),
    DriftTarget(
        origin="https://ckan.publishing.service.gov.uk",
        checks=(
            DriftCheck(
                action="status_show",
                parameters={},
                ordering="canonicalized",
                rationale=_STATUS_RATIONALE,
                expected_keys=_STATUS_EXPECTED_KEYS,
            ),
            DriftCheck(
                action="package_show",
                parameters={"id": "0-1-annual-probability-extents14"},
                ordering="canonicalized",
                rationale=_DGU_SHOW_RATIONALE,
                expected_keys=_DGU_DATASET_EXPECTED_KEYS,
            ),
        ),
    ),
)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the default drift targets and print one JSON advisory line per check."""
    parser = argparse.ArgumentParser(
        prog="python -m datasluice.connectors.catalog.ckan.drift",
        description="Single-shot CKAN drift-read checker over the recorded representative targets.",
    )
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        metavar="SUBSTRING",
        help="restrict the run to default targets whose origin contains SUBSTRING (repeatable)",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="also write the JSON-lines advisory output to this path",
    )
    arguments = parser.parse_args(argv)
    if arguments.target:
        targets = tuple(target for target in DEFAULT_TARGETS if any(seed in target.origin for seed in arguments.target))
    else:
        targets = DEFAULT_TARGETS
    records = run_drift_checks(targets)
    rendered = serialize_records(records)
    print(rendered, end="")
    if arguments.json_out is not None:
        arguments.json_out.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
