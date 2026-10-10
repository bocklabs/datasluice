"""Bounded, redacted, read-only public uData drift monitoring.

The reviewed record at ``.planning/phases/04-udata-connector/04-DRIFT-TARGETS.md`` currently
approves no public target, so ``APPROVED_TARGETS`` is empty and both runners fail closed
before any transport dispatch. Public drift monitoring stays disabled until two distinct
deployments report the exact pinned uData release.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Protocol

from datasluice.connectors.catalog.udata.clients import declared_udata_profile
from datasluice.connectors.catalog.udata.mapping import NATIVE_PAGE_FIELDS, PLATFORM
from datasluice.connectors.catalog.udata.probes import (
    SITE_OPERATION_ID,
    SITE_PATH,
    UDataVersionError,
    parse_site_version,
    require_exact_version,
)
from datasluice.connectors.catalog.udata.settings import normalize_origin
from datasluice.domain.catalog.operations import AuthClass, MutationClass, OperationId
from datasluice.domain.catalog.redaction import redact_mapping, redact_string
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import CatalogUnavailableError, CatalogValidationError
from datasluice.runtime.clients import AsyncCatalogTransport
from datasluice.runtime.defaults import create_default_async_transport, create_default_sync_transport
from datasluice.runtime.resilience import DeadlineMonitor
from datasluice.runtime.transport.base import RedirectPolicy, RuntimeRequest, RuntimeResponse

if TYPE_CHECKING:
    from datasluice.domain.catalog.observability import TLSPolicy

DRIFT_SCHEMA_VERSION: Final = "udata-drift/1"
PINNED_UDATA_VERSION: Final = "17.6.0"
DRIFT_OPERATION: Final = "udata/api-v1.public-drift-read"

MAX_PAGE_SIZE: Final = 1
MAX_READS_PER_TARGET: Final = 3
MAX_TARGETS_PER_RUN: Final = 2
MAX_READS_PER_RUN: Final = MAX_TARGETS_PER_RUN * MAX_READS_PER_TARGET
MAX_RESPONSE_BYTES: Final = 64 * 1024
MAX_FINGERPRINT_KEYS: Final = 32
MAX_FINGERPRINT_ITEMS: Final = 8
RESPONSE_TIMEOUT_SECONDS: Final = 10.0
CONNECT_TIMEOUT_SECONDS: Final = 5.0
DAILY_CADENCE_SECONDS: Final = 86400.0

READ_BUDGET = TimeBudget(
    connect=CONNECT_TIMEOUT_SECONDS,
    read=RESPONSE_TIMEOUT_SECONDS,
    write=RESPONSE_TIMEOUT_SECONDS,
    total=MAX_READS_PER_TARGET * RESPONSE_TIMEOUT_SECONDS,
)

OUTAGE = "outage"
INCOMPATIBLE = "incompatible"
DRIFTED = "drifted"
MATCHED = "matched"
SKIPPED = "skipped"

JSON_OBJECT = "json-object"
JSON_LIST = "json-list"
NON_JSON = "non-json"
HTTP_ERROR = "http-error"
UNAVAILABLE = "unavailable"

_SITE_READ_ID = SITE_OPERATION_ID
_DATASET_SEARCH_READ_ID = "udata/api-v2.search-datasets"
_TOPICS_READ_ID = "udata/api-v2.list-topics"

_ALLOWED_QUERY_KEYS: Final = frozenset({"page", "page_size"})
_PATH_PREFIXES: Final = ("/api/1/", "/api/2/")
_ALIAS_RE: Final = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")
_VERSION_SHAPE: Final = re.compile(r"\d+\.\d+\.\d+\Z")
_SITE_ENVELOPE_KEYS: Final = frozenset({"feed_size", "id", "keywords", "metrics", "title", "version"})
_PAGE_ENVELOPE_KEYS: Final = frozenset(NATIVE_PAGE_FIELDS)
_SEARCH_ENVELOPE_KEYS: Final = frozenset(NATIVE_PAGE_FIELDS) | {"facets"}

MATCHED_DETAIL: Final = "the observed envelope matched the pinned structural skeleton"
DRIFTED_DETAIL: Final = "the observed envelope key set differed from the pinned structural skeleton"
NON_JSON_DETAIL: Final = "the deployment returned a payload that is not the documented JSON envelope"
OUTAGE_DETAIL: Final = "the public read could not be completed against the reviewed target"
INCOMPATIBLE_DETAIL: Final = "the deployment did not prove the exact pinned uData release"
SKIPPED_DETAIL: Final = "the read was skipped after the target failed its availability or version gate"


class UDataDriftUnavailableError(CatalogUnavailableError):
    """No reviewed public target is approved, so the drift runner dispatches nothing."""

    def __init__(self, *, approved_targets: int) -> None:
        """Record the empty reviewed allowlist and the typed caller remedy."""
        super().__init__(
            "The reviewed uData public drift allowlist approves no target, so drift monitoring stays disabled.",
            operation=DRIFT_OPERATION,
            platform=PLATFORM.value,
            capability_state="unavailable",
            metadata={"approved_targets": approved_targets, "version_state": "unapproved-targets"},
            safe_action=(
                "Record a reviewed target decision in "
                ".planning/phases/04-udata-connector/04-DRIFT-TARGETS.md and populate APPROVED_TARGETS "
                "before enabling the scheduled drift run."
            ),
        )
        self.approved_targets = approved_targets


def _require_harmless_profile_read(operation_id: str) -> str | None:
    """Return the reason a declared profile operation is not a harmless public read.

    The pinned capability profile is the independent authority on read-only
    classification: an operation it does not declare, or declares as a mutation,
    administration, or credentialed call, is never dispatched to a public
    deployment even when a drift target names it.
    """
    platform, _, tail = operation_id.partition("/")
    service, dot, method = tail.partition(".")
    if platform != PLATFORM.value or not dot or not service or not method:
        return "Drift reads must name a declared uData profile operation identity."
    declared = declared_udata_profile().operations.get(OperationId(platform=platform, service=service, method=method))
    if declared is None:
        return f"The drift read {operation_id!r} is not declared by the pinned uData capability profile."
    if declared.mutation_class is not MutationClass.READ:
        return (
            f"The drift read {operation_id!r} is declared {declared.mutation_class.value} by the pinned "
            "capability profile; only read-class operations are dispatched to a public target."
        )
    if declared.auth_class is not AuthClass.PUBLIC:
        return (
            f"The drift read {operation_id!r} requires {declared.auth_class.value} authorization; "
            "public drift monitoring dispatches only anonymous reads."
        )
    return None


def _read_query_problem(query: tuple[tuple[str, str], ...]) -> str | None:
    if not isinstance(query, tuple):
        return "Drift read query parameters must be a tuple of name and value pairs."
    page_size: int | None = None
    if any(
        not isinstance(pair, tuple) or len(pair) != 2 or not all(isinstance(part, str) for part in pair)
        for pair in query
    ):
        return "Drift read query parameters must be pairs of strings."
    for name, value in query:
        if name not in _ALLOWED_QUERY_KEYS:
            return "Drift read query parameters may only bound page and page_size."
        if any(character.isspace() for character in value) or len(value) > 8:
            return "Drift read query values are bounded numeric strings."
        if name == "page_size" and value.isdigit():
            page_size = int(value)
        elif name == "page_size":
            return "Drift read page_size must be a positive integer string."
    if page_size is not None and page_size > MAX_PAGE_SIZE:
        return f"Drift read page_size cannot exceed {MAX_PAGE_SIZE}."
    return None


def _read_operation_problem(operation: ReadOperation) -> str | None:
    if not isinstance(operation.operation_id, str) or not operation.operation_id:
        return "Drift reads require a non-empty operation identity."
    if len(operation.operation_id) > 128:
        return "Drift read operation identities are bounded to 128 characters."
    if operation.method != "GET":
        return (
            f"The drift read {operation.operation_id!r} must be an anonymous GET; "
            "mutations, administration, token changes, and uploads are never dispatched to a public target."
        )
    if not isinstance(operation.path, str) or not operation.path.startswith(_PATH_PREFIXES):
        return "Drift read paths must be fixed uData v1 or v2 API paths."
    if "<" in operation.path or ">" in operation.path:
        return "Drift read paths cannot carry route placeholders."
    if len(operation.path) > 128 or any(character.isspace() for character in operation.path):
        return "Drift read paths are bounded paths without whitespace."
    query_problem = _read_query_problem(operation.query)
    if query_problem is not None:
        return query_problem
    if not isinstance(operation.expected_envelope_keys, frozenset):
        return "Drift read expectations require a frozenset of envelope key names."
    if not 1 <= len(operation.expected_envelope_keys) <= MAX_FINGERPRINT_KEYS:
        return f"Drift read expectations carry between 1 and {MAX_FINGERPRINT_KEYS} envelope key names."
    if not all(isinstance(key, str) and key for key in operation.expected_envelope_keys):
        return "Drift read envelope key names must be non-empty strings."
    return _require_harmless_profile_read(operation.operation_id)


@dataclass(frozen=True, slots=True)
class ReadOperation:
    """One structurally reviewed anonymous GET read permitted against a public target."""

    operation_id: str
    method: str
    path: str
    query: tuple[tuple[str, str], ...]
    expected_envelope_keys: frozenset[str]

    def __post_init__(self) -> None:
        problem = _read_operation_problem(self)
        if problem is not None:
            raise ValueError(problem)

    def assert_readable(self) -> None:
        """Re-validate the read at dispatch time before any transport call.

        Raises:
            CatalogValidationError: When the entry no longer describes an anonymous
                bounded GET read, which is how a deliberately inserted mutation row
                is refused before any public request is dispatched.
        """
        problem = _read_operation_problem(self)
        if problem is not None:
            raise CatalogValidationError(
                problem,
                operation=self.operation_id,
                platform=PLATFORM.value,
                capability_state="forbidden",
                safe_action="Configure only the reviewed read-only allowlist entries on a drift target.",
            )

    def build_request(self, origin: str) -> RuntimeRequest:
        """Build the bounded anonymous request for this read against one origin."""
        self.assert_readable()
        query = "&".join(f"{name}={value}" for name, value in self.query)
        return RuntimeRequest(
            method="GET",
            url=f"{origin}{self.path}" + (f"?{query}" if query else ""),
            headers={},
            body=None,
            redirect_policy=RedirectPolicy.NO_FOLLOW,
            max_response_bytes=MAX_RESPONSE_BYTES,
        )


@dataclass(frozen=True, slots=True)
class DriftTarget:
    """One reviewed public deployment and the bounded reads permitted against it."""

    alias: str
    origin: str
    reads: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.alias, str) or not _ALIAS_RE.fullmatch(self.alias):
            raise ValueError("Drift target aliases are bounded lowercase identifiers.")
        if not isinstance(self.reads, tuple) or not self.reads:
            raise CatalogValidationError(
                "Drift targets carry a non-empty tuple of approved read operation identities.",
                operation=DRIFT_OPERATION,
                platform=PLATFORM.value,
                safe_action="Approve at least the site version probe read for every drift target.",
            )
        if len(self.reads) > MAX_READS_PER_TARGET:
            raise CatalogValidationError(
                f"Drift targets cannot carry more than {MAX_READS_PER_TARGET} bounded reads.",
                operation=DRIFT_OPERATION,
                platform=PLATFORM.value,
                safe_action="Keep the reviewed small read set on each drift target.",
            )
        if len(set(self.reads)) != len(self.reads):
            raise CatalogValidationError(
                "Drift target reads cannot repeat an operation identity.",
                operation=DRIFT_OPERATION,
                platform=PLATFORM.value,
                safe_action="List each approved read at most once per drift target.",
            )
        unknown = [read for read in self.reads if read not in READ_ALLOWLIST]
        if unknown:
            raise CatalogValidationError(
                f"The drift read {unknown[0]!r} is not on the reviewed read-only allowlist.",
                operation=unknown[0],
                platform=PLATFORM.value,
                capability_state="forbidden",
                safe_action=(
                    "Approve only site, small dataset page, and small topics page reads; "
                    "mutations, administration, tokens, uploads, and deletes are never permitted."
                ),
            )
        if self.reads[0] != _SITE_READ_ID:
            raise CatalogValidationError(
                "Every drift target proves the exact pinned site version before any other read.",
                operation=DRIFT_OPERATION,
                platform=PLATFORM.value,
                safe_action="Place the site version probe read first on every drift target.",
            )
        if not isinstance(self.origin, str) or not self.origin:
            raise ValueError("Drift targets require a non-empty origin string.")
        normalized = normalize_origin(self.origin)
        if not normalized.startswith("https://"):
            raise CatalogValidationError(
                "Public drift targets must be sanitized HTTPS origins.",
                operation=DRIFT_OPERATION,
                platform=PLATFORM.value,
                safe_action="Review and record an HTTPS public deployment before approving a drift target.",
            )
        object.__setattr__(self, "origin", normalized)


_SITE_READ = ReadOperation(
    operation_id=_SITE_READ_ID,
    method="GET",
    path=SITE_PATH,
    query=(),
    expected_envelope_keys=_SITE_ENVELOPE_KEYS,
)

_DATASET_SEARCH_READ = ReadOperation(
    operation_id=_DATASET_SEARCH_READ_ID,
    method="GET",
    path="/api/2/datasets/search/",
    query=(("page", "1"), ("page_size", str(MAX_PAGE_SIZE))),
    expected_envelope_keys=_SEARCH_ENVELOPE_KEYS,
)

_TOPICS_READ = ReadOperation(
    operation_id=_TOPICS_READ_ID,
    method="GET",
    path="/api/2/topics/",
    query=(("page", "1"), ("page_size", str(MAX_PAGE_SIZE))),
    expected_envelope_keys=_PAGE_ENVELOPE_KEYS,
)

READ_ALLOWLIST: Mapping[str, ReadOperation] = MappingProxyType(
    {
        _SITE_READ.operation_id: _SITE_READ,
        _DATASET_SEARCH_READ.operation_id: _DATASET_SEARCH_READ,
        _TOPICS_READ.operation_id: _TOPICS_READ,
    }
)

APPROVED_TARGETS: tuple[DriftTarget, ...] = ()


class DriftTransport(Protocol):
    """The bounded synchronous read surface the drift runner consumes."""

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one request and return its fully buffered response."""

    def close(self) -> None:
        """Release transport resources."""


type AsyncDriftTransport = AsyncCatalogTransport


@dataclass(frozen=True, slots=True)
class DriftRecord:
    """One bounded redacted advisory row for a single read against one target."""

    schema_version: str
    target: str
    operation: str
    response_class: str
    outcome: str
    fingerprint: str
    envelope_keys: tuple[str, ...]
    item_count: int | None
    status_code: int | None
    detail: str
    metadata: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        """Return the JSON-safe advisory mapping for this record."""
        return {
            "schema_version": self.schema_version,
            "target": self.target,
            "operation": self.operation,
            "response_class": self.response_class,
            "outcome": self.outcome,
            "fingerprint": self.fingerprint,
            "envelope_keys": list(self.envelope_keys),
            "item_count": self.item_count,
            "status_code": self.status_code,
            "detail": self.detail,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class DriftReport:
    """The immutable advisory output of one drift run."""

    schema_version: str
    profile_version: str
    generated_at: str
    advisory_only: bool
    cadence_seconds: float
    approved_targets: int
    targets: tuple[str, ...]
    records: tuple[DriftRecord, ...]

    def to_dict(self) -> dict[str, object]:
        """Return the JSON-safe advisory mapping for the whole report."""
        return {
            "schema_version": self.schema_version,
            "profile_version": self.profile_version,
            "generated_at": self.generated_at,
            "advisory_only": self.advisory_only,
            "cadence_seconds": self.cadence_seconds,
            "approved_targets": self.approved_targets,
            "targets": list(self.targets),
            "records": [record.to_dict() for record in self.records],
        }

    def to_json(self) -> str:
        """Render the redacted report as one bounded JSON document."""
        return json.dumps(self.to_dict(), sort_keys=True, allow_nan=False)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _reject_nonfinite(value: str) -> float:
    raise ValueError(f"Non-finite JSON constant {value}.")


def _envelope_keys(payload: object) -> tuple[str, ...]:
    if not isinstance(payload, Mapping):
        return ()
    keys = sorted(str(key) for key in payload)
    return tuple(keys[:MAX_FINGERPRINT_KEYS])


def _fingerprint(
    *,
    operation_id: str,
    response_class: str,
    status_code: int | None,
    envelope_keys: tuple[str, ...],
    item_count: int | None,
    version_state: str,
) -> str:
    canonical = json.dumps(
        {
            "envelope_keys": list(envelope_keys),
            "item_count": item_count,
            "operation": operation_id,
            "response_class": response_class,
            "schema_version": DRIFT_SCHEMA_VERSION,
            "status_code": status_code,
            "version_state": version_state,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def _detail(detail: str) -> str:
    redacted = redact_string(detail)
    return redacted if isinstance(redacted, str) else str(redacted)


def _metadata(metadata: Mapping[str, object]) -> dict[str, object]:
    redacted = redact_mapping(metadata)
    return dict(redacted) if isinstance(redacted, Mapping) else {}


def _record(
    *,
    target: DriftTarget,
    operation_id: str,
    response_class: str,
    outcome: str,
    status_code: int | None = None,
    envelope_keys: tuple[str, ...] = (),
    item_count: int | None = None,
    version_state: str = "unverified",
    detail: str,
    metadata: Mapping[str, object] | None = None,
) -> DriftRecord:
    fingerprint = _fingerprint(
        operation_id=operation_id,
        response_class=response_class,
        status_code=status_code,
        envelope_keys=envelope_keys,
        item_count=item_count,
        version_state=version_state,
    )
    return DriftRecord(
        schema_version=DRIFT_SCHEMA_VERSION,
        target=target.alias,
        operation=operation_id,
        response_class=response_class,
        outcome=outcome,
        fingerprint=fingerprint,
        envelope_keys=envelope_keys,
        item_count=item_count,
        status_code=status_code,
        detail=_detail(detail),
        metadata=_metadata(dict(metadata) if metadata is not None else {}),
    )


def _error_metadata(error: BaseException) -> dict[str, object]:
    metadata: dict[str, object] = {"error_type": type(error).__name__}
    if isinstance(error, UDataVersionError):
        metadata["version_state"] = error.version_state
    return metadata


def _decode_payload(response: RuntimeResponse) -> tuple[object, str]:
    try:
        payload = json.loads(response.body, parse_constant=_reject_nonfinite)
    except (TypeError, ValueError):
        return None, NON_JSON
    if isinstance(payload, Mapping):
        return payload, JSON_OBJECT
    if isinstance(payload, list):
        return payload, JSON_LIST
    return payload, NON_JSON


def _observed_version(payload: object) -> str | None:
    if isinstance(payload, Mapping):
        version = payload.get("version")
        if isinstance(version, str) and _VERSION_SHAPE.fullmatch(version):
            return version
    return None


def _bounded_item_count(payload: object) -> int | None:
    if isinstance(payload, Mapping):
        data = payload.get("data")
        if isinstance(data, list):
            return min(len(data), MAX_FINGERPRINT_ITEMS)
    return None


def _structural_outcome(
    target: DriftTarget,
    operation: ReadOperation,
    response: RuntimeResponse,
    payload: object,
    response_class: str,
) -> DriftRecord:
    envelope_keys = _envelope_keys(payload)
    item_count = _bounded_item_count(payload)
    if response_class == NON_JSON or not isinstance(payload, Mapping):
        return _record(
            target=target,
            operation_id=operation.operation_id,
            response_class=response_class,
            outcome=DRIFTED,
            status_code=response.status_code,
            envelope_keys=envelope_keys,
            item_count=item_count,
            detail=NON_JSON_DETAIL,
        )
    expected = set(operation.expected_envelope_keys)
    observed = {str(key) for key in payload}
    if observed == expected:
        return _record(
            target=target,
            operation_id=operation.operation_id,
            response_class=response_class,
            outcome=MATCHED,
            status_code=response.status_code,
            envelope_keys=envelope_keys,
            item_count=item_count,
            detail=MATCHED_DETAIL,
            metadata={"envelope_key_count": len(envelope_keys), "item_count": item_count},
        )
    return _record(
        target=target,
        operation_id=operation.operation_id,
        response_class=response_class,
        outcome=DRIFTED,
        status_code=response.status_code,
        envelope_keys=envelope_keys,
        item_count=item_count,
        detail=DRIFTED_DETAIL,
        metadata={"envelope_key_count": len(envelope_keys), "item_count": item_count},
    )


def _version_record(
    target: DriftTarget,
    operation: ReadOperation,
    response: RuntimeResponse,
    payload: object,
    response_class: str,
) -> tuple[DriftRecord, str | None]:
    envelope_keys = _envelope_keys(payload)
    metadata: dict[str, object] = {
        "pinned_version": PINNED_UDATA_VERSION,
        "site_identity_present": isinstance(payload, Mapping) and bool(payload.get("id")),
        "envelope_key_count": len(envelope_keys),
    }
    try:
        observed = require_exact_version(parse_site_version(payload), PINNED_UDATA_VERSION)
    except UDataVersionError as error:
        metadata["version_state"] = error.version_state
        observed_version = _observed_version(payload)
        if observed_version is not None:
            metadata["observed_version"] = observed_version
        return (
            _record(
                target=target,
                operation_id=operation.operation_id,
                response_class=response_class,
                outcome=INCOMPATIBLE,
                status_code=response.status_code,
                envelope_keys=envelope_keys,
                version_state=error.version_state,
                detail=INCOMPATIBLE_DETAIL,
                metadata=metadata,
            ),
            INCOMPATIBLE,
        )
    metadata["version_state"] = "exact"
    metadata["observed_version"] = observed.version
    expected = set(operation.expected_envelope_keys)
    if isinstance(payload, Mapping) and {str(key) for key in payload} == expected:
        return (
            _record(
                target=target,
                operation_id=operation.operation_id,
                response_class=response_class,
                outcome=MATCHED,
                status_code=response.status_code,
                envelope_keys=envelope_keys,
                version_state="exact",
                detail=MATCHED_DETAIL,
                metadata=metadata,
            ),
            None,
        )
    return (
        _record(
            target=target,
            operation_id=operation.operation_id,
            response_class=response_class,
            outcome=DRIFTED,
            status_code=response.status_code,
            envelope_keys=envelope_keys,
            version_state="exact",
            detail=DRIFTED_DETAIL,
            metadata=metadata,
        ),
        None,
    )


def _transport_failure_record(target: DriftTarget, operation_id: str, error: BaseException) -> DriftRecord:
    return _record(
        target=target,
        operation_id=operation_id,
        response_class=UNAVAILABLE,
        outcome=OUTAGE,
        detail=OUTAGE_DETAIL,
        metadata=_error_metadata(error),
    )


def _http_error_record(target: DriftTarget, operation_id: str, response: RuntimeResponse) -> DriftRecord:
    return _record(
        target=target,
        operation_id=operation_id,
        response_class=HTTP_ERROR,
        outcome=OUTAGE,
        status_code=response.status_code,
        detail=OUTAGE_DETAIL,
    )


def _skipped_record(target: DriftTarget, operation_id: str, reason: str) -> DriftRecord:
    return _record(
        target=target,
        operation_id=operation_id,
        response_class=UNAVAILABLE,
        outcome=SKIPPED,
        detail=SKIPPED_DETAIL,
        metadata={"skipped_because": reason},
    )


def _resolved_targets(targets: Sequence[DriftTarget] | None) -> tuple[DriftTarget, ...]:
    resolved = APPROVED_TARGETS if targets is None else tuple(targets)
    if not resolved:
        raise UDataDriftUnavailableError(approved_targets=0)
    if len(resolved) > MAX_TARGETS_PER_RUN:
        raise CatalogValidationError(
            f"A single drift run cannot exceed {MAX_TARGETS_PER_RUN} reviewed targets.",
            operation=DRIFT_OPERATION,
            platform=PLATFORM.value,
            safe_action="Split larger drift runs across the daily cadence instead of one broad run.",
        )
    if sum(len(target.reads) for target in resolved) > MAX_READS_PER_RUN:
        raise CatalogValidationError(
            f"A single drift run cannot exceed {MAX_READS_PER_RUN} bounded reads.",
            operation=DRIFT_OPERATION,
            platform=PLATFORM.value,
            safe_action="Keep each reviewed target to the small approved read set.",
        )
    return resolved


def _require_allowlisted_read(operation_id: str) -> ReadOperation:
    operation = READ_ALLOWLIST.get(operation_id)
    if operation is None:
        raise CatalogValidationError(
            f"The drift read {operation_id!r} is not on the reviewed read-only allowlist.",
            operation=operation_id,
            platform=PLATFORM.value,
            capability_state="forbidden",
            safe_action="Configure only the reviewed read-only allowlist entries on a drift target.",
        )
    operation.assert_readable()
    return operation


def _classified_read(
    target: DriftTarget, operation: ReadOperation, response: RuntimeResponse
) -> tuple[DriftRecord, str | None]:
    if not 200 <= response.status_code < 300:
        return _http_error_record(target, operation.operation_id, response), OUTAGE
    payload, response_class = _decode_payload(response)
    if operation.operation_id == _SITE_READ_ID:
        return _version_record(target, operation, response, payload, response_class)
    return _structural_outcome(target, operation, response, payload, response_class), None


def _dispatch_read(
    target: DriftTarget,
    operation_id: str,
    transport: DriftTransport,
    deadline: DeadlineMonitor,
) -> tuple[DriftRecord, str | None]:
    operation = _require_allowlisted_read(operation_id)
    request = operation.build_request(target.origin)
    if request.method != "GET" or request.body is not None or request.files:
        raise CatalogValidationError(
            "Drift reads dispatch only anonymous bodyless GET requests.",
            operation=operation_id,
            platform=PLATFORM.value,
            safe_action="Keep the drift allowlist restricted to anonymous bodyless GET reads.",
        )
    try:
        deadline.assert_dispatchable(operation.operation_id, PLATFORM.value)
        response = transport.send(request)
    except Exception as error:
        return _transport_failure_record(target, operation_id, error), OUTAGE
    return _classified_read(target, operation, response)


def _run_target(target: DriftTarget, transport: DriftTransport) -> tuple[DriftRecord, ...]:
    deadline = DeadlineMonitor(READ_BUDGET)
    records: list[DriftRecord] = []
    blocker: str | None = None
    for operation_id in target.reads:
        if blocker is not None:
            records.append(_skipped_record(target, operation_id, blocker))
            continue
        record, blocker = _dispatch_read(target, operation_id, transport, deadline)
        records.append(record)
    return tuple(records)


async def _dispatch_read_async(
    target: DriftTarget,
    operation_id: str,
    transport: AsyncDriftTransport,
    deadline: DeadlineMonitor,
) -> tuple[DriftRecord, str | None]:
    operation = _require_allowlisted_read(operation_id)
    request = operation.build_request(target.origin)
    if request.method != "GET" or request.body is not None or request.files:
        raise CatalogValidationError(
            "Drift reads dispatch only anonymous bodyless GET requests.",
            operation=operation_id,
            platform=PLATFORM.value,
            safe_action="Keep the drift allowlist restricted to anonymous bodyless GET reads.",
        )
    try:
        deadline.assert_dispatchable(operation.operation_id, PLATFORM.value)
        response = await asyncio.wait_for(transport.send(request), timeout=deadline.remaining())
    except Exception as error:
        return _transport_failure_record(target, operation_id, error), OUTAGE
    return _classified_read(target, operation, response)


async def _run_target_async(target: DriftTarget, transport: AsyncDriftTransport) -> tuple[DriftRecord, ...]:
    deadline = DeadlineMonitor(READ_BUDGET)
    records: list[DriftRecord] = []
    blocker: str | None = None
    for operation_id in target.reads:
        if blocker is not None:
            records.append(_skipped_record(target, operation_id, blocker))
            continue
        record, blocker = await _dispatch_read_async(target, operation_id, transport, deadline)
        records.append(record)
    return tuple(records)


def _build_report(records: Sequence[DriftRecord], targets: Sequence[DriftTarget], *, now: datetime) -> DriftReport:
    return DriftReport(
        schema_version=DRIFT_SCHEMA_VERSION,
        profile_version=declared_udata_profile().profile_version,
        generated_at=now.astimezone(UTC).isoformat(),
        advisory_only=True,
        cadence_seconds=DAILY_CADENCE_SECONDS,
        approved_targets=len(APPROVED_TARGETS),
        targets=tuple(target.alias for target in targets),
        records=tuple(records),
    )


def run_udata_drift(
    targets: Sequence[DriftTarget] | None = None,
    *,
    transport: DriftTransport | None = None,
    tls_policy: TLSPolicy | None = None,
    now: Callable[[], datetime] = _utc_now,
) -> DriftReport:
    """Run the reviewed bounded public drift reads synchronously.

    Args:
        targets: The reviewed targets to exercise; defaults to ``APPROVED_TARGETS``.
        transport: An injected borrowed transport; when omitted the runner owns one.
        tls_policy: The TLS policy applied to a runner-owned transport.
        now: The injected wall clock used for the report timestamp.

    Returns:
        The immutable advisory report; no read ever writes the profile, oracle, or fixtures.

    Raises:
        UDataDriftUnavailableError: When the reviewed allowlist approves no target.
    """
    resolved = _resolved_targets(targets)
    if transport is not None:
        records = [record for target in resolved for record in _run_target(target, transport)]
        return _build_report(records, resolved, now=now())
    active = create_default_sync_transport(tls_policy=tls_policy, budget=READ_BUDGET)
    try:
        records = [record for target in resolved for record in _run_target(target, active)]
    finally:
        active.close()
    return _build_report(records, resolved, now=now())


async def run_udata_drift_async(
    targets: Sequence[DriftTarget] | None = None,
    *,
    transport: AsyncDriftTransport | None = None,
    tls_policy: TLSPolicy | None = None,
    now: Callable[[], datetime] = _utc_now,
) -> DriftReport:
    """Run the reviewed bounded public drift reads asynchronously.

    Args:
        targets: The reviewed targets to exercise; defaults to ``APPROVED_TARGETS``.
        transport: An injected borrowed transport; when omitted the runner owns one.
        tls_policy: The TLS policy applied to a runner-owned transport.
        now: The injected wall clock used for the report timestamp.

    Returns:
        The immutable advisory report; no read ever writes the profile, oracle, or fixtures.

    Raises:
        UDataDriftUnavailableError: When the reviewed allowlist approves no target.
    """
    resolved = _resolved_targets(targets)
    if transport is not None:
        records = [record for target in resolved for record in await _run_target_async(target, transport)]
        return _build_report(records, resolved, now=now())
    active = create_default_async_transport(tls_policy=tls_policy, budget=READ_BUDGET)
    try:
        records = [record for target in resolved for record in await _run_target_async(target, active)]
    finally:
        await active.aclose()
    return _build_report(records, resolved, now=now())
