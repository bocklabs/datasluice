"""Concrete CKAN capability probe runners resolving every v2 optional tier from one shared status snapshot."""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from time import monotonic
from types import MappingProxyType

from datasluice.connectors.catalog.ckan.clients import declared_ckan_profile
from datasluice.connectors.catalog.ckan.mapping import parse_action_envelope
from datasluice.connectors.catalog.ckan.settings import normalize_origin
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    DeclaredCapabilityProfile,
    EvidenceProvenance,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.errors.catalog import CatalogError, ForbiddenError, UnauthenticatedError
from datasluice.runtime.clients import AsyncCatalogTransport
from datasluice.runtime.events import EventEmitter
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest, RuntimeResponse

DEFAULT_SNAPSHOT_TTL: float = 300.0

PLATFORM = CatalogPlatform.CKAN
_ACTION_PATH = "/api/3/action/"
_STATUS_OPERATION = "ckan/action-api-v3.discovery-help-and-status"
_ADVISORY_OUTCOME = "line_drift_advisory"

ADVISORY_METADATA_KEYS = frozenset({"observed_version_present", "line_state"})

_VERSION_PATTERN = re.compile(r"\s*(\d+)\.(\d+)\.(\d+)")
_AUTHORIZATION_TYPE = "authorization error"
_NOT_FOUND_TYPE = "not found error"
_UNKNOWN_TYPE = "unknown error"
_FORBIDDEN_MESSAGE_MARKERS = ("unauthorized to", "not authorized")


def _operation_id(value: str) -> OperationId:
    platform, _, tail = value.partition("/")
    service, dot, method = tail.partition(".")
    if not dot:
        return OperationId(platform=platform, service="native", method=tail)
    return OperationId(platform=platform, service=service, method=method)


DATASTORE_CRUD_OPERATION_ID = _operation_id("ckan/datastore-extension.query-and-record-crud")
SQL_SEARCH_OPERATION_ID = _operation_id("ckan/datastore-extension.sql-search")
COLLABORATORS_OPERATION_ID = _operation_id("ckan/action-api-v3.dataset-collaborators")
ACTIVITY_OPERATION_ID = _operation_id("ckan/action-api-v3.activity")
RESOURCE_VIEWS_OPERATION_ID = _operation_id("ckan/action-api-v3.resource-views")
EXTENSION_PROBES_OPERATION_ID = _operation_id("ckan/plugin-provided-action-and-extension-probes")

_GATE_CONFIG_OPERATION_IDS = frozenset({SQL_SEARCH_OPERATION_ID, COLLABORATORS_OPERATION_ID})

_VIEW_PLUGINS = frozenset({"image_view", "text_view", "webpage_view", "datatables_view"})
_PLUGIN_OPERATION_TABLE: Mapping[str, frozenset[OperationId]] = MappingProxyType(
    {
        "datastore": frozenset({DATASTORE_CRUD_OPERATION_ID}),
        "activity": frozenset({ACTIVITY_OPERATION_ID}),
        **{name: frozenset({RESOURCE_VIEWS_OPERATION_ID}) for name in sorted(_VIEW_PLUGINS)},
    }
)
_PLUGIN_MAPPED_OPERATION_IDS = frozenset().union(*_PLUGIN_OPERATION_TABLE.values())
_PARENT_PLUGIN_REQUIREMENTS: Mapping[OperationId, str] = MappingProxyType({SQL_SEARCH_OPERATION_ID: "datastore"})


class LineState(StrEnum):
    """Deployment version position relative to the pinned CKAN API line (D-08)."""

    PINNED_LINE = "pinned-line"
    IN_LINE_DRIFT = "in-line-drift"
    FOREIGN_LINE = "foreign-line"
    UNVERIFIED = "unverified"


def _pinned_version() -> tuple[int, int, int]:
    match = _VERSION_PATTERN.match(declared_ckan_profile().profile_version)
    if match is None:
        raise ValueError("The pinned CKAN profile version is not a comparable semantic version.")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def version_line_state(version_or_none: str | None) -> LineState:
    """Classify one observed deployment version against the pinned CKAN line.

    Args:
        version_or_none: The ``ckan_version`` reported by the deployment, when present.

    Returns:
        The bounded line state: exact pinned version, drift inside the pinned
        major.minor line, a foreign major.minor line, or unverified when the
        version is missing or malformed.
    """
    if version_or_none is None:
        return LineState.UNVERIFIED
    match = _VERSION_PATTERN.match(version_or_none)
    if match is None:
        return LineState.UNVERIFIED
    pinned = _pinned_version()
    observed = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    if observed[:2] != pinned[:2]:
        return LineState.FOREIGN_LINE
    return LineState.PINNED_LINE if observed == pinned else LineState.IN_LINE_DRIFT


def map_extensions_to_operations(extensions: Iterable[str]) -> frozenset[OperationId]:
    """Translate one deployment plugin list onto the v2 optional OperationIds it enables."""
    names = {name.strip() for name in extensions if isinstance(name, str)}
    enabled: set[OperationId] = set()
    for name in names:
        enabled |= _PLUGIN_OPERATION_TABLE.get(name, frozenset())
    return frozenset(enabled)


def _status_extensions(payload: object) -> tuple[str, ...] | None:
    if not isinstance(payload, Mapping):
        return None
    extensions = payload.get("extensions")
    if not isinstance(extensions, list | tuple):
        return None
    return tuple(entry for entry in extensions if isinstance(entry, str))


def _classify_error_payload(operation_id: OperationId, payload: Mapping[object, object]) -> ProbeResponseClass:
    kind = str(payload.get("__type", "")).strip().lower()
    message = payload.get("message")
    lowered = message.lower() if isinstance(message, str) else ""
    if kind == _NOT_FOUND_TYPE:
        if operation_id in _GATE_CONFIG_OPERATION_IDS:
            return ProbeResponseClass.DEPLOYMENT_DISABLED
        return ProbeResponseClass.UNAVAILABLE
    if kind == _AUTHORIZATION_TYPE:
        if any(part in lowered for part in _FORBIDDEN_MESSAGE_MARKERS):
            return ProbeResponseClass.FORBIDDEN
        return ProbeResponseClass.UNAUTHORIZED
    return ProbeResponseClass.UNAVAILABLE


def classify_probe_response(operation_id: OperationId, payload: object) -> ProbeResponseClass:
    """Classify one observed snapshot payload into the bounded probe response class.

    Args:
        operation_id: The probed v2 OperationId asking for classification.
        payload: Either a realized bare result value, the parsed ``status_show``
            mapping carrying ``extensions``, or a bounded envelope error marker
            mapping with ``__type`` (and optionally ``message``) keys.

    Returns:
        The family-scoped response class: config-gated ids resolve
        DEPLOYMENT_DISABLED from status-derived absence or a wire not-found
        envelope, absent-plugin families resolve UNSUPPORTED, authorization and
        forbidden envelopes resolve their own classes, and every realized
        payload proves SUCCESS.
    """
    if isinstance(payload, Mapping) and isinstance(payload.get("__type"), str):
        return _classify_error_payload(operation_id, payload)
    extensions = _status_extensions(payload)
    if operation_id in _GATE_CONFIG_OPERATION_IDS:
        if extensions is None:
            return ProbeResponseClass.SUCCESS
        parent = _PARENT_PLUGIN_REQUIREMENTS.get(operation_id)
        if parent is not None and parent not in set(extensions):
            return ProbeResponseClass.UNSUPPORTED
        return ProbeResponseClass.DEPLOYMENT_DISABLED
    if extensions is None:
        return ProbeResponseClass.SUCCESS
    if operation_id is EXTENSION_PROBES_OPERATION_ID:
        return ProbeResponseClass.SUCCESS if extensions else ProbeResponseClass.UNSUPPORTED
    enabled = map_extensions_to_operations(extensions)
    if operation_id in _PLUGIN_MAPPED_OPERATION_IDS and operation_id not in enabled:
        return ProbeResponseClass.UNSUPPORTED
    return ProbeResponseClass.SUCCESS


@dataclass(frozen=True, slots=True)
class _Snapshot:
    payload: object
    fetched_at: float
    line_state: LineState
    version_present: bool


def _failure_marker(exc: Exception) -> dict[str, object]:
    if isinstance(exc, ForbiddenError):
        return {"__type": "Authorization Error", "message": "not authorized"}
    if isinstance(exc, UnauthenticatedError):
        return {"__type": "Authorization Error"}
    return {"__type": "Unknown Error"}


def _decode_snapshot(response: RuntimeResponse, *, fetched_at: float) -> _Snapshot:
    if not 200 <= response.status_code < 300:
        marker = {
            401: {"__type": "Authorization Error"},
            403: {"__type": "Authorization Error", "message": "not authorized"},
        }.get(response.status_code, {"__type": _UNKNOWN_TYPE})
        return _Snapshot(payload=marker, fetched_at=fetched_at, line_state=LineState.UNVERIFIED, version_present=False)
    try:
        body = json.loads(response.body)
    except (TypeError, ValueError):
        body = None
    try:
        result = parse_action_envelope(body, operation=_STATUS_OPERATION, platform=PLATFORM)
    except CatalogError as exc:
        return _Snapshot(
            payload=_failure_marker(exc), fetched_at=fetched_at, line_state=LineState.UNVERIFIED, version_present=False
        )
    version = result.get("ckan_version") if isinstance(result, Mapping) else None
    version_text = version if isinstance(version, str) else None
    return _Snapshot(
        payload=result,
        fetched_at=fetched_at,
        line_state=version_line_state(version_text),
        version_present=version is not None,
    )


class StatusSnapshotCache:
    """Single-flight TTL cache owning exactly one status_show read per snapshot window."""

    def __init__(
        self,
        *,
        origin: str,
        reader: Callable[[RuntimeRequest], RuntimeResponse] | None = None,
        ttl_seconds: float = DEFAULT_SNAPSHOT_TTL,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if type(ttl_seconds) not in (int, float) or ttl_seconds != ttl_seconds or ttl_seconds < 0:
            raise ValueError("Snapshot TTL must be a finite non-negative number.")
        self._origin = normalize_origin(origin)
        self._reader = reader
        self._ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._snapshot: _Snapshot | None = None

    @property
    def status_url(self) -> str:
        """Return the single status_show endpoint backing every snapshot."""
        return f"{self._origin}{_ACTION_PATH}status_show"

    def snapshot(self) -> _Snapshot:
        """Return the fresh snapshot, refreshing through one synchronous read when stale."""
        if self._reader is None:
            raise TypeError("Synchronous snapshot refresh requires a transport reader.")
        with self._lock:
            current = self._snapshot
            if current is not None and self._clock() - current.fetched_at < self._ttl_seconds:
                return current
        snapshot = _decode_snapshot(self._reader(self._request()), fetched_at=self._clock())
        with self._lock:
            self._snapshot = snapshot
        return snapshot

    async def snapshot_async(self, send: Callable[[RuntimeRequest], Awaitable[RuntimeResponse]]) -> _Snapshot:
        """Return the fresh snapshot, refreshing through one asynchronous read when stale."""
        with self._lock:
            current = self._snapshot
            if current is not None and self._clock() - current.fetched_at < self._ttl_seconds:
                return current
        snapshot = _decode_snapshot(await send(self._request()), fetched_at=self._clock())
        with self._lock:
            self._snapshot = snapshot
        return snapshot

    def _request(self) -> RuntimeRequest:
        return RuntimeRequest(
            method="POST",
            url=self.status_url,
            headers={"Content-Type": "application/json"},
            body=b"{}",
        )


def maybe_emit_line_advisory(
    emitter: EventEmitter,
    *,
    operation_id: OperationId,
    line_state: LineState,
    observed_version_present: bool,
) -> bool:
    """Emit one bounded line-drift advisory for drift states; pinned lines never advise.

    Args:
        emitter: The caller-injected event emitter carrying the advisory.
        operation_id: The probe triggering the advisory, named on the envelope.
        line_state: The observed snapshot line state.
        observed_version_present: Whether the deployment reported any version.

    Returns:
        Whether an advisory envelope was emitted.
    """
    if line_state is LineState.PINNED_LINE:
        return False
    emitter.record(
        operation_id=str(operation_id),
        platform=PLATFORM.value,
        outcome=_ADVISORY_OUTCOME,
        metadata={"observed_version_present": observed_version_present, "line_state": line_state.value},
    )
    return True


class _EvidenceCore:
    """Shared classification core keeping sync and async runner behavior identical."""

    __slots__ = ("_clock", "_emitter", "_last_advised_at", "_last_state", "_ttl_seconds")

    def __init__(self, *, emitter: EventEmitter | None, clock: Callable[[], float], ttl_seconds: float) -> None:
        self._emitter = emitter
        self._clock = clock
        self._ttl_seconds = float(ttl_seconds)
        self._last_state: LineState | None = None
        self._last_advised_at = float("-inf")

    def evidence_from(self, snapshot: _Snapshot, operation_id: OperationId, deployment_url: str) -> ProbeEvidence:
        self._advise(operation_id, snapshot)
        provenance = (
            EvidenceProvenance.UNVERIFIED
            if snapshot.line_state in {LineState.FOREIGN_LINE, LineState.UNVERIFIED}
            else EvidenceProvenance.VERIFIED_LINE
        )
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url=deployment_url,
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=classify_probe_response(operation_id, snapshot.payload),
            provenance=provenance,
        )

    def _advise(self, operation_id: OperationId, snapshot: _Snapshot) -> None:
        if self._emitter is None:
            return
        state = snapshot.line_state
        previous = self._last_state
        self._last_state = state
        if state is LineState.PINNED_LINE:
            return
        now = self._clock()
        if state is previous and (now - self._last_advised_at) < self._ttl_seconds:
            return
        maybe_emit_line_advisory(
            self._emitter,
            operation_id=operation_id,
            line_state=state,
            observed_version_present=snapshot.version_present,
        )
        self._last_advised_at = now


class CKANProbeRunner:
    """Synchronous probe runner answering v2 optional tiers from one shared status_show read."""

    def __init__(
        self,
        transport: CatalogTransport,
        origin: str,
        profile: DeclaredCapabilityProfile,
        *,
        emitter: EventEmitter | None = None,
        clock: Callable[[], float] = monotonic,
        ttl_seconds: float = DEFAULT_SNAPSHOT_TTL,
    ) -> None:
        if not isinstance(profile, DeclaredCapabilityProfile):
            raise TypeError("CKAN probe runners require the declared CKAN capability profile.")
        self._profile = profile
        self._snapshots = StatusSnapshotCache(
            origin=origin, reader=transport.send, ttl_seconds=ttl_seconds, clock=clock
        )
        self._core = _EvidenceCore(emitter=emitter, clock=clock, ttl_seconds=ttl_seconds)

    @property
    def snapshots(self) -> StatusSnapshotCache:
        """Expose the runner-owned snapshot cache for caller introspection."""
        return self._snapshots

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        """Return bounded evidence for one operation from the shared status snapshot."""
        if not isinstance(operation_id, OperationId):
            raise TypeError("CKAN probe runners require an OperationId.")
        return self._core.evidence_from(self._snapshots.snapshot(), operation_id, self._snapshots.status_url)


class CKANAsyncProbeRunner:
    """Asynchronous probe runner answering v2 optional tiers from one shared status_show read."""

    def __init__(
        self,
        transport: AsyncCatalogTransport,
        origin: str,
        profile: DeclaredCapabilityProfile,
        *,
        emitter: EventEmitter | None = None,
        clock: Callable[[], float] = monotonic,
        ttl_seconds: float = DEFAULT_SNAPSHOT_TTL,
    ) -> None:
        if not isinstance(profile, DeclaredCapabilityProfile):
            raise TypeError("CKAN probe runners require the declared CKAN capability profile.")
        self._profile = profile
        self._transport = transport
        self._snapshots = StatusSnapshotCache(origin=origin, ttl_seconds=ttl_seconds, clock=clock)
        self._core = _EvidenceCore(emitter=emitter, clock=clock, ttl_seconds=ttl_seconds)

    @property
    def snapshots(self) -> StatusSnapshotCache:
        """Expose the runner-owned snapshot cache for caller introspection."""
        return self._snapshots

    async def probe(self, operation_id: OperationId) -> ProbeEvidence:
        """Return bounded evidence for one operation from the shared status snapshot."""
        if not isinstance(operation_id, OperationId):
            raise TypeError("CKAN probe runners require an OperationId.")
        snapshot = await self._snapshots.snapshot_async(self._transport.send)
        return self._core.evidence_from(snapshot, operation_id, self._snapshots.status_url)
