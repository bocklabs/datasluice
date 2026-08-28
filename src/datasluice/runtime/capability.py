"""Lazy per-operation capability probing and effective-state caching."""

from __future__ import annotations

import asyncio
import inspect
import math
import threading
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Protocol, cast, runtime_checkable
from urllib.parse import urlsplit

from datasluice.contracts.catalog.protocols import CatalogOperationGuard
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    DeclaredCapabilityProfile,
    EffectiveCapabilityProfile,
    EffectiveCapabilityState,
    EffectiveOperationCapability,
    ProbeEvidence,
    ProbeResponseClass,
)
from datasluice.errors.catalog import (
    CatalogError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    UnauthenticatedError,
    UnsupportedCapabilityError,
)
from datasluice.runtime.constants import DEFAULT_CAPABILITY_CACHE_TTL_SECONDS


@runtime_checkable
class ProbeRunner(Protocol):
    """Synchronous seam for probing one declared operation."""

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        """Return bounded evidence for one operation."""


@runtime_checkable
class AsyncProbeRunner(Protocol):
    """Asynchronous seam for probing one declared operation."""

    async def probe(self, operation_id: OperationId) -> ProbeEvidence:
        """Return bounded evidence for one operation."""


@dataclass(slots=True)
class _CacheEntry:
    profile: EffectiveCapabilityProfile
    timestamp: float


@dataclass(slots=True)
class _SyncFlight:
    event: threading.Event
    profile: EffectiveCapabilityProfile | None = None
    error: BaseException | None = None


class EffectiveCapabilityCache:
    """Keep effective capability profiles in an in-memory per-client cache."""

    def __init__(
        self,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        probe_runner: ProbeRunner | None = None,
        *,
        async_probe_runner: AsyncProbeRunner | None = None,
        ttl_seconds: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if type(ttl_seconds) not in (int, float) or not math.isfinite(ttl_seconds) or ttl_seconds < 0:
            raise ValueError("Capability cache TTL must be a finite non-negative number.")
        self._declared_profile = (
            profile.declared_profile if isinstance(profile, EffectiveCapabilityProfile) else profile
        )
        self._baseline = _baseline_profile(profile)
        self._probe_runner = probe_runner
        self._async_probe_runner = async_probe_runner
        self._ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: dict[OperationId, _CacheEntry] = {}
        self._sync_flights: dict[OperationId, _SyncFlight] = {}
        self._async_flights: dict[OperationId, asyncio.Future[EffectiveCapabilityProfile]] = {}

    @property
    def baseline_profile(self) -> EffectiveCapabilityProfile:
        """Return the immutable declared baseline used before effective probing."""
        return self._baseline

    @property
    def probe_runner(self) -> ProbeRunner | None:
        """Return the caller-owned synchronous probe runner, or ``None``."""
        return self._probe_runner

    def resolve(self, operation_id: OperationId) -> EffectiveCapabilityProfile:
        """Resolve one operation, probing it once per fresh cache interval."""
        self._validate_operation_id(operation_id)
        with self._lock:
            cached = self._fresh_entry(operation_id)
            if cached is not None:
                return cached.profile
        if operation_id not in self._declared_profile.operations or self._probe_runner is None:
            return self._baseline

        with self._lock:
            flight = self._sync_flights.get(operation_id)
            leader = flight is None
            if leader:
                flight = _SyncFlight(event=threading.Event())
                self._sync_flights[operation_id] = flight
        if not leader:
            flight.event.wait()
            if flight.error is not None:
                raise flight.error
            if flight.profile is None:
                raise RuntimeError("Capability probe completed without an effective profile.")
            return flight.profile

        try:
            effective = self._resolve_from_runner(operation_id)
            completed_at = self._clock()
        except BaseException as exc:
            with self._lock:
                flight.error = self._follower_failure(operation_id, exc)
                self._sync_flights.pop(operation_id, None)
                flight.event.set()
            raise
        with self._lock:
            self._entries[operation_id] = _CacheEntry(profile=effective, timestamp=completed_at)
            flight.profile = effective
            self._sync_flights.pop(operation_id, None)
            flight.event.set()
        return effective

    async def resolve_async(self, operation_id: OperationId) -> EffectiveCapabilityProfile:
        """Resolve one operation through an async runner with single-flight sharing."""
        self._validate_operation_id(operation_id)
        with self._lock:
            cached = self._fresh_entry(operation_id)
            if cached is not None:
                return cached.profile
        if operation_id not in self._declared_profile.operations or self._async_probe_runner is None:
            return self._baseline

        loop = asyncio.get_running_loop()
        with self._lock:
            flight = self._async_flights.get(operation_id)
            leader = flight is None
            if leader:
                flight = loop.create_future()
                self._async_flights[operation_id] = flight
        if not leader:
            return await flight

        try:
            effective = await self._resolve_from_async_runner(operation_id)
            completed_at = self._clock()
        except BaseException as exc:
            with self._lock:
                self._async_flights.pop(operation_id, None)
                if not flight.done():
                    flight.set_exception(self._follower_failure(operation_id, exc))
                    flight.exception()
            raise
        with self._lock:
            self._entries[operation_id] = _CacheEntry(profile=effective, timestamp=completed_at)
            self._async_flights.pop(operation_id, None)
            if not flight.done():
                flight.set_result(effective)
        return effective

    def peek(self, operation_id: OperationId) -> EffectiveCapabilityProfile:
        """Return a fresh cached profile or the declared baseline without probing."""
        self._validate_operation_id(operation_id)
        with self._lock:
            cached = self._fresh_entry(operation_id)
            return cached.profile if cached is not None else self._baseline

    def record_response(
        self,
        operation_id: OperationId,
        response_class: ProbeResponseClass,
    ) -> EffectiveCapabilityProfile:
        """Record a bounded origin response as the operation's effective state."""
        self._validate_operation_id(operation_id)
        if not isinstance(response_class, ProbeResponseClass):
            raise ValueError("Capability response classes must use ProbeResponseClass.")
        state = {
            ProbeResponseClass.SUCCESS: _declared_state(self._declared_profile, operation_id),
            ProbeResponseClass.UNSUPPORTED: EffectiveCapabilityState.UNSUPPORTED,
            ProbeResponseClass.UNAUTHORIZED: EffectiveCapabilityState.UNAUTHORIZED,
            ProbeResponseClass.FORBIDDEN: EffectiveCapabilityState.FORBIDDEN,
            ProbeResponseClass.UNAVAILABLE: EffectiveCapabilityState.UNAVAILABLE,
            ProbeResponseClass.DEPLOYMENT_DISABLED: EffectiveCapabilityState.DEPLOYMENT_DISABLED,
        }[response_class]
        effective = self._profile_with_state(operation_id, state)
        with self._lock:
            self._entries[operation_id] = _CacheEntry(profile=effective, timestamp=self._clock())
        return effective

    def record_evidence(self, evidence: ProbeEvidence) -> EffectiveCapabilityProfile:
        """Validate and record runner evidence without dispatching an operation."""
        self._validate_evidence(evidence.operation_id, evidence)
        effective = self._profile_from_evidence(evidence.operation_id, evidence)
        with self._lock:
            self._entries[evidence.operation_id] = _CacheEntry(profile=effective, timestamp=self._clock())
        return effective

    def invalidate(self, operation_id: OperationId | None = None) -> None:
        """Discard all cached state or the state for one operation."""
        if operation_id is not None:
            self._validate_operation_id(operation_id)
        with self._lock:
            if operation_id is None:
                self._entries.clear()
            else:
                self._entries.pop(operation_id, None)

    def _follower_failure(self, operation_id: OperationId, exc: BaseException) -> BaseException:
        """Convert leader cancellation into a typed failure shared with waiting followers."""
        if isinstance(exc, asyncio.CancelledError):
            return CatalogUnavailableError(
                "The capability probe was cancelled before it completed.",
                operation=str(operation_id),
                platform=operation_id.platform,
                capability_state="unavailable",
                safe_action="Retry the operation once the catalog deployment is reachable.",
            )
        return exc

    def _fresh_entry(self, operation_id: OperationId) -> _CacheEntry | None:
        entry = self._entries.get(operation_id)
        if entry is None:
            return None
        if self._clock() - entry.timestamp >= self._ttl_seconds:
            self._entries.pop(operation_id, None)
            return None
        return entry

    def _resolve_from_runner(self, operation_id: OperationId) -> EffectiveCapabilityProfile:
        runner = self._probe_runner
        if runner is None:
            return self._baseline
        try:
            result = runner.probe(operation_id)
            if inspect.isawaitable(result):
                raise TypeError("Synchronous capability runners cannot return awaitables.")
            evidence = cast(ProbeEvidence, result)
            self._validate_evidence(operation_id, evidence)
        except CatalogValidationError:
            raise
        except (TypeError, ValueError) as exc:
            raise self._invalid_evidence_error(operation_id) from exc
        return self._profile_from_evidence(operation_id, evidence)

    async def _resolve_from_async_runner(self, operation_id: OperationId) -> EffectiveCapabilityProfile:
        runner = self._async_probe_runner
        if runner is None:
            return self._baseline
        try:
            result = runner.probe(operation_id)
            evidence = await result if inspect.isawaitable(result) else result
            self._validate_evidence(operation_id, evidence)
        except CatalogValidationError:
            raise
        except (TypeError, ValueError) as exc:
            raise self._invalid_evidence_error(operation_id) from exc
        return self._profile_from_evidence(operation_id, evidence)

    def _profile_from_evidence(self, operation_id: OperationId, evidence: ProbeEvidence) -> EffectiveCapabilityProfile:
        derived = EffectiveCapabilityProfile.derive(self._declared_profile, [evidence])
        capabilities = dict(self._baseline.capabilities)
        capabilities[operation_id] = derived.for_operation(operation_id)
        return EffectiveCapabilityProfile(declared_profile=self._declared_profile, capabilities=capabilities)

    def _profile_with_state(
        self, operation_id: OperationId, state: EffectiveCapabilityState
    ) -> EffectiveCapabilityProfile:
        if operation_id not in self._declared_profile.operations:
            return self._baseline
        capabilities = dict(self._baseline.capabilities)
        capabilities[operation_id] = EffectiveOperationCapability(
            operation=self._declared_profile.operations[operation_id], state=state
        )
        return EffectiveCapabilityProfile(declared_profile=self._declared_profile, capabilities=capabilities)

    def _validate_evidence(self, operation_id: OperationId, evidence: ProbeEvidence) -> None:
        if not isinstance(evidence, ProbeEvidence):
            raise TypeError("Capability probe runners must return ProbeEvidence.")
        if evidence.operation_id != operation_id:
            raise ValueError("Capability probe evidence must match the requested operation.")
        try:
            parsed = urlsplit(evidence.deployment_url)
        except ValueError as exc:
            raise ValueError("Capability probe evidence must contain a valid deployment URL.") from exc
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Capability probe evidence must contain a sanitized HTTPS deployment URL.")

    def _invalid_evidence_error(self, operation_id: OperationId) -> CatalogValidationError:
        return CatalogValidationError(
            "Capability probe returned invalid evidence.",
            operation=str(operation_id),
            platform=operation_id.platform,
            capability_state="invalid-probe-evidence",
            safe_action="Fix the probe runner to return sanitized HTTPS evidence before retrying.",
        )

    def _validate_operation_id(self, operation_id: OperationId) -> None:
        if not isinstance(operation_id, OperationId):
            raise TypeError("Capability cache operations require an OperationId.")


def build_catalog_operation_guard(
    operation_id: OperationId,
    profile: EffectiveCapabilityProfile,
    permissions: EffectivePermissions | None = None,
) -> CatalogOperationGuard:
    """Build the normalized guard for one effective operation profile."""
    return _EffectiveCapabilityGuard(operation_id=operation_id, profile=profile, permissions=permissions)


class _EffectiveCapabilityGuard(CatalogOperationGuard):
    """Raise the normalized error matching the cached capability state."""

    def require_allowed(self) -> None:
        if self.profile is not None:
            decision = self.profile.guard(self.operation_id)
            if not decision.allowed:
                error_type: type[CatalogError] = {
                    EffectiveCapabilityState.UNAUTHORIZED: UnauthenticatedError,
                    EffectiveCapabilityState.FORBIDDEN: ForbiddenError,
                }.get(decision.state, UnsupportedCapabilityError)
                raise error_type(
                    f"The catalog operation is {decision.state.value}.",
                    operation=str(self.operation_id),
                    platform=self.operation_id.platform,
                    capability_state=decision.state.value,
                    safe_action=decision.remedy or "Inspect the deployment capability profile before retrying.",
                )
        if self.permissions is not None:
            self.permissions.require(str(self.operation_id))


def _baseline_profile(profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile) -> EffectiveCapabilityProfile:
    if isinstance(profile, EffectiveCapabilityProfile):
        return profile
    return EffectiveCapabilityProfile(
        declared_profile=profile,
        capabilities={
            operation_id: EffectiveOperationCapability(
                operation=operation,
                state=EffectiveCapabilityState(operation.capability_class.value),
            )
            for operation_id, operation in profile.operations.items()
        },
    )


def _declared_state(profile: DeclaredCapabilityProfile, operation_id: OperationId) -> EffectiveCapabilityState:
    operation = profile.operations.get(operation_id)
    if operation is None:
        return EffectiveCapabilityState.UNSUPPORTED
    return EffectiveCapabilityState(operation.capability_class.value)
