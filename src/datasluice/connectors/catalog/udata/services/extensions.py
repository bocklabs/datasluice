"""Dual-mode uData stock extension service and harmless capability evidence."""

from __future__ import annotations

import asyncio
import math
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.udata.models.extensions import (
    AVAILABLE,
    AvatarRequest,
    ExtensionEvidence,
    TagSuggestQuery,
)
from datasluice.connectors.catalog.udata.services.taxonomies import AsyncCatalogService, SyncCatalogService
from datasluice.connectors.catalog.udata.wire import extensions as wire
from datasluice.domain.catalog.auth import credential_scope
from datasluice.domain.catalog.profiles import ProbeResponseClass
from datasluice.errors.catalog import NativeCatalogError
from datasluice.runtime.constants import DEFAULT_CAPABILITY_CACHE_TTL_SECONDS

from .datasets import _error_status, _header

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
    from datasluice.domain.catalog.models import MappingRecord

type Read[T] = Callable[[], T]
type EvidenceKey = tuple[str, str]
_AsyncFlight = asyncio.Future[ExtensionEvidence | None]


@dataclass(slots=True)
class _SyncFlight:
    event: threading.Event
    generation: int
    evidence: ExtensionEvidence | None = None


class ExtensionEvidenceCache:
    """Credential-scoped bounded extension evidence with an expiring single-flight TTL cache.

    Cache keys pair one credential scope with one operation, so evidence for
    one capability never satisfies another and a rotated credential starts
    from an empty cache. Exactly one harmless probe runs per key and mode at a
    time, and an explicit refresh discards every entry plus any result still
    in flight.
    """

    def __init__(self, *, ttl_seconds: float, clock: Callable[[], float] = monotonic) -> None:
        """Pin the evidence lifetime and the monotonic clock used to expire it."""
        if type(ttl_seconds) not in (int, float) or not math.isfinite(ttl_seconds) or ttl_seconds < 0:
            raise ValueError("Extension evidence TTL must be a finite non-negative number.")
        self._ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: dict[EvidenceKey, tuple[ExtensionEvidence, float]] = {}
        self._sync_flights: dict[EvidenceKey, _SyncFlight] = {}
        self._async_flights: dict[EvidenceKey, tuple[_AsyncFlight, int]] = {}
        self._generation = 0

    @property
    def ttl_seconds(self) -> float:
        """Return the bounded evidence lifetime in seconds."""
        return self._ttl_seconds

    def evidence(self, scope: str, operation: str) -> ExtensionEvidence | None:
        """Return one fresh cached entry, or ``None`` without dispatching anything."""
        with self._lock:
            entry = self._entries.get((scope, operation))
            if entry is None:
                return None
            if self._clock() - entry[1] >= self._ttl_seconds:
                self._entries.pop((scope, operation), None)
                return None
            return entry[0]

    def proven_operations(self, scope: str) -> tuple[str, ...]:
        """Return the operations currently proven available for one credential scope."""
        with self._lock:
            now = self._clock()
            return tuple(
                sorted(
                    operation
                    for (entry_scope, operation), (evidence, recorded_at) in self._entries.items()
                    if entry_scope == scope and evidence.available and now - recorded_at < self._ttl_seconds
                )
            )

    def record(self, evidence: ExtensionEvidence) -> None:
        """Record one bounded allowlisted evidence entry for its scope and operation."""
        with self._lock:
            self._record_locked(evidence)

    def invalidate(self) -> None:
        """Discard every cached entry so the next proof dispatches a fresh harmless probe."""
        with self._lock:
            self._entries.clear()
            self._generation += 1

    def sync_flight(self, key: EvidenceKey) -> tuple[_SyncFlight, bool]:
        """Return the shared synchronous flight for *key* and whether this caller leads it."""
        with self._lock:
            flight = self._sync_flights.get(key)
            if flight is not None:
                return flight, False
            flight = _SyncFlight(event=threading.Event(), generation=self._generation)
            self._sync_flights[key] = flight
            return flight, True

    @staticmethod
    def wait_sync_flight(flight: _SyncFlight) -> ExtensionEvidence | None:
        """Wait for the leading proof and return its evidence, or ``None`` when it was dropped."""
        flight.event.wait()
        return flight.evidence

    def complete_sync_flight(self, key: EvidenceKey, flight: _SyncFlight, evidence: ExtensionEvidence) -> None:
        """Publish one completed proof, retaining it only when no refresh intervened."""
        with self._lock:
            if self._sync_flights.get(key) is flight:
                self._sync_flights.pop(key, None)
            if flight.generation == self._generation:
                self._record_locked(evidence)
            flight.evidence = evidence
            flight.event.set()

    def drop_sync_flight(self, key: EvidenceKey, flight: _SyncFlight) -> None:
        """Drop one failed proof so every waiting caller re-proves on its own."""
        with self._lock:
            if self._sync_flights.get(key) is flight:
                self._sync_flights.pop(key, None)
            flight.evidence = None
            flight.event.set()

    def async_flight(self, key: EvidenceKey) -> tuple[_AsyncFlight, bool]:
        """Return the shared asynchronous flight for *key* and whether this caller leads it."""
        with self._lock:
            existing = self._async_flights.get(key)
            if existing is not None:
                return existing[0], False
            created: _AsyncFlight = asyncio.get_running_loop().create_future()
            self._async_flights[key] = (created, self._generation)
            return created, True

    def complete_async_flight(self, key: EvidenceKey, flight: _AsyncFlight, evidence: ExtensionEvidence) -> None:
        """Publish one completed async proof, retaining it only when no refresh intervened."""
        with self._lock:
            entry = self._async_flights.get(key)
            if entry is not None and entry[0] is flight:
                self._async_flights.pop(key, None)
                if entry[1] == self._generation:
                    self._record_locked(evidence)
        if not flight.done():
            flight.set_result(evidence)

    def drop_async_flight(self, key: EvidenceKey, flight: _AsyncFlight) -> None:
        """Drop one failed async proof so every waiting caller re-proves on its own."""
        with self._lock:
            entry = self._async_flights.get(key)
            if entry is not None and entry[0] is flight:
                self._async_flights.pop(key, None)
        if not flight.done():
            flight.set_result(None)

    def _record_locked(self, evidence: ExtensionEvidence) -> None:
        self._entries[(evidence.credential_scope, evidence.operation_id)] = (evidence, self._clock())


def _evidence(operation: str, scope: str, response_class: ProbeResponseClass) -> ExtensionEvidence:
    """Build one bounded evidence record from a fail-closed response class."""
    state = AVAILABLE if response_class is ProbeResponseClass.SUCCESS else response_class.value
    return ExtensionEvidence(
        operation_id=operation,
        state=state,
        response_class=response_class.value,
        credential_scope=scope,
    )


def _classify_failure(error: Exception) -> ProbeResponseClass:
    """Classify one failed harmless read into a fail-closed response class.

    Route absence resolves unsupported, authorization failures resolve to
    their own classes, and every transport failure, unexpected status, or
    ambiguous decode resolves unavailable instead of assuming availability.
    """
    status = _error_status(error)
    if status == 401:
        return ProbeResponseClass.UNAUTHORIZED
    if status == 403:
        return ProbeResponseClass.FORBIDDEN
    if status in (404, 410):
        return ProbeResponseClass.UNSUPPORTED
    if status == 423:
        return ProbeResponseClass.DEPLOYMENT_DISABLED
    return ProbeResponseClass.UNAVAILABLE


def _evidence_from_sync_read(operation: str, scope: str, read: Read[object]) -> ExtensionEvidence:
    """Classify one harmless synchronous read into bounded evidence."""
    try:
        read()
    except Exception as error:
        return _evidence(operation, scope, _classify_failure(error))
    return _evidence(operation, scope, ProbeResponseClass.SUCCESS)


async def _evidence_from_async_read(operation: str, scope: str, read: Read[Awaitable[object]]) -> ExtensionEvidence:
    """Classify one harmless asynchronous read into bounded evidence."""
    try:
        await read()
    except Exception as error:
        return _evidence(operation, scope, _classify_failure(error))
    return _evidence(operation, scope, ProbeResponseClass.SUCCESS)


def _prove_sync(
    cache: ExtensionEvidenceCache,
    scope: str,
    operation: str,
    read: Read[object],
) -> ExtensionEvidence:
    """Prove one operation harmlessly, sharing exactly one in-flight probe per key."""
    key = (scope, operation)
    while True:
        cached = cache.evidence(scope, operation)
        if cached is not None:
            return cached
        flight, leader = cache.sync_flight(key)
        if not leader:
            evidence = cache.wait_sync_flight(flight)
            if evidence is not None:
                return evidence
            continue
        try:
            evidence = _evidence_from_sync_read(operation, scope, read)
        except BaseException:
            cache.drop_sync_flight(key, flight)
            raise
        cache.complete_sync_flight(key, flight, evidence)
        return evidence


async def _prove_async(
    cache: ExtensionEvidenceCache,
    scope: str,
    operation: str,
    read: Read[Awaitable[object]],
) -> ExtensionEvidence:
    """Prove one operation harmlessly in async, mirroring the synchronous single flight."""
    key = (scope, operation)
    while True:
        cached = cache.evidence(scope, operation)
        if cached is not None:
            return cached
        flight, leader = cache.async_flight(key)
        if not leader:
            evidence = await asyncio.shield(flight)
            if evidence is not None:
                return evidence
            continue
        try:
            evidence = await _evidence_from_async_read(operation, scope, read)
        except BaseException:
            cache.drop_async_flight(key, flight)
            raise
        cache.complete_async_flight(key, flight, evidence)
        return evidence


def _sync_probe_read(service: SyncExtensionsService, operation: str) -> object:
    """Dispatch the operation's own harmless read-only probe."""
    if operation == wire.REASON_CATEGORIES_OPERATION:
        return service.reason_categories()
    if operation == wire.SUGGEST_TAGS_OPERATION:
        return service.suggest_tags(wire.probe_tag_suggest_query())
    if operation == wire.AVATAR_OPERATION:
        return service.avatar(wire.probe_avatar_request())
    return service.captchetat()


async def _async_probe_read(service: AsyncExtensionsService, operation: str) -> object:
    """Dispatch the operation's own harmless read-only probe in async."""
    if operation == wire.REASON_CATEGORIES_OPERATION:
        return await service.reason_categories()
    if operation == wire.SUGGEST_TAGS_OPERATION:
        return await service.suggest_tags(wire.probe_tag_suggest_query())
    if operation == wire.AVATAR_OPERATION:
        return await service.avatar(wire.probe_avatar_request())
    return await service.captchetat()


class SyncExtensionsService(SyncCatalogService):
    """Typed synchronous methods for the assigned stock extension family."""

    def __init__(self, client: SyncUDataClient, evidence_cache: ExtensionEvidenceCache | None = None) -> None:
        """Bind the service to one strict sync client and its bounded evidence cache."""
        self._client = client
        self._evidence_cache = (
            evidence_cache
            if evidence_cache is not None
            else ExtensionEvidenceCache(ttl_seconds=DEFAULT_CAPABILITY_CACHE_TTL_SECONDS)
        )

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def reason_categories(self) -> tuple[MappingRecord, ...]:
        """GET /api/1/access_type/reason_categories/ (row 1)."""
        return self._read(
            wire.reason_categories_request(),
            wire.REASON_CATEGORIES_OPERATION,
            wire.parse_reason_categories,
        )

    def suggest_tags(self, query: TagSuggestQuery) -> tuple[MappingRecord, ...]:
        """GET /api/1/tags/suggest/ (row 196)."""
        return self._read(wire.suggest_tags_request(query), wire.SUGGEST_TAGS_OPERATION, wire.parse_tag_suggestions)

    def avatar(self, client_input: AvatarRequest) -> MappingRecord:
        """GET /api/1/avatars/<identifier>/<size>/ (row 232); retains bounded image metadata only."""
        method, path, headers, _ = wire.avatar_request(client_input)
        _, body, response = self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=wire.AVATAR_OPERATION,
            headers=headers,
            raw_text=True,
            max_response_bytes=wire.AVATAR_MAX_BYTES,
        )
        return wire.parse_avatar(
            cast("bytes", body),
            response_media_type=_header(response.headers, "content-type"),
            operation=wire.AVATAR_OPERATION,
        )

    def captchetat(self) -> MappingRecord:
        """GET /api/2/captchetat/ (row 262)."""
        return self._read(wire.captchetat_request(), wire.CAPTCHETAT_OPERATION, wire.parse_captchetat)

    def capability(self, operation: str) -> ExtensionEvidence:
        """Return bounded evidence for one stock extension operation, proving it harmlessly.

        The proof dispatches only that operation's own harmless read-only
        probe, so availability is never inferred from another capability's
        successful read. Absence, ambiguity, or an unsafe-to-confirm state
        resolves to a fail-closed state instead of an assumption.
        """
        wire.require_extension_operation(operation)
        scope = credential_scope(self._client.credentials)
        return _prove_sync(self._evidence_cache, scope, operation, lambda: _sync_probe_read(self, operation))

    def evidence(self, operation: str) -> ExtensionEvidence | None:
        """Return one fresh cached evidence entry without dispatching anything."""
        wire.require_extension_operation(operation)
        return self._evidence_cache.evidence(credential_scope(self._client.credentials), operation)

    def proven_operations(self) -> tuple[str, ...]:
        """Return the operations currently proven available for the configured credential scope."""
        return self._evidence_cache.proven_operations(credential_scope(self._client.credentials))

    def refresh(self) -> None:
        """Discard every cached evidence entry so the next proof runs a fresh harmless probe."""
        self._evidence_cache.invalidate()


class AsyncExtensionsService(AsyncCatalogService):
    """Typed asynchronous methods for the assigned stock extension family."""

    def __init__(self, client: AsyncUDataClient, evidence_cache: ExtensionEvidenceCache | None = None) -> None:
        """Bind the service to one strict async client and its bounded evidence cache."""
        self._client = client
        self._evidence_cache = (
            evidence_cache
            if evidence_cache is not None
            else ExtensionEvidenceCache(ttl_seconds=DEFAULT_CAPABILITY_CACHE_TTL_SECONDS)
        )

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def reason_categories(self) -> tuple[MappingRecord, ...]:
        """GET /api/1/access_type/reason_categories/ (row 1)."""
        return await self._read(
            wire.reason_categories_request(),
            wire.REASON_CATEGORIES_OPERATION,
            wire.parse_reason_categories,
        )

    async def suggest_tags(self, query: TagSuggestQuery) -> tuple[MappingRecord, ...]:
        """GET /api/1/tags/suggest/ (row 196)."""
        return await self._read(
            wire.suggest_tags_request(query), wire.SUGGEST_TAGS_OPERATION, wire.parse_tag_suggestions
        )

    async def avatar(self, client_input: AvatarRequest) -> MappingRecord:
        """GET /api/1/avatars/<identifier>/<size>/ (row 232); retains bounded image metadata only."""
        method, path, headers, _ = wire.avatar_request(client_input)
        _, body, response = await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=wire.AVATAR_OPERATION,
            headers=headers,
            raw_text=True,
            max_response_bytes=wire.AVATAR_MAX_BYTES,
        )
        return wire.parse_avatar(
            cast("bytes", body),
            response_media_type=_header(response.headers, "content-type"),
            operation=wire.AVATAR_OPERATION,
        )

    async def captchetat(self) -> MappingRecord:
        """GET /api/2/captchetat/ (row 262)."""
        return await self._read(wire.captchetat_request(), wire.CAPTCHETAT_OPERATION, wire.parse_captchetat)

    async def capability(self, operation: str) -> ExtensionEvidence:
        """Return bounded evidence for one stock extension operation, proving it harmlessly."""
        wire.require_extension_operation(operation)
        scope = credential_scope(self._client.credentials)
        return await _prove_async(self._evidence_cache, scope, operation, lambda: _async_probe_read(self, operation))

    async def evidence(self, operation: str) -> ExtensionEvidence | None:
        """Return one fresh cached evidence entry without dispatching anything."""
        wire.require_extension_operation(operation)
        return self._evidence_cache.evidence(credential_scope(self._client.credentials), operation)

    async def proven_operations(self) -> tuple[str, ...]:
        """Return the operations currently proven available for the configured credential scope."""
        return self._evidence_cache.proven_operations(credential_scope(self._client.credentials))

    async def refresh(self) -> None:
        """Discard every cached evidence entry so the next proof runs a fresh harmless probe."""
        self._evidence_cache.invalidate()
