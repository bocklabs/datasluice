"""Transport-backed strict-version sync and async uData clients."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import date
from functools import lru_cache
from importlib import resources
from time import monotonic, sleep
from types import TracebackType
from typing import Self, cast

from datasluice.connectors.catalog.udata.mapping import (
    _DATASETS_OPERATION_ID,
    _PAGED_PATH,
    PLATFORM,
    parse_native_page,
    shape_dataset_page,
    unimplemented_family,
)
from datasluice.connectors.catalog.udata.probes import (
    AsyncSiteVersionGate,
    SiteVersion,
    SiteVersionGate,
)
from datasluice.connectors.catalog.udata.services.datasets import AsyncDatasetsService, SyncDatasetsService
from datasluice.connectors.catalog.udata.settings import UDataClientSettings, normalize_origin
from datasluice.contracts.catalog.native.udata import UDataResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.auth import credential_scope as _credential_scope
from datasluice.domain.catalog.models import ResultEnvelope
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import (
    DeclaredCapabilityProfile,
    EffectiveCapabilityProfile,
    ProbeEvidence,
    ProbeResponseClass,
)
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogUnavailableError,
    CatalogValidationError,
    NativeCatalogError,
    map_catalog_error,
)
from datasluice.runtime.capability import (
    AsyncProbeRunner,
    EffectiveCapabilityCache,
    ProbeRunner,
    build_catalog_operation_guard,
)
from datasluice.runtime.clients import (
    AsyncCatalogTransport,
    _auth_headers,
    _capability_value,
    _circuit_key,
    _refreshed_credential,
    _refreshed_credential_async,
)
from datasluice.runtime.constants import (
    DEFAULT_BREAKER_COOLDOWN_SECONDS,
    DEFAULT_BREAKER_FAILURE_THRESHOLD,
    DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
)
from datasluice.runtime.defaults import create_default_async_transport, create_default_sync_transport
from datasluice.runtime.events import EventEmitter
from datasluice.runtime.extras import require_extra
from datasluice.runtime.resilience import BreakerRegistry, DeadlineMonitor, RetryLoop
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest, RuntimeResponse, TransportFailure

_PROFILE_RESOURCE = "udata-17.6.json"
_PAGER_PARAMS = frozenset({"page", "page_size"})

_STATUS_RESPONSE_CLASSES = {
    401: ProbeResponseClass.UNAUTHORIZED,
    403: ProbeResponseClass.FORBIDDEN,
    423: ProbeResponseClass.DEPLOYMENT_DISABLED,
}


def _operation_id_from(value: str) -> OperationId:
    """Derive the internal OperationId from one pinned profile identity string."""
    platform, _, tail = value.partition("/")
    service, dot, method = tail.partition(".")
    if not dot:
        return OperationId(platform=platform, service="native", method=tail)
    return OperationId(platform=platform, service=service, method=method)


@lru_cache(maxsize=1)
def declared_udata_profile() -> DeclaredCapabilityProfile:
    """Load and pin the checked-in uData capability profile."""
    document = json.loads(
        resources.files("datasluice.contracts")
        .joinpath("catalog")
        .joinpath("profiles")
        .joinpath(_PROFILE_RESOURCE)
        .read_text(encoding="utf-8")
    )
    if document.get("platform") != PLATFORM.value:
        raise ValueError(f"The pinned {_PROFILE_RESOURCE} does not declare the uData platform.")
    operations: dict[OperationId, OperationSpec] = {}
    for item in document["operations"]:
        operation_id = _operation_id_from(item["id"])
        mutation = item["mutation"]
        operations[operation_id] = OperationSpec(
            id=operation_id,
            tier=OperationTier.NATIVE,
            request_type="CatalogOperationRequest",
            response_type="UDataResultItem",
            auth_class=AuthClass(item["authentication"]),
            mutation_class=MutationClass(mutation),
            idempotency=Idempotency.SAFE if mutation == "read" else Idempotency.CONDITIONAL,
            concurrency=ConcurrencyRequirement.NONE if mutation == "read" else ConcurrencyRequirement.OPTIONAL,
            atomicity=Atomicity.NONE if mutation == "read" else Atomicity.SINGLE_RESOURCE,
            capability_class=CapabilityClass(item["capability"]),
        )
    return DeclaredCapabilityProfile(
        profile_version=document["profile_version"],
        schema_version=document["schema_version"],
        platform_api_version=document["platform_api_version"],
        official_source_uri=document["official_source_uri"],
        source_accessed_at=date.fromisoformat(document["source_accessed_at"]),
        fixture_fingerprint=document["fixture_fingerprint"],
        operations=operations,
    )


def _origin_checked_runner(runner: ProbeRunner, origin: str) -> ProbeRunner:
    """Wrap a probe runner so only same-origin evidence is accepted."""

    class _Checked:
        def probe(self, operation_id: OperationId) -> ProbeEvidence:
            evidence = runner.probe(operation_id)
            if not evidence.deployment_url.startswith(origin + "/") and evidence.deployment_url != origin:
                raise CatalogValidationError(
                    "Capability probe evidence does not match the configured deployment origin.",
                    operation=str(operation_id),
                    platform=PLATFORM.value,
                    safe_action="Configure a probe runner scoped to the client origin.",
                )
            return evidence

    return _Checked()  # type: ignore[return-value]


def _origin_checked_async_runner(runner: AsyncProbeRunner, origin: str) -> AsyncProbeRunner:
    """Wrap an async probe runner so only same-origin evidence is accepted."""

    class _CheckedAsync:
        async def probe(self, operation_id: OperationId) -> ProbeEvidence:
            evidence = await runner.probe(operation_id)
            if not evidence.deployment_url.startswith(origin + "/") and evidence.deployment_url != origin:
                raise CatalogValidationError(
                    "Capability probe evidence does not match the configured deployment origin.",
                    operation=str(operation_id),
                    platform=PLATFORM.value,
                    safe_action="Configure a probe runner scoped to the client origin.",
                )
            return evidence

    return _CheckedAsync()  # type: ignore[return-value]


def _enforce_caller_guards(operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> None:
    if guard.operation_id != operation.operation_id:
        raise ValueError(
            f"Catalog operation guard operation_id {guard.operation_id} does not match request operation_id "
            f"{operation.operation_id}."
        )
    guard.require_allowed()


def _response_header(response: RuntimeResponse, name: str) -> str | None:
    """Read one response header without depending on its casing."""
    wanted = name.lower()
    for key, value in response.headers.items():
        if key.lower() == wanted:
            return value
    return None


def _page_request(
    *,
    origin: str,
    params: Mapping[str, int],
) -> RuntimeRequest:
    """Build one anonymous-safe paged dataset request; credentials attach later."""
    query = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
    url = f"{origin}{_PAGED_PATH}" + (f"?{query}" if query else "")
    return RuntimeRequest(method="GET", url=url, headers={}, body=None)


class _UDataClientCore:
    """Shared strict-gate state for the sync and async uData clients."""

    def __init__(
        self,
        transport: CatalogTransport | AsyncCatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        origin: str,
        credentials: object | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        emitter: EventEmitter | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        owns_transport: bool = True,
        site_gate: SiteVersionGate | AsyncSiteVersionGate | None = None,
        probe_runner: ProbeRunner | None = None,
        async_probe_runner: AsyncProbeRunner | None = None,
        async_gate: bool = False,
    ) -> None:
        self._transport = transport
        self._credential_scope = _credential_scope(credentials)
        self._owns_transport = owns_transport
        self._origin = normalize_origin(origin)
        checked_origin = self._origin
        self._capabilities = EffectiveCapabilityCache(
            profile,
            probe_runner=_origin_checked_runner(probe_runner, checked_origin) if probe_runner else None,
            async_probe_runner=(
                _origin_checked_async_runner(async_probe_runner, checked_origin) if async_probe_runner else None
            ),
            namespace=checked_origin,
            deployment_origin=checked_origin,
            ttl_seconds=capability_cache_ttl,
            clock=clock,
        )
        self._profile = self._capabilities.baseline_profile
        pinned = self._profile.declared_profile.profile_version
        if site_gate is not None:
            self._site_gate: SiteVersionGate | AsyncSiteVersionGate = site_gate
        elif not async_gate:
            self._site_gate = SiteVersionGate(
                pinned_version=pinned,
                origin=self._origin,
                transport=cast(CatalogTransport, transport),
                ttl_seconds=capability_cache_ttl,
                clock=clock,
            )
        else:
            self._site_gate = AsyncSiteVersionGate(
                pinned_version=pinned,
                origin=self._origin,
                transport=cast(AsyncCatalogTransport, transport),
                ttl_seconds=capability_cache_ttl,
                clock=clock,
            )
        self._credentials = credentials
        self._budget = budget or TimeBudget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold, cooldown=breaker_cooldown, clock=clock
        )
        self._max_attempts = max_attempts
        self._clock = clock
        self._emitter = emitter or EventEmitter()
        self._closed = False

    @property
    def transport(self) -> CatalogTransport | AsyncCatalogTransport:
        """Expose the underlying transport as the public introspection seam."""
        return self._transport

    def _resolved_credential(self) -> object | None:
        """Resolve the current credential once for pre-dispatch validation."""
        return _refreshed_credential(self._credentials)

    async def _resolved_credential_async(self) -> object | None:
        """Resolve the current credential asynchronously for pre-dispatch validation."""
        return await _refreshed_credential_async(self._credentials)

    @property
    def credentials(self) -> object | None:
        """Expose the injected caller-owned credential resolver or provider."""
        return self._credentials

    def _emit(self, owning_id: OperationId, outcome: str, **metadata: object) -> None:
        self._emitter.record(
            operation_id=str(owning_id),
            platform=PLATFORM.value,
            outcome=outcome,
            metadata=metadata,
        )

    def _emit_breaker_change(self, owning_id: OperationId, before: bool, after: bool) -> None:
        if before != after:
            self._emit(owning_id, "breaker_state_change", breaker_open=after)

    def _validate_page_params(self, operation: CatalogOperationRequest) -> dict[str, int]:
        unknown = set(operation.payload) - _PAGER_PARAMS
        if unknown:
            raise CatalogValidationError(
                f"The tracer dataset list accepts only {sorted(_PAGER_PARAMS)} parameters.",
                operation=str(operation.operation_id),
                platform=PLATFORM.value,
                safe_action="Pass optional positive-integer page and page_size values only.",
            )
        params: dict[str, int] = {}
        for key in _PAGER_PARAMS:
            value = operation.payload.get(key)
            if value is None:
                continue
            if type(value) is not int or value < 1:
                raise CatalogValidationError(
                    f"The uData dataset list parameter {key!r} must be a positive integer.",
                    operation=str(operation.operation_id),
                    platform=PLATFORM.value,
                    safe_action=f"Pass {key!r} as a positive integer.",
                )
            params[key] = value
        return params

    def _validate_status(
        self,
        owning_id: OperationId,
        response: RuntimeResponse,
        *,
        redirect_mode: bool = False,
        credential_scope: str = "anonymous",
    ) -> None:
        if redirect_mode and response.status_code in {301, 302, 303, 307, 308}:
            return
        if 200 <= response.status_code < 300:
            return
        if response.status_code in {401, 403, 423}:
            self._capabilities.record_response(
                owning_id, _STATUS_RESPONSE_CLASSES[response.status_code], credential_scope=credential_scope
            )
        raise map_catalog_error(
            NativeCatalogError(
                "Catalog operation returned an unsuccessful HTTP status.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                status_code=response.status_code,
                retry_after=response.retry_after,
            )
        )

    def _decode(
        self, owning_id: OperationId, response: RuntimeResponse, *, credential_scope: str = "anonymous"
    ) -> ResultEnvelope[UDataResultItem]:
        if not 200 <= response.status_code < 300:
            if response.status_code in _STATUS_RESPONSE_CLASSES:
                self._capabilities.record_response(
                    owning_id, _STATUS_RESPONSE_CLASSES[response.status_code], credential_scope=credential_scope
                )
            raise map_catalog_error(
                NativeCatalogError(
                    "Catalog operation returned an unsuccessful HTTP status.",
                    operation=str(owning_id),
                    platform=PLATFORM.value,
                    status_code=response.status_code,
                    retry_after=response.retry_after,
                )
            )
        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog operation returned an invalid JSON result.",
                operation=str(owning_id),
                platform=PLATFORM.value,
            ) from exc
        return shape_dataset_page(parse_native_page(payload, operation=str(owning_id)), operation=str(owning_id))

    def capability(self, operation_id: str) -> str:
        """Return the cached effective classification without dispatching transport I/O."""
        operation = _operation_id_from(operation_id)
        state = (
            self._capabilities.peek(operation, credential_scope=_credential_scope(self._credentials))
            .guard(operation)
            .state
        )
        return _capability_value(state)

    def invalidate(self, operation_id: str | OperationId | None = None) -> None:
        """Discard all effective capability state or one operation's state."""
        target = None
        if isinstance(operation_id, OperationId):
            target = operation_id
        elif operation_id is not None:
            target = _operation_id_from(operation_id)
        self._capabilities.invalidate(target)
        self._site_gate.invalidate()

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe pinned-profile metadata."""
        declared = self._profile.declared_profile
        return {"platform": next(iter(declared.operations)).platform, "profile_version": declared.profile_version}


class SyncUDataClient(_UDataClientCore):
    """Synchronous strict-version uData client: one anonymous probe, one dataset read."""

    def __init__(
        self,
        transport: CatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        origin: str,
        credentials: object | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], None] = sleep,
        emitter: EventEmitter | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        owns_transport: bool = True,
        probe_runner: ProbeRunner | None = None,
    ) -> None:
        """Build the shared sync core over the caller-owned or borrowed transport."""
        self._retry_sleep = retry_sleep
        super().__init__(
            transport,
            profile,
            origin=origin,
            credentials=credentials,
            budget=budget,
            breakers=breakers,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown=breaker_cooldown,
            max_attempts=max_attempts,
            clock=clock,
            emitter=emitter,
            capability_cache_ttl=capability_cache_ttl,
            owns_transport=owns_transport,
            probe_runner=probe_runner,
        )

    def site_version(self) -> SiteVersion:
        """Run (or reuse) the anonymous exact-version site probe."""
        if self._closed:
            raise RuntimeError("The synchronous uData client is closed.")
        return self._require_site_version()

    @property
    def datasets(self) -> SyncDatasetsService:
        """Expose the complete typed dataset service."""
        return SyncDatasetsService(self)

    def _require_site_version(self) -> SiteVersion:
        gate = self._site_gate
        if isinstance(gate, SiteVersionGate):
            return gate.require_current(self._credentials)
        raise RuntimeError("The synchronous uData client requires a synchronous site gate.")

    def datasets_list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[UDataResultItem]:
        """Execute the single bounded dataset list read behind the version gate."""
        owning_id = _operation_id_from(_DATASETS_OPERATION_ID)
        return self._dispatch(operation, guard, owning_id=owning_id)

    def _dataset_call(
        self,
        *,
        method: str,
        path: str,
        owning_operation: str,
        query: Mapping[str, str] | None = None,
        json_body: object = None,
        raw_text: bool = False,
        redirect_mode: bool = False,
        permissions: EffectivePermissions | None = None,
        credential: object | None = None,
        idempotency_policy: IdempotencyPolicy | None = None,
        allow_retry: bool = False,
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one guarded dataset request scoped to its owning route operation."""
        if self._closed:
            raise RuntimeError("The synchronous uData client is closed.")
        owning_id = _operation_id_from(owning_operation)
        self._require_site_version()
        resolved_credential = credential if credential is not None else _refreshed_credential(self._credentials)
        scope = _credential_scope(resolved_credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = self._capabilities.resolve(owning_id, credential_scope=scope)
        guard = build_catalog_operation_guard(owning_id, effective, permissions=permissions)
        guard.require_allowed()
        headers = _auth_headers(resolved_credential)
        if idempotency_policy is not None and idempotency_policy.key is not None:
            headers["Idempotency-Key"] = idempotency_policy.key
        try:
            body = json.dumps(json_body, allow_nan=False).encode() if json_body is not None else None
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog mutation input could not be serialized as JSON.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                metadata={"phase": "serialization"},
            ) from exc
        if body is not None:
            headers = {"Content-Type": "application/json", **headers}
        request = RuntimeRequest(method=method, url=self._origin + path, headers=headers, body=body)
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        sync_transport = cast(CatalogTransport, self._transport)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                "The catalog origin circuit is open after consecutive transport failures.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action="Wait for the circuit cool-down or explicitly reset the circuit before retrying.",
            )

        def send() -> RuntimeResponse:
            before = self._breakers.inspect(key)
            try:
                response = sync_transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            self._emit_breaker_change(owning_id, before.open, after.open)
            return response

        try:
            response = RetryLoop(
                budget=self._budget,
                idempotency=idempotency_policy
                or IdempotencyPolicy(safe=method == "GET", explicit_retry_opt_in=allow_retry),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=self._retry_sleep,
            ).run(send)
        except BudgetExhaustedError:
            self._emit(owning_id, "budget_exhausted")
            raise
        except Exception:
            self._emit(owning_id, "failed")
            raise
        self._validate_status(owning_id, response, redirect_mode=redirect_mode, credential_scope=scope)
        if redirect_mode and response.status_code in {301, 302, 303, 307, 308}:
            location = _response_header(response, "location")
            if not location:
                raise NativeCatalogError(
                    "The uData redirect response omits its Location header.",
                    operation=str(owning_id),
                    platform=PLATFORM.value,
                    status_code=response.status_code,
                )
            return response.status_code, dict(response.headers), response
        if raw_text:
            self._emit(owning_id, "succeeded")
            return response.status_code, response.body, response
        if not response.body:
            self._emit(owning_id, "succeeded")
            return response.status_code, None, response
        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog operation returned an invalid JSON result.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                status_code=response.status_code,
                metadata={"ambiguous": json_body is not None and method != "GET"},
            ) from exc
        self._emit(owning_id, "succeeded")
        return response.status_code, payload, response

    def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        *,
        owning_id: OperationId,
    ) -> ResultEnvelope[UDataResultItem]:
        if self._closed:
            raise RuntimeError("The synchronous uData client is closed.")
        _enforce_caller_guards(operation, guard)
        if operation.operation_id != owning_id:
            raise unimplemented_family(str(operation.operation_id))
        self._require_site_version()
        credential = _refreshed_credential(self._credentials)
        scope = _credential_scope(credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = self._capabilities.resolve(owning_id, credential_scope=scope)
        build_catalog_operation_guard(owning_id, effective).require_allowed()
        params = self._validate_page_params(operation)
        request = _page_request(origin=self._origin, params=params)
        if credential is not None:
            request = RuntimeRequest(
                method=request.method,
                url=request.url,
                headers=_auth_headers(credential),
                body=request.body,
            )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        sync_transport = cast(CatalogTransport, self._transport)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                "The catalog origin circuit is open after consecutive transport failures.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action="Wait for the circuit cool-down or explicitly reset the circuit before retrying.",
            )
        attempts = 0

        def send() -> RuntimeResponse:
            nonlocal attempts
            attempts += 1
            before = self._breakers.inspect(key)
            try:
                response = sync_transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            self._emit_breaker_change(owning_id, before.open, after.open)
            return response

        try:
            response = RetryLoop(
                budget=self._budget,
                idempotency=IdempotencyPolicy(safe=True),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=self._retry_sleep,
            ).run(send)
            result = self._decode(owning_id, response, credential_scope=scope)
        except BudgetExhaustedError:
            self._emit(
                owning_id,
                "budget_exhausted",
                budget_usage=max(0.0, self._budget.total - deadline.remaining()),
            )
            raise
        except Exception:
            self._emit(owning_id, "failed", retry_count=max(0, attempts - 1))
            raise
        self._emit(
            owning_id,
            "succeeded",
            retry_count=max(0, attempts - 1),
            budget_usage=max(0.0, self._budget.total - deadline.remaining()),
        )
        return result

    def close(self) -> None:
        """Close the client and its owned transport exactly once."""
        if not self._closed:
            self._closed = True
            if self._owns_transport:
                cast(CatalogTransport, self._transport).close()

    def __enter__(self) -> Self:
        """Enter the client context."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the client context."""
        self.close()


class AsyncUDataClient(_UDataClientCore):
    """Asynchronous strict-version uData client: one anonymous probe, one dataset read."""

    def __init__(
        self,
        transport: AsyncCatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        origin: str,
        credentials: object | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        emitter: EventEmitter | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        owns_transport: bool = True,
        async_probe_runner: AsyncProbeRunner | None = None,
    ) -> None:
        """Build the shared async core over the caller-owned or borrowed transport."""
        self._retry_sleep = retry_sleep
        super().__init__(
            transport,
            profile,
            origin=origin,
            credentials=credentials,
            budget=budget,
            breakers=breakers,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown=breaker_cooldown,
            max_attempts=max_attempts,
            clock=clock,
            emitter=emitter,
            capability_cache_ttl=capability_cache_ttl,
            owns_transport=owns_transport,
            async_probe_runner=async_probe_runner,
            async_gate=True,
        )

    async def site_version(self) -> SiteVersion:
        """Run (or reuse) the anonymous exact-version site probe."""
        if self._closed:
            raise RuntimeError("The asynchronous uData client is closed.")
        gate = self._site_gate
        if isinstance(gate, AsyncSiteVersionGate):
            return await gate.require_current_async(self._credentials)
        raise RuntimeError("The asynchronous uData client requires an asynchronous site gate.")

    @property
    def datasets(self) -> AsyncDatasetsService:
        """Expose the complete typed dataset service."""
        return AsyncDatasetsService(self)

    async def datasets_list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[UDataResultItem]:
        """Execute the single bounded dataset list read behind the version gate."""
        owning_id = _operation_id_from(_DATASETS_OPERATION_ID)
        return await self._dispatch(operation, guard, owning_id=owning_id)

    async def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        *,
        owning_id: OperationId,
    ) -> ResultEnvelope[UDataResultItem]:
        if self._closed:
            raise RuntimeError("The asynchronous uData client is closed.")
        _enforce_caller_guards(operation, guard)
        if operation.operation_id != owning_id:
            raise unimplemented_family(str(operation.operation_id))
        await self.site_version()
        credential = await _refreshed_credential_async(self._credentials)
        scope = _credential_scope(credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = await self._capabilities.resolve_async(owning_id, credential_scope=scope)
        build_catalog_operation_guard(owning_id, effective).require_allowed()
        params = self._validate_page_params(operation)
        request = _page_request(origin=self._origin, params=params)
        if credential is not None:
            request = RuntimeRequest(
                method=request.method,
                url=request.url,
                headers=_auth_headers(credential),
                body=request.body,
            )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        async_transport = cast(AsyncCatalogTransport, self._transport)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                "The catalog origin circuit is open after consecutive transport failures.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action="Wait for the circuit cool-down or explicitly reset the circuit before retrying.",
            )
        attempts = 0

        async def send() -> RuntimeResponse:
            nonlocal attempts
            attempts += 1
            before = self._breakers.inspect(key)
            try:
                response = await async_transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            self._emit_breaker_change(owning_id, before.open, after.open)
            return response

        try:
            response = await RetryLoop(
                budget=self._budget,
                idempotency=IdempotencyPolicy(safe=True),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=lambda _: None,
            ).run_async(send, sleep=self._retry_sleep)
            result = self._decode(owning_id, response, credential_scope=scope)
        except BudgetExhaustedError:
            self._emit(
                owning_id,
                "budget_exhausted",
                budget_usage=max(0.0, self._budget.total - deadline.remaining()),
            )
            raise
        except Exception:
            self._emit(owning_id, "failed", retry_count=max(0, attempts - 1))
            raise
        self._emit(
            owning_id,
            "succeeded",
            retry_count=max(0, attempts - 1),
            budget_usage=max(0.0, self._budget.total - deadline.remaining()),
        )
        return result

    async def _dataset_call_async(
        self,
        *,
        method: str,
        path: str,
        owning_operation: str,
        query: Mapping[str, str] | None = None,
        json_body: object = None,
        raw_text: bool = False,
        redirect_mode: bool = False,
        permissions: EffectivePermissions | None = None,
        credential: object | None = None,
        idempotency_policy: IdempotencyPolicy | None = None,
        allow_retry: bool = False,
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one guarded async dataset request scoped to its owning route operation."""
        if self._closed:
            raise RuntimeError("The asynchronous uData client is closed.")
        owning_id = _operation_id_from(owning_operation)
        await self.site_version()
        resolved_credential = (
            credential if credential is not None else await _refreshed_credential_async(self._credentials)
        )
        scope = _credential_scope(resolved_credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = await self._capabilities.resolve_async(owning_id, credential_scope=scope)
        guard = build_catalog_operation_guard(owning_id, effective, permissions=permissions)
        guard.require_allowed()
        headers = _auth_headers(resolved_credential)
        if idempotency_policy is not None and idempotency_policy.key is not None:
            headers["Idempotency-Key"] = idempotency_policy.key
        try:
            body = json.dumps(json_body, allow_nan=False).encode() if json_body is not None else None
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog mutation input could not be serialized as JSON.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                metadata={"phase": "serialization"},
            ) from exc
        if body is not None:
            headers = {"Content-Type": "application/json", **headers}
        request = RuntimeRequest(method=method, url=self._origin + path, headers=headers, body=body)
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        async_transport = cast(AsyncCatalogTransport, self._transport)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                "The catalog origin circuit is open after consecutive transport failures.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action="Wait for the circuit cool-down or explicitly reset the circuit before retrying.",
            )

        async def send() -> RuntimeResponse:
            before = self._breakers.inspect(key)
            try:
                response = await async_transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            self._emit_breaker_change(owning_id, before.open, after.open)
            return response

        try:
            response = await RetryLoop(
                budget=self._budget,
                idempotency=idempotency_policy
                or IdempotencyPolicy(safe=method == "GET", explicit_retry_opt_in=allow_retry),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=lambda _: None,
            ).run_async(send, sleep=self._retry_sleep)
        except BudgetExhaustedError:
            self._emit(owning_id, "budget_exhausted")
            raise
        except Exception:
            self._emit(owning_id, "failed")
            raise
        self._validate_status(owning_id, response, redirect_mode=redirect_mode, credential_scope=scope)
        if redirect_mode and response.status_code in {301, 302, 303, 307, 308}:
            location = _response_header(response, "location")
            if not location:
                raise NativeCatalogError(
                    "The uData redirect response omits its Location header.",
                    operation=str(owning_id),
                    platform=PLATFORM.value,
                )
            return response.status_code, dict(response.headers), response
        if raw_text:
            self._emit(owning_id, "succeeded")
            return response.status_code, response.body, response
        if not response.body:
            self._emit(owning_id, "succeeded")
            return response.status_code, None, response
        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog operation returned an invalid JSON result.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                status_code=response.status_code,
                metadata={"ambiguous": json_body is not None and method != "GET"},
            ) from exc
        self._emit(owning_id, "succeeded")
        return response.status_code, payload, response

    async def aclose(self) -> None:
        """Close the client and its owned transport exactly once."""
        if not self._closed:
            self._closed = True
            if self._owns_transport:
                await cast(AsyncCatalogTransport, self._transport).aclose()

    async def __aenter__(self) -> Self:
        """Enter the async client context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the async client context."""
        await self.aclose()


def create_sync_client(settings: UDataClientSettings) -> SyncUDataClient:
    """Construct one synchronous uData client from immutable settings."""
    require_extra("udata")
    override = settings.sync_transport
    if override is None:
        transport = create_default_sync_transport(tls_policy=settings.tls_policy, budget=settings.budget)
        owns_transport = True
    elif hasattr(override, "send"):
        transport = cast(CatalogTransport, override)
        owns_transport = False
    else:
        factory = cast("Callable[[], CatalogTransport]", override)
        transport = factory()
        owns_transport = True
    return SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=settings.base_url,
        credentials=settings.credential,
        budget=settings.budget,
        breakers=settings.breakers,
        max_attempts=settings.max_attempts,
        retry_sleep=settings.retry_sleep if settings.retry_sleep is not None else sleep,
        capability_cache_ttl=settings.capability_cache_ttl,
        owns_transport=owns_transport,
        probe_runner=settings.probe_runner,
    )


def create_async_client(settings: UDataClientSettings) -> AsyncUDataClient:
    """Construct one asynchronous uData client from immutable settings."""
    require_extra("udata")
    override = settings.async_transport
    if override is None:
        transport = create_default_async_transport(tls_policy=settings.tls_policy, budget=settings.budget)
        owns_transport = True
    elif hasattr(override, "send"):
        transport = cast(AsyncCatalogTransport, override)
        owns_transport = False
    else:
        factory = cast("Callable[[], AsyncCatalogTransport]", override)
        transport = factory()
        owns_transport = True
    return AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=settings.base_url,
        credentials=settings.credential,
        budget=settings.budget,
        breakers=settings.breakers,
        max_attempts=settings.max_attempts,
        retry_sleep=settings.async_retry_sleep if settings.async_retry_sleep is not None else asyncio.sleep,
        capability_cache_ttl=settings.capability_cache_ttl,
        owns_transport=owns_transport,
        async_probe_runner=settings.async_probe_runner,
    )
