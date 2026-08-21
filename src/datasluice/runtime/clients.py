"""Context-managed sync and async clients for normalized catalog calls."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import fields
from time import monotonic, sleep
from types import TracebackType
from typing import Protocol, Self, cast
from urllib.parse import urlsplit

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import CredentialResolver, SecretValue
from datasluice.domain.catalog.models import DatasetRecord, ResultEnvelope
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    DeclaredCapabilityProfile,
    EffectiveCapabilityProfile,
    EffectiveCapabilityState,
    ProbeResponseClass,
)
from datasluice.domain.catalog.resilience import CircuitKey, TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogUnavailableError,
    NativeCatalogError,
    map_catalog_error,
)
from datasluice.runtime.capability import (
    AsyncProbeRunner,
    EffectiveCapabilityCache,
    ProbeRunner,
    build_catalog_operation_guard,
)
from datasluice.runtime.constants import (
    DEFAULT_BREAKER_COOLDOWN_SECONDS,
    DEFAULT_BREAKER_FAILURE_THRESHOLD,
    DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
    DEFAULT_CONNECT_BUDGET_SECONDS,
    DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    DEFAULT_READ_BUDGET_SECONDS,
    DEFAULT_WRITE_BUDGET_SECONDS,
)
from datasluice.runtime.events import EventEmitter
from datasluice.runtime.resilience import BreakerRegistry, DeadlineMonitor, RetryLoop
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest, RuntimeResponse, TransportFailure
from datasluice.runtime.transport.user_agent import build_user_agent


class AsyncCatalogTransport(Protocol):
    """Async counterpart to the synchronous catalog transport port."""

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one request."""

    async def aclose(self) -> None:
        """Release asynchronous resources."""


def _request_for(operation: CatalogOperationRequest, credential: object | None = None) -> RuntimeRequest:
    url = operation.payload.get("url")
    if not isinstance(url, str) or not url:
        raise ValueError("Catalog runtime requests require a non-empty payload URL.")
    body = operation.payload.get("body")
    if body is not None and not isinstance(body, bytes):
        raise ValueError("Catalog runtime request bodies must be bytes.")
    method = operation.payload.get("method", "GET")
    if not isinstance(method, str):
        raise ValueError("Catalog runtime request methods must be strings.")
    headers = operation.payload.get("headers", {})
    if not isinstance(headers, Mapping):
        raise ValueError("Catalog runtime request headers must be a mapping.")
    access_token = getattr(credential, "access_token", None)
    if isinstance(access_token, SecretValue):
        headers = {"Authorization": f"Bearer {access_token.reveal()}", **headers}
    return RuntimeRequest(method=method, url=url, headers={"User-Agent": build_user_agent(), **headers}, body=body)


def _default_budget() -> TimeBudget:
    return TimeBudget(
        connect=DEFAULT_CONNECT_BUDGET_SECONDS,
        read=DEFAULT_READ_BUDGET_SECONDS,
        write=DEFAULT_WRITE_BUDGET_SECONDS,
        total=DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    )


def _credential_scope(credentials: object | None) -> str:
    credential = credentials.explicit if isinstance(credentials, CredentialResolver) else None
    if credential is None:
        return "anonymous"
    digest = hashlib.sha256()
    for field in fields(credential):
        value = getattr(credential, field.name)
        digest.update(field.name.encode())
        digest.update((value.reveal() if isinstance(value, SecretValue) else str(value)).encode())
    return f"{type(credential).__name__.lower()}-{digest.hexdigest()[:16]}"


def _circuit_key(request: RuntimeRequest, credentials: object | None) -> CircuitKey:
    parsed = urlsplit(request.url)
    return CircuitKey(origin=f"{parsed.scheme}://{parsed.netloc}", credential_scope=_credential_scope(credentials))


def _refreshed_credential(credentials: object | None) -> object | None:
    if credentials is not None and not isinstance(credentials, CredentialResolver):
        resolve = getattr(credentials, "resolve", None)
        if callable(resolve):
            return cast(Callable[[], object], resolve)()
    return None


def _circuit_open_error(operation: CatalogOperationRequest) -> CatalogUnavailableError:
    return CatalogUnavailableError(
        "The catalog origin circuit is open after consecutive transport failures.",
        operation=str(operation.operation_id),
        platform=operation.operation_id.platform,
        capability_state="unavailable",
        safe_action="Wait for the circuit cool-down or explicitly reset the circuit before retrying.",
    )


def _result(
    operation: CatalogOperationRequest,
    response: RuntimeResponse,
    decoder: Callable[[object], object],
    record_response: Callable[[ProbeResponseClass], object] | None = None,
) -> ResultEnvelope[object]:
    if not 200 <= response.status_code < 300:
        response_class = {
            401: ProbeResponseClass.UNAUTHORIZED,
            403: ProbeResponseClass.FORBIDDEN,
        }.get(response.status_code)
        if response_class is not None and record_response is not None:
            record_response(response_class)
        native = NativeCatalogError(
            "Catalog operation returned an unsuccessful HTTP status.",
            operation=str(operation.operation_id),
            platform=operation.operation_id.platform,
            status_code=response.status_code,
            retry_after=response.retry_after,
        )
        raise map_catalog_error(native)
    try:
        payload = json.loads(response.body)
    except (TypeError, ValueError) as exc:
        raise NativeCatalogError(
            "Catalog operation returned an invalid JSON result.",
            operation=str(operation.operation_id),
            platform=operation.operation_id.platform,
        ) from exc
    return ResultEnvelope.from_dict(payload, item_decoder=decoder)


def _enforce_guards(
    operation: CatalogOperationRequest,
    caller_guard: CatalogOperationGuard,
    effective: EffectiveCapabilityProfile | None = None,
    *,
    caller_checked: bool = False,
) -> None:
    if caller_guard.operation_id != operation.operation_id:
        raise ValueError(
            f"Catalog operation guard operation_id {caller_guard.operation_id} does not match request operation_id "
            f"{operation.operation_id}."
        )
    if not caller_checked:
        caller_guard.require_allowed()
    if effective is not None:
        build_catalog_operation_guard(operation.operation_id, effective).require_allowed()


class SyncCatalogClient:
    """Synchronous normalized client over an injected runtime transport."""

    def __init__(
        self,
        transport: CatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        credentials: object | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], None] = sleep,
        emitter: EventEmitter | None = None,
        probe_runner: ProbeRunner | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
    ) -> None:
        self._transport = transport
        self._capabilities = EffectiveCapabilityCache(
            profile,
            probe_runner=probe_runner,
            ttl_seconds=capability_cache_ttl,
            clock=clock,
        )
        self._profile = self._capabilities.baseline_profile
        self._credentials = credentials
        self._budget = budget or _default_budget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold, cooldown=breaker_cooldown, clock=clock
        )
        self._max_attempts = max_attempts
        self._clock = clock
        self._retry_sleep = retry_sleep
        self._emitter = emitter or EventEmitter()
        self._closed = False

    @property
    def datasets(self) -> Self:
        """Return the synchronous dataset service projection."""
        return self

    @property
    def resources(self) -> Self:
        """Return the synchronous resource service projection."""
        return self

    @property
    def organizations(self) -> Self:
        """Return the synchronous organization service projection."""
        return self

    def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        decoder: Callable[[object], object],
    ) -> ResultEnvelope[object]:
        if self._closed:
            raise RuntimeError("The synchronous catalog client is closed.")
        _enforce_guards(operation, guard)
        effective = self._capabilities.resolve(operation.operation_id)
        _enforce_guards(operation, guard, effective, caller_checked=True)
        request = _request_for(operation, _refreshed_credential(self._credentials))
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(operation.operation_id), operation.operation_id.platform)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(operation, "breaker_open")
            raise _circuit_open_error(operation)

        attempts = 0

        def send() -> RuntimeResponse:
            nonlocal attempts
            attempts += 1
            before = self._breakers.inspect(key)
            try:
                response = self._transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(operation, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            self._emit_breaker_change(operation, before.open, after.open)
            return response

        try:
            response = RetryLoop(
                budget=self._budget,
                idempotency=_idempotency_for(operation),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=self._retry_sleep,
            ).run(send)
            result = _result(
                operation,
                response,
                decoder,
                record_response=lambda response_class: self._capabilities.record_response(
                    operation.operation_id, response_class
                ),
            )
        except BudgetExhaustedError:
            self._emit(operation, "budget_exhausted", budget_usage=max(0.0, self._budget.total - deadline.remaining()))
            raise
        except Exception:
            self._emit(operation, "failed", retry_count=max(0, attempts - 1))
            raise
        self._emit(
            operation,
            "succeeded",
            retry_count=max(0, attempts - 1),
            budget_usage=max(0.0, self._budget.total - deadline.remaining()),
        )
        return result

    def _emit(self, operation: CatalogOperationRequest, outcome: str, **metadata: object) -> None:
        self._emitter.record(
            operation_id=str(operation.operation_id),
            platform=operation.operation_id.platform,
            outcome=outcome,
            metadata=metadata,
        )

    def _emit_breaker_change(self, operation: CatalogOperationRequest, before: bool, after: bool) -> None:
        if before != after:
            self._emit(operation, "breaker_state_change", breaker_open=after)

    def get(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[DatasetRecord]:
        """Dispatch a dataset get operation."""
        return cast(ResultEnvelope[DatasetRecord], self._dispatch(operation, guard, DatasetRecord.from_dict))

    def list(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[DatasetRecord]:
        """Dispatch a dataset list operation."""
        return cast(ResultEnvelope[DatasetRecord], self._dispatch(operation, guard, DatasetRecord.from_dict))

    def capability(self, operation_id: str) -> str:
        """Return the effective classification without dispatching transport I/O."""
        operation = _operation_id(operation_id, self._profile)
        state = self._capabilities.resolve(operation).guard(operation).state
        return _capability_value(state)

    def invalidate(self, operation_id: str | OperationId | None = None) -> None:
        """Discard all effective capability state or one operation's state."""
        self._capabilities.invalidate(_coerce_operation_id(operation_id, self._profile))

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe pinned-profile metadata."""
        declared = self._profile.declared_profile
        return {"platform": next(iter(declared.operations)).platform, "profile_version": declared.profile_version}

    def close(self) -> None:
        """Close the owned transport exactly once."""
        if not self._closed:
            self._closed = True
            self._transport.close()

    def __enter__(self) -> Self:
        """Enter the client context."""
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Close the client context."""
        self.close()


class AsyncCatalogClient:
    """Asynchronous normalized client over an injected async transport."""

    def __init__(
        self,
        transport: AsyncCatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        credentials: object | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        emitter: EventEmitter | None = None,
        probe_runner: AsyncProbeRunner | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
    ) -> None:
        self._transport = transport
        self._capabilities = EffectiveCapabilityCache(
            profile,
            async_probe_runner=probe_runner,
            ttl_seconds=capability_cache_ttl,
            clock=clock,
        )
        self._profile = self._capabilities.baseline_profile
        self._credentials = credentials
        self._budget = budget or _default_budget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold, cooldown=breaker_cooldown, clock=clock
        )
        self._max_attempts = max_attempts
        self._clock = clock
        self._retry_sleep = retry_sleep
        self._emitter = emitter or EventEmitter()
        self._closed = False

    @property
    def datasets(self) -> Self:
        """Return the asynchronous dataset service projection."""
        return self

    @property
    def resources(self) -> Self:
        """Return the asynchronous resource service projection."""
        return self

    @property
    def organizations(self) -> Self:
        """Return the asynchronous organization service projection."""
        return self

    async def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        decoder: Callable[[object], object],
    ) -> ResultEnvelope[object]:
        if self._closed:
            raise RuntimeError("The asynchronous catalog client is closed.")
        _enforce_guards(operation, guard)
        effective = await self._capabilities.resolve_async(operation.operation_id)
        _enforce_guards(operation, guard, effective, caller_checked=True)
        credential = None
        if self._credentials is not None and not isinstance(self._credentials, CredentialResolver):
            resolve_async = getattr(self._credentials, "resolve_async", None)
            if callable(resolve_async):
                credential = await resolve_async()
        request = _request_for(operation, credential)
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(operation.operation_id), operation.operation_id.platform)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(operation, "breaker_open")
            raise _circuit_open_error(operation)

        attempts = 0

        async def send() -> RuntimeResponse:
            nonlocal attempts
            attempts += 1
            before = self._breakers.inspect(key)
            try:
                response = await self._transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(operation, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            self._emit_breaker_change(operation, before.open, after.open)
            return response

        try:
            response = await RetryLoop(
                budget=self._budget,
                idempotency=_idempotency_for(operation),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=lambda _: None,
            ).run_async(send, sleep=self._retry_sleep)
            result = _result(
                operation,
                response,
                decoder,
                record_response=lambda response_class: self._capabilities.record_response(
                    operation.operation_id, response_class
                ),
            )
        except BudgetExhaustedError:
            self._emit(operation, "budget_exhausted", budget_usage=max(0.0, self._budget.total - deadline.remaining()))
            raise
        except Exception:
            self._emit(operation, "failed", retry_count=max(0, attempts - 1))
            raise
        self._emit(
            operation,
            "succeeded",
            retry_count=max(0, attempts - 1),
            budget_usage=max(0.0, self._budget.total - deadline.remaining()),
        )
        return result

    def _emit(self, operation: CatalogOperationRequest, outcome: str, **metadata: object) -> None:
        self._emitter.record(
            operation_id=str(operation.operation_id),
            platform=operation.operation_id.platform,
            outcome=outcome,
            metadata=metadata,
        )

    def _emit_breaker_change(self, operation: CatalogOperationRequest, before: bool, after: bool) -> None:
        if before != after:
            self._emit(operation, "breaker_state_change", breaker_open=after)

    async def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[DatasetRecord]:
        """Dispatch an asynchronous dataset get operation."""
        return cast(ResultEnvelope[DatasetRecord], await self._dispatch(operation, guard, DatasetRecord.from_dict))

    async def list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[DatasetRecord]:
        """Dispatch an asynchronous dataset list operation."""
        return cast(ResultEnvelope[DatasetRecord], await self._dispatch(operation, guard, DatasetRecord.from_dict))

    def capability(self, operation_id: str) -> str:
        """Return the cached effective classification without dispatching transport I/O."""
        operation = _operation_id(operation_id, self._profile)
        state = self._capabilities.peek(operation).guard(operation).state
        return _capability_value(state)

    def invalidate(self, operation_id: str | OperationId | None = None) -> None:
        """Discard all effective capability state or one operation's state."""
        self._capabilities.invalidate(_coerce_operation_id(operation_id, self._profile))

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe pinned-profile metadata."""
        declared = self._profile.declared_profile
        return {"platform": next(iter(declared.operations)).platform, "profile_version": declared.profile_version}

    async def aclose(self) -> None:
        """Close the owned asynchronous transport exactly once."""
        if not self._closed:
            self._closed = True
            await self._transport.aclose()

    async def __aenter__(self) -> Self:
        """Enter the asynchronous client context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Close the asynchronous client context."""
        await self.aclose()


def _operation_id(value: str, profile: EffectiveCapabilityProfile) -> OperationId:
    return next(
        (operation_id for operation_id in profile.capabilities if str(operation_id) == value),
        OperationId("unknown", "unknown", "unknown"),
    )


def _coerce_operation_id(value: str | OperationId | None, profile: EffectiveCapabilityProfile) -> OperationId | None:
    if value is None:
        return None
    return value if isinstance(value, OperationId) else _operation_id(value, profile)


def _capability_value(state: EffectiveCapabilityState) -> str:
    return (
        "available"
        if state
        in {
            EffectiveCapabilityState.CORE,
            EffectiveCapabilityState.OPTIONAL,
            EffectiveCapabilityState.AUTHENTICATED,
            EffectiveCapabilityState.ADMIN,
        }
        else state.value
    )


def _idempotency_for(operation: CatalogOperationRequest) -> IdempotencyPolicy:
    return (
        operation.mutation_policy.idempotency if operation.mutation_policy is not None else IdempotencyPolicy(safe=True)
    )
