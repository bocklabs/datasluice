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
    EffectiveOperationCapability,
)
from datasluice.domain.catalog.resilience import CircuitKey, TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import CatalogUnavailableError, NativeCatalogError, map_catalog_error
from datasluice.runtime.constants import (
    DEFAULT_BREAKER_COOLDOWN_SECONDS,
    DEFAULT_BREAKER_FAILURE_THRESHOLD,
    DEFAULT_CONNECT_BUDGET_SECONDS,
    DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    DEFAULT_READ_BUDGET_SECONDS,
    DEFAULT_WRITE_BUDGET_SECONDS,
)
from datasluice.runtime.resilience import BreakerRegistry, DeadlineMonitor, RetryLoop
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest, RuntimeResponse, TransportFailure
from datasluice.runtime.transport.user_agent import build_user_agent


class AsyncCatalogTransport(Protocol):
    """Async counterpart to the synchronous catalog transport port."""

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one request."""

    async def aclose(self) -> None:
        """Release asynchronous resources."""


def _effective_profile(profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile) -> EffectiveCapabilityProfile:
    if isinstance(profile, EffectiveCapabilityProfile):
        return profile
    states = {
        "core": EffectiveCapabilityState.CORE,
        "optional": EffectiveCapabilityState.OPTIONAL,
        "authenticated": EffectiveCapabilityState.AUTHENTICATED,
        "admin": EffectiveCapabilityState.ADMIN,
    }
    return EffectiveCapabilityProfile(
        declared_profile=profile,
        capabilities={
            operation_id: EffectiveOperationCapability(
                operation=operation, state=states[operation.capability_class.value]
            )
            for operation_id, operation in profile.operations.items()
        },
    )


def _request_for(operation: CatalogOperationRequest) -> RuntimeRequest:
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
    return RuntimeRequest(method=method, url=url, headers={"User-Agent": build_user_agent(), **headers}, body=body)


def _default_budget() -> TimeBudget:
    return TimeBudget(
        connect=DEFAULT_CONNECT_BUDGET_SECONDS,
        read=DEFAULT_READ_BUDGET_SECONDS,
        write=DEFAULT_WRITE_BUDGET_SECONDS,
        total=DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    )


def _credential_scope(credentials: CredentialResolver | None) -> str:
    credential = credentials.explicit if credentials is not None else None
    if credential is None:
        return "anonymous"
    digest = hashlib.sha256()
    for field in fields(credential):
        value = getattr(credential, field.name)
        digest.update(field.name.encode())
        digest.update((value.reveal() if isinstance(value, SecretValue) else str(value)).encode())
    return f"{type(credential).__name__.lower()}-{digest.hexdigest()[:16]}"


def _circuit_key(request: RuntimeRequest, credentials: CredentialResolver | None) -> CircuitKey:
    parsed = urlsplit(request.url)
    return CircuitKey(origin=f"{parsed.scheme}://{parsed.netloc}", credential_scope=_credential_scope(credentials))


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
) -> ResultEnvelope[object]:
    if not 200 <= response.status_code < 300:
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


class SyncCatalogClient:
    """Synchronous normalized client over an injected runtime transport."""

    def __init__(
        self,
        transport: CatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        credentials: CredentialResolver | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], None] = sleep,
    ) -> None:
        self._transport = transport
        self._profile = _effective_profile(profile)
        self._credentials = credentials
        self._budget = budget or _default_budget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold, cooldown=breaker_cooldown, clock=clock
        )
        self._max_attempts = max_attempts
        self._clock = clock
        self._retry_sleep = retry_sleep
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
        decoder: Callable[[object], object],
    ) -> ResultEnvelope[object]:
        if self._closed:
            raise RuntimeError("The synchronous catalog client is closed.")
        CatalogOperationGuard(operation.operation_id, profile=self._profile).require_allowed()
        request = _request_for(operation)
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(operation.operation_id), operation.operation_id.platform)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            raise _circuit_open_error(operation)

        def send() -> RuntimeResponse:
            try:
                response = self._transport.send(request)
            except TransportFailure:
                self._breakers.record_transport_failure(key)
                raise
            self._breakers.record_response(key, response.status_code)
            return response

        response = RetryLoop(
            budget=self._budget,
            idempotency=_idempotency_for(operation),
            deadline=deadline,
            max_attempts=self._max_attempts,
            sleep=self._retry_sleep,
        ).run(send)
        return _result(operation, response, decoder)

    def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard | None = None
    ) -> ResultEnvelope[DatasetRecord]:
        """Dispatch a dataset get operation."""
        return cast(ResultEnvelope[DatasetRecord], self._dispatch(operation, DatasetRecord.from_dict))

    def list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard | None = None
    ) -> ResultEnvelope[DatasetRecord]:
        """Dispatch a dataset list operation."""
        return cast(ResultEnvelope[DatasetRecord], self._dispatch(operation, DatasetRecord.from_dict))

    def capability(self, operation_id: str) -> str:
        """Return the non-dispatching profile classification."""
        return "available" if self._profile.guard(_operation_id(operation_id, self._profile)).allowed else "unavailable"

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
        credentials: CredentialResolver | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._transport = transport
        self._profile = _effective_profile(profile)
        self._credentials = credentials
        self._budget = budget or _default_budget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold, cooldown=breaker_cooldown, clock=clock
        )
        self._max_attempts = max_attempts
        self._clock = clock
        self._retry_sleep = retry_sleep
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
        decoder: Callable[[object], object],
    ) -> ResultEnvelope[object]:
        if self._closed:
            raise RuntimeError("The asynchronous catalog client is closed.")
        CatalogOperationGuard(operation.operation_id, profile=self._profile).require_allowed()
        request = _request_for(operation)
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(operation.operation_id), operation.operation_id.platform)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            raise _circuit_open_error(operation)

        async def send() -> RuntimeResponse:
            try:
                response = await self._transport.send(request)
            except TransportFailure:
                self._breakers.record_transport_failure(key)
                raise
            self._breakers.record_response(key, response.status_code)
            return response

        response = await RetryLoop(
            budget=self._budget,
            idempotency=_idempotency_for(operation),
            deadline=deadline,
            max_attempts=self._max_attempts,
            sleep=lambda _: None,
        ).run_async(send, sleep=self._retry_sleep)
        return _result(operation, response, decoder)

    async def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard | None = None
    ) -> ResultEnvelope[DatasetRecord]:
        """Dispatch an asynchronous dataset get operation."""
        return cast(ResultEnvelope[DatasetRecord], await self._dispatch(operation, DatasetRecord.from_dict))

    async def list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard | None = None
    ) -> ResultEnvelope[DatasetRecord]:
        """Dispatch an asynchronous dataset list operation."""
        return cast(ResultEnvelope[DatasetRecord], await self._dispatch(operation, DatasetRecord.from_dict))

    def capability(self, operation_id: str) -> str:
        """Return the non-dispatching profile classification."""
        return "available" if self._profile.guard(_operation_id(operation_id, self._profile)).allowed else "unavailable"

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


def _idempotency_for(operation: CatalogOperationRequest) -> IdempotencyPolicy:
    return (
        operation.mutation_policy.idempotency if operation.mutation_policy is not None else IdempotencyPolicy(safe=True)
    )
