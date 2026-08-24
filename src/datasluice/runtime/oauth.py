"""OAuth token flows that always use the injected runtime transport."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import RLock
from time import monotonic, time
from typing import Protocol, cast
from urllib.parse import urlencode, urlsplit

from datasluice.domain.catalog.auth import OAuthFlow, SecretValue
from datasluice.domain.catalog.resilience import CircuitKey, TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogError,
    CatalogUnavailableError,
    CatalogValidationError,
    UnauthenticatedError,
)
from datasluice.runtime.constants import (
    DEFAULT_BREAKER_COOLDOWN_SECONDS,
    DEFAULT_BREAKER_FAILURE_THRESHOLD,
    DEFAULT_CONNECT_BUDGET_SECONDS,
    DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    DEFAULT_READ_BUDGET_SECONDS,
    DEFAULT_WRITE_BUDGET_SECONDS,
)
from datasluice.runtime.events import EventEmitter
from datasluice.runtime.resilience import BreakerRegistry, DeadlineMonitor, RetryLoop
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest, RuntimeResponse

PKCE_CHALLENGE_METHOD = "S256"
PKCE_VERIFIER_BYTES = 48
DEFAULT_REFRESH_SKEW_SECONDS = 30
_RFC6749_ERROR_CODES = frozenset(
    {
        "invalid_request",
        "invalid_client",
        "invalid_grant",
        "unauthorized_client",
        "unsupported_grant_type",
        "invalid_scope",
    }
)


class AsyncTokenTransport(Protocol):
    """Async transport projection used only by asynchronous OAuth flows."""

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one token-endpoint request."""

    async def aclose(self) -> None:
        """Release asynchronous resources."""


def _is_async_transport(transport: object) -> bool:
    return inspect.iscoroutinefunction(getattr(transport, "send", None))


@dataclass(frozen=True, slots=True)
class OAuthCredential:
    """Secret-wrapped OAuth tokens returned by a runtime flow."""

    access_token: SecretValue
    expires_at: float | None
    refresh_token: SecretValue | None = None

    @property
    def expires_in(self) -> int | None:
        """Return the remaining whole token lifetime in seconds."""
        return None if self.expires_at is None else max(0, int(self.expires_at - time()))


def _form(values: Mapping[str, str]) -> bytes:
    return urlencode(values).encode()


def _token_request(flow: OAuthFlow, values: Mapping[str, str]) -> RuntimeRequest:
    return RuntimeRequest(
        method="POST",
        url=flow.token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        body=_form(values),
    )


def _rfc6749_error(response: RuntimeResponse) -> str | None:
    try:
        payload = json.loads(response.body)
    except (TypeError, ValueError):
        return None
    code = payload.get("error") if isinstance(payload, dict) else None
    return code if isinstance(code, str) and code in _RFC6749_ERROR_CODES else None


def _token_error(response: RuntimeResponse) -> CatalogError:
    code = _rfc6749_error(response)
    detail = f"OAuth token endpoint rejected the request with status {response.status_code}."
    if code is not None:
        detail = f"OAuth token endpoint rejected the request ({code}) with status {response.status_code}."
    if response.status_code == 401:
        return UnauthenticatedError(
            detail,
            operation="oauth.token",
            platform="runtime",
            capability_state="unauthorized",
            safe_action="Confirm the OAuth client credentials and retry.",
        )
    return CatalogValidationError(
        detail,
        operation="oauth.token",
        platform="runtime",
        safe_action="Confirm the OAuth client configuration and retry.",
    )


def _credential(response: RuntimeResponse) -> OAuthCredential:
    if not 200 <= response.status_code < 300:
        raise _token_error(response)
    try:
        payload = json.loads(response.body)
    except (TypeError, ValueError) as exc:
        raise CatalogValidationError(
            "OAuth token endpoint returned an invalid JSON response.",
            operation="oauth.token",
            platform="runtime",
            safe_action="Confirm the OAuth endpoint response and retry.",
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("access_token"), str) or not payload["access_token"]:
        raise CatalogValidationError(
            "OAuth token endpoint response did not contain an access token.",
            operation="oauth.token",
            platform="runtime",
            safe_action="Confirm the OAuth endpoint response and retry.",
        )
    expires_in = payload.get("expires_in")
    if expires_in is not None and (type(expires_in) not in (int, float) or expires_in < 0):
        raise CatalogValidationError(
            "OAuth token endpoint returned an invalid expiry.",
            operation="oauth.token",
            platform="runtime",
            safe_action="Confirm the OAuth endpoint response and retry.",
        )
    refresh_token = payload.get("refresh_token")
    if refresh_token is not None and (not isinstance(refresh_token, str) or not refresh_token):
        raise CatalogValidationError(
            "OAuth token endpoint returned an invalid refresh token.",
            operation="oauth.token",
            platform="runtime",
            safe_action="Confirm the OAuth endpoint response and retry.",
        )
    return OAuthCredential(
        access_token=SecretValue(payload["access_token"]),
        expires_at=None if expires_in is None else time() + float(expires_in),
        refresh_token=None if refresh_token is None else SecretValue(refresh_token),
    )


class ClientCredentialsFlow:
    """Exchange a client secret for an OAuth access token."""

    def __init__(
        self, flow: OAuthFlow, client_secret: SecretValue, transport: CatalogTransport | AsyncTokenTransport
    ) -> None:
        if not isinstance(flow, OAuthFlow) or not isinstance(client_secret, SecretValue):
            raise TypeError("Client credential flows require an OAuthFlow and SecretValue client secret.")
        self._flow = flow
        self._client_secret = client_secret
        self._transport = transport

    def _request(self) -> RuntimeRequest:
        values: dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self._flow.client_id,
            "client_secret": self._client_secret.reveal(),
        }
        if self._flow.scopes:
            values["scope"] = " ".join(sorted(self._flow.scopes))
        return _token_request(self._flow, values)

    def fetch(self) -> OAuthCredential:
        """Synchronously exchange client credentials through the runtime transport."""
        if _is_async_transport(self._transport):
            raise TypeError("The synchronous OAuth flow requires a synchronous runtime transport.")
        transport = cast(CatalogTransport, self._transport)
        return _credential(transport.send(self._request()))

    async def fetch_async(self) -> OAuthCredential:
        """Asynchronously exchange client credentials through the async runtime transport."""
        if not _is_async_transport(self._transport):
            raise TypeError("The asynchronous OAuth flow requires an asynchronous runtime transport.")
        transport = cast(AsyncTokenTransport, self._transport)
        return _credential(await transport.send(self._request()))


class AuthorizationCodeFlow:
    """Authorization-code OAuth flow with an RFC 7636 S256 proof key."""

    def __init__(self, flow: OAuthFlow, transport: CatalogTransport | AsyncTokenTransport) -> None:
        if not isinstance(flow, OAuthFlow):
            raise TypeError("Authorization-code flows require an OAuthFlow.")
        self._flow = flow
        self._transport = transport
        self._verifier = secrets.token_urlsafe(PKCE_VERIFIER_BYTES)

    @property
    def verifier(self) -> str:
        """Return the generated PKCE verifier for this one flow instance."""
        return self._verifier

    def authorization_url(self, *, state: str) -> str:
        """Build the authorization URL with the mandatory S256 challenge."""
        if not isinstance(state, str) or not state:
            raise ValueError("OAuth authorization state must be a non-empty string.")
        challenge = base64.urlsafe_b64encode(hashlib.sha256(self._verifier.encode()).digest()).rstrip(b"=").decode()
        query = urlencode(
            {
                "response_type": "code",
                "client_id": self._flow.client_id,
                **({"redirect_uri": self._flow.redirect_uri} if self._flow.redirect_uri is not None else {}),
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": PKCE_CHALLENGE_METHOD,
                **({"scope": " ".join(sorted(self._flow.scopes))} if self._flow.scopes else {}),
            }
        )
        return f"{self._flow.authorization_url}?{query}"

    def _request(self, code: str) -> RuntimeRequest:
        if not isinstance(code, str) or not code:
            raise ValueError("OAuth authorization codes must be non-empty strings.")
        values: dict[str, str] = {
            "grant_type": "authorization_code",
            "client_id": self._flow.client_id,
            "code": code,
            "code_verifier": self._verifier,
        }
        if self._flow.redirect_uri is not None:
            values["redirect_uri"] = self._flow.redirect_uri
        return _token_request(self._flow, values)

    def exchange(self, code: str) -> OAuthCredential:
        """Synchronously exchange an authorization code through the runtime transport."""
        if _is_async_transport(self._transport):
            raise TypeError("The synchronous OAuth flow requires a synchronous runtime transport.")
        transport = cast(CatalogTransport, self._transport)
        return _credential(transport.send(self._request(code)))

    async def exchange_async(self, code: str) -> OAuthCredential:
        """Asynchronously exchange an authorization code through the async runtime transport."""
        if not _is_async_transport(self._transport):
            raise TypeError("The asynchronous OAuth flow requires an asynchronous runtime transport.")
        transport = cast(AsyncTokenTransport, self._transport)
        return _credential(await transport.send(self._request(code)))


def _default_budget() -> TimeBudget:
    return TimeBudget(
        connect=DEFAULT_CONNECT_BUDGET_SECONDS,
        read=DEFAULT_READ_BUDGET_SECONDS,
        write=DEFAULT_WRITE_BUDGET_SECONDS,
        total=DEFAULT_OPERATION_TOTAL_BUDGET_SECONDS,
    )


def _refresh_key(flow: OAuthFlow, credential: OAuthCredential) -> CircuitKey:
    refresh_token = credential.refresh_token
    assert refresh_token is not None
    digest = hashlib.sha256(refresh_token.reveal().encode()).hexdigest()[:16]
    parsed = urlsplit(flow.token_url)
    return CircuitKey(origin=f"{parsed.scheme}://{parsed.netloc}", credential_scope=f"oauth-{digest}")


class RefreshingCredentialProvider:
    """Refresh a near-expiry OAuth credential once through the runtime pipeline."""

    def __init__(
        self,
        flow: OAuthFlow,
        credential: OAuthCredential,
        transport: CatalogTransport | AsyncTokenTransport,
        *,
        skew_seconds: float = DEFAULT_REFRESH_SKEW_SECONDS,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        emitter: EventEmitter | None = None,
        clock: Callable[[], float] = time,
        monotonic_clock: Callable[[], float] = monotonic,
    ) -> None:
        if not isinstance(flow, OAuthFlow) or not isinstance(credential, OAuthCredential):
            raise TypeError("Refreshing providers require an OAuthFlow and OAuthCredential.")
        if credential.refresh_token is None or not flow.supports_refresh:
            raise ValueError("Refreshing providers require a refresh-capable flow and refresh token.")
        if type(skew_seconds) not in (int, float) or skew_seconds < 0:
            raise ValueError("OAuth refresh skew must be a non-negative number.")
        if not callable(clock) or not callable(monotonic_clock):
            raise TypeError("OAuth refresh providers require clock callables.")
        self._flow = flow
        self._credential = credential
        self._transport = transport
        self._skew_seconds = float(skew_seconds)
        self._budget = budget or _default_budget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=DEFAULT_BREAKER_FAILURE_THRESHOLD,
            cooldown=DEFAULT_BREAKER_COOLDOWN_SECONDS,
            clock=monotonic_clock,
        )
        self._emitter = emitter or EventEmitter()
        self._clock = clock
        self._monotonic_clock = monotonic_clock
        self._sync_lock = RLock()
        self._async_lock: asyncio.Lock | None = None
        self._async_lock_loop: asyncio.AbstractEventLoop | None = None

    def _needs_refresh(self) -> bool:
        return (
            self._credential.expires_at is not None
            and self._credential.expires_at <= self._clock() + self._skew_seconds
        )

    def _async_lock_for(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        lock = self._async_lock
        if lock is None or self._async_lock_loop is not loop:
            lock = asyncio.Lock()
            self._async_lock = lock
            self._async_lock_loop = loop
        return lock

    def _request(self) -> RuntimeRequest:
        refresh_token = self._credential.refresh_token
        assert refresh_token is not None
        return _token_request(
            self._flow,
            {
                "grant_type": "refresh_token",
                "client_id": self._flow.client_id,
                "refresh_token": refresh_token.reveal(),
            },
        )

    def _emit(self, outcome: str, **metadata: object) -> None:
        self._emitter.record(operation_id="oauth.refresh", platform="runtime", outcome=outcome, metadata=metadata)

    def _updated_credential(self, response: RuntimeResponse) -> OAuthCredential:
        updated = _credential(response)
        return OAuthCredential(
            access_token=updated.access_token,
            expires_at=updated.expires_at,
            refresh_token=updated.refresh_token or self._credential.refresh_token,
        )

    def _refresh_sync(self) -> OAuthCredential:
        transport = cast(CatalogTransport, self._transport)
        key = _refresh_key(self._flow, self._credential)
        if not self._breakers.admit(key):
            self._emit("breaker_open")
            raise CatalogUnavailableError(
                "The OAuth token endpoint circuit is open.",
                operation="oauth.refresh",
                platform="runtime",
                capability_state="unavailable",
                safe_action="Wait for the circuit cool-down before refreshing credentials.",
            )
        deadline = DeadlineMonitor(self._budget, clock=self._monotonic_clock)
        before = self._breakers.inspect(key)
        try:
            deadline.assert_dispatchable("oauth.refresh", "runtime")
            response = RetryLoop(
                budget=self._budget,
                idempotency=IdempotencyPolicy(safe=True),
                deadline=deadline,
                max_attempts=1,
                sleep=lambda _: None,
            ).run(lambda: transport.send(self._request()))
            deadline.assert_dispatchable()
        except BudgetExhaustedError:
            self._breakers.release_trial(key)
            self._emit("budget_exhausted")
            raise
        except Exception:
            after = self._breakers.record_transport_failure(key)
            if before.open != after.open:
                self._emit("breaker_state_change", breaker_open=after.open)
            self._emit("failed")
            raise
        after = self._breakers.record_response(key, response.status_code)
        if before.open != after.open:
            self._emit("breaker_state_change", breaker_open=after.open)
        try:
            refreshed = self._updated_credential(response)
        except Exception:
            self._emit("failed")
            raise
        self._emit("succeeded")
        return refreshed

    def resolve(self) -> OAuthCredential:
        """Return the current credential, refreshing it once when it is near expiry."""
        with self._sync_lock:
            if self._needs_refresh():
                if _is_async_transport(self._transport):
                    raise TypeError("The synchronous refresh provider requires a synchronous runtime transport.")
                self._credential = self._refresh_sync()
            return self._credential

    async def _refresh_async(self) -> OAuthCredential:
        transport = cast(AsyncTokenTransport, self._transport)
        key = _refresh_key(self._flow, self._credential)
        if not self._breakers.admit(key):
            self._emit("breaker_open")
            raise CatalogUnavailableError(
                "The OAuth token endpoint circuit is open.",
                operation="oauth.refresh",
                platform="runtime",
                capability_state="unavailable",
                safe_action="Wait for the circuit cool-down before refreshing credentials.",
            )
        deadline = DeadlineMonitor(self._budget, clock=self._monotonic_clock)
        before = self._breakers.inspect(key)
        try:
            deadline.assert_dispatchable("oauth.refresh", "runtime")
            response = await RetryLoop(
                budget=self._budget,
                idempotency=IdempotencyPolicy(safe=True),
                deadline=deadline,
                max_attempts=1,
                sleep=lambda _: None,
            ).run_async(lambda: transport.send(self._request()), sleep=asyncio.sleep)
            deadline.assert_dispatchable()
        except BudgetExhaustedError:
            self._breakers.release_trial(key)
            self._emit("budget_exhausted")
            raise
        except Exception:
            after = self._breakers.record_transport_failure(key)
            if before.open != after.open:
                self._emit("breaker_state_change", breaker_open=after.open)
            self._emit("failed")
            raise
        after = self._breakers.record_response(key, response.status_code)
        if before.open != after.open:
            self._emit("breaker_state_change", breaker_open=after.open)
        try:
            refreshed = self._updated_credential(response)
        except Exception:
            self._emit("failed")
            raise
        self._emit("succeeded")
        return refreshed

    async def resolve_async(self) -> OAuthCredential:
        """Asynchronously return the current credential, refreshing only on the async transport."""
        async with self._async_lock_for():
            with self._sync_lock:
                if self._needs_refresh():
                    if not _is_async_transport(self._transport):
                        raise TypeError("The asynchronous refresh provider requires an asynchronous runtime transport.")
                    self._credential = await self._refresh_async()
                return self._credential
