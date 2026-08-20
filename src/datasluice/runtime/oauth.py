"""OAuth token flows that always use the injected runtime transport."""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from time import time
from typing import Protocol, cast
from urllib.parse import urlencode

from datasluice.domain.catalog.auth import OAuthFlow, SecretValue
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest, RuntimeResponse

PKCE_CHALLENGE_METHOD = "S256"
PKCE_VERIFIER_BYTES = 48


class AsyncTokenTransport(Protocol):
    """Async transport projection used only by asynchronous OAuth flows."""

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one token-endpoint request."""


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


def _token_error(response: RuntimeResponse) -> CatalogValidationError:
    return CatalogValidationError(
        "OAuth token endpoint rejected the request.",
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
        return _token_request(
            self._flow,
            {
                "grant_type": "client_credentials",
                "client_id": self._flow.client_id,
                "client_secret": self._client_secret.reveal(),
            },
        )

    def fetch(self) -> OAuthCredential:
        """Synchronously exchange client credentials through the runtime transport."""
        if not hasattr(self._transport, "close"):
            raise TypeError("The synchronous OAuth flow requires a synchronous runtime transport.")
        transport = cast(CatalogTransport, self._transport)
        return _credential(transport.send(self._request()))

    async def fetch_async(self) -> OAuthCredential:
        """Asynchronously exchange client credentials through the async runtime transport."""
        if not hasattr(self._transport, "aclose"):
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
                "redirect_uri": self._flow.authorization_url,
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
        return _token_request(
            self._flow,
            {
                "grant_type": "authorization_code",
                "client_id": self._flow.client_id,
                "code": code,
                "code_verifier": self._verifier,
            },
        )

    def exchange(self, code: str) -> OAuthCredential:
        """Synchronously exchange an authorization code through the runtime transport."""
        if not hasattr(self._transport, "close"):
            raise TypeError("The synchronous OAuth flow requires a synchronous runtime transport.")
        transport = cast(CatalogTransport, self._transport)
        return _credential(transport.send(self._request(code)))

    async def exchange_async(self, code: str) -> OAuthCredential:
        """Asynchronously exchange an authorization code through the async runtime transport."""
        if not hasattr(self._transport, "aclose"):
            raise TypeError("The asynchronous OAuth flow requires an asynchronous runtime transport.")
        transport = cast(AsyncTokenTransport, self._transport)
        return _credential(await transport.send(self._request(code)))
