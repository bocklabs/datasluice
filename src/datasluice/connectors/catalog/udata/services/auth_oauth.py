"""Typed sync and async OAuth and authentication services for stock uData /oauth routes."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.oauth import (
    OAuthAuthorizeDecision,
    OAuthClientRequest,
    OAuthConsentOutcome,
    OAuthConsentSummary,
    OAuthErrorDocument,
    OAuthRevokeRequest,
    OAuthTokenRequest,
    OAuthTokenResult,
)
from datasluice.connectors.catalog.udata.wire import oauth as wire
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import NativeCatalogError
from datasluice.runtime.transport.base import RuntimeResponse

from .datasets import _enforce_mutation_policy, _error_status, _mutation_outcome, _require_mutation_permission
from .organizations_memberships import _attach, _receipt

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

type Permissions = EffectivePermissions
type Policy = MutationPolicy | None

# wire.sends_credential is the single credential-transmission contract for this
# family, so the uData X-API-KEY never reaches a route that does not authenticate
# with it. That keeps OAuth and API-token credentials strictly distinct.
_DESTRUCTIVE = frozenset({"revoke_token"})
_KIND = ResourceKind("oauth-token")


def _target(name: str) -> str:
    """Bind every OAuth mutation receipt to a bounded, non-secret target."""
    return "self" if name == "authorize_post" else f"request:{name}"


def _scrub(response: RuntimeResponse | None) -> RuntimeResponse | None:
    """Drop the retained response body so no form secret survives a failure."""
    return None if response is None else RuntimeResponse(status_code=response.status_code, headers={}, body=b"")


def _json_or_none(payload: object) -> object:
    """raw_text returns bytes; only a JSON media body becomes a decoded document."""
    if isinstance(payload, bytes):
        try:
            return json.loads(payload)
        except ValueError:
            return None
    return payload


def _media_type(headers: Mapping[str, str]) -> str:
    for key, value in headers.items():
        if key.lower() == "content-type":
            return value
    return "application/octet-stream"


def _success_receipt(operation: str, target: str, policy: Policy, status: int) -> MutationReceipt:
    """Build the redacted receipt for one successful OAuth mutation."""
    return _receipt(operation, target, policy, "succeeded", status, "oauth", kind=_KIND)


def _token_result(name: str, payload: object, receipt: MutationReceipt) -> OAuthTokenResult:
    return wire.parse_token(name, payload, receipt)


def _revocation_result(payload: object, receipt: MutationReceipt) -> OAuthTokenResult:
    """RFC 7009 revocation answers 200 with an empty body and no document."""
    return (
        OAuthTokenResult(receipt=receipt, token_type="Bearer")
        if payload is None
        else wire.parse_token("revoke_token", payload, receipt)
    )


def _consent_outcome(accepted: bool, status: int, media_type: str) -> OAuthConsentOutcome:
    """Report the status stock returned instead of inventing a consent document."""
    return OAuthConsentOutcome(
        accepted=accepted, status_code=status, media_type=media_type.split(";", 1)[0].strip().lower()
    )


def _error_receipt(
    error: BaseException, name: str, policy: Policy, response: RuntimeResponse | None, *, interrupted: bool
) -> None:
    operation = wire.OPERATIONS[name]
    outcome = "ambiguous" if interrupted else _mutation_outcome(error, response)
    _attach(
        error, _receipt(operation, _target(name), policy, outcome, _error_status(error, response), name, kind=_KIND)
    )


class SyncAuthOAuthService:
    """Named synchronous stock uData /oauth routes."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def _read(self, name: str, query: OAuthClientRequest, permissions: Permissions) -> tuple[int, object, str]:
        operation = wire.OPERATIONS[name]
        credential = _require_mutation_permission(self._client._resolved_credential(), operation, permissions)
        method, path, headers, body = wire.build_request(name, query)
        try:
            status, payload, response = self._client._dataset_call(
                method=method,
                path=path,
                headers=headers,
                owning_operation=operation,
                permissions=permissions,
                credential=credential,
                raw_text=True,
                emit_success=False,
                form_body=body,
                omit_credential=not wire.sends_credential(name),
            )
        except (Exception, KeyboardInterrupt):
            self._client._emit(operation, "failed")
            raise
        self._client._emit(operation, "succeeded")
        return status, payload, _media_type(response.headers)

    def _write(
        self, name: str, body: object, permissions: Permissions, mutation_policy: Policy
    ) -> tuple[object, int, str]:
        operation = wire.OPERATIONS[name]
        response: RuntimeResponse | None = None
        payload: object = None
        try:
            if name != "access_token":
                _enforce_mutation_policy(operation, _target(name), mutation_policy, destructive=name in _DESTRUCTIVE)
            credential = _require_mutation_permission(self._client._resolved_credential(), operation, permissions)
            method, path, headers, encoded = wire.build_request(name, body)
            status, payload, response = self._client._dataset_call(
                method=method,
                path=path,
                headers=headers,
                owning_operation=operation,
                permissions=permissions,
                credential=credential,
                idempotency_policy=mutation_policy.idempotency if mutation_policy else None,
                emit_success=False,
                form_body=encoded,
                omit_credential=not wire.sends_credential(name),
            )
        except BaseException as error:
            self._client._emit(operation, "failed")
            _error_receipt(
                error, name, mutation_policy, _scrub(response), interrupted=isinstance(error, KeyboardInterrupt)
            )
            raise
        self._client._emit(operation, "succeeded")
        return payload, status, _media_type(response.headers if response is not None else {})

    def access_token(self, body: OAuthTokenRequest, permissions: Permissions) -> OAuthTokenResult:
        """Exchange one typed grant for a token without retaining its secrets."""
        payload, status, _ = self._write("access_token", body, permissions, None)
        return _token_result(
            "access_token",
            payload,
            _success_receipt(wire.OPERATIONS["access_token"], _target("access_token"), None, status),
        )

    def revoke_token(
        self, body: OAuthRevokeRequest, permissions: Permissions, mutation_policy: Policy = None
    ) -> OAuthTokenResult:
        """Revoke one typed token under an explicit confirmed mutation policy."""
        payload, status, _ = self._write("revoke_token", body, permissions, mutation_policy)
        receipt = _success_receipt(wire.OPERATIONS["revoke_token"], _target("revoke_token"), mutation_policy, status)
        return _revocation_result(payload, receipt)

    def client_info(self, query: OAuthClientRequest, permissions: Permissions) -> OAuthConsentSummary:
        status, payload, media_type = self._read("client_info", query, permissions)
        return wire.parse_consent_summary("client_info", _json_or_none(payload), status, media_type)

    def authorize(self, query: OAuthClientRequest, permissions: Permissions) -> OAuthConsentSummary:
        """Read the consent page for one client, sending the query stock requires."""
        status, payload, media_type = self._read("authorize", query, permissions)
        return wire.parse_consent_summary("authorize", _json_or_none(payload), status, media_type)

    def authorize_post(
        self,
        body: OAuthAuthorizeDecision,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OAuthConsentOutcome:
        """Submit one consent decision and report the status stock returned."""
        _payload, status, media_type = self._write("authorize_post", body, permissions, mutation_policy)
        return _consent_outcome(body.accept, status, media_type)

    def oauth_error(self) -> OAuthErrorDocument:
        """Read the one public OAuth route without credential evidence."""
        method, path, headers, _ = wire.build_request("oauth_error")
        status, _, response = self._client._dataset_call(
            method=method,
            path=path,
            headers=headers,
            owning_operation=wire.OPERATIONS["oauth_error"],
            raw_text=True,
            emit_success=False,
            omit_credential=not wire.sends_credential("oauth_error"),
            accept_status=wire.is_error_page,
        )
        return wire.parse_error_document("oauth_error", status, _media_type(response.headers))


class AsyncAuthOAuthService:
    """Named asynchronous stock uData /oauth routes."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def _read_async(
        self, name: str, query: OAuthClientRequest, permissions: Permissions
    ) -> tuple[int, object, str]:
        operation = wire.OPERATIONS[name]
        credential = _require_mutation_permission(
            await self._client._resolved_credential_async(), operation, permissions
        )
        method, path, headers, body = wire.build_request(name, query)
        try:
            status, payload, response = await self._client._dataset_call_async(
                method=method,
                path=path,
                headers=headers,
                owning_operation=operation,
                permissions=permissions,
                credential=credential,
                raw_text=True,
                emit_success=False,
                form_body=body,
                omit_credential=not wire.sends_credential(name),
            )
        except (Exception, asyncio.CancelledError):
            self._client._emit(operation, "failed")
            raise
        self._client._emit(operation, "succeeded")
        return status, payload, _media_type(response.headers)

    async def _write_async(
        self, name: str, body: object, permissions: Permissions, mutation_policy: Policy
    ) -> tuple[object, int, str]:
        operation = wire.OPERATIONS[name]
        response: RuntimeResponse | None = None
        payload: object = None
        try:
            if name != "access_token":
                _enforce_mutation_policy(operation, _target(name), mutation_policy, destructive=name in _DESTRUCTIVE)
            credential = _require_mutation_permission(
                await self._client._resolved_credential_async(), operation, permissions
            )
            method, path, headers, encoded = wire.build_request(name, body)
            status, payload, response = await self._client._dataset_call_async(
                method=method,
                path=path,
                headers=headers,
                owning_operation=operation,
                permissions=permissions,
                credential=credential,
                idempotency_policy=mutation_policy.idempotency if mutation_policy else None,
                emit_success=False,
                form_body=encoded,
                omit_credential=not wire.sends_credential(name),
            )
        except BaseException as error:
            self._client._emit(operation, "failed")
            _error_receipt(
                error, name, mutation_policy, _scrub(response), interrupted=isinstance(error, KeyboardInterrupt)
            )
            raise
        self._client._emit(operation, "succeeded")
        return payload, status, _media_type(response.headers if response is not None else {})

    async def access_token(self, body: OAuthTokenRequest, permissions: Permissions) -> OAuthTokenResult:
        """Exchange one typed grant for a token without retaining its secrets."""
        payload, status, _ = await self._write_async("access_token", body, permissions, None)
        return _token_result(
            "access_token",
            payload,
            _success_receipt(wire.OPERATIONS["access_token"], _target("access_token"), None, status),
        )

    async def revoke_token(
        self, body: OAuthRevokeRequest, permissions: Permissions, mutation_policy: Policy = None
    ) -> OAuthTokenResult:
        """Revoke one typed token under an explicit confirmed mutation policy."""
        payload, status, _ = await self._write_async("revoke_token", body, permissions, mutation_policy)
        receipt = _success_receipt(wire.OPERATIONS["revoke_token"], _target("revoke_token"), mutation_policy, status)
        return _revocation_result(payload, receipt)

    async def client_info(self, query: OAuthClientRequest, permissions: Permissions) -> OAuthConsentSummary:
        status, payload, media_type = await self._read_async("client_info", query, permissions)
        return wire.parse_consent_summary("client_info", _json_or_none(payload), status, media_type)

    async def authorize(self, query: OAuthClientRequest, permissions: Permissions) -> OAuthConsentSummary:
        """Read the consent page for one client, sending the query stock requires."""
        status, payload, media_type = await self._read_async("authorize", query, permissions)
        return wire.parse_consent_summary("authorize", _json_or_none(payload), status, media_type)

    async def authorize_post(
        self,
        body: OAuthAuthorizeDecision,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OAuthConsentOutcome:
        """Submit one consent decision and report the status stock returned."""
        _payload, status, media_type = await self._write_async("authorize_post", body, permissions, mutation_policy)
        return _consent_outcome(body.accept, status, media_type)

    async def oauth_error(self) -> OAuthErrorDocument:
        """Read the one public OAuth route without credential evidence."""
        method, path, headers, _ = wire.build_request("oauth_error")
        status, _, response = await self._client._dataset_call_async(
            method=method,
            path=path,
            headers=headers,
            owning_operation=wire.OPERATIONS["oauth_error"],
            raw_text=True,
            emit_success=False,
            omit_credential=not wire.sends_credential("oauth_error"),
            accept_status=wire.is_error_page,
        )
        return wire.parse_error_document("oauth_error", status, _media_type(response.headers))
