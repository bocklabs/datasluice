"""PROHIB-04-01 deployment boundary for uData dispatch.

A public deployment may serve declared reads, and refuses every non-read route
before any request is built.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.oauth import OAuthClientRequest, OAuthRevokeRequest
from datasluice.connectors.catalog.udata.models.users import ApiTokenCreateInput
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.connectors.catalog.udata.wire import oauth as oauth_wire
from datasluice.connectors.catalog.udata.wire import users as users_wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.operations import MutationClass
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogError, CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

PUBLIC = "https://www.data.gouv.fr"
_LOCAL = "http://127.0.0.1:5640"
_JSON = "application/json"
SITE = {"id": "site", "title": "uData", "version": "17.6.0"}
CREDENTIAL = UDataCredential(api_key="unit-credential")
PERMISSIONS = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA)


class _Router:
    def __init__(self, origin: str) -> None:
        self._origin = origin
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        body = SITE if request.url.endswith("/api/1/site/") else {"client": {"name": "Portal"}, "scopes": ["default"]}
        return RuntimeResponse(status_code=200, headers={"Content-Type": _JSON}, body=json.dumps(body).encode())

    def close(self) -> None:
        return None


class _AsyncRouter:
    def __init__(self, origin: str) -> None:
        self._sync = _Router(origin)
        self.requests = self._sync.requests

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._sync.send(request)

    async def aclose(self) -> None:
        return None


def _confirmed(operation: str, target: str, *, destructive: bool = True) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def test_public_origin_refuses_a_confirmed_destructive_oauth_revocation() -> None:
    router = _Router(PUBLIC)
    client = SyncUDataClient(router, declared_udata_profile(), origin=PUBLIC, credentials=CREDENTIAL)
    with client, pytest.raises(CatalogError, match="controlled local deployment"):
        client.auth_oauth.revoke_token(
            OAuthRevokeRequest(token="opaque-access-token"),
            PERMISSIONS,
            _confirmed(oauth_wire.OPERATIONS["revoke_token"], "request:revoke_token"),
        )
    assert all(request.url.endswith("/api/1/site/") for request in router.requests)


def test_public_origin_refuses_api_token_creation() -> None:
    router = _Router(PUBLIC)
    client = SyncUDataClient(router, declared_udata_profile(), origin=PUBLIC, credentials=CREDENTIAL)
    with client, pytest.raises(CatalogError, match="controlled local deployment"):
        client.users_tokens.create_api_token(
            ApiTokenCreateInput(name="evidence"),
            PERMISSIONS,
            _confirmed(users_wire.OPERATIONS["create_api_token"], "new-api-token", destructive=False),
        )
    assert all(request.url.endswith("/api/1/site/") for request in router.requests)


def test_async_public_origin_refuses_the_same_revocation() -> None:
    router = _AsyncRouter(PUBLIC)
    client = AsyncUDataClient(router, declared_udata_profile(), origin=PUBLIC, credentials=CREDENTIAL)

    async def run() -> None:
        await client.auth_oauth.revoke_token(
            OAuthRevokeRequest(token="opaque-access-token"),
            PERMISSIONS,
            _confirmed(oauth_wire.OPERATIONS["revoke_token"], "request:revoke_token"),
        )

    with pytest.raises(CatalogError, match="controlled local deployment"):
        asyncio.run(run())
    assert all(request.url.endswith("/api/1/site/") for request in router.requests)


def test_public_origin_still_serves_a_declared_read() -> None:
    router = _Router(PUBLIC)
    client = SyncUDataClient(router, declared_udata_profile(), origin=PUBLIC, credentials=CREDENTIAL)
    with client:
        client.auth_oauth.client_info(OAuthClientRequest(client_id="client-id"), PERMISSIONS)
    assert router.requests[-1].url.startswith(f"{PUBLIC}/oauth/client_info")


def test_controlled_origin_still_dispatches_the_destructive_revocation() -> None:
    router = _Router(_LOCAL)
    client = SyncUDataClient(router, declared_udata_profile(), origin=_LOCAL, credentials=CREDENTIAL)
    with client:
        client.auth_oauth.revoke_token(
            OAuthRevokeRequest(token="opaque-access-token"),
            PERMISSIONS,
            _confirmed(oauth_wire.OPERATIONS["revoke_token"], "request:revoke_token"),
        )
    assert router.requests[-1].url == f"{_LOCAL}/oauth/revoke"


def test_every_non_read_profile_route_is_refused_on_a_public_origin() -> None:
    """The boundary keys off declared metadata, so no route can be forgotten."""
    router = _Router(PUBLIC)
    client = SyncUDataClient(router, declared_udata_profile(), origin=PUBLIC, credentials=CREDENTIAL)
    non_read = {
        operation_id
        for operation_id, spec in declared_udata_profile().operations.items()
        if spec.mutation_class is not MutationClass.READ
    }
    assert non_read
    for operation_id in non_read:
        with pytest.raises(CatalogValidationError, match="controlled local deployment"):
            client._require_dispatchable(operation_id, client._profile)
    assert {"udata/oauth.revoke-token", "udata/api-v1.create-api-token"} <= {str(id_) for id_ in non_read}
    assert all(request.url.endswith("/api/1/site/") for request in router.requests)


def test_a_public_origin_is_still_accepted_at_construction_for_reads() -> None:
    assert UDataClientSettings(base_url=PUBLIC).base_url == PUBLIC
