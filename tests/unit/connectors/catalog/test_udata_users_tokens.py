"""Pinned user and token service contract."""

from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import pickle
import secrets
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.users import (
    ApiTokenCreateInput,
    UserCreateInput,
    UserDeleteOptions,
    UserListQuery,
    UserSuggestQuery,
    UserUpdateInput,
)
from datasluice.connectors.catalog.udata.secrets import OneTimeUDataToken
from datasluice.connectors.catalog.udata.wire import users as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogUnavailableError, CatalogValidationError, ForbiddenError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

ORIGIN = "http://127.0.0.1:5640"
SITE = {"id": "site", "title": "uData", "version": "17.6.0"}
CREDENTIAL = UDataCredential(api_key="unit-credential")
PERMISSIONS = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA)


class _Router:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, object]]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        status, payload = self.routes[(request.method, request.url)]
        return RuntimeResponse(
            status_code=status, headers={"Content-Type": "application/json"}, body=json.dumps(payload).encode()
        )

    def close(self) -> None:
        return None


class _AsyncRouter:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, object]]) -> None:
        self._sync = _Router(routes)
        self.requests = self._sync.requests

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._sync.send(request)

    async def aclose(self) -> None:
        return None


def _routes(*items: tuple[str, str, int, object]) -> dict[tuple[str, str], tuple[int, object]]:
    routes = {("GET", f"{ORIGIN}/api/1/site/"): (200, SITE)}
    routes.update({(method, f"{ORIGIN}{path}"): (status, body) for method, path, status, body in items})
    return routes


def _policy(name: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=wire.OPERATIONS[name], target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


ASSIGNED_METHODS = frozenset(
    {
        "get_me",
        "update_me",
        "delete_me",
        "my_avatar",
        "my_reuses",
        "my_datasets",
        "my_metrics",
        "my_org_datasets",
        "my_org_community_resources",
        "my_org_reuses",
        "my_org_discussions",
        "list_api_tokens",
        "create_api_token",
        "revoke_api_token",
        "list_org_invitations",
        "accept_org_invitation",
        "refuse_org_invitation",
        "list_users",
        "create_user",
        "user_avatar",
        "get_user",
        "update_user",
        "delete_user",
        "rotate_user_password",
        "get_user_contact_point",
        "follow_user",
        "suggest_users",
        "user_roles",
        "my_org_topics",
        "list_user_followers",
        "unfollow_user",
    }
)


def test_users_tokens_contract_exposes_every_assigned_method_in_both_modes() -> None:
    assert hasattr(SyncUDataClient, "users_tokens")
    assert hasattr(AsyncUDataClient, "users_tokens")
    module = importlib.import_module("datasluice.connectors.catalog.udata.services.users_tokens")
    assert ASSIGNED_METHODS <= set(dir(module.SyncUsersTokensService))
    assert ASSIGNED_METHODS <= set(dir(module.AsyncUsersTokensService))


@pytest.mark.parametrize(
    ("name", "method", "path"),
    [
        ("get_me", "GET", "/api/1/me/"),
        ("update_me", "PUT", "/api/1/me/"),
        ("delete_me", "DELETE", "/api/1/me/"),
        ("my_avatar", "POST", "/api/1/me/avatar/"),
        ("my_reuses", "GET", "/api/1/me/reuses/"),
        ("my_datasets", "GET", "/api/1/me/datasets/"),
        ("my_metrics", "GET", "/api/1/me/metrics/"),
        ("my_org_datasets", "GET", "/api/1/me/org_datasets/"),
        ("my_org_community_resources", "GET", "/api/1/me/org_community_resources/"),
        ("my_org_reuses", "GET", "/api/1/me/org_reuses/"),
        ("my_org_discussions", "GET", "/api/1/me/org_discussions/"),
        ("list_api_tokens", "GET", "/api/1/me/api_tokens/"),
        ("create_api_token", "POST", "/api/1/me/api_tokens/"),
        ("revoke_api_token", "DELETE", "/api/1/me/api_tokens/token-id/"),
        ("list_org_invitations", "GET", "/api/1/me/org_invitations/"),
        ("accept_org_invitation", "POST", "/api/1/me/org_invitations/token-id/accept/"),
        ("refuse_org_invitation", "POST", "/api/1/me/org_invitations/token-id/refuse/"),
        ("list_users", "GET", "/api/1/users/"),
        ("create_user", "POST", "/api/1/users/"),
        ("user_avatar", "POST", "/api/1/users/token-id/avatar/"),
        ("get_user", "GET", "/api/1/users/token-id/"),
        ("update_user", "PUT", "/api/1/users/token-id/"),
        ("delete_user", "DELETE", "/api/1/users/token-id/"),
        ("rotate_user_password", "POST", "/api/1/users/token-id/rotate_password/"),
        ("get_user_contact_point", "GET", "/api/1/users/token-id/contacts/"),
        ("follow_user", "POST", "/api/1/users/token-id/followers/"),
        ("suggest_users", "GET", "/api/1/users/suggest/"),
        ("user_roles", "GET", "/api/1/users/roles/"),
        ("my_org_topics", "GET", "/api/2/me/org_topics/"),
        ("list_user_followers", "GET", "/api/1/users/token-id/followers/"),
        ("unfollow_user", "DELETE", "/api/1/users/token-id/followers/"),
    ],
)
def test_every_assigned_user_route_has_exact_verb_and_path(name: str, method: str, path: str) -> None:
    request = wire.build_request(name, identifier="token-id")
    assert request[0] == method
    assert request[1] == path
    assert request[2] == {}
    assert request[3] is None


def test_user_wire_preserves_query_omission_and_body_presence() -> None:
    assert wire.build_request("list_users", query=UserListQuery(q="a b", page=2, page_size=5))[1] == (
        "/api/1/users/?page=2&page_size=5&q=a+b"
    )
    assert wire.build_request("suggest_users", query=UserSuggestQuery("sam", size=3))[1] == (
        "/api/1/users/suggest/?q=sam&size=3"
    )
    assert wire.build_request("update_user", identifier="person", body=UserUpdateInput({"website": None}))[3] == {
        "website": None
    }
    assert wire.build_request("create_user", body=UserCreateInput("Ada", "Lovelace", "ada@example.org"))[3] == {
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@example.org",
    }
    expiry = datetime.now(UTC) + timedelta(days=1)
    assert wire.build_request("create_api_token", body=ApiTokenCreateInput(name="evidence", expires_at=expiry))[3] == {
        "name": "evidence",
        "expires_at": expiry.isoformat(),
    }
    assert wire.build_request("create_api_token", body=ApiTokenCreateInput())[3] == {}
    assert wire.build_request("delete_user", identifier="person", query=UserDeleteOptions(no_mail=True))[1] == (
        "/api/1/users/person/?send_legal_notice=false&no_mail=true&delete_comments=false"
    )


def test_user_and_api_token_inputs_reject_invalid_documented_values() -> None:
    with pytest.raises(ValueError, match="active must be a boolean"):
        UserCreateInput("Ada", "Lovelace", "ada@example.org", fields={"active": "false"})
    with pytest.raises(ValueError, match="roles must be a list"):
        UserUpdateInput({"roles": "admin"})
    with pytest.raises(ValueError, match="roles must be a list"):
        UserUpdateInput({"roles": [{"id": "admin"}]})
    with pytest.raises(ValueError, match="255 characters"):
        ApiTokenCreateInput(name="x" * 256)
    with pytest.raises(ValueError, match="in the future"):
        ApiTokenCreateInput(expires_at=datetime.now(UTC) - timedelta(seconds=1))


@pytest.mark.parametrize(
    "fields",
    [
        pytest.param({"first_name": []}, id="name-type"),
        pytest.param({"last_name": ""}, id="name-empty"),
        pytest.param({"email": 1}, id="email-type"),
        pytest.param({"website": "example.org"}, id="website-url"),
        pytest.param({"website": "https://exa mple.org"}, id="website-whitespace"),
    ],
)
def test_invalid_user_updates_fail_before_dispatch(fields: dict[str, object]) -> None:
    router = _Router(
        _routes(("PUT", "/api/1/me/", 200, {"id": "person", "first_name": "Ada", "last_name": "Lovelace"}))
    )
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client, pytest.raises(ValueError):
        client.users_tokens.update_me(UserUpdateInput(fields), PERMISSIONS, _policy("update_me", "me"))
    assert router.requests == []


def test_empty_user_create_fields_do_not_alias_the_callers_mapping() -> None:
    fields: dict[str, object] = {}
    user = UserCreateInput("Ada", "Lovelace", "ada@example.org", fields=fields)
    fields["active"] = "false"
    assert user.payload() == {"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.org"}


def test_api_token_metadata_preserves_safe_stock_fields() -> None:
    token = wire.parse_token(
        {
            "id": "token-id",
            "token_prefix": "udata_abcd",
            "name": "evidence",
            "created_at": "2026-09-24T00:00:00Z",
            "expires_at": None,
            "revoked_at": None,
            "kind": "api_key",
            "scopes": ["admin"],
            "last_used_at": "2026-09-24T01:00:00Z",
            "user_agents": ["controlled-test"],
            "token_hash": "excluded-hash",
        },
        operation=wire.OPERATIONS["list_api_tokens"],
    )
    assert token.to_dict() == {
        "id": "token-id",
        "token_prefix": "udata_abcd",
        "name": "evidence",
        "created_at": "2026-09-24T00:00:00Z",
        "expires_at": None,
        "revoked_at": None,
        "kind": "api_key",
        "scopes": ["admin"],
        "last_used_at": "2026-09-24T01:00:00Z",
        "user_agents": ["controlled-test"],
    }
    with pytest.raises(CatalogValidationError):
        wire.parse_token(
            {"id": "token-id", "token_prefix": "udata_abcd", "scopes": "admin"},
            operation=wire.OPERATIONS["list_api_tokens"],
        )
    for payload in (
        {"id": "", "token_prefix": "udata_abcd"},
        {"id": "token-id", "token_prefix": ""},
    ):
        with pytest.raises(CatalogValidationError):
            wire.parse_token(payload, operation=wire.OPERATIONS["list_api_tokens"])


def test_one_time_token_cannot_enter_ordinary_retained_sinks() -> None:
    from datasluice.connectors.catalog.udata.models.users import ApiTokenCreationResult, ApiTokenMetadata

    plaintext = "evidence-opaque-only"
    secret = OneTimeUDataToken(plaintext)
    receipt = MutationReceipt(
        operation="udata/api-v1.create-api-token",
        outcome="succeeded",
        target=CatalogId(CatalogPlatform.UDATA, ResourceKind("api-token"), "token-id"),
    )
    result = ApiTokenCreationResult(receipt, ApiTokenMetadata("token-id", "udata_abcd"), secret)
    if any(
        plaintext in representation
        for representation in (repr(secret), str(secret), repr(result), json.dumps(result.to_dict()))
    ):
        pytest.fail("One-time token entered a retained representation.")
    with pytest.raises(TypeError):
        json.dumps(result)
    with pytest.raises(TypeError):
        pickle.dumps(result)
    with pytest.raises(TypeError):
        asdict(result)
    assert hmac.compare_digest(secret.reveal_once(), plaintext)
    with pytest.raises(RuntimeError):
        secret.reveal_once()


def test_token_create_returns_only_allowlisted_metadata_and_reveal_once_secret() -> None:
    plaintext = secrets.token_urlsafe(36)
    router = _Router(
        _routes(
            (
                "POST",
                "/api/1/me/api_tokens/",
                201,
                {
                    "id": "token-id",
                    "token_prefix": "udata_abcd",
                    "name": "evidence",
                    "token": plaintext,
                    "token_hash": "must-be-ignored",
                },
            )
        )
    )
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client:
        result = client.users_tokens.create_api_token(
            ApiTokenCreateInput(name="evidence"), PERMISSIONS, _policy("create_api_token", "new-api-token")
        )
    assert result.metadata.id == "token-id"
    assert result.receipt.target.value == "token-id"
    assert result.receipt.operation == wire.OPERATIONS["create_api_token"]
    if plaintext in repr(result) or plaintext in json.dumps(result.to_dict()):
        pytest.fail("One-time token entered a retained result.")
    assert "token_hash" not in json.dumps(result.to_dict())
    assert hmac.compare_digest(result.secret.reveal_once(), plaintext)
    with pytest.raises(RuntimeError):
        result.secret.reveal_once()
    assert router.requests[-1].method == "POST"
    assert router.requests[-1].url == f"{ORIGIN}/api/1/me/api_tokens/"
    assert json.loads(router.requests[-1].body or b"{}") == {"name": "evidence"}


def test_revoke_requires_exact_target_and_receipts_deployment_disabled() -> None:
    router = _Router(_routes(("DELETE", "/api/1/me/api_tokens/token-id/", 423, {"message": "read only"})))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client:
        with pytest.raises(ForbiddenError) as denied:
            client.users_tokens.revoke_api_token(
                "token-id", PERMISSIONS, _policy("revoke_api_token", "other", destructive=True)
            )
        assert not router.requests
        with pytest.raises(CatalogUnavailableError) as unavailable:
            client.users_tokens.revoke_api_token(
                "token-id", PERMISSIONS, _policy("revoke_api_token", "token-id", destructive=True)
            )
    denied_receipt = denied.value.__dict__["mutation_receipt"]
    assert isinstance(denied_receipt, MutationReceipt)
    assert denied_receipt.outcome == "rejected"
    assert denied_receipt.target.value == "token-id"
    assert unavailable.value.capability_state == "deployment-disabled"
    unavailable_receipt = unavailable.value.__dict__["mutation_receipt"]
    assert isinstance(unavailable_receipt, MutationReceipt)
    assert unavailable_receipt.target.value == "token-id"
    assert unavailable_receipt.audit_metadata["status_code"] == 423


def test_user_admin_mutations_deny_non_admin_before_dispatch() -> None:
    router = _Router(_routes())
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    email = "controlled@example.org"
    target = f"request:{hashlib.sha256(email.encode()).hexdigest()[:24]}"
    with client:
        with pytest.raises(ForbiddenError) as create_denied:
            client.users_tokens.create_user(
                UserCreateInput("Controlled", "User", email), PERMISSIONS, _policy("create_user", target)
            )
        with pytest.raises(ForbiddenError) as delete_denied:
            client.users_tokens.delete_user("user-id", PERMISSIONS, _policy("delete_user", "user-id", destructive=True))
    assert router.requests == []
    create_receipt = create_denied.value.__dict__["mutation_receipt"]
    delete_receipt = delete_denied.value.__dict__["mutation_receipt"]
    assert create_receipt.target.value == target
    assert delete_receipt.target.value == "user-id"
    assert create_receipt.outcome == "rejected"
    assert delete_receipt.outcome == "rejected"


def test_sync_interruption_after_dispatch_retains_ambiguous_target_receipt() -> None:
    class _InterruptedRouter(_Router):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            if request.method == "POST" and request.url.endswith("/api/1/me/api_tokens/"):
                self.requests.append(request)
                raise KeyboardInterrupt
            return super().send(request)

    router = _InterruptedRouter(_routes())
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client, pytest.raises(KeyboardInterrupt) as raised:
        client.users_tokens.create_api_token(
            ApiTokenCreateInput(), PERMISSIONS, _policy("create_api_token", "new-api-token")
        )
    receipt = raised.value.__dict__["mutation_receipt"]
    assert isinstance(receipt, MutationReceipt)
    assert receipt.outcome == "ambiguous"
    assert receipt.target.value == "new-api-token"
    assert router.requests[-1].method == "POST"


def test_async_token_create_matches_sync_receipt_and_secret_boundary() -> None:
    import asyncio

    plaintext = secrets.token_urlsafe(36)
    router = _AsyncRouter(
        _routes(
            (
                "POST",
                "/api/1/me/api_tokens/",
                201,
                {
                    "id": "token-id",
                    "token_prefix": "udata_abcd",
                    "token": plaintext,
                },
            )
        )
    )
    client = AsyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)

    async def run() -> None:
        async with client:
            result = await client.users_tokens.create_api_token(
                ApiTokenCreateInput(), PERMISSIONS, _policy("create_api_token", "new-api-token")
            )
        assert result.receipt.target.value == "token-id"
        if plaintext in json.dumps(result.to_dict()):
            pytest.fail("One-time token entered an async retained result.")
        assert hmac.compare_digest(result.secret.reveal_once(), plaintext)

    asyncio.run(run())
