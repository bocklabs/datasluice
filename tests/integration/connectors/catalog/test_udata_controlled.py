"""Controlled-environment tracer proof against the loopback uData 17.6 stack."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from importlib import resources
from inspect import isawaitable
from io import BytesIO
from typing import Any, cast
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

import pytest

from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    _create_controlled_async_client,
    _create_controlled_sync_client,
    create_async_client,
    create_sync_client,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.mapping import UDataPageEnvelope
from datasluice.connectors.catalog.udata.models.activity_discussions import (
    ActivityQuery,
    CommentInput,
    DiscussionCreateInput,
    DiscussionSearchQuery,
    DiscussionUpdateInput,
)
from datasluice.connectors.catalog.udata.models.datasets import DatasetListQuery, DatasetSuggestQuery
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
from datasluice.connectors.catalog.udata.models.organizations import (
    MembershipRequestInput,
    OrganizationCreateInput,
    OrganizationInvitationInput,
    OrganizationListQuery,
    OrganizationMemberInput,
    OrganizationMutationResult,
    OrganizationRefusalInput,
    OrganizationSuggestQuery,
    OrganizationUpdateInput,
)
from datasluice.connectors.catalog.udata.models.reuses import (
    ReuseCreateInput,
    ReuseFollowersQuery,
    ReuseListQuery,
    ReuseSearchQuery,
    ReuseSuggestQuery,
    ReuseUpdateInput,
)
from datasluice.connectors.catalog.udata.models.root_profile import SiteMutationResult, SitePatchInput, SiteProfile
from datasluice.connectors.catalog.udata.models.taxonomies import BadgeCreateInput, SuggestQuery, TaxonomyMutationResult
from datasluice.connectors.catalog.udata.models.users import (
    ApiTokenCreateInput,
    ApiTokenCreationResult,
    ApiTokenMetadata,
    UserAvatarInput,
    UserCreateInput,
    UserDeleteOptions,
    UserListQuery,
    UserMutationResult,
    UserSuggestQuery,
    UserUpdateInput,
)
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import MappingRecord, NativeRecord
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.domain.catalog.udata import SiteDocument
from datasluice.errors.catalog import CatalogError

if os.environ.get("UDATA_EVIDENCE_ORIGIN", "http://127.0.0.1:5640") != "http://127.0.0.1:5640":
    pytest.skip(
        allow_module_level=True,
        reason=(
            "Controlled uData evidence is restricted to the fixed loopback stack "
            "http://127.0.0.1:5640; set UDATA_EVIDENCE_ORIGIN to the stock loopback origin."
        ),
    )

ORIGIN = "http://127.0.0.1:5640"
_USER_READ_MAX_BYTES = 131072
_CONTROLLED_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jV1sAAAAASUVORK5CYII="
)
_TOKEN_SAFE_FIELDS = frozenset(
    {
        "id",
        "token_prefix",
        "name",
        "created_at",
        "expires_at",
        "revoked_at",
        "kind",
        "scopes",
        "last_used_at",
        "user_agents",
    }
)
pytestmark = [
    pytest.mark.udata_controlled,
    pytest.mark.skipif(
        os.environ.get("UDATA_EVIDENCE_CONTROLLED") != "1",
        reason="controlled uData evidence runs only against the local digest-pinned stack",
    ),
]


_FAMILY_OPERATION_ID = next(
    op_id
    for op_id in declared_udata_profile().operations
    if op_id.method == "dataset-list-search-show-create-update-delete"
)
_ROOT_CONTRACT_RESOURCE = resources.files("datasluice.contracts").joinpath("catalog/fixtures/udata/root_profile.json")


def _controlled_site_row() -> dict[str, object]:
    document = json.loads(_ROOT_CONTRACT_RESOURCE.read_text(encoding="utf-8"))
    rows = document["rows"]
    row = next(row for row in rows if row["row"] == 184)
    assert isinstance(row, dict)
    return row


class _DirectNoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:
        return None


def _direct_request(
    token: str,
    method: str,
    path: str,
    *,
    body: object | None = None,
    content_type: str = "application/json",
    include_body: bool = False,
    max_bytes: int = 8192,
) -> tuple[int, object, dict[str, str]]:
    data = None if body is None else body if isinstance(body, bytes) else json.dumps(body).encode()
    headers = {"Accept": "*/*", "X-API-KEY": token}
    if data is not None:
        headers["Content-Type"] = content_type
    request = Request(f"{ORIGIN}{path}", data=data, headers=headers, method=method)
    try:
        response = build_opener(_DirectNoRedirect()).open(request, timeout=10)
    except HTTPError as error:
        response = error
    with response:
        payload_bytes = response.read(max_bytes + 1)
        assert len(payload_bytes) <= max_bytes
        media_type = response.headers.get_content_type()
        payload = (
            json.loads(payload_bytes)
            if payload_bytes and media_type == "application/json"
            else payload_bytes
            if include_body
            else None
        )
        status = response.status
        assert type(status) is int
        return status, payload, {key.lower(): value for key, value in response.headers.items()}


def _assert_created_response(status: int, payload: object, cleanup_ids: list[str]) -> tuple[dict[str, object], str]:
    identifier = payload.get("id") if isinstance(payload, dict) else None
    if isinstance(identifier, str) and identifier:
        cleanup_ids.append(identifier)
    if isinstance(payload, dict):
        payload.pop("token", None)
    assert status == 201, "Controlled create response returned an unexpected status."
    assert isinstance(payload, dict), "Controlled create response was not an object."
    assert isinstance(identifier, str), "Controlled create response ID was not a string."
    assert identifier, "Controlled create response omitted its ID."
    return payload, identifier


def _disposable_user_token(user_id: str, cleanup_ids: list[str]) -> tuple[str, str]:
    program = """
import sys
import json
from udata.app import create_app, standalone
from udata.models import User
from udata.core.api_token.models import ApiToken
app = standalone(create_app())
with app.app_context():
    user = User.objects(id=sys.argv[1]).first()
    assert user is not None
    record, token = ApiToken.generate(user, name="controlled-user-route")
    print(json.dumps({"id": str(record.id), "token": token}))
"""
    issued = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "dev/udata-evidence/.env",
            "-f",
            "dev/udata-evidence/compose.yaml",
            "exec",
            "-T",
            "udata",
            "python",
            "-c",
            program,
            user_id,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        issued_token = json.loads(issued.stdout)
    except ValueError:
        issued_token = None
    token_id = issued_token.get("id") if isinstance(issued_token, Mapping) else None
    token = issued_token.get("token") if isinstance(issued_token, Mapping) else None
    if isinstance(token_id, str) and token_id:
        cleanup_ids.append(token_id)
    if (
        issued.returncode
        or not isinstance(token_id, str)
        or not token_id
        or not isinstance(token, str)
        or not token.startswith("udata_")
    ):
        raise AssertionError("Disposable user token generation failed.")
    return token_id, token


def test_disposable_token_id_is_registered_before_token_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"id": "token-id", "token": "invalid"}),
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    cleanup_ids: list[str] = []
    with pytest.raises(AssertionError, match="Disposable user token generation failed"):
        _disposable_user_token("user-id", cleanup_ids)
    assert cleanup_ids == ["token-id"]


@pytest.mark.parametrize(
    ("created_id", "kind"),
    [
        pytest.param("organization-id", "organization", id="raw-organization"),
        pytest.param("token-id", "management-token", id="raw-management-token"),
    ],
)
def test_created_response_keeps_cleanup_id_when_status_assertion_fails(created_id: str, kind: str) -> None:
    cleanup_ids: list[str] = []
    payload: dict[str, object] = {"id": created_id}
    if kind == "management-token":
        payload["token"] = "temporary"

    with pytest.raises(AssertionError, match="unexpected status"):
        _assert_created_response(500, payload, cleanup_ids)

    assert cleanup_ids == [created_id]
    assert "token" not in payload


def _controlled_user_state(user_ids: tuple[str, ...]) -> dict[str, dict[str, bool]]:
    program = """
import json
import sys
from udata.app import create_app, standalone
from udata.models import User
app = standalone(create_app())
with app.app_context():
    state = {}
    for user_id in sys.argv[1:]:
        user = User.objects(id=user_id).first()
        assert user is not None
        state[user_id] = {
            "avatar_present": bool(user.avatar),
            "password_rotation_requested": user.password_rotation_demanded is not None,
        }
    print(json.dumps(state))
"""
    checked = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "dev/udata-evidence/.env",
            "-f",
            "dev/udata-evidence/compose.yaml",
            "exec",
            "-T",
            "udata",
            "python",
            "-c",
            program,
            *user_ids,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if checked.returncode:
        raise AssertionError("Controlled user state readback failed.")
    state = json.loads(checked.stdout)
    if not isinstance(state, dict) or set(state) != set(user_ids):
        raise AssertionError("Controlled user state readback omitted a target.")
    if any(
        not isinstance(values, dict)
        or set(values) != {"avatar_present", "password_rotation_requested"}
        or any(type(value) is not bool for value in values.values())
        for values in state.values()
    ):
        raise AssertionError("Controlled user state readback has an invalid shape.")
    return state


def _record_payload(record: object) -> Mapping[str, object]:
    if isinstance(record, NativeRecord):
        return record.payload
    assert isinstance(record, MappingRecord)
    return {key: value for key, value in record.payload.items() if key not in {"operation", "resource_kind"}}


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(item) for item in value]
    return value


def _assert_page_matches_raw(method: str, payload: object, page: UDataPageEnvelope) -> None:
    assert isinstance(payload, Mapping)
    raw_items = payload.get("data")
    assert isinstance(raw_items, list)
    typed_items = [_record_payload(item) for item in page.items]
    assert [_plain_json(item) for item in typed_items] == raw_items, method

    page_fields = ("data", "page", "page_size", "previous_page", "next_page", "total")
    assert page.native_page.present_fields == frozenset(name for name in page_fields if name in payload)
    for name in page_fields[1:]:
        if name in payload:
            assert getattr(page.native_page, name) == payload[name]

    raw_page = payload.get("page")
    if raw_page is None:
        assert page.page is None
    else:
        assert type(raw_page) is int
        assert page.page is not None
        assert page.page.cursor == str(raw_page)
        next_cursor = str(raw_page + 1) if payload.get("next_page") else None
        assert page.page.next_cursor == next_cursor
        assert page.page.total_items == payload.get("total")


def _assert_records_match_raw(method: str, payload: object, records: tuple[object, ...]) -> None:
    if method == "available_organization_badges":
        assert isinstance(payload, Mapping)
        raw_items = [{"id": key, "label": value} for key, value in payload.items()]
    elif isinstance(payload, Mapping):
        raw_items = payload.get("data", payload)
    else:
        raw_items = payload
    assert isinstance(raw_items, list)
    assert all(isinstance(item, Mapping) for item in raw_items)
    assert [_plain_json(_record_payload(item)) for item in records] == raw_items, method


def _assert_typed_read_matches_raw(
    method: str, status: int, payload: object, headers: Mapping[str, str], typed: object
) -> None:
    if isinstance(typed, SiteDocument):
        assert isinstance(payload, bytes)
        assert typed.status_code == status
        media_type = headers.get("content-type", "application/octet-stream")
        assert typed.media_type == media_type
        assert typed.location == headers.get("location")
        if status in {301, 302, 303, 307, 308}:
            assert typed.size_bytes == 0
            assert typed.sha256 == hashlib.sha256(b"").hexdigest()
        else:
            assert typed.size_bytes == len(payload)
            assert typed.sha256 == hashlib.sha256(payload).hexdigest()
    elif isinstance(typed, UDataPageEnvelope):
        _assert_page_matches_raw(method, payload, typed)
    elif isinstance(typed, NativeRecord):
        assert isinstance(payload, Mapping)
        assert _plain_json(typed.payload) == payload, method
    elif isinstance(typed, tuple):
        _assert_records_match_raw(method, payload, typed)
    elif isinstance(typed, Mapping):
        assert isinstance(payload, Mapping)
        assert _plain_json(typed) == payload, method
    else:
        assert typed == payload


_PAGE_FIELDS = ("page", "page_size", "previous_page", "next_page", "total")
_VOLATILE_USER_FIELDS = frozenset({"token", "token_hash", "password", "last_login_at"})


def _canonical_digest(value: object) -> str:
    """Return the order-independent digest of one JSON-safe projection of *value*."""
    return hashlib.sha256(json.dumps(_plain_json(value), sort_keys=True).encode()).hexdigest()


def _assert_page_read_matches_raw(method: str, raw: object, page: UDataPageEnvelope) -> None:
    assert isinstance(raw, Mapping)
    raw_items = raw.get("data")
    assert isinstance(raw_items, list)
    typed_items = [_record_payload(item) for item in page.items]
    assert _canonical_digest(typed_items) == _canonical_digest(raw_items), method
    for key in _PAGE_FIELDS:
        if key in raw:
            assert getattr(page.native_page, key) == raw[key]


def _assert_native_record_matches_raw(method: str, raw: object, record: NativeRecord) -> None:
    assert isinstance(raw, Mapping)
    safe = {key: value for key, value in raw.items() if key not in _VOLATILE_USER_FIELDS}
    assert isinstance(raw.get("last_login_at"), str)
    assert isinstance(record.payload.get("last_login_at"), str)
    assert set(record.payload) - {"last_login_at"} == set(safe), method
    for key, value in safe.items():
        assert _canonical_digest(record.payload[key]) == _canonical_digest(value), (method, key)


def _token_item_projections(
    typed: Sequence[ApiTokenMetadata], raw_items: Sequence[Mapping[str, object]]
) -> tuple[list[Mapping[str, object]], list[Mapping[str, object]]]:
    """Return the safe raw token rows alongside the typed token rows restricted to the same keys."""
    assert all(set(item.to_dict()) == _TOKEN_SAFE_FIELDS for item in typed)
    assert all(isinstance(item, Mapping) for item in raw_items)
    safe_items: list[Mapping[str, object]] = [
        {key: value for key, value in item.items() if key in _TOKEN_SAFE_FIELDS} for item in raw_items
    ]
    typed_items: list[Mapping[str, object]] = [
        {key: value for key, value in item.to_dict().items() if key in safe_items[index]}
        for index, item in enumerate(typed)
    ]
    return safe_items, typed_items


def _assert_sequence_read_matches_raw(method: str, raw: object, typed: tuple[object, ...]) -> None:
    raw_items = raw.get("data") if isinstance(raw, Mapping) else raw
    assert isinstance(raw_items, list)
    if typed and isinstance(typed[0], ApiTokenMetadata):
        safe_items, typed_items = _token_item_projections(
            cast("Sequence[ApiTokenMetadata]", typed), cast("Sequence[Mapping[str, object]]", raw_items)
        )
    else:
        safe_items = raw_items
        typed_items = [_record_payload(item) for item in typed]
    assert _canonical_digest(typed_items) == _canonical_digest(safe_items), method


def _assert_user_read_matches_raw(method: str, raw: object, typed: object) -> None:
    if isinstance(typed, UDataPageEnvelope):
        _assert_page_read_matches_raw(method, raw, typed)
    elif isinstance(typed, NativeRecord):
        _assert_native_record_matches_raw(method, raw, typed)
    elif isinstance(typed, MappingRecord):
        assert isinstance(raw, Mapping)
        assert _canonical_digest(typed.payload) == _canonical_digest(raw), method
    elif isinstance(typed, tuple):
        _assert_sequence_read_matches_raw(method, raw, typed)
    else:
        assert _canonical_digest(typed) == _canonical_digest(raw), method


def _follower_ids(payload: object) -> set[str]:
    if not isinstance(payload, Mapping):
        raise AssertionError("The controlled follower read omitted its data list.")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise AssertionError("The controlled follower read omitted its data list.")
    identifiers: set[str] = set()
    for row in rows:
        follower = row.get("follower") if isinstance(row, Mapping) else None
        if not isinstance(follower, Mapping) or not isinstance(follower.get("id"), str):
            raise AssertionError("The controlled follower read returned an invalid record.")
        identifiers.add(follower["id"])
    return identifiers


def _typed_follower_ids(page: UDataPageEnvelope) -> set[str]:
    identifiers: set[str] = set()
    for item in page.items:
        follower = item.payload.get("follower")
        if not isinstance(follower, Mapping) or not isinstance(follower.get("id"), str):
            raise AssertionError("The typed controlled follower read returned an invalid record.")
        identifiers.add(follower["id"])
    return identifiers


def test_controlled_user_reads_match_raw_routes_in_both_modes() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled user reads require a seeded disposable admin")
    status, current_user, _ = _direct_request(token, "GET", "/api/1/me/")
    assert status == 200
    assert isinstance(current_user, Mapping)
    user_id = current_user.get("id")
    assert isinstance(user_id, str)
    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    reads = (
        ("/api/1/me/", "get_me", (permissions,)),
        ("/api/1/me/reuses/", "my_reuses", (permissions,)),
        ("/api/1/me/datasets/", "my_datasets", (permissions,)),
        ("/api/1/me/metrics/", "my_metrics", (permissions,)),
        ("/api/1/me/org_datasets/", "my_org_datasets", (permissions,)),
        ("/api/1/me/org_community_resources/", "my_org_community_resources", (permissions,)),
        ("/api/1/me/org_reuses/", "my_org_reuses", (permissions,)),
        ("/api/1/me/org_discussions/", "my_org_discussions", (permissions,)),
        ("/api/1/me/api_tokens/", "list_api_tokens", (permissions,)),
        ("/api/1/me/org_invitations/", "list_org_invitations", (permissions,)),
        ("/api/1/users/?page=1&page_size=20", "list_users", (permissions, UserListQuery())),
        (f"/api/1/users/{user_id}/", "get_user", (user_id,)),
        (f"/api/1/users/{user_id}/contacts/?page=1&page_size=20", "get_user_contact_point", (user_id,)),
        ("/api/1/users/suggest/?q=ev&size=10", "suggest_users", (UserSuggestQuery("ev"),)),
        ("/api/1/users/roles/", "user_roles", ()),
        ("/api/2/me/org_topics/?page=1&page_size=20", "my_org_topics", (permissions,)),
        (f"/api/1/users/{user_id}/followers/?page=1&page_size=20", "list_user_followers", (user_id,)),
    )

    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        for path, method, args in reads:
            raw_status, raw_payload, _ = _direct_request(token, "GET", path, max_bytes=_USER_READ_MAX_BYTES)
            try:
                typed = getattr(client.users_tokens, method)(*args)
            except CatalogError as error:
                assert error.metadata.get("status_code") == raw_status, method
            else:
                assert raw_status == 200, method
                _assert_user_read_matches_raw(method, raw_payload, typed)

    async def run_async() -> None:
        async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
            for path, method, args in reads:
                raw_status, raw_payload, _ = _direct_request(token, "GET", path, max_bytes=_USER_READ_MAX_BYTES)
                operation = getattr(client.users_tokens, method)(*args)
                try:
                    typed = await operation if isawaitable(operation) else operation
                except CatalogError as error:
                    assert error.metadata.get("status_code") == raw_status, method
                else:
                    assert raw_status == 200, method
                    _assert_user_read_matches_raw(method, raw_payload, typed)

    asyncio.run(run_async())


def _user_mutation_policy(name: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    """Return the confirmed overwrite policy the controlled user mutations run under."""
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(
            confirmed=True, operation=f"udata/api-v1.{name.replace('_', '-')}", target=target
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _assert_user_mutation(status: int, result: object, name: str, target: str | None = None) -> None:
    """Assert one typed user mutation returned a succeeded receipt agreeing with the raw status."""
    assert isinstance(result, (UserMutationResult, ApiTokenCreationResult))
    receipt = result.receipt
    assert receipt.operation == f"udata/api-v1.{name.replace('_', '-')}"
    assert receipt.outcome == "succeeded"
    assert receipt.audit_metadata["status_code"] == status
    if target is not None:
        assert receipt.target.value == target


@dataclass
class _UserMutationRun:
    """Mutable per-run state shared by the controlled user mutation phases."""

    async_mode: bool
    admin_token: str
    admin_permissions: EffectivePermissions
    admin_client: SyncUDataClient | AsyncUDataClient
    organizations: dict[str, str] = field(default_factory=dict)
    organization_cleanup_ids: list[str] = field(default_factory=list)
    users: dict[tuple[str, str], tuple[str, str]] = field(default_factory=dict)
    created_user_ids: list[str] = field(default_factory=list)
    token_ids: list[str] = field(default_factory=list)
    user_token_ids: list[str] = field(default_factory=list)
    cleanup_errors: list[str] = field(default_factory=list)


async def _typed_call(service: object, name: str, *args: object) -> object:
    """Invoke the typed method *name* on *service*, awaiting it when it returns an awaitable."""
    result = getattr(service, name)(*args)
    return await result if isawaitable(result) else result


def _new_user_client(run: _UserMutationRun, token: str) -> SyncUDataClient | AsyncUDataClient:
    """Return a throwaway client authenticated as the disposable user holding *token*."""
    settings = UDataClientSettings(base_url=ORIGIN, credential=UDataCredential(api_key=token))
    return create_async_client(settings) if run.async_mode else create_sync_client(settings)


async def _close_client(client: SyncUDataClient | AsyncUDataClient) -> None:
    """Close *client*, awaiting the asynchronous transport when the run is asynchronous."""
    if isinstance(client, AsyncUDataClient):
        await client.aclose()
    else:
        assert isinstance(client, SyncUDataClient)
        client.close()


async def _user_call(run: _UserMutationRun, user_token: str, name: str, *args: object) -> object:
    """Invoke one user-scoped typed method on a throwaway client, closing it afterwards."""
    client = _new_user_client(run, user_token)
    try:
        return await _typed_call(client.users_tokens, name, *args)
    finally:
        await _close_client(client)


async def _seed_controlled_organization(run: _UserMutationRun) -> None:
    """Create the paired raw and typed organizations the membership phases share."""
    run_id = uuid4().hex
    raw_org_name = f"Raw user evidence {run_id}"
    typed_org_name = f"Typed user evidence {run_id}"
    raw_org_status, raw_org, _ = _direct_request(
        run.admin_token,
        "POST",
        "/api/1/organizations/",
        body={"name": raw_org_name, "description": "Disposable user invitation evidence"},
    )
    raw_org, raw_org_id = _assert_created_response(raw_org_status, raw_org, run.organization_cleanup_ids)
    run.organizations["raw"] = raw_org_id
    typed_org = await _typed_call(
        run.admin_client.organizations_memberships,
        "create_organization",
        OrganizationCreateInput(name=typed_org_name, description="Disposable user invitation evidence"),
        run.admin_permissions,
        _user_mutation_policy("create_organization", typed_org_name),
    )
    typed_org_id = (
        typed_org.record.id.value
        if isinstance(typed_org, OrganizationMutationResult) and typed_org.record is not None
        else None
    )
    if isinstance(typed_org_id, str):
        run.organization_cleanup_ids.append(typed_org_id)
    assert isinstance(typed_org, OrganizationMutationResult)
    assert typed_org.record is not None
    assert isinstance(typed_org_id, str)
    run.organizations["typed"] = typed_org_id
    assert typed_org.receipt.audit_metadata["status_code"] == raw_org_status


async def _seed_controlled_users(run: _UserMutationRun) -> None:
    """Create the four disposable users, two per membership decision, raw and typed."""
    for decision in ("accept", "refuse"):
        run_id = uuid4().hex
        raw_email = f"raw-{run_id}@gmail.com"
        typed_email = f"typed-{run_id}@gmail.com"
        raw_status, raw_user, _ = _direct_request(
            run.admin_token,
            "POST",
            "/api/1/users/",
            body={"first_name": "Raw", "last_name": "Evidence", "email": raw_email, "active": True},
        )
        raw_id = raw_user.get("id") if isinstance(raw_user, Mapping) else None
        if isinstance(raw_id, str):
            run.created_user_ids.append(raw_id)
        typed = await _typed_call(
            run.admin_client.users_tokens,
            "create_user",
            UserCreateInput("Typed", "Evidence", typed_email, fields={"active": True}),
            run.admin_permissions,
            _user_mutation_policy("create_user", f"request:{hashlib.sha256(typed_email.encode()).hexdigest()[:24]}"),
        )
        if isinstance(typed, UserMutationResult) and typed.record is not None:
            run.created_user_ids.append(typed.record.id.value)
        assert raw_status == 201
        assert isinstance(raw_user, Mapping)
        assert isinstance(typed, UserMutationResult)
        assert typed.record is not None
        _assert_user_mutation(raw_status, typed, "create_user", typed.record.id.value)
        typed_id = typed.record.id.value
        assert isinstance(raw_id, str)
        raw_read_status, raw_created_user, _ = _direct_request(run.admin_token, "GET", f"/api/1/users/{raw_id}/")
        typed_created_user = await _typed_call(run.admin_client.users_tokens, "get_user", typed_id)
        assert raw_read_status == 200
        assert isinstance(raw_created_user, Mapping)
        assert isinstance(typed_created_user, NativeRecord)
        assert raw_created_user.get("email") == raw_email
        assert typed_created_user.payload.get("email") == typed_email
        assert raw_created_user.get("active") is True
        assert typed_created_user.payload.get("active") is True
        _, raw_user_token = _disposable_user_token(raw_id, run.user_token_ids)
        _, typed_user_token = _disposable_user_token(typed_id, run.user_token_ids)
        run.users[(decision, "raw")] = (raw_id, raw_user_token)
        run.users[(decision, "typed")] = (typed_id, typed_user_token)

    assert len({user_id for user_id, _ in run.users.values()}) == 4


async def _exercise_profile_updates(run: _UserMutationRun) -> None:
    """Differentially exercise the self and administrative profile updates of one user."""
    raw_id, raw_user_token = run.users[("accept", "raw")]
    typed_id, typed_user_token = run.users[("accept", "typed")]
    typed_permissions = EffectivePermissions.for_credential(
        UDataCredential(api_key=typed_user_token), platform=CatalogPlatform.UDATA
    )
    raw_status, raw_updated, _ = _direct_request(
        raw_user_token, "PUT", "/api/1/me/", body={"website": "https://example.org/raw"}
    )
    typed_updated = await _user_call(
        run,
        typed_user_token,
        "update_me",
        UserUpdateInput({"website": "https://example.org/typed"}),
        typed_permissions,
        _user_mutation_policy("update_me", "me"),
    )
    assert raw_status == 200
    assert isinstance(raw_updated, Mapping)
    _assert_user_mutation(raw_status, typed_updated, "update_me", "me")
    raw_state_status, raw_state, _ = _direct_request(raw_user_token, "GET", "/api/1/me/")
    typed_state = await _user_call(run, typed_user_token, "get_me", typed_permissions)
    assert raw_state_status == 200
    assert isinstance(raw_state, Mapping)
    assert isinstance(typed_state, NativeRecord)
    assert raw_state.get("website") == "https://example.org/raw"
    assert typed_state.payload.get("website") == "https://example.org/typed"

    raw_status, raw_admin_updated, _ = _direct_request(
        run.admin_token, "PUT", f"/api/1/users/{raw_id}/", body={"about": "raw evidence"}
    )
    typed_admin_updated = await _typed_call(
        run.admin_client.users_tokens,
        "update_user",
        typed_id,
        UserUpdateInput({"about": "typed evidence"}),
        run.admin_permissions,
        _user_mutation_policy("update_user", typed_id),
    )
    assert raw_status == 200
    assert isinstance(raw_admin_updated, Mapping)
    _assert_user_mutation(raw_status, typed_admin_updated, "update_user", typed_id)
    raw_state_status, raw_state, _ = _direct_request(run.admin_token, "GET", f"/api/1/users/{raw_id}/")
    typed_state = await _typed_call(run.admin_client.users_tokens, "get_user", typed_id)
    assert raw_state_status == 200
    assert isinstance(raw_state, Mapping)
    assert isinstance(typed_state, NativeRecord)
    assert raw_state.get("about") == "raw evidence"
    assert typed_state.payload.get("about") == "typed evidence"


async def _exercise_avatar_uploads(run: _UserMutationRun, png: bytes) -> None:
    """Differentially exercise the self and administrative avatar uploads of two users."""
    raw_id, raw_user_token = run.users[("accept", "raw")]
    typed_id, typed_user_token = run.users[("accept", "typed")]
    typed_permissions = EffectivePermissions.for_credential(
        UDataCredential(api_key=typed_user_token), platform=CatalogPlatform.UDATA
    )
    raw_avatar_body, raw_avatar_type = _multipart_body(png, "raw.png", media_type="image/png")
    raw_status, _, _ = _direct_request(
        raw_user_token, "POST", "/api/1/me/avatar/", body=raw_avatar_body, content_type=raw_avatar_type
    )
    typed_avatar = await _user_call(
        run,
        typed_user_token,
        "my_avatar",
        UserAvatarInput(BytesIO(png), "typed.png", len(png), "image/png"),
        typed_permissions,
        _user_mutation_policy("my_avatar", "me"),
    )
    _assert_user_mutation(raw_status, typed_avatar, "my_avatar", "me")
    avatar_state = _controlled_user_state((raw_id, typed_id))
    assert avatar_state[raw_id]["avatar_present"]
    assert avatar_state[typed_id]["avatar_present"]
    raw_admin_id, _ = run.users[("refuse", "raw")]
    typed_admin_id, _ = run.users[("refuse", "typed")]
    raw_status, _, _ = _direct_request(
        run.admin_token,
        "POST",
        f"/api/1/users/{raw_admin_id}/avatar/",
        body=raw_avatar_body,
        content_type=raw_avatar_type,
    )
    typed_avatar = await _typed_call(
        run.admin_client.users_tokens,
        "user_avatar",
        typed_admin_id,
        UserAvatarInput(BytesIO(png), "typed-admin.png", len(png), "image/png"),
        run.admin_permissions,
        _user_mutation_policy("user_avatar", typed_admin_id),
    )
    _assert_user_mutation(raw_status, typed_avatar, "user_avatar", typed_admin_id)
    avatar_state = _controlled_user_state((raw_admin_id, typed_admin_id))
    assert avatar_state[raw_admin_id]["avatar_present"]
    assert avatar_state[typed_admin_id]["avatar_present"]


async def _exercise_follow_lifecycle(run: _UserMutationRun, admin_id: str) -> None:
    """Differentially exercise following and unfollowing a user as the administrator."""
    raw_id, _ = run.users[("accept", "raw")]
    typed_id, _ = run.users[("accept", "typed")]
    raw_status, _, _ = _direct_request(run.admin_token, "POST", f"/api/1/users/{raw_id}/followers/")
    typed_follow = await _typed_call(
        run.admin_client.users_tokens,
        "follow_user",
        typed_id,
        run.admin_permissions,
        _user_mutation_policy("follow_user", typed_id),
    )
    _assert_user_mutation(raw_status, typed_follow, "follow_user", typed_id)
    raw_follower_status, raw_followers, _ = _direct_request(
        run.admin_token,
        "GET",
        f"/api/1/users/{raw_id}/followers/?page=1&page_size=20",
    )
    typed_followers = await _typed_call(run.admin_client.users_tokens, "list_user_followers", typed_id)
    assert raw_follower_status == 200
    assert admin_id in _follower_ids(raw_followers)
    assert isinstance(typed_followers, UDataPageEnvelope)
    assert admin_id in _typed_follower_ids(typed_followers)
    raw_status, _, _ = _direct_request(run.admin_token, "DELETE", f"/api/1/users/{raw_id}/followers/")
    typed_unfollow = await _typed_call(
        run.admin_client.users_tokens,
        "unfollow_user",
        typed_id,
        run.admin_permissions,
        _user_mutation_policy("unfollow_user", typed_id, destructive=True),
    )
    _assert_user_mutation(raw_status, typed_unfollow, "unfollow_user", typed_id)
    raw_follower_status, raw_followers, _ = _direct_request(
        run.admin_token,
        "GET",
        f"/api/1/users/{raw_id}/followers/?page=1&page_size=20",
    )
    typed_followers = await _typed_call(run.admin_client.users_tokens, "list_user_followers", typed_id)
    assert raw_follower_status == 200
    assert admin_id not in _follower_ids(raw_followers)
    assert isinstance(typed_followers, UDataPageEnvelope)
    assert admin_id not in _typed_follower_ids(typed_followers)


async def _list_controlled_tokens(run: _UserMutationRun) -> tuple[list[object], tuple[object, ...]]:
    """Return the raw and typed API token listings, asserting both sides are well formed."""
    raw_list_status, raw_token_list, _ = _direct_request(run.admin_token, "GET", "/api/1/me/api_tokens/")
    typed_token_list = await _typed_call(run.admin_client.users_tokens, "list_api_tokens", run.admin_permissions)
    raw_token_rows = raw_token_list.get("data") if isinstance(raw_token_list, Mapping) else raw_token_list
    assert raw_list_status == 200
    assert isinstance(raw_token_rows, list)
    assert isinstance(typed_token_list, tuple)
    return raw_token_rows, typed_token_list


def _assert_controlled_tokens_active(
    raw_token_rows: list[object],
    typed_token_list: tuple[object, ...],
    expected_ids: set[str],
) -> None:
    """Assert both token listings report every id in *expected_ids* as unrevoked."""
    raw_active_token_ids = {
        item["id"]
        for item in raw_token_rows
        if isinstance(item, Mapping) and isinstance(item.get("id"), str) and not item.get("revoked_at")
    }
    typed_active_token_ids = {
        item.id for item in typed_token_list if isinstance(item, ApiTokenMetadata) and item.revoked_at is None
    }
    assert expected_ids <= raw_active_token_ids
    assert expected_ids <= typed_active_token_ids


def _assert_controlled_tokens_revoked(
    raw_token_rows: list[object],
    typed_token_list: tuple[object, ...],
    expected_ids: set[str],
) -> None:
    """Assert both token listings report every id in *expected_ids* as absent or revoked."""
    raw_tokens_by_id = {
        item["id"]: item for item in raw_token_rows if isinstance(item, Mapping) and isinstance(item.get("id"), str)
    }
    typed_tokens_by_id = {item.id: item for item in typed_token_list if isinstance(item, ApiTokenMetadata)}
    for token_id in expected_ids:
        raw_state = raw_tokens_by_id.get(token_id)
        typed_state = typed_tokens_by_id.get(token_id)
        assert raw_state is None or raw_state.get("revoked_at") is not None
        assert typed_state is None or typed_state.revoked_at is not None


async def _exercise_api_token_lifecycle(run: _UserMutationRun) -> None:
    """Differentially exercise creating, listing, and revoking one API token."""
    raw_status, raw_token, _ = _direct_request(
        run.admin_token, "POST", "/api/1/me/api_tokens/", body={"name": "raw controlled"}
    )
    raw_token, raw_token_id = _assert_created_response(raw_status, raw_token, run.token_ids)
    typed_token = await _typed_call(
        run.admin_client.users_tokens,
        "create_api_token",
        ApiTokenCreateInput(name="typed controlled"),
        run.admin_permissions,
        _user_mutation_policy("create_api_token", "new-api-token"),
    )
    if isinstance(typed_token, ApiTokenCreationResult):
        run.token_ids.append(typed_token.metadata.id)
    assert isinstance(typed_token, ApiTokenCreationResult)
    _assert_user_mutation(raw_status, typed_token, "create_api_token", typed_token.metadata.id)
    if not typed_token.secret.reveal_once().startswith("udata_"):
        pytest.fail("Created token did not match the controlled token shape.")
    expected_ids = {raw_token_id, typed_token.metadata.id}
    _assert_controlled_tokens_active(*await _list_controlled_tokens(run), expected_ids)
    raw_status, _, _ = _direct_request(run.admin_token, "DELETE", f"/api/1/me/api_tokens/{raw_token_id}/")
    typed_revoke = await _typed_call(
        run.admin_client.users_tokens,
        "revoke_api_token",
        typed_token.metadata.id,
        run.admin_permissions,
        _user_mutation_policy("revoke_api_token", typed_token.metadata.id, destructive=True),
    )
    _assert_user_mutation(raw_status, typed_revoke, "revoke_api_token", typed_token.metadata.id)
    _assert_controlled_tokens_revoked(*await _list_controlled_tokens(run), expected_ids)


async def _exercise_membership_decisions(run: _UserMutationRun) -> None:
    """Differentially exercise accepting and refusing one organization invitation per user."""
    for decision in ("refuse", "accept"):
        raw_id, raw_user_token = run.users[(decision, "raw")]
        typed_id, typed_user_token = run.users[(decision, "typed")]
        typed_permissions = EffectivePermissions.for_credential(
            UDataCredential(api_key=typed_user_token), platform=CatalogPlatform.UDATA
        )
        raw_org_status, raw_org_state, _ = _direct_request(
            run.admin_token, "GET", f"/api/1/organizations/{run.organizations['raw']}/"
        )
        assert raw_org_status == 200
        assert isinstance(raw_org_state, Mapping)
        assert not any(member["user"]["id"] == raw_id for member in raw_org_state["members"]), decision
        raw_status, raw_invitation, _ = _direct_request(
            run.admin_token,
            "POST",
            f"/api/1/organizations/{run.organizations['raw']}/member/",
            body={"user": raw_id, "role": "editor"},
        )
        assert raw_status == 201
        typed_invitation = await _typed_call(
            run.admin_client.organizations_memberships,
            "invite_organization_member",
            run.organizations["typed"],
            OrganizationInvitationInput(user=typed_id, role="editor"),
            run.admin_permissions,
            _user_mutation_policy("invite_organization_member", run.organizations["typed"]),
        )
        assert isinstance(raw_invitation, Mapping)
        assert isinstance(typed_invitation, OrganizationMutationResult)
        assert typed_invitation.value is not None
        raw_invitation_id = raw_invitation["id"]
        typed_invitation_id = typed_invitation.value["id"]
        assert isinstance(raw_invitation_id, str)
        assert isinstance(typed_invitation_id, str)
        raw_status, _, _ = _direct_request(
            raw_user_token, "POST", f"/api/1/me/org_invitations/{raw_invitation_id}/{decision}/"
        )
        typed_decision = await _user_call(
            run,
            typed_user_token,
            f"{decision}_org_invitation",
            typed_invitation_id,
            typed_permissions,
            _user_mutation_policy(f"{decision}_org_invitation", typed_invitation_id),
        )
        _assert_user_mutation(raw_status, typed_decision, f"{decision}_org_invitation", typed_invitation_id)
        if decision == "accept":
            _assert_membership_accepted(run, raw_id, typed_id)
        else:
            await _assert_no_pending_invitation(run, raw_user_token, typed_user_token, typed_permissions)


def _assert_membership_accepted(run: _UserMutationRun, raw_id: str, typed_id: str) -> None:
    """Assert both the raw and the typed organization list the accepted user as a member."""
    for org_id, user_id in ((run.organizations["raw"], raw_id), (run.organizations["typed"], typed_id)):
        member_status, member_org, _ = _direct_request(run.admin_token, "GET", f"/api/1/organizations/{org_id}/")
        assert member_status == 200
        assert isinstance(member_org, Mapping)
        assert any(member["user"]["id"] == user_id for member in member_org["members"])


async def _assert_no_pending_invitation(
    run: _UserMutationRun,
    raw_user_token: str,
    typed_user_token: str,
    typed_permissions: EffectivePermissions,
) -> None:
    """Assert the refused invitation left no pending invitation on either the raw or typed side."""
    raw_pending_status, raw_pending, _ = _direct_request(raw_user_token, "GET", "/api/1/me/org_invitations/")
    assert raw_pending_status == 200
    assert isinstance(raw_pending, list)
    assert not raw_pending
    typed_pending = await _user_call(run, typed_user_token, "list_org_invitations", typed_permissions)
    assert not typed_pending


async def _exercise_rotation_and_deletion(run: _UserMutationRun) -> None:
    """Differentially exercise password rotation and the self and administrative deletions."""
    raw_id, _ = run.users[("refuse", "raw")]
    typed_id, _ = run.users[("refuse", "typed")]
    before_rotation = _controlled_user_state((raw_id, typed_id))
    assert not before_rotation[raw_id]["password_rotation_requested"]
    assert not before_rotation[typed_id]["password_rotation_requested"]
    raw_status, _, _ = _direct_request(run.admin_token, "POST", f"/api/1/users/{raw_id}/rotate_password/")
    typed_rotated = await _typed_call(
        run.admin_client.users_tokens,
        "rotate_user_password",
        typed_id,
        run.admin_permissions,
        _user_mutation_policy("rotate_user_password", typed_id),
    )
    _assert_user_mutation(raw_status, typed_rotated, "rotate_user_password", typed_id)
    after_rotation = _controlled_user_state((raw_id, typed_id))
    assert after_rotation[raw_id]["password_rotation_requested"]
    assert after_rotation[typed_id]["password_rotation_requested"]

    raw_refuse_id, raw_refuse_token = run.users[("refuse", "raw")]
    typed_refuse_id, typed_refuse_token = run.users[("refuse", "typed")]
    typed_refuse_permissions = EffectivePermissions.for_credential(
        UDataCredential(api_key=typed_refuse_token), platform=CatalogPlatform.UDATA
    )
    raw_status, _, _ = _direct_request(raw_refuse_token, "DELETE", "/api/1/me/")
    typed_deleted_me = await _user_call(
        run,
        typed_refuse_token,
        "delete_me",
        typed_refuse_permissions,
        _user_mutation_policy("delete_me", "me", destructive=True),
    )
    _assert_user_mutation(raw_status, typed_deleted_me, "delete_me", "me")
    raw_accept_id = run.users[("accept", "raw")][0]
    raw_delete_status, _, _ = _direct_request(
        run.admin_token,
        "DELETE",
        f"/api/1/users/{raw_accept_id}/?send_legal_notice=false&no_mail=true&delete_comments=false",
    )
    typed_accept_id = run.users[("accept", "typed")][0]
    typed_deleted_user = await _typed_call(
        run.admin_client.users_tokens,
        "delete_user",
        typed_accept_id,
        run.admin_permissions,
        _user_mutation_policy("delete_user", typed_accept_id, destructive=True),
        UserDeleteOptions(no_mail=True),
    )
    _assert_user_mutation(raw_delete_status, typed_deleted_user, "delete_user", typed_accept_id)
    assert raw_delete_status == 204
    for user_id in (raw_refuse_id, typed_refuse_id, run.users[("accept", "raw")][0], typed_accept_id):
        deleted_status, deleted_user, _ = _direct_request(run.admin_token, "GET", f"/api/1/users/{user_id}/")
        assert deleted_status in {200, 404, 410}
        if deleted_status == 200:
            assert isinstance(deleted_user, Mapping)
            assert deleted_user.get("active") is False


def _cleanup_api_tokens(run: _UserMutationRun) -> None:
    """Delete every API token the run created, recording rather than raising on failure."""
    for token_id in run.token_ids:
        try:
            status, _, _ = _direct_request(run.admin_token, "DELETE", f"/api/1/me/api_tokens/{token_id}/")
            if status not in {204, 404, 410}:
                run.cleanup_errors.append(f"token deletion returned {status}")
        except Exception as error:
            run.cleanup_errors.append(f"token deletion raised {type(error).__name__}")


def _cleanup_user_tokens(run: _UserMutationRun) -> None:
    """Revoke every disposable user token inside the controlled container."""
    if not run.user_token_ids:
        return
    revoke_user_tokens = """
import sys
from udata.app import create_app, standalone
from udata.core.api_token.models import ApiToken
app = standalone(create_app())
with app.app_context():
    for token_id in sys.argv[1:]:
        token = ApiToken.objects(id=token_id).first()
        if token is not None and token.revoked_at is None:
            token.revoke()
        token = ApiToken.objects(id=token_id).first()
        assert token is None or token.revoked_at is not None
"""
    try:
        revoked = subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                "dev/udata-evidence/.env",
                "-f",
                "dev/udata-evidence/compose.yaml",
                "exec",
                "-T",
                "udata",
                "python",
                "-c",
                revoke_user_tokens,
                *run.user_token_ids,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if revoked.returncode:
            run.cleanup_errors.append("disposable user token revocation failed")
    except Exception as error:
        run.cleanup_errors.append(f"disposable user token revocation raised {type(error).__name__}")


def _cleanup_users(run: _UserMutationRun) -> None:
    """Delete every disposable user the run created and verify it is no longer active."""
    for user_id in run.created_user_ids:
        try:
            status, _, _ = _direct_request(
                run.admin_token,
                "DELETE",
                f"/api/1/users/{user_id}/?send_legal_notice=false&no_mail=true&delete_comments=false",
            )
            if status not in {204, 404, 410}:
                run.cleanup_errors.append(f"user deletion returned {status}")
            status, payload, _ = _direct_request(run.admin_token, "GET", f"/api/1/users/{user_id}/")
            if status == 200 and (not isinstance(payload, Mapping) or payload.get("active") is not False):
                run.cleanup_errors.append("user remains active after cleanup")
            elif status not in {200, 404, 410}:
                run.cleanup_errors.append(f"user cleanup verification returned {status}")
        except Exception as error:
            run.cleanup_errors.append(f"user cleanup raised {type(error).__name__}")


def _cleanup_organizations(run: _UserMutationRun) -> None:
    """Delete every organization the run created, recording rather than raising on failure."""
    for org_id in run.organization_cleanup_ids:
        try:
            deleted_status, _, _ = _direct_request(run.admin_token, "DELETE", f"/api/1/organizations/{org_id}/")
            if deleted_status not in {204, 404, 410}:
                run.cleanup_errors.append(f"organization deletion returned {deleted_status}")
        except Exception as error:
            run.cleanup_errors.append(f"organization deletion raised {type(error).__name__}")


async def _cleanup_controlled_user_run(run: _UserMutationRun) -> None:
    """Release every resource the run created and close its client."""
    _cleanup_api_tokens(run)
    _cleanup_user_tokens(run)
    _cleanup_users(run)
    _cleanup_organizations(run)
    if isinstance(run.admin_client, AsyncUDataClient):
        try:
            await run.admin_client.aclose()
        except Exception as error:
            run.cleanup_errors.append(f"client close raised {type(error).__name__}")
    else:
        assert isinstance(run.admin_client, SyncUDataClient)
        try:
            run.admin_client.close()
        except Exception as error:
            run.cleanup_errors.append(f"client close raised {type(error).__name__}")
    assert not run.cleanup_errors, "; ".join(run.cleanup_errors)


def test_controlled_user_mutations_match_raw_routes_in_both_modes() -> None:
    admin_token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not admin_token:
        pytest.skip("controlled user mutations require a seeded disposable administrator")
    admin_status, admin_user, _ = _direct_request(admin_token, "GET", "/api/1/me/")
    assert admin_status == 200
    assert isinstance(admin_user, Mapping)
    admin_id = admin_user.get("id")
    assert isinstance(admin_id, str)
    png = _CONTROLLED_PNG

    async def exercise(async_mode: bool) -> None:
        admin_credential = UDataCredential(api_key=admin_token)
        admin_permissions = EffectivePermissions.for_credential(
            admin_credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
        )
        admin_client = (
            create_async_client(UDataClientSettings(base_url=ORIGIN, credential=admin_credential))
            if async_mode
            else create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=admin_credential))
        )
        run = _UserMutationRun(
            async_mode=async_mode,
            admin_token=admin_token,
            admin_permissions=admin_permissions,
            admin_client=admin_client,
        )
        try:
            await _seed_controlled_organization(run)
            await _seed_controlled_users(run)
            await _exercise_profile_updates(run)
            await _exercise_avatar_uploads(run, png)
            await _exercise_follow_lifecycle(run, admin_id)
            await _exercise_api_token_lifecycle(run)
            await _exercise_membership_decisions(run)
            await _exercise_rotation_and_deletion(run)
        finally:
            await _cleanup_controlled_user_run(run)

    asyncio.run(exercise(False))
    asyncio.run(exercise(True))


def _assert_direct_delete(result: tuple[int, object, dict[str, str]]) -> int:
    status, payload, _ = result
    assert status == 204
    assert payload is None
    return status


def _assert_typed_delete(result: object, *, expected_status: int = 204, operation: str | None = None) -> None:
    receipt = getattr(result, "receipt", None)
    assert receipt is not None
    assert receipt.outcome == "succeeded"
    assert receipt.audit_metadata["status_code"] == expected_status
    if operation is not None:
        assert receipt.operation == operation


def _assert_dataset_absent(result: tuple[int, object, dict[str, str]]) -> None:
    status, payload, _ = result
    assert status in {404, 410} or (
        status == 200 and isinstance(payload, Mapping) and isinstance(payload.get("deleted"), str)
    )


def _multipart_body(content: bytes, file_name: str, *, media_type: str | None = None) -> tuple[bytes, str]:
    boundary = "datasluice-evidence-boundary"
    part_headers = [f'Content-Disposition: form-data; name="file"; filename="{file_name}"'.encode()]
    if media_type is not None:
        part_headers.append(f"Content-Type: {media_type}".encode())
    body = b"\r\n".join(
        (
            f"--{boundary}".encode(),
            *part_headers,
            b"",
            content,
            f"--{boundary}--".encode(),
            b"",
        )
    )
    return body, f"multipart/form-data; boundary={boundary}"


def _direct_site_patch(
    row: Mapping[str, object], token: str, body: Mapping[str, object]
) -> tuple[int, str, dict[str, object]]:
    method = row["method"]
    path = row["path"]
    request_media_type = row["request_media_type"]
    assert isinstance(method, str)
    assert isinstance(path, str)
    assert isinstance(request_media_type, str)
    request_fields = row["request_fields"]
    assert isinstance(request_fields, list)
    assert set(body) <= set(request_fields)
    request = Request(
        f"{ORIGIN}{path}",
        data=json.dumps(dict(body)).encode(),
        headers={"Content-Type": request_media_type, "X-API-KEY": token},
        method=method,
    )
    try:
        response = build_opener(_DirectNoRedirect()).open(request, timeout=10)
    except HTTPError as error:
        response = error
    with response:
        response_body = response.read(8193)
        assert len(response_body) <= 8192
        payload = json.loads(response_body)
        assert isinstance(payload, Mapping)
        status = response.status
        media_type = response.headers.get_content_type()
        assert type(status) is int
        assert isinstance(media_type, str)
        return status, media_type, {field: payload.get(field) for field in ("id", "title", "version", "feed_size")}


def test_controlled_stack_proves_exact_version_then_one_dataset_read() -> None:
    settings = UDataClientSettings(base_url=ORIGIN)
    with create_sync_client(settings) as client:
        assert client.site_version().version == "17.6.0"
        envelope = client.datasets_list(
            CatalogOperationRequest(operation_id=_FAMILY_OPERATION_ID, payload={"page": 1, "page_size": 5}),
            CatalogOperationGuard(operation_id=_FAMILY_OPERATION_ID),
        )

    assert envelope.page is not None
    assert envelope.page.total_items is not None
    assert envelope.page.total_items > 0
    assert envelope.items, "expected seeded datasets on the controlled stack"
    for record in envelope.items:
        assert record.id.value


def test_controlled_stack_proves_dataset_family_reads() -> None:
    settings = UDataClientSettings(base_url=ORIGIN)
    with create_sync_client(settings) as client:
        page = client.datasets.list(DatasetListQuery(page=1, page_size=3))
        suggestions = client.datasets.suggest(DatasetSuggestQuery(q="evidence", size=3))
        v2_page = client.datasets.list_v2(DatasetListQuery(page=1, page_size=3))

    assert page.page is not None
    assert page.page.total_items is not None
    assert page.items, "expected seeded datasets on the controlled stack"
    assert isinstance(suggestions, tuple)
    assert v2_page.page is not None


def _taxonomy_payloads(records: object) -> list[object]:
    assert isinstance(records, tuple)
    return [record.payload for record in records]


def _taxonomy_read_routes(dataset_id: str) -> tuple[tuple[str, str, tuple[object, ...]], ...]:
    """Return the raw route, typed method name, and arguments of every controlled taxonomy read."""
    return (
        ("/api/1/datasets/badges/", "available_badges", ()),
        ("/api/1/datasets/suggest/formats/?q=cs&size=5", "suggest_formats", (SuggestQuery("cs", 5),)),
        ("/api/1/datasets/suggest/mime/?q=js&size=5", "suggest_mime", (SuggestQuery("js", 5),)),
        ("/api/1/datasets/licenses/", "licenses", ()),
        ("/api/1/datasets/frequencies/", "frequencies", ()),
        ("/api/1/datasets/extensions/", "extensions", ()),
        ("/api/1/datasets/schemas/", "schemas", ()),
        (f"/api/2/datasets/{dataset_id}/schemas/", "dataset_schemas", (dataset_id,)),
    )


def _assert_taxonomy_read_matches_raw(method: str, payload: object, typed: Any) -> None:
    """Assert one taxonomy read agrees with the payload its raw route returned."""
    if method == "extensions":
        assert payload == list(typed)
    elif method == "available_badges":
        assert _plain_json(typed.payload) == payload
    else:
        assert _taxonomy_payloads(typed) == payload


def test_controlled_taxonomy_reads_match_raw_routes_in_both_modes() -> None:
    with create_sync_client(UDataClientSettings(base_url=ORIGIN)) as client:
        dataset_id = client.datasets.list(DatasetListQuery(page=1, page_size=1)).items[0].id.value
        for path, method, args in _taxonomy_read_routes(dataset_id):
            status, payload, _ = _direct_request("", "GET", path)
            typed = getattr(client.taxonomies, method)(*args)
            assert status == 200
            _assert_taxonomy_read_matches_raw(method, payload, typed)

    async def run_async() -> None:
        async with create_async_client(UDataClientSettings(base_url=ORIGIN)) as client:
            dataset_id = (await client.datasets.list(DatasetListQuery(page=1, page_size=1))).items[0].id.value
            for path, method, args in _taxonomy_read_routes(dataset_id):
                status, payload, _ = _direct_request("", "GET", path)
                operation = getattr(client.taxonomies, method)(*args)
                typed = await operation
                assert status == 200
                _assert_taxonomy_read_matches_raw(method, payload, typed)

    asyncio.run(run_async())


def test_controlled_taxonomy_badge_lifecycle_matches_raw_in_both_modes() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled taxonomy mutations require UDATA_EVIDENCE_ADMIN_TOKEN")
    from datasluice.connectors.catalog.udata.models.datasets import DatasetCreateInput, DatasetDeleteOptions
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )

    def policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
        return MutationPolicy(
            destructive=destructive,
            confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
            concurrency=ConcurrencyPolicy(overwrite=True),
        )

    def exercise(mode: str) -> None:
        dataset_id: str | None = None
        raw_kind = "inspire" if mode == "sync" else "hvd"
        typed_kind = "pivotal-data"
        try:
            with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
                created = client.datasets.create(
                    DatasetCreateInput(title=f"Taxonomy evidence {mode}", description="d"),
                    permissions,
                    policy("udata/api-v1.create-dataset", f"Taxonomy evidence {mode}"),
                )
                assert created.record is not None
                dataset_id = created.record.id.value
                try:
                    client.taxonomies.add_badge(dataset_id, BadgeCreateInput(typed_kind), permissions, None)
                except CatalogError as error:
                    assert "mutation_receipt" in error.__dict__
                else:
                    raise AssertionError("unconfirmed taxonomy mutation was dispatched")

                raw_status, raw_payload, _ = _direct_request(
                    token, "POST", f"/api/1/datasets/{dataset_id}/badges/", body={"kind": raw_kind}
                )
                typed = client.taxonomies.add_badge(
                    dataset_id,
                    BadgeCreateInput(typed_kind),
                    permissions,
                    policy("udata/api-v1.add-dataset-badge", dataset_id),
                )
                assert raw_status in {200, 201}
                assert isinstance(raw_payload, Mapping)
                assert raw_payload["kind"] == raw_kind
                assert isinstance(typed, TaxonomyMutationResult)
                assert typed.record is not None
                assert typed.record.payload["kind"] == typed_kind
                assert typed.receipt.operation == "udata/api-v1.add-dataset-badge"
                assert typed.receipt.audit_metadata["status_code"] in {200, 201}

                status, payload, _ = _direct_request(token, "GET", "/api/1/datasets/badges/")
                available = client.taxonomies.available_badges()
                assert status == 200
                assert isinstance(payload, Mapping)
                available_kinds = set(available.payload)
                assert {raw_kind, typed_kind} <= set(available_kinds)

                raw_delete_status, _, _ = _direct_request(
                    token, "DELETE", f"/api/1/datasets/{dataset_id}/badges/{raw_kind}/"
                )
                deleted = client.taxonomies.delete_badge(
                    dataset_id,
                    typed_kind,
                    permissions,
                    policy("udata/api-v1.delete-dataset-badge", f"{dataset_id}:{typed_kind}", destructive=True),
                )
                assert raw_delete_status == 204
                assert deleted.record is None
                assert deleted.receipt.operation == "udata/api-v1.delete-dataset-badge"
                assert deleted.receipt.audit_metadata["status_code"] == 204
                _, dataset_payload, _ = _direct_request(token, "GET", f"/api/1/datasets/{dataset_id}/")
                remaining = (
                    {badge["kind"] for badge in dataset_payload.get("badges", [])}
                    if isinstance(dataset_payload, Mapping)
                    else set()
                )
                assert raw_kind not in remaining
                assert typed_kind not in remaining
        finally:
            if dataset_id is not None:
                with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
                    client.datasets.delete(
                        dataset_id,
                        permissions,
                        DatasetDeleteOptions(),
                        MutationPolicy(
                            destructive=True,
                            confirmation=ConfirmationPolicy(
                                confirmed=True, operation="udata/api-v1.delete-dataset", target=dataset_id
                            ),
                            concurrency=ConcurrencyPolicy(overwrite=True),
                        ),
                    )

    exercise("sync")

    async def run_async() -> None:
        async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
            created = await client.datasets.create(
                DatasetCreateInput(title="Taxonomy evidence async", description="d"),
                permissions,
                policy("udata/api-v1.create-dataset", "Taxonomy evidence async"),
            )
            assert created.record is not None
            dataset_id = created.record.id.value
            try:
                typed = await client.taxonomies.add_badge(
                    dataset_id,
                    BadgeCreateInput("pivotal-data"),
                    permissions,
                    policy("udata/api-v1.add-dataset-badge", dataset_id),
                )
                deleted = await client.taxonomies.delete_badge(
                    dataset_id,
                    "pivotal-data",
                    permissions,
                    policy("udata/api-v1.delete-dataset-badge", f"{dataset_id}:pivotal-data", destructive=True),
                )
                assert typed.record is not None
                assert deleted.record is None
            finally:
                await client.datasets.delete(
                    dataset_id,
                    permissions,
                    DatasetDeleteOptions(),
                    MutationPolicy(
                        destructive=True,
                        confirmation=ConfirmationPolicy(
                            confirmed=True, operation="udata/api-v1.delete-dataset", target=dataset_id
                        ),
                        concurrency=ConcurrencyPolicy(overwrite=True),
                    ),
                )

    asyncio.run(run_async())


def test_controlled_organization_family_matches_raw_shapes_and_cleans_up() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled organization evidence requires UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    organization_id = "evidence-organization"
    settings = UDataClientSettings(base_url=ORIGIN, credential=credential)
    with create_sync_client(settings) as client:
        direct_status, direct_payload, _ = _direct_request(token, "GET", f"/api/1/organizations/{organization_id}/")
        typed = client.organizations_memberships.get_organization(organization_id)
        assert direct_status == 200
        assert isinstance(direct_payload, Mapping)
        assert direct_payload["id"] == typed.id.value
        assert direct_payload["name"] == typed.payload["name"]

        direct_status, direct_payload, _ = _direct_request(token, "GET", "/api/1/organizations/?page=1&page_size=20")
        page = client.organizations_memberships.list_organizations(OrganizationListQuery(page=1, page_size=20))
        assert direct_status == 200
        assert isinstance(direct_payload, Mapping)
        assert isinstance(direct_payload.get("data"), list)
        assert {item["id"] for item in direct_payload["data"]} >= {item.id.value for item in page.items}

        direct_status, direct_payload, _ = _direct_request(
            token, "GET", f"/api/1/organizations/{organization_id}/datasets/?page=1&page_size=20"
        )
        datasets = client.organizations_memberships.list_organization_datasets(organization_id)
        assert direct_status == 200
        assert isinstance(direct_payload, Mapping)
        assert isinstance(direct_payload.get("data"), list)
        assert {item["id"] for item in direct_payload["data"]} == {item.id.value for item in datasets.items}

        direct_status, _, _ = _direct_request(token, "GET", "/api/1/organizations/roles/")
        assert direct_status == 200
        assert client.organizations_memberships.org_roles()
        assert client.organizations_memberships.get_organization_extras(organization_id) is not None
        assert client.organizations_memberships.list_organization_followers(organization_id) is not None

        created_id: str | None = None
        try:
            created = client.organizations_memberships.create_organization(
                OrganizationCreateInput(name="Controlled Organization", description="Task 04-04"),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True,
                        operation="udata/api-v1.create-organization",
                        target="Controlled Organization",
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
            assert created.record is not None
            created_id = created.record.id.value
            assert created.receipt.outcome == "succeeded"
            update = client.organizations_memberships.update_organization(
                created_id,
                OrganizationUpdateInput(description="Task 04-04 updated"),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.update-organization", target=created_id
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
            assert update.receipt.outcome == "succeeded"
            status, payload, _ = _direct_request(token, "GET", f"/api/1/organizations/{created_id}/")
            assert status == 200
            assert isinstance(payload, Mapping)
            assert payload["description"] == "Task 04-04 updated"
        finally:
            if created_id is not None:
                deleted = client.organizations_memberships.delete_organization(
                    created_id,
                    permissions,
                    MutationPolicy(
                        destructive=True,
                        confirmation=ConfirmationPolicy(
                            confirmed=True, operation="udata/api-v1.delete-organization", target=created_id
                        ),
                        concurrency=ConcurrencyPolicy(overwrite=True),
                    ),
                )
                assert deleted.receipt.outcome == "succeeded"
                status, payload, _ = _direct_request(token, "GET", f"/api/1/organizations/{created_id}/")
                assert status in {404, 410} or (
                    status == 200 and isinstance(payload, Mapping) and payload.get("deleted")
                )


def test_controlled_async_organization_reads_match_raw_shapes() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled organization evidence requires UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")

    credential = UDataCredential(api_key=token)

    async def run() -> None:
        async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
            status, payload, _ = _direct_request(token, "GET", "/api/1/organizations/evidence-organization/")
            typed = await client.organizations_memberships.get_organization("evidence-organization")
            assert status == 200
            assert isinstance(payload, Mapping)
            assert payload["id"] == typed.id.value
            status, payload, _ = _direct_request(token, "GET", "/api/1/organizations/?page=1&page_size=20")
            page = await client.organizations_memberships.list_organizations(
                OrganizationListQuery(page=1, page_size=20)
            )
            assert status == 200
            assert isinstance(payload, Mapping)
            assert page.items
            assert {item["id"] for item in payload["data"]} >= {item.id.value for item in page.items}

    asyncio.run(run())


def test_controlled_organization_read_matrix_matches_raw_routes() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled organization evidence requires UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    credential = UDataCredential(api_key=token)
    organization_id = "evidence-organization"
    admin_permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    reads = (
        (f"/api/1/organizations/{organization_id}/", "get_organization", (organization_id,)),
        (
            "/api/1/organizations/?page=1&page_size=20",
            "list_organizations",
            (OrganizationListQuery(page=1, page_size=20),),
        ),
        (f"/api/1/organizations/{organization_id}/datasets.csv", "organization_datasets_csv", (organization_id,)),
        (
            f"/api/1/organizations/{organization_id}/dataservices.csv",
            "organization_dataservices_csv",
            (organization_id,),
        ),
        (f"/api/1/organizations/{organization_id}/discussions.csv", "organization_discussions_csv", (organization_id,)),
        (
            f"/api/1/organizations/{organization_id}/datasets-resources.csv",
            "organization_datasets_resources_csv",
            (organization_id,),
        ),
        (f"/api/1/organizations/{organization_id}/catalog", "rdf_organization", (organization_id,)),
        (f"/api/1/organizations/{organization_id}/catalog.ttl", "rdf_organization_format", (organization_id, "ttl")),
        ("/api/1/organizations/badges/", "available_organization_badges", ()),
        (
            f"/api/1/organizations/{organization_id}/contacts/?page=1&page_size=20",
            "get_organization_contact_point",
            (organization_id,),
        ),
        (
            f"/api/1/organizations/{organization_id}/contacts/suggest/?q=ev&size=10",
            "suggest_org_contact_points",
            (organization_id, OrganizationSuggestQuery("ev")),
        ),
        (
            f"/api/1/organizations/{organization_id}/membership/",
            "list_membership_requests",
            (organization_id, admin_permissions),
        ),
        (
            f"/api/1/organizations/{organization_id}/assignments/",
            "list_organization_assignments",
            (organization_id, admin_permissions),
        ),
        ("/api/1/organizations/suggest/?q=ev&size=10", "suggest_organizations", (OrganizationSuggestQuery("ev"),)),
        (
            f"/api/1/organizations/{organization_id}/datasets/?page=1&page_size=20",
            "list_organization_datasets",
            (organization_id,),
        ),
        (f"/api/1/organizations/{organization_id}/reuses/", "list_organization_reuses", (organization_id,)),
        (f"/api/1/organizations/{organization_id}/discussions/", "list_organization_discussions", (organization_id,)),
        ("/api/1/organizations/roles/", "org_roles", ()),
        ("/api/2/organizations/search/?page=1&page_size=20", "search_organizations", ()),
        (f"/api/2/organizations/{organization_id}/extras/", "get_organization_extras", (organization_id,)),
        (f"/api/1/organizations/{organization_id}/followers/", "list_organization_followers", (organization_id,)),
    )

    def verify_read(
        method: str, status: int, payload: object, headers: Mapping[str, str], operation: Callable[[], object]
    ) -> None:
        try:
            typed = operation()
        except CatalogError as error:
            assert error.metadata.get("status_code") == status
        else:
            assert status in {200, 302}
            _assert_typed_read_matches_raw(method, status, payload, headers, typed)

    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        for path, method, args in reads:
            status, payload, headers = _direct_request(token, "GET", path, include_body=True)
            verify_read(
                method,
                status,
                payload,
                headers,
                lambda method=method, args=args: getattr(client.organizations_memberships, method)(*args),
            )

    async def run_async() -> None:
        async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
            for path, method, args in reads:
                status, payload, headers = _direct_request(token, "GET", path, include_body=True)
                operation = getattr(client.organizations_memberships, method)(*args)
                try:
                    typed = await operation if isawaitable(operation) else operation
                except CatalogError as error:
                    assert error.metadata.get("status_code") == status
                else:
                    assert status in {200, 302}
                    _assert_typed_read_matches_raw(method, status, payload, headers, typed)

    asyncio.run(run_async())


def _organization_mutation_policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    """Return the confirmed overwrite policy the controlled organization mutations run under."""
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _assert_organization_mutation(
    raw_status: int, typed: OrganizationMutationResult, operation: str, statuses: set[int]
) -> None:
    """Assert one typed organization mutation reproduced a raw status its route may return."""
    receipt = typed.receipt
    assert raw_status in statuses
    assert receipt.operation == operation
    assert receipt.outcome == "succeeded"
    assert receipt.audit_metadata["status_code"] == raw_status


@dataclass
class _OrganizationMutationRun:
    """Mutable per-run state shared by the controlled organization mutation phases."""

    run_id: str
    admin_token: str
    member_token: str
    organization_admin_token: str
    admin_permissions: EffectivePermissions
    member_permissions: EffectivePermissions
    org_admin_permissions: EffectivePermissions
    admin_client: SyncUDataClient | AsyncUDataClient
    member_client: SyncUDataClient | AsyncUDataClient
    org_admin_client: SyncUDataClient | AsyncUDataClient
    raw_org_id: str | None = None
    typed_org_id: str | None = None
    raw_delete_status: int | None = None
    cleanup_errors: list[Exception] = field(default_factory=list)


async def _typed_membership_call(client: SyncUDataClient | AsyncUDataClient, method: str, *args: object) -> Any:
    """Invoke the typed organization method *method* on *client*, awaiting it when it returns an awaitable."""
    result = getattr(client.organizations_memberships, method)(*args)
    return await result if isawaitable(result) else result


async def _record_organization_cleanup(
    run: _OrganizationMutationRun,
    action: Callable[[], object],
    *,
    expected_status: int | None = None,
    operation: str | None = None,
) -> object | None:
    """Run one cleanup action, recording rather than raising when it fails or disagrees with raw."""
    try:
        result = action()
        if isawaitable(result):
            result = await result
    except Exception as error:
        run.cleanup_errors.append(error)
        return None
    if isinstance(result, OrganizationMutationResult):
        receipt = result.receipt
        if (
            expected_status is None
            or operation is None
            or receipt.operation != operation
            or receipt.outcome != "succeeded"
            or receipt.audit_metadata["status_code"] != expected_status
        ):
            run.cleanup_errors.append(AssertionError("typed organization delete receipt did not match raw delete"))
    return result


@dataclass(frozen=True)
class _SeededOrganizations:
    """The paired raw and typed organizations the seeding phase created for every later phase."""

    raw_org_id: str
    typed_org_id: str


def _organization_run(
    run_id: str,
    admin_token: str,
    member_token: str,
    organization_admin_token: str,
    admin_client: SyncUDataClient | AsyncUDataClient,
    member_client: SyncUDataClient | AsyncUDataClient,
    org_admin_client: SyncUDataClient | AsyncUDataClient,
) -> _OrganizationMutationRun:
    """Build the per-run state and permissions the controlled organization differential shares."""
    admin_credential = UDataCredential(api_key=admin_token)
    return _OrganizationMutationRun(
        run_id=run_id,
        admin_token=admin_token,
        member_token=member_token,
        organization_admin_token=organization_admin_token,
        admin_permissions=EffectivePermissions.for_credential(
            admin_credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
        ),
        member_permissions=EffectivePermissions.for_credential(
            UDataCredential(api_key=member_token), platform=CatalogPlatform.UDATA
        ),
        org_admin_permissions=EffectivePermissions.for_credential(
            UDataCredential(api_key=organization_admin_token), platform=CatalogPlatform.UDATA
        ),
        admin_client=admin_client,
        member_client=member_client,
        org_admin_client=org_admin_client,
    )


async def _seed_controlled_organizations(run: _OrganizationMutationRun) -> _SeededOrganizations:
    """Create the paired raw and typed organizations the organization phases share."""
    raw_status, raw_org, _ = _direct_request(
        run.admin_token,
        "POST",
        "/api/1/organizations/",
        body={"name": f"Raw Org {run.run_id}", "description": "route differential"},
    )
    assert raw_status == 201
    assert isinstance(raw_org, Mapping)
    assert isinstance(raw_org.get("id"), str)
    run.raw_org_id = raw_org["id"]
    typed_created = await _typed_membership_call(
        run.admin_client,
        "create_organization",
        OrganizationCreateInput(name=f"Typed Org {run.run_id}", description="route differential"),
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.create-organization", f"Typed Org {run.run_id}"),
    )
    _assert_organization_mutation(201, typed_created, "udata/api-v1.create-organization", {201})
    assert typed_created.record is not None
    run.typed_org_id = typed_created.record.id.value
    return _SeededOrganizations(run.raw_org_id, run.typed_org_id)


async def _exercise_organization_update(run: _OrganizationMutationRun, orgs: _SeededOrganizations) -> None:
    """Differentially exercise the administrative organization description update."""
    raw_org_id, typed_org_id = orgs.raw_org_id, orgs.typed_org_id
    direct_status, direct_updated, _ = _direct_request(
        run.admin_token,
        "PUT",
        f"/api/1/organizations/{raw_org_id}/",
        body={"description": "raw updated"},
    )
    typed_updated = await _typed_membership_call(
        run.admin_client,
        "update_organization",
        typed_org_id,
        OrganizationUpdateInput(description="typed updated"),
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.update-organization", typed_org_id),
    )
    assert isinstance(direct_updated, Mapping)
    assert direct_updated["description"] == "raw updated"
    _assert_organization_mutation(direct_status, typed_updated, "udata/api-v1.update-organization", {200})
    assert typed_updated.record is not None
    assert typed_updated.record.payload["description"] == "typed updated"


async def _exercise_membership_request_and_accept(run: _OrganizationMutationRun, orgs: _SeededOrganizations) -> object:
    """Differentially request membership as the member and accept it, returning the member id."""
    raw_org_id, typed_org_id = orgs.raw_org_id, orgs.typed_org_id
    direct_status, direct_request, _ = _direct_request(
        run.member_token,
        "POST",
        f"/api/1/organizations/{raw_org_id}/membership/",
        body={"comment": "raw join"},
    )
    typed_request = await _typed_membership_call(
        run.member_client,
        "membership_request",
        typed_org_id,
        MembershipRequestInput(comment="typed join"),
        run.member_permissions,
        _organization_mutation_policy("udata/api-v1.membership-request", typed_org_id),
    )
    _assert_organization_mutation(direct_status, typed_request, "udata/api-v1.membership-request", {200, 201})
    assert isinstance(direct_request, Mapping)
    assert isinstance(typed_request.value, Mapping)
    raw_request_id = direct_request["id"]
    typed_request_id = typed_request.value["id"]
    member_id = direct_request["user"]["id"]
    raw_status, raw_requests, _ = _direct_request(
        run.admin_token, "GET", f"/api/1/organizations/{raw_org_id}/membership/"
    )
    typed_requests = await _typed_membership_call(
        run.admin_client, "list_membership_requests", typed_org_id, run.admin_permissions
    )
    assert raw_status == 200
    assert isinstance(raw_requests, list)
    assert any(item["id"] == raw_request_id for item in raw_requests)
    assert any(item.payload["id"] == typed_request_id for item in typed_requests)

    direct_status, direct_member, _ = _direct_request(
        run.admin_token,
        "POST",
        f"/api/1/organizations/{raw_org_id}/membership/{raw_request_id}/accept/",
    )
    typed_member = await _typed_membership_call(
        run.admin_client,
        "accept_membership",
        typed_org_id,
        typed_request_id,
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.accept-membership", f"{typed_org_id}/{typed_request_id}"),
    )
    assert isinstance(direct_member, Mapping)
    assert direct_member["user"]["id"] == member_id
    _assert_organization_mutation(direct_status, typed_member, "udata/api-v1.accept-membership", {200})
    assert isinstance(typed_member.value, Mapping)
    assert typed_member.value["user"]["id"] == member_id
    return member_id


async def _exercise_member_role_and_assignments(
    run: _OrganizationMutationRun, orgs: _SeededOrganizations, member_id: object
) -> None:
    """Differentially exercise the member role update, assignment listing, and assignment sync."""
    raw_org_id, typed_org_id = orgs.raw_org_id, orgs.typed_org_id
    raw_status, raw_role, _ = _direct_request(
        run.admin_token,
        "PUT",
        f"/api/1/organizations/{raw_org_id}/member/{member_id}/",
        body={"role": "partial_editor"},
    )
    typed_role = await _typed_membership_call(
        run.admin_client,
        "update_organization_member",
        typed_org_id,
        member_id,
        OrganizationMemberInput("partial_editor"),
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.update-organization-member", f"{typed_org_id}/{member_id}"),
    )
    assert isinstance(raw_role, Mapping)
    assert raw_role["role"] == "partial_editor"
    _assert_organization_mutation(raw_status, typed_role, "udata/api-v1.update-organization-member", {200})
    assert isinstance(typed_role.value, Mapping)
    assert typed_role.value["role"] == "partial_editor"
    raw_status, raw_assignments, _ = _direct_request(
        run.admin_token, "GET", f"/api/1/organizations/{raw_org_id}/assignments/"
    )
    typed_assignments = await _typed_membership_call(
        run.admin_client, "list_organization_assignments", typed_org_id, run.admin_permissions
    )
    assert raw_status == 200
    assert isinstance(raw_assignments, list)
    assert not typed_assignments
    raw_status, raw_synced, _ = _direct_request(
        run.admin_token,
        "PUT",
        f"/api/1/organizations/{raw_org_id}/member/{member_id}/assignments/",
        body=[],
    )
    typed_synced = await _typed_membership_call(
        run.admin_client,
        "sync_member_assignments",
        typed_org_id,
        member_id,
        [],
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.sync-member-assignments", f"{typed_org_id}/{member_id}"),
    )
    assert raw_status == 200
    assert raw_synced == []
    _assert_organization_mutation(raw_status, typed_synced, "udata/api-v1.sync-member-assignments", {200})


async def _exercise_member_removal(
    run: _OrganizationMutationRun, orgs: _SeededOrganizations, member_id: object
) -> None:
    """Differentially remove the member from both organizations."""
    raw_org_id, typed_org_id = orgs.raw_org_id, orgs.typed_org_id
    raw_status, raw_member_delete, _ = _direct_request(
        run.admin_token,
        "DELETE",
        f"/api/1/organizations/{raw_org_id}/member/{member_id}/",
    )
    typed_member_delete = await _typed_membership_call(
        run.admin_client,
        "delete_organization_member",
        typed_org_id,
        member_id,
        run.admin_permissions,
        _organization_mutation_policy(
            "udata/api-v1.delete-organization-member",
            f"{typed_org_id}/{member_id}",
            destructive=True,
        ),
    )
    assert raw_status == 204
    assert raw_member_delete is None
    _assert_organization_mutation(raw_status, typed_member_delete, "udata/api-v1.delete-organization-member", {204})


async def _exercise_membership_refusal(run: _OrganizationMutationRun, orgs: _SeededOrganizations) -> None:
    """Differentially request membership as the organization administrator and refuse it."""
    raw_org_id, typed_org_id = orgs.raw_org_id, orgs.typed_org_id
    direct_status, raw_pending, _ = _direct_request(
        run.organization_admin_token,
        "POST",
        f"/api/1/organizations/{raw_org_id}/membership/",
        body={"comment": "raw refusal"},
    )
    typed_pending = await _typed_membership_call(
        run.org_admin_client,
        "membership_request",
        typed_org_id,
        MembershipRequestInput(comment="typed refusal"),
        run.org_admin_permissions,
        _organization_mutation_policy("udata/api-v1.membership-request", typed_org_id),
    )
    _assert_organization_mutation(direct_status, typed_pending, "udata/api-v1.membership-request", {201})
    assert isinstance(raw_pending, Mapping)
    assert isinstance(typed_pending.value, Mapping)
    raw_status, raw_refused, _ = _direct_request(
        run.admin_token,
        "POST",
        f"/api/1/organizations/{raw_org_id}/membership/{raw_pending['id']}/refuse/",
        body={"comment": "raw refused"},
    )
    typed_refused = await _typed_membership_call(
        run.admin_client,
        "refuse_membership",
        typed_org_id,
        typed_pending.value["id"],
        OrganizationRefusalInput("typed refused"),
        run.admin_permissions,
        _organization_mutation_policy(
            "udata/api-v1.refuse-membership",
            f"{typed_org_id}/{typed_pending.value['id']}",
        ),
    )
    assert raw_refused == {}
    _assert_organization_mutation(raw_status, typed_refused, "udata/api-v1.refuse-membership", {200})


async def _exercise_invitation_and_cancel(run: _OrganizationMutationRun, orgs: _SeededOrganizations) -> None:
    """Differentially invite the seeded organization administrator and cancel the invitation."""
    raw_org_id, typed_org_id = orgs.raw_org_id, orgs.typed_org_id
    seeded_status, seeded_organization, _ = _direct_request(
        run.admin_token, "GET", "/api/1/organizations/evidence-organization/"
    )
    assert seeded_status == 200
    assert isinstance(seeded_organization, Mapping)
    seeded_members = seeded_organization["members"]
    assert isinstance(seeded_members, list)
    invited_user_id = next(
        member["user"]["id"]
        for member in seeded_members
        if member["user"]["email"] == "organization-admin@evidence.invalid"
    )
    direct_status, raw_invitation, _ = _direct_request(
        run.admin_token,
        "POST",
        f"/api/1/organizations/{raw_org_id}/member/",
        body={"user": invited_user_id, "role": "editor"},
    )
    typed_invitation = await _typed_membership_call(
        run.admin_client,
        "invite_organization_member",
        typed_org_id,
        OrganizationInvitationInput(user=invited_user_id, role="editor"),
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.invite-organization-member", typed_org_id),
    )
    _assert_organization_mutation(direct_status, typed_invitation, "udata/api-v1.invite-organization-member", {201})
    assert isinstance(raw_invitation, Mapping)
    assert isinstance(typed_invitation.value, Mapping)
    assert raw_invitation["user"]["id"] == invited_user_id
    assert typed_invitation.value["user"]["id"] == invited_user_id
    raw_status, raw_cancel, _ = _direct_request(
        run.admin_token,
        "POST",
        f"/api/1/organizations/{raw_org_id}/membership/{raw_invitation['id']}/cancel/",
    )
    typed_cancel = await _typed_membership_call(
        run.admin_client,
        "cancel_membership",
        typed_org_id,
        typed_invitation.value["id"],
        run.admin_permissions,
        _organization_mutation_policy(
            "udata/api-v1.cancel-membership",
            f"{typed_org_id}/{typed_invitation.value['id']}",
        ),
    )
    assert raw_cancel == {}
    _assert_organization_mutation(raw_status, typed_cancel, "udata/api-v1.cancel-membership", {200})


async def _exercise_organization_badges(run: _OrganizationMutationRun, orgs: _SeededOrganizations) -> None:
    """Differentially add and delete the certified badge on both organizations."""
    raw_org_id, typed_org_id = orgs.raw_org_id, orgs.typed_org_id
    direct_status, raw_badge, _ = _direct_request(
        run.admin_token,
        "POST",
        f"/api/1/organizations/{raw_org_id}/badges/",
        body={"kind": "certified"},
    )
    typed_badge = await _typed_membership_call(
        run.admin_client,
        "add_organization_badge",
        typed_org_id,
        "certified",
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.add-organization-badge", f"{typed_org_id}/certified"),
    )
    assert isinstance(raw_badge, Mapping)
    _assert_organization_mutation(direct_status, typed_badge, "udata/api-v1.add-organization-badge", {200, 201})
    raw_status, raw_badge_delete, _ = _direct_request(
        run.admin_token, "DELETE", f"/api/1/organizations/{raw_org_id}/badges/certified/"
    )
    typed_badge_delete = await _typed_membership_call(
        run.admin_client,
        "delete_organization_badge",
        typed_org_id,
        "certified",
        run.admin_permissions,
        _organization_mutation_policy(
            "udata/api-v1.delete-organization-badge",
            f"{typed_org_id}/certified",
            destructive=True,
        ),
    )
    _assert_organization_mutation(raw_status, typed_badge_delete, "udata/api-v1.delete-organization-badge", {200, 204})


async def _exercise_organization_logo(run: _OrganizationMutationRun, org_id: str, side: int, png: bytes) -> None:
    """Upload and resize the logo of one organization with an independent multipart filename."""
    from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput

    # Each upload uses the same tiny valid PNG bytes with an independent filename.
    boundary = f"udata-org-logo-{run.run_id}-{int(side)}"
    logo_name = f"logo-{run.run_id}-{int(side)}.png"
    raw_logo_body = b"\r\n".join(
        (
            f"--{boundary}".encode(),
            f'Content-Disposition: form-data; name="file"; filename="{logo_name}"'.encode(),
            b"Content-Type: image/png",
            b"",
            png,
            f"--{boundary}--".encode(),
            b"",
        )
    )
    direct_status, _, _ = _direct_request(
        run.admin_token,
        "POST",
        f"/api/1/organizations/{org_id}/logo/",
        body=raw_logo_body,
        content_type=f"multipart/form-data; boundary={boundary}",
    )
    typed_logo = await _typed_membership_call(
        run.admin_client,
        "organization_logo",
        org_id,
        ResourceUploadInput(BytesIO(png), logo_name, len(png), "image/png"),
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.organization-logo", org_id),
    )
    _assert_organization_mutation(direct_status, typed_logo, "udata/api-v1.organization-logo", {200})
    resize_status, _, _ = _direct_request(
        run.admin_token,
        "PUT",
        f"/api/1/organizations/{org_id}/logo/",
        body=raw_logo_body,
        content_type=f"multipart/form-data; boundary={boundary}",
    )
    typed_resize = await _typed_membership_call(
        run.admin_client,
        "resize_organization_logo",
        org_id,
        ResourceUploadInput(BytesIO(png), logo_name, len(png), "image/png"),
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.resize-organization-logo", org_id),
    )
    _assert_organization_mutation(resize_status, typed_resize, "udata/api-v1.resize-organization-logo", {200})


async def _exercise_organization_logos(run: _OrganizationMutationRun, orgs: _SeededOrganizations, png: bytes) -> None:
    """Upload and resize the logo of both the raw and the typed organization."""
    for side, org_id in enumerate((orgs.raw_org_id, orgs.typed_org_id)):
        await _exercise_organization_logo(run, org_id, side, png)


async def _exercise_organization_extras_pair(run: _OrganizationMutationRun, org_id: str) -> None:
    """Differentially write, read, and delete the extras of one organization."""
    extras_path = f"/api/2/organizations/{org_id}/extras/"
    direct_status, direct_extras, _ = _direct_request(run.admin_token, "PUT", extras_path, body={"matrix": run.run_id})
    typed_extras = await _typed_membership_call(
        run.admin_client,
        "update_organization_extras",
        org_id,
        {"matrix": run.run_id},
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v2.update-organization-extras", org_id),
    )
    assert direct_extras == {"matrix": run.run_id}
    _assert_organization_mutation(direct_status, typed_extras, "udata/api-v2.update-organization-extras", {200})
    direct_status, direct_extras, _ = _direct_request(run.admin_token, "GET", extras_path)
    typed_extras_value = await _typed_membership_call(run.admin_client, "get_organization_extras", org_id)
    assert direct_extras == typed_extras_value == {"matrix": run.run_id}
    assert direct_status == 200
    assert typed_extras_value == {"matrix": run.run_id}
    direct_status, direct_deleted, _ = _direct_request(run.admin_token, "DELETE", extras_path, body=["matrix"])
    typed_deleted = await _typed_membership_call(
        run.admin_client,
        "delete_organization_extras",
        org_id,
        ("matrix",),
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v2.delete-organization-extras", org_id, destructive=True),
    )
    assert direct_deleted is None or isinstance(direct_deleted, Mapping)
    _assert_organization_mutation(direct_status, typed_deleted, "udata/api-v2.delete-organization-extras", {200, 204})


async def _exercise_organization_extras(run: _OrganizationMutationRun, orgs: _SeededOrganizations) -> None:
    """Differentially exercise the extras of both the raw and the typed organization."""
    for org_id in (orgs.raw_org_id, orgs.typed_org_id):
        await _exercise_organization_extras_pair(run, org_id)


async def _exercise_organization_followers(run: _OrganizationMutationRun, orgs: _SeededOrganizations) -> None:
    """Differentially list, follow, and unfollow one organization."""
    raw_org_id, typed_org_id = orgs.raw_org_id, orgs.typed_org_id
    raw_follow_status, _, _ = _direct_request(run.admin_token, "GET", f"/api/1/organizations/{raw_org_id}/followers/")
    typed_followers = await _typed_membership_call(run.admin_client, "list_organization_followers", typed_org_id)
    assert raw_follow_status == 200
    assert isinstance(typed_followers, tuple)
    raw_follow_status, raw_follow, _ = _direct_request(
        run.admin_token, "POST", f"/api/1/organizations/{raw_org_id}/followers/"
    )
    typed_follow = await _typed_membership_call(
        run.admin_client,
        "follow_organization",
        typed_org_id,
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.follow-organization", typed_org_id),
    )
    assert isinstance(raw_follow, Mapping)
    _assert_organization_mutation(raw_follow_status, typed_follow, "udata/api-v1.follow-organization", {200, 201})
    raw_unfollow_status, raw_unfollow, _ = _direct_request(
        run.admin_token, "DELETE", f"/api/1/organizations/{raw_org_id}/followers/"
    )
    typed_unfollow = await _typed_membership_call(
        run.admin_client,
        "unfollow_organization",
        typed_org_id,
        run.admin_permissions,
        _organization_mutation_policy("udata/api-v1.unfollow-organization", typed_org_id, destructive=True),
    )
    assert isinstance(raw_unfollow, Mapping)
    _assert_organization_mutation(raw_unfollow_status, typed_unfollow, "udata/api-v1.unfollow-organization", {200})


async def _cleanup_organization_run(run: _OrganizationMutationRun) -> None:
    """Release both organizations the run created, recording rather than raising on failure."""
    raw_org_id, typed_org_id = run.raw_org_id, run.typed_org_id
    if raw_org_id is not None:
        raw_delete_result = await _record_organization_cleanup(
            run,
            lambda: _assert_direct_delete(
                _direct_request(run.admin_token, "DELETE", f"/api/1/organizations/{raw_org_id}/")
            ),
        )
        if isinstance(raw_delete_result, int):
            run.raw_delete_status = raw_delete_result
        await _record_organization_cleanup(
            run,
            lambda: _assert_dataset_absent(
                _direct_request(run.admin_token, "GET", f"/api/1/organizations/{raw_org_id}/")
            ),
        )
    if typed_org_id is not None:
        await _record_organization_cleanup(
            run,
            lambda: _typed_membership_call(
                run.admin_client,
                "delete_organization",
                typed_org_id,
                run.admin_permissions,
                _organization_mutation_policy("udata/api-v1.delete-organization", typed_org_id, destructive=True),
            ),
            expected_status=run.raw_delete_status if run.raw_delete_status is not None else 204,
            operation="udata/api-v1.delete-organization",
        )
        await _record_organization_cleanup(
            run,
            lambda: _assert_dataset_absent(
                _direct_request(run.admin_token, "GET", f"/api/1/organizations/{typed_org_id}/")
            ),
        )
    assert not run.cleanup_errors, f"{len(run.cleanup_errors)} organization route-matrix cleanup operations failed"


async def _exercise_organization_mutations(run: _OrganizationMutationRun, png: bytes) -> None:
    """Run the whole controlled organization mutation differential in one execution mode."""
    try:
        orgs = await _seed_controlled_organizations(run)
        await _exercise_organization_update(run, orgs)
        member_id = await _exercise_membership_request_and_accept(run, orgs)
        await _exercise_member_role_and_assignments(run, orgs, member_id)
        await _exercise_member_removal(run, orgs, member_id)
        await _exercise_membership_refusal(run, orgs)
        await _exercise_invitation_and_cancel(run, orgs)
        await _exercise_organization_badges(run, orgs)
        await _exercise_organization_logos(run, orgs, png)
        await _exercise_organization_extras(run, orgs)
        await _exercise_organization_followers(run, orgs)
    finally:
        await _cleanup_organization_run(run)


def _organization_settings(token: str) -> UDataClientSettings:
    """Return the client settings for one controlled organization role token."""
    return UDataClientSettings(base_url=ORIGIN, credential=UDataCredential(api_key=token))


def test_controlled_organization_mutations_match_raw_routes_in_both_modes() -> None:
    """Every assigned organization mutation route agrees with its raw response in both modes."""
    admin_token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    member_token = os.environ.get("UDATA_EVIDENCE_MEMBER_TOKEN")
    organization_admin_token = os.environ.get("UDATA_EVIDENCE_ORGANIZATION_ADMIN_TOKEN")
    if not admin_token or not member_token or not organization_admin_token:
        pytest.skip("controlled organization mutations require disposable admin and member tokens")
    png = _CONTROLLED_PNG
    tokens = (admin_token, member_token, organization_admin_token)

    async def run_sync_matrix() -> None:
        with (
            create_sync_client(_organization_settings(admin_token)) as admin_client,
            create_sync_client(_organization_settings(member_token)) as member_client,
            create_sync_client(_organization_settings(organization_admin_token)) as org_admin_client,
        ):
            await _exercise_organization_mutations(
                _organization_run("sync", *tokens, admin_client, member_client, org_admin_client), png
            )

    async def run_async_matrix() -> None:
        async with (
            create_async_client(_organization_settings(admin_token)) as admin_client,
            create_async_client(_organization_settings(member_token)) as member_client,
            create_async_client(_organization_settings(organization_admin_token)) as org_admin_client,
        ):
            await _exercise_organization_mutations(
                _organization_run("async", *tokens, admin_client, member_client, org_admin_client), png
            )

    asyncio.run(run_sync_matrix())
    asyncio.run(run_async_matrix())


def test_controlled_stack_proves_authenticated_dataset_mutation_chain() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled mutations require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    from datasluice.connectors.catalog.udata.models.datasets import (
        DatasetCreateInput,
        DatasetDeleteOptions,
        DatasetUpdateInput,
    )
    from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    settings = UDataClientSettings(base_url=ORIGIN, credential=credential)
    dataset_id: str | None = None
    cleanup_outcome = None
    cleanup_error: Exception | None = None
    with create_sync_client(settings) as client:
        assert client.site_version().version == "17.6.0"
        try:
            record = client.datasets.create(
                DatasetCreateInput(title="Evidence dataset", description="d"),
                permissions=permissions,
                mutation_policy=MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.create-dataset", target="Evidence dataset"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
            record_value = record.record
            assert record_value is not None
            dataset_id = record_value.id.value
            updated = client.datasets.update(
                dataset_id,
                DatasetUpdateInput(title="Evidence dataset v2"),
                permissions=permissions,
                mutation_policy=MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.update-dataset", target=dataset_id
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
            assert updated.record is not None
            assert updated.record.payload["title"] == "Evidence dataset v2"
        finally:
            if dataset_id is not None:
                try:
                    cleanup_result = client.datasets.delete(
                        dataset_id,
                        permissions,
                        DatasetDeleteOptions(),
                        MutationPolicy(
                            confirmation=ConfirmationPolicy(
                                confirmed=True, operation="udata/api-v1.delete-dataset", target=dataset_id
                            ),
                            concurrency=ConcurrencyPolicy(overwrite=True),
                            destructive=True,
                        ),
                    )
                    cleanup_outcome = cleanup_result.receipt
                except Exception as error:
                    cleanup_error = error

    assert cleanup_error is None, f"controlled cleanup failed for {dataset_id}: {cleanup_error}"
    assert cleanup_outcome is not None
    assert cleanup_outcome.audit_metadata["status_code"] == 204


def test_controlled_resource_family_mutation_and_read_chain() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled resources require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    from io import BytesIO

    from datasluice.connectors.catalog.udata.models.datasets import DatasetCreateInput, DatasetDeleteOptions
    from datasluice.connectors.catalog.udata.models.resources import (
        ResourceCreateInput,
        ResourceUpdateInput,
        ResourceUploadInput,
    )
    from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )

    def resource_policy(
        target: str,
        *,
        destructive: bool = False,
        operation: str = "udata/api-v1.dataset-resource-create-update-reorder-upload-delete",
    ) -> MutationPolicy:
        return MutationPolicy(
            destructive=destructive,
            confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
            concurrency=ConcurrencyPolicy(overwrite=True),
        )

    dataset_id: str | None = None
    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        try:
            created_dataset = client.datasets.create(
                DatasetCreateInput(title="Controlled resource evidence", description="d"),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.create-dataset", target="Controlled resource evidence"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
            assert created_dataset.record is not None
            dataset_id = created_dataset.record.id.value
            created = client.resources.create(
                dataset_id,
                ResourceCreateInput(title="Remote", url="https://example.com/data.csv"),
                permissions,
                resource_policy(
                    dataset_id, operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-create"
                ),
            )
            assert created.record is not None
            resource_id = created.record.id.value
            direct = build_opener(_DirectNoRedirect()).open(
                Request(f"{ORIGIN}/api/1/datasets/{dataset_id}/resources/{resource_id}/"), timeout=10
            )
            with direct:
                direct_record = json.loads(direct.read(8193))
            typed = client.resources.get(dataset_id, resource_id)
            assert direct_record["id"] == typed.id.value == resource_id
            assert client.resources.redirect(resource_id) == "https://example.com/data.csv"
            assert client.resources.get_dataset_v2(dataset_id).id.value == dataset_id
            assert client.resources.list_v2(dataset_id).items[0].id.value == resource_id
            assert client.resources.get_v2(resource_id).id.value == resource_id
            assert client.resources.resource_types()
            assert (
                client.resources.update(
                    dataset_id,
                    resource_id,
                    ResourceUpdateInput({"title": "Updated"}),
                    permissions,
                    resource_policy(
                        resource_id,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-update",
                    ),
                ).record
                is not None
            )
            assert (
                client.resources.reorder(
                    dataset_id,
                    (ResourceUpdateInput({"id": resource_id}),),
                    permissions,
                    resource_policy(
                        dataset_id,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-reorder",
                    ),
                )
                .records[0]
                .id.value
                == resource_id
            )
            assert client.resources.update_extras_v2(
                dataset_id,
                resource_id,
                {"evidence": "value"},
                permissions,
                resource_policy(
                    resource_id,
                    operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-extras-update",
                ),
            ).extras == {"evidence": "value"}
            assert client.resources.get_extras_v2(dataset_id, resource_id)["evidence"] == "value"
            assert (
                client.resources.delete_extras_v2(
                    dataset_id,
                    resource_id,
                    ("evidence",),
                    permissions,
                    resource_policy(
                        resource_id,
                        destructive=True,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-extras-delete",
                    ),
                ).receipt.outcome
                == "succeeded"
            )
            assert client.resources.get_extras_v2(dataset_id, resource_id) == {}
            uploaded = client.resources.upload(
                dataset_id,
                ResourceUploadInput(BytesIO(b"abc"), "evidence.csv", 3),
                permissions,
                resource_policy(
                    dataset_id, operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-new"
                ),
            )
            assert uploaded.record is not None
            assert (
                client.resources.upload(
                    dataset_id,
                    ResourceUploadInput(BytesIO(b"def"), "evidence-updated.csv", 3),
                    permissions,
                    resource_policy(
                        uploaded.record.id.value,
                        destructive=True,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-replace",
                    ),
                    resource_id=uploaded.record.id.value,
                ).record
                is not None
            )
            uploaded_community = client.resources.upload_community(
                dataset_id,
                ResourceUploadInput(BytesIO(b"abc"), "community-new.csv", 3),
                permissions,
                resource_policy(
                    dataset_id,
                    operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-community-new",
                ),
            )
            assert uploaded_community.record is not None
            assert (
                client.resources.delete_community(
                    uploaded_community.record.id.value,
                    permissions,
                    resource_policy(
                        uploaded_community.record.id.value,
                        destructive=True,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-delete",
                    ),
                ).receipt.outcome
                == "succeeded"
            )
            community = client.resources.create_community(
                dataset_id,
                ResourceCreateInput(title="Community", url="https://example.com/community.csv"),
                permissions,
                resource_policy(
                    dataset_id,
                    operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-create",
                ),
            )
            assert community.record is not None
            community_id = community.record.id.value
            assert client.resources.get_community(community_id).id.value == community_id
            assert client.resources.list_community({"dataset": dataset_id}).items
            assert (
                client.resources.update_community(
                    community_id,
                    ResourceUpdateInput({"title": "Community updated"}),
                    permissions,
                    resource_policy(
                        community_id,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-update",
                    ),
                ).record
                is not None
            )
            assert (
                client.resources.reupload_community(
                    community_id,
                    ResourceUploadInput(BytesIO(b"abc"), "community-reupload.csv", 3),
                    permissions,
                    resource_policy(
                        community_id,
                        destructive=True,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-community-replace",
                    ),
                ).record
                is not None
            )
            assert (
                client.resources.delete_community(
                    community_id,
                    permissions,
                    resource_policy(
                        community_id,
                        destructive=True,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-delete",
                    ),
                ).receipt.outcome
                == "succeeded"
            )
            assert (
                client.resources.delete(
                    dataset_id,
                    resource_id,
                    permissions,
                    resource_policy(
                        resource_id,
                        destructive=True,
                        operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-delete",
                    ),
                ).receipt.outcome
                == "succeeded"
            )
        finally:
            if dataset_id is not None:
                client.datasets.delete(
                    dataset_id,
                    permissions,
                    DatasetDeleteOptions(),
                    MutationPolicy(
                        confirmation=ConfirmationPolicy(
                            confirmed=True, operation="udata/api-v1.delete-dataset", target=dataset_id
                        ),
                        concurrency=ConcurrencyPolicy(overwrite=True),
                        destructive=True,
                    ),
                )


def _resource_mutation_policy(target: str, operation: str, *, destructive: bool = False) -> MutationPolicy:
    """Return the confirmed overwrite policy the controlled resource mutations run under."""
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


@dataclass
class _ResourceRun:
    """Mutable per-run state for the controlled bounded dataset resource route differential."""

    token: str
    permissions: EffectivePermissions
    client: SyncUDataClient
    dataset_id: str | None = None
    direct_resource_id: str | None = None
    typed_resource_id: str | None = None
    direct_upload_id: str | None = None
    typed_upload_id: str | None = None
    direct_community_upload_id: str | None = None
    typed_community_upload_id: str | None = None
    direct_community_id: str | None = None
    typed_community_id: str | None = None
    cleanup_errors: list[Exception] = field(default_factory=list)

    def cleanup(self, action: Callable[[], object]) -> None:
        """Run one cleanup action, recording rather than raising on failure."""
        try:
            action()
        except Exception as error:
            self.cleanup_errors.append(error)


@dataclass(frozen=True)
class _SeededResources:
    """The dataset and resource identifiers the seeding phase produced for every later phase."""

    dataset_id: str
    direct_resource_id: str
    typed_resource_id: str


def _seed_controlled_resources(run: _ResourceRun) -> _SeededResources:
    """Create the controlled dataset and its paired raw and typed resources."""
    from datasluice.connectors.catalog.udata.models.datasets import DatasetCreateInput
    from datasluice.connectors.catalog.udata.models.resources import ResourceCreateInput

    created_dataset = run.client.datasets.create(
        DatasetCreateInput(title="Raw differential evidence", description="d"),
        run.permissions,
        _resource_mutation_policy("Raw differential evidence", "udata/api-v1.create-dataset"),
    )
    assert created_dataset.record is not None
    dataset_id = created_dataset.record.id.value
    run.dataset_id = dataset_id

    status, raw, _ = _direct_request(
        run.token,
        "POST",
        f"/api/1/datasets/{dataset_id}/resources/",
        body=ResourceCreateInput(title="Raw resource", url="https://example.com/raw.csv").payload(),
    )
    assert status == 201
    assert isinstance(raw, Mapping)
    assert isinstance(raw.get("id"), str)
    direct_resource_id = raw["id"]
    run.direct_resource_id = direct_resource_id
    typed_created = run.client.resources.create(
        dataset_id,
        ResourceCreateInput(title="Typed resource", url="https://example.com/typed.csv"),
        run.permissions,
        _resource_mutation_policy(
            dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-create"
        ),
    )
    assert typed_created.record is not None
    typed_resource_id = typed_created.record.id.value
    run.typed_resource_id = typed_resource_id

    raw_status, raw_get, _ = _direct_request(
        run.token, "GET", f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/"
    )
    typed_get = run.client.resources.get(dataset_id, direct_resource_id)
    assert raw_status == 200
    assert isinstance(raw_get, Mapping)
    assert raw_get["id"] == typed_get.id.value
    return _SeededResources(dataset_id, direct_resource_id, typed_resource_id)


def _exercise_resource_update_and_reorder(run: _ResourceRun, seeded: _SeededResources) -> None:
    """Differentially update the raw resource and reorder both resources."""
    from datasluice.connectors.catalog.udata.models.resources import ResourceUpdateInput

    dataset_id, direct_resource_id, typed_resource_id = (
        seeded.dataset_id,
        seeded.direct_resource_id,
        seeded.typed_resource_id,
    )
    update_body = ResourceUpdateInput({"title": "Raw updated"}).payload()
    raw_status, raw_update, _ = _direct_request(
        run.token,
        "PUT",
        f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/",
        body=update_body,
    )
    typed_update = run.client.resources.update(
        dataset_id,
        direct_resource_id,
        ResourceUpdateInput({"title": "Typed updated"}),
        run.permissions,
        _resource_mutation_policy(
            direct_resource_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-update"
        ),
    )
    assert raw_status == 200
    assert isinstance(raw_update, Mapping)
    assert typed_update.record is not None
    assert raw_update["id"] == typed_update.record.id.value
    assert typed_update.record.id.value == direct_resource_id

    reorder_body = [
        {"id": direct_resource_id, "order": 0},
        {"id": typed_resource_id, "order": 1},
    ]
    raw_status, raw_reorder, _ = _direct_request(
        run.token, "PUT", f"/api/1/datasets/{dataset_id}/resources/", body=reorder_body
    )
    typed_reorder = run.client.resources.reorder(
        dataset_id,
        (ResourceUpdateInput(reorder_body[0]), ResourceUpdateInput(reorder_body[1])),
        run.permissions,
        _resource_mutation_policy(
            dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-reorder"
        ),
    )
    assert raw_status == 200
    assert isinstance(raw_reorder, list)
    assert typed_reorder.records
    assert raw_reorder[0]["id"] == typed_reorder.records[0].id.value


def _exercise_resource_extras(run: _ResourceRun, seeded: _SeededResources) -> None:
    """Differentially write, read, and delete the v2 extras of the raw resource."""
    dataset_id, direct_resource_id = seeded.dataset_id, seeded.direct_resource_id
    extras_path = f"/api/2/datasets/{dataset_id}/resources/{direct_resource_id}/extras/"
    raw_status, raw_extras, _ = _direct_request(run.token, "PUT", extras_path, body={"raw": "value", "typed": "value"})
    typed_extras = run.client.resources.update_extras_v2(
        dataset_id,
        direct_resource_id,
        {"raw": "value", "typed": "value"},
        run.permissions,
        _resource_mutation_policy(
            direct_resource_id,
            "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-extras-update",
        ),
    )
    assert raw_status == 200
    assert raw_extras == {"raw": "value", "typed": "value"}
    assert typed_extras.extras == {"raw": "value", "typed": "value"}
    raw_status, raw_extras, _ = _direct_request(run.token, "GET", extras_path)
    typed_extras_read = run.client.resources.get_extras_v2(dataset_id, direct_resource_id)
    assert raw_status == 200
    assert raw_extras == {"raw": "value", "typed": "value"}
    assert typed_extras_read == {"raw": "value", "typed": "value"}
    raw_status, raw_deleted_extras, _ = _direct_request(run.token, "DELETE", extras_path, body=["raw"])
    typed_deleted_extras = run.client.resources.delete_extras_v2(
        dataset_id,
        direct_resource_id,
        ("typed",),
        run.permissions,
        _resource_mutation_policy(
            direct_resource_id,
            "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-extras-delete",
            destructive=True,
        ),
    )
    assert raw_status == 204
    assert raw_deleted_extras is None
    assert typed_deleted_extras.receipt.outcome == "succeeded"


def _exercise_resource_v2_reads(run: _ResourceRun, seeded: _SeededResources) -> None:
    """Differentially exercise every v2 dataset and resource read route plus the redirect."""
    dataset_id, direct_resource_id = seeded.dataset_id, seeded.direct_resource_id
    raw_status, raw_dataset, _ = _direct_request(run.token, "GET", f"/api/2/datasets/{dataset_id}/")
    typed_dataset = run.client.resources.get_dataset_v2(dataset_id)
    assert raw_status == 200
    assert isinstance(raw_dataset, Mapping)
    assert raw_dataset["id"] == typed_dataset.id.value
    raw_status, raw_resource_page, _ = _direct_request(run.token, "GET", f"/api/2/datasets/{dataset_id}/resources/")
    typed_resource_page = run.client.resources.list_v2(dataset_id)
    assert raw_status == 200
    assert isinstance(raw_resource_page, Mapping)
    assert typed_resource_page.items
    raw_status, raw_resource, _ = _direct_request(run.token, "GET", f"/api/2/datasets/resources/{direct_resource_id}/")
    typed_resource = run.client.resources.get_v2(direct_resource_id)
    assert raw_status == 200
    assert isinstance(raw_resource, Mapping)
    assert typed_resource.id.value == direct_resource_id
    raw_status, raw_types, _ = _direct_request(run.token, "GET", "/api/1/datasets/resource_types/")
    typed_types = run.client.resources.resource_types()
    assert raw_status == 200
    assert isinstance(raw_types, list)
    assert len(raw_types) == len(typed_types)
    raw_status, raw_redirect, raw_headers = _direct_request(run.token, "GET", f"/api/1/datasets/r/{direct_resource_id}")
    assert raw_status == 302
    assert raw_redirect is None
    assert raw_headers.get("location") == run.client.resources.redirect(direct_resource_id)


def _exercise_resource_upload_replace(run: _ResourceRun, seeded: _SeededResources) -> None:
    """Differentially replace the content of the raw resource and then delete it raw."""
    from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput

    dataset_id, direct_resource_id = seeded.dataset_id, seeded.direct_resource_id
    replacement_body, replacement_type = _multipart_body(b"raw", "raw.csv")
    raw_status, raw_replaced, _ = _direct_request(
        run.token,
        "POST",
        f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/upload/",
        body=replacement_body,
        content_type=replacement_type,
    )
    typed_replaced = run.client.resources.upload(
        dataset_id,
        ResourceUploadInput(BytesIO(b"typed"), "typed.csv", 5),
        run.permissions,
        _resource_mutation_policy(
            direct_resource_id,
            "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-replace",
            destructive=True,
        ),
        resource_id=direct_resource_id,
    )
    assert raw_status == 200
    assert isinstance(raw_replaced, Mapping)
    assert typed_replaced.record is not None
    assert raw_replaced["id"] == typed_replaced.record.id.value
    assert typed_replaced.record.id.value == direct_resource_id

    raw_status, raw_deleted, _ = _direct_request(
        run.token, "DELETE", f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/"
    )
    assert raw_status == 204
    assert raw_deleted is None
    run.direct_resource_id = None


def _exercise_resource_uploads(run: _ResourceRun, seeded: _SeededResources) -> None:
    """Differentially upload one dataset resource and one community resource on both sides."""
    from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput

    dataset_id = seeded.dataset_id
    direct_upload_body, direct_upload_type = _multipart_body(b"raw", "raw-new.csv")
    raw_status, raw_upload, _ = _direct_request(
        run.token,
        "POST",
        f"/api/1/datasets/{dataset_id}/upload/",
        body=direct_upload_body,
        content_type=direct_upload_type,
    )
    direct_upload_id = raw_upload["id"] if raw_status == 201 and isinstance(raw_upload, Mapping) else None
    run.direct_upload_id = direct_upload_id
    typed_upload = run.client.resources.upload(
        dataset_id,
        ResourceUploadInput(BytesIO(b"typed"), "typed-new.csv", 5),
        run.permissions,
        _resource_mutation_policy(
            dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-new"
        ),
    )
    assert raw_status == 201
    assert typed_upload.record is not None
    assert direct_upload_id is not None
    typed_upload_id = typed_upload.record.id.value
    run.typed_upload_id = typed_upload_id

    community_body, community_type = _multipart_body(b"raw", "raw-community.csv")
    raw_status, raw_community_upload, _ = _direct_request(
        run.token,
        "POST",
        f"/api/1/datasets/{dataset_id}/upload/community/",
        body=community_body,
        content_type=community_type,
    )
    direct_community_upload_id = (
        raw_community_upload["id"] if raw_status == 201 and isinstance(raw_community_upload, Mapping) else None
    )
    run.direct_community_upload_id = direct_community_upload_id
    typed_community_upload = run.client.resources.upload_community(
        dataset_id,
        ResourceUploadInput(BytesIO(b"typed"), "typed-community.csv", 5),
        run.permissions,
        _resource_mutation_policy(
            dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-community-new"
        ),
    )
    assert raw_status == 201
    assert typed_community_upload.record is not None
    assert direct_community_upload_id is not None
    typed_community_upload_id = typed_community_upload.record.id.value
    run.typed_community_upload_id = typed_community_upload_id


def _exercise_community_resource(run: _ResourceRun, seeded: _SeededResources) -> None:
    """Differentially create, read, update, reupload, and delete one community resource."""
    from datasluice.connectors.catalog.udata.models.resources import (
        ResourceCreateInput,
        ResourceUpdateInput,
        ResourceUploadInput,
    )

    dataset_id = seeded.dataset_id
    community_create_body = ResourceCreateInput(
        title="Raw community", url="https://example.com/raw-community.csv"
    ).payload() | {"dataset": dataset_id}
    raw_status, raw_community, _ = _direct_request(
        run.token, "POST", "/api/1/datasets/community_resources/", body=community_create_body
    )
    direct_community_id = raw_community["id"] if raw_status == 201 and isinstance(raw_community, Mapping) else None
    run.direct_community_id = direct_community_id
    typed_community = run.client.resources.create_community(
        dataset_id,
        ResourceCreateInput(title="Typed community", url="https://example.com/typed-community.csv"),
        run.permissions,
        _resource_mutation_policy(
            dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-create"
        ),
    )
    assert raw_status == 201
    assert typed_community.record is not None
    assert direct_community_id is not None
    typed_community_id = typed_community.record.id.value
    run.typed_community_id = typed_community_id

    raw_status, raw_community_get, _ = _direct_request(
        run.token, "GET", f"/api/1/datasets/community_resources/{direct_community_id}/"
    )
    typed_community_get = run.client.resources.get_community(direct_community_id)
    assert raw_status == 200
    assert isinstance(raw_community_get, Mapping)
    assert raw_community_get["id"] == typed_community_get.id.value
    assert typed_community_get.id.value == direct_community_id
    raw_status, raw_community_list, _ = _direct_request(
        run.token, "GET", f"/api/1/datasets/community_resources/?dataset={dataset_id}"
    )
    typed_community_list = run.client.resources.list_community({"dataset": dataset_id})
    assert raw_status == 200
    assert isinstance(raw_community_list, Mapping)
    assert typed_community_list.items

    community_update_body = ResourceUpdateInput({"title": "Raw community updated"}).payload()
    raw_status, raw_community_update, _ = _direct_request(
        run.token,
        "PUT",
        f"/api/1/datasets/community_resources/{direct_community_id}/",
        body=community_update_body,
    )
    typed_community_update = run.client.resources.update_community(
        direct_community_id,
        ResourceUpdateInput({"title": "Typed community updated"}),
        run.permissions,
        _resource_mutation_policy(
            direct_community_id,
            "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-update",
        ),
    )
    assert raw_status == 200
    assert isinstance(raw_community_update, Mapping)
    assert typed_community_update.record is not None

    community_replace_body, community_replace_type = _multipart_body(b"raw", "raw-community-replace.csv")
    raw_status, raw_community_replace, _ = _direct_request(
        run.token,
        "POST",
        f"/api/1/datasets/community_resources/{direct_community_id}/upload/",
        body=community_replace_body,
        content_type=community_replace_type,
    )
    typed_community_replace = run.client.resources.reupload_community(
        direct_community_id,
        ResourceUploadInput(BytesIO(b"typed"), "typed-community-replace.csv", 5),
        run.permissions,
        _resource_mutation_policy(
            direct_community_id,
            "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-community-replace",
            destructive=True,
        ),
    )
    assert raw_status == 200
    assert isinstance(raw_community_replace, Mapping)
    assert typed_community_replace.record is not None

    raw_status, raw_community_deleted, _ = _direct_request(
        run.token, "DELETE", f"/api/1/datasets/community_resources/{direct_community_id}/"
    )
    assert raw_status == 204
    assert raw_community_deleted is None
    run.direct_community_id = None
    typed_deleted_community = run.client.resources.delete_community(
        typed_community_id,
        run.permissions,
        _resource_mutation_policy(
            typed_community_id,
            "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-delete",
            destructive=True,
        ),
    )
    assert typed_deleted_community.receipt.outcome == "succeeded"


def _cleanup_controlled_resource_run(run: _ResourceRun) -> None:
    """Release every resource, upload, community resource, and dataset the run created."""
    from datasluice.connectors.catalog.udata.models.datasets import DatasetDeleteOptions

    dataset_id = run.dataset_id
    direct_upload_id = run.direct_upload_id
    typed_upload_id = run.typed_upload_id
    direct_community_upload_id = run.direct_community_upload_id
    typed_community_upload_id = run.typed_community_upload_id
    direct_resource_id = run.direct_resource_id
    typed_resource_id = run.typed_resource_id
    if direct_upload_id is not None and dataset_id is not None:
        run.cleanup(
            lambda: _assert_direct_delete(
                _direct_request(run.token, "DELETE", f"/api/1/datasets/{dataset_id}/resources/{direct_upload_id}/")
            ),
        )
    if direct_community_upload_id is not None:
        run.cleanup(
            lambda: _assert_direct_delete(
                _direct_request(
                    run.token,
                    "DELETE",
                    f"/api/1/datasets/community_resources/{direct_community_upload_id}/",
                )
            ),
        )
    if typed_upload_id is not None and dataset_id is not None:
        run.cleanup(
            lambda: _assert_typed_delete(
                run.client.resources.delete(
                    dataset_id,
                    typed_upload_id,
                    run.permissions,
                    _resource_mutation_policy(
                        typed_upload_id,
                        "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-delete",
                        destructive=True,
                    ),
                )
            ),
        )
    if typed_community_upload_id is not None:
        run.cleanup(
            lambda: _assert_typed_delete(
                run.client.resources.delete_community(
                    typed_community_upload_id,
                    run.permissions,
                    _resource_mutation_policy(
                        typed_community_upload_id,
                        "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-delete",
                        destructive=True,
                    ),
                )
            ),
        )
    if direct_resource_id is not None and dataset_id is not None:
        run.cleanup(
            lambda: _assert_direct_delete(
                _direct_request(run.token, "DELETE", f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/")
            ),
        )
    if typed_resource_id is not None and dataset_id is not None:
        run.cleanup(
            lambda: _assert_typed_delete(
                run.client.resources.delete(
                    dataset_id,
                    typed_resource_id,
                    run.permissions,
                    _resource_mutation_policy(
                        typed_resource_id,
                        "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-delete",
                        destructive=True,
                    ),
                )
            ),
        )
    if dataset_id is not None:
        run.cleanup(
            lambda: _assert_typed_delete(
                run.client.datasets.delete(
                    dataset_id,
                    run.permissions,
                    DatasetDeleteOptions(),
                    _resource_mutation_policy(dataset_id, "udata/api-v1.delete-dataset", destructive=True),
                )
            ),
        )
        run.cleanup(lambda: _assert_dataset_absent(_direct_request(run.token, "GET", f"/api/1/datasets/{dataset_id}/")))
    assert not run.cleanup_errors, f"{len(run.cleanup_errors)} controlled cleanup operations failed"


def test_controlled_resource_routes_match_bounded_raw_differential() -> None:
    """Every assigned dataset resource route agrees with its bounded raw differential."""
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled resources require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    client = create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential))
    run = _ResourceRun(token=token, permissions=permissions, client=client)
    with client:
        try:
            seeded = _seed_controlled_resources(run)
            _exercise_resource_update_and_reorder(run, seeded)
            _exercise_resource_extras(run, seeded)
            _exercise_resource_v2_reads(run, seeded)
            _exercise_resource_upload_replace(run, seeded)
            _exercise_resource_uploads(run, seeded)
            _exercise_community_resource(run, seeded)
        finally:
            _cleanup_controlled_resource_run(run)


def test_controlled_stack_proves_site_patch_is_confirmed_and_receipt_bearing() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled mutations require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    with _create_controlled_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        before = client.root_profile.get()
        result = client.root_profile.set_site(
            SitePatchInput(title=before.title),
            permissions=permissions,
            mutation_policy=MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True,
                    operation="udata/api-v1.set_site",
                    target=before.site_id,
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )

    assert result.profile is not None
    assert result.profile.title == before.title
    assert result.receipt.outcome == "succeeded"
    assert result.receipt.audit_metadata["status_code"] in {200, 204}


def test_controlled_row_184_differential_matches_independent_fixture_contract() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled mutations require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    row = _controlled_site_row()
    from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    with _create_controlled_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        before = client.root_profile.get()
        assert before.feed_size is not None
        mutation_feed_size = before.feed_size + 1
        try:
            direct_status, direct_media, direct_fields = _direct_site_patch(
                row, token, {"feed_size": mutation_feed_size}
            )
            reset_status, _, reset_fields = _direct_site_patch(row, token, {"feed_size": before.feed_size})
            reset_observed_feed_size = client.root_profile.get().feed_size
            typed = client.root_profile.set_site(
                SitePatchInput(feed_size=mutation_feed_size),
                permissions=permissions,
                mutation_policy=MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True,
                        operation="udata/api-v1.set_site",
                        target=before.site_id,
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
            after = client.root_profile.get()
        finally:
            restored_status, _, restored_fields = _direct_site_patch(row, token, {"feed_size": before.feed_size})
            restored_observed_feed_size = client.root_profile.get().feed_size

    expected_media = row["response_media_type"]
    assert isinstance(expected_media, str)
    assert direct_status in {200, 204}
    assert direct_media == expected_media
    assert direct_fields["feed_size"] == mutation_feed_size
    assert reset_status in {200, 204}
    assert reset_fields["feed_size"] == before.feed_size
    assert reset_observed_feed_size == before.feed_size
    assert restored_status in {200, 204}
    assert restored_fields["feed_size"] == before.feed_size
    assert restored_observed_feed_size == before.feed_size
    assert typed.receipt.audit_metadata["status_code"] == direct_status
    assert typed.profile is not None
    assert typed.profile.feed_size == mutation_feed_size
    assert after.feed_size == mutation_feed_size
    assert {
        field: typed.profile.payload.get(field) for field in ("id", "title", "version", "feed_size")
    } == direct_fields


def test_controlled_async_stack_proves_site_patch_is_confirmed_and_receipt_bearing() -> None:
    import asyncio

    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled mutations require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )

    async def run() -> tuple[SiteProfile, SiteMutationResult]:
        settings = UDataClientSettings(base_url=ORIGIN, credential=credential)
        async with await _create_controlled_async_client(settings) as client:
            before = await client.root_profile.get()
            result = await client.root_profile.set_site(
                SitePatchInput(title=before.title),
                permissions=permissions,
                mutation_policy=MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True,
                        operation="udata/api-v1.set_site",
                        target=before.site_id,
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
            return before, result

    before, result = asyncio.run(run())
    assert result.profile is not None
    assert result.profile.title == before.title
    assert result.receipt.outcome == "succeeded"
    assert result.receipt.audit_metadata["status_code"] in {200, 204}


def test_controlled_async_stack_proves_exact_version_then_one_dataset_read() -> None:
    import asyncio

    settings = UDataClientSettings(base_url=ORIGIN)

    async def run() -> tuple[str, int | None]:
        async with create_async_client(settings) as client:
            version = (await client.site_version()).version
            envelope = await client.datasets_list(
                CatalogOperationRequest(operation_id=_FAMILY_OPERATION_ID, payload={}),
                CatalogOperationGuard(operation_id=_FAMILY_OPERATION_ID),
            )
            return version, envelope.page.total_items if envelope.page else None

    version, total = asyncio.run(run())

    assert version == "17.6.0"
    assert total is not None
    assert total >= 0


_OAUTH_FORM = "application/x-www-form-urlencoded"


def _direct_following_request(
    token: str, method: str, path: str, *, max_bytes: int = 8192
) -> tuple[int, object, dict[str, str]]:
    """Raw probe that follows redirects, matching the typed client's default."""
    request = Request(f"{ORIGIN}{path}", headers={"Accept": "*/*", "X-API-KEY": token}, method=method)
    with build_opener().open(request, timeout=10) as response:
        body = response.read(max_bytes + 1)
        assert len(body) <= max_bytes
        media_type = response.headers.get_content_type()
        return (
            response.status,
            json.loads(body) if body and media_type == "application/json" else None,
            {key.lower(): value for key, value in response.headers.items()},
        )


def _oauth_form(fields: Mapping[str, str]) -> bytes:
    from urllib.parse import urlencode

    return urlencode(fields).encode()


def _assert_oauth_page_matches_raw(name: str, raw_status: int, raw_headers: Mapping[str, str], typed: object) -> None:
    """The stock /oauth pages keep only status and media type, never their body."""
    expected = raw_headers.get("content-type", "application/octet-stream").split(";")[0].lower()
    assert isinstance(typed, (OAuthErrorDocument, OAuthConsentSummary)), name
    assert typed.status_code == raw_status, name
    assert typed.media_type == expected, name
    assert typed.session_gated is True, name


def _oauth_mutations(
    raw_revoke: tuple[int, object, dict[str, str]], raw_authorize_post: tuple[int, object, dict[str, str]]
) -> tuple[tuple[str, tuple[int, object, dict[str, str]], object], ...]:
    """Bind every OAuth mutation to the raw status it must reproduce in both modes."""
    return (
        ("revoke_token", raw_revoke, OAuthRevokeRequest(token="controlled-absent-token")),
        ("authorize_post", raw_authorize_post, OAuthAuthorizeDecision(accept=True)),
    )


def _oauth_mutation_policy(name: str) -> MutationPolicy:
    target = "self" if name == "authorize_post" else f"request:{name}"
    return MutationPolicy(
        destructive=name == "revoke_token",
        confirmation=ConfirmationPolicy(
            confirmed=True, operation=f"udata/oauth.{name.replace('_', '-')}", target=target
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _assert_oauth_mutation_sync(
    client: SyncUDataClient,
    name: str,
    raw: tuple[int, object, dict[str, str]],
    body: object,
    permissions: EffectivePermissions,
) -> None:
    """The disposable stack has no OAuth client, so the stock endpoints reject every
    form. Either outcome is valid; what matters is that the typed status equals the raw
    status and that no access token is ever retained."""
    try:
        result = getattr(client.auth_oauth, name)(body, permissions, _oauth_mutation_policy(name))
    except CatalogError as error:
        assert error.metadata.get("status_code") == raw[0], (name, error.metadata)
        receipt = error.metadata.get("receipt")
        if isinstance(receipt, Mapping):
            assert receipt["audit_metadata"]["status_code"] == raw[0], name
    else:
        if isinstance(result, OAuthConsentOutcome):
            assert result.status_code == raw[0], name
        else:
            assert isinstance(result, OAuthTokenResult), name
            assert result.receipt.audit_metadata["status_code"] == raw[0], name


async def _assert_oauth_mutation_async(
    client: AsyncUDataClient,
    name: str,
    raw: tuple[int, object, dict[str, str]],
    body: object,
    permissions: EffectivePermissions,
) -> None:
    """The async mutation path must reproduce the same status as the sync path."""
    try:
        result = await getattr(client.auth_oauth, name)(body, permissions, _oauth_mutation_policy(name))
    except CatalogError as error:
        assert error.metadata.get("status_code") == raw[0], (name, error.metadata)
    else:
        if isinstance(result, OAuthConsentOutcome):
            assert result.status_code == raw[0], name
        else:
            assert isinstance(result, OAuthTokenResult), name
            assert result.receipt.audit_metadata["status_code"] == raw[0], name


_CONTROLLED_OAUTH_TOKEN_BODY = OAuthTokenRequest(
    grant_type="client_credentials", client_id="absent", client_secret="absent"
)


@dataclass(frozen=True)
class _ControlledOAuthRaw:
    """The raw loopback OAuth responses the typed client must reproduce in both modes."""

    error: tuple[int, object, dict[str, str]]
    client_info: tuple[int, object, dict[str, str]]
    authorize: tuple[int, object, dict[str, str]]
    revoke: tuple[int, object, dict[str, str]]
    token: tuple[int, object, dict[str, str]]
    authorize_post: tuple[int, object, dict[str, str]]

    @property
    def reads(self) -> tuple[tuple[str, tuple[int, object, dict[str, str]]], ...]:
        """Return every raw read probe paired with the typed client method that must match it."""
        return (("oauth_error", self.error), ("client_info", self.client_info), ("authorize", self.authorize))

    @property
    def mutations(self) -> tuple[tuple[str, tuple[int, object, dict[str, str]], object], ...]:
        """Return every raw mutation probe paired with its typed client method and request body."""
        return _oauth_mutations(self.revoke, self.authorize_post)


def _probe_controlled_oauth_raw(token: str) -> _ControlledOAuthRaw:
    """Probe every assigned /oauth route, so each expected status comes from the deployment."""
    # Raw first, so each expected status comes from the deployment, not from us.
    raw_error = _direct_request(token, "GET", "/oauth/error", max_bytes=1024)
    raw_client_info = _direct_request(token, "GET", "/oauth/client_info?client_id=absent", max_bytes=8192)
    # The typed client follows redirects, so the raw probe must too, otherwise the
    # two legitimately differ by exactly that hop.
    raw_authorize = _direct_following_request(
        token, "GET", "/oauth/authorize?client_id=absent&response_type=code", max_bytes=8192
    )
    raw_revoke = _direct_request(
        token,
        "POST",
        "/oauth/revoke",
        body=_oauth_form({"token": "controlled-absent-token"}),
        content_type=_OAUTH_FORM,
        max_bytes=2048,
    )
    raw_token = _direct_request(
        token,
        "POST",
        "/oauth/token",
        body=_oauth_form({"grant_type": "client_credentials", "client_id": "absent", "client_secret": "absent"}),
        content_type=_OAUTH_FORM,
        max_bytes=2048,
    )
    # The consent POST is login_required in stock, so the raw probe establishes the
    # status the typed client must reproduce rather than assuming one.
    raw_authorize_post = _direct_request(
        token,
        "POST",
        "/oauth/authorize",
        body=_oauth_form({"accept": "y"}),
        content_type=_OAUTH_FORM,
        max_bytes=2048,
    )
    assert raw_revoke[0] in {200, 400, 401}, raw_revoke[0]
    assert raw_token[0] in {400, 401}, raw_token[0]
    # The stock /oauth/error page renders api/oauth_error.html, which this image
    # does not ship, so the deployment itself answers 500. The connector must
    # surface that status unchanged rather than mask or invent a body.
    assert raw_error[0] >= 200
    return _ControlledOAuthRaw(
        error=raw_error,
        client_info=raw_client_info,
        authorize=raw_authorize,
        revoke=raw_revoke,
        token=raw_token,
        authorize_post=raw_authorize_post,
    )


def _controlled_oauth_read_call(
    client: SyncUDataClient | AsyncUDataClient, name: str, permissions: EffectivePermissions
) -> Any:
    """Return the pending typed OAuth read call *name* on either transport."""
    if name == "oauth_error":
        return client.auth_oauth.oauth_error()
    return getattr(client.auth_oauth, name)(OAuthClientRequest(client_id="absent"), permissions)


def _assert_oauth_status_agrees(error: CatalogError, raw: tuple[int, object, dict[str, str]], name: str) -> None:
    """Assert the propagated status equals the raw route status, never masked by the client."""
    assert error.metadata.get("status_code") == raw[0], (name, error.metadata)


def _assert_oauth_read_result(raw: tuple[int, object, dict[str, str]], name: str, typed: Any) -> None:
    """Assert one succeeded typed OAuth read matches the raw page, or that the raw route is not a fault."""
    if raw[1] is None:
        _assert_oauth_page_matches_raw(name, raw[0], raw[2], typed)
    else:
        assert raw[0] < 500, (name, raw[0])


def _check_sync_oauth_read(
    client: SyncUDataClient, raw: tuple[int, object, dict[str, str]], name: str, permissions: EffectivePermissions
) -> None:
    """Assert the synchronous OAuth read *name* agrees with its raw response."""
    try:
        typed = _controlled_oauth_read_call(client, name, permissions)
    except CatalogError as error:
        _assert_oauth_status_agrees(error, raw, name)
    else:
        _assert_oauth_read_result(raw, name, typed)


async def _check_async_oauth_read(
    client: AsyncUDataClient, raw: tuple[int, object, dict[str, str]], name: str, permissions: EffectivePermissions
) -> None:
    """Assert the asynchronous OAuth read *name* agrees with its raw response."""
    try:
        typed = await _controlled_oauth_read_call(client, name, permissions)
    except CatalogError as error:
        _assert_oauth_status_agrees(error, raw, name)
    else:
        _assert_oauth_read_result(raw, name, typed)


def _check_sync_oauth_reads(
    client: SyncUDataClient, raw: _ControlledOAuthRaw, permissions: EffectivePermissions
) -> None:
    """Compare every synchronous OAuth read route against its raw response."""
    for name, response in raw.reads:
        _check_sync_oauth_read(client, response, name, permissions)


async def _check_async_oauth_reads(
    client: AsyncUDataClient, raw: _ControlledOAuthRaw, permissions: EffectivePermissions
) -> None:
    """Compare every asynchronous OAuth read route against its raw response."""
    for name, response in raw.reads:
        await _check_async_oauth_read(client, response, name, permissions)


def _check_sync_oauth_mutations(
    client: SyncUDataClient, raw: _ControlledOAuthRaw, permissions: EffectivePermissions
) -> None:
    """Assert every synchronous OAuth mutation agrees with the raw status it must reproduce."""
    for name, response, body in raw.mutations:
        _assert_oauth_mutation_sync(client, name, response, body, permissions)


async def _check_async_oauth_mutations(
    client: AsyncUDataClient, raw: _ControlledOAuthRaw, permissions: EffectivePermissions
) -> None:
    """Assert the asynchronous token exchange fails identically and every mutation agrees."""
    with pytest.raises(CatalogError) as token_error:
        await client.auth_oauth.access_token(_CONTROLLED_OAUTH_TOKEN_BODY, permissions)
    assert token_error.value.metadata.get("status_code") == raw.token[0]
    for name, response, body in raw.mutations:
        await _assert_oauth_mutation_async(client, name, response, body, permissions)


def test_controlled_oauth_routes_match_raw_semantics_in_both_modes() -> None:
    """Every assigned /oauth route is compared against its raw loopback response."""
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled OAuth evidence requires a seeded disposable admin")
    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    raw = _probe_controlled_oauth_raw(token)

    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        _check_sync_oauth_reads(client, raw, permissions)

        async def run_reads_async() -> None:
            async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
                await _check_async_oauth_reads(client, raw, permissions)

        asyncio.run(run_reads_async())

        # The RFC 6749 exchange carries its own client secret, so the uData API key
        # must never accompany it and no token may be retained.
        with pytest.raises(CatalogError) as token_error:
            client.auth_oauth.access_token(_CONTROLLED_OAUTH_TOKEN_BODY, permissions)
        assert token_error.value.metadata.get("status_code") == raw.token[0]

        _check_sync_oauth_mutations(client, raw, permissions)

        async def run_mutations_async() -> None:
            async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
                await _check_async_oauth_mutations(client, raw, permissions)

        asyncio.run(run_mutations_async())


def test_controlled_oauth_rejects_unconfirmed_destructive_revocation() -> None:
    """A revoke without a confirmed policy fails closed before any dispatch."""
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled OAuth evidence requires a seeded disposable admin")
    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        with pytest.raises(CatalogError) as denial:
            client.auth_oauth.revoke_token(OAuthRevokeRequest(token="controlled-absent-token"), permissions)
    assert denial.value.metadata.get("status_code") is None


def test_controlled_activity_and_discussion_reads_match_raw_routes_in_both_modes() -> None:
    """Every assigned read route returns the same native envelope typed and raw."""
    with create_sync_client(UDataClientSettings(base_url=ORIGIN)) as client:
        for path, method, args in (
            ("/api/1/activity/?page=1&page_size=20", "activity", (ActivityQuery(),)),
            ("/api/1/discussions/?page=1&page_size=20", "list_discussions", ()),
            ("/api/2/discussions/search/?page=1&page_size=20", "search_discussions", (DiscussionSearchQuery(),)),
        ):
            status, payload, _ = _direct_request("", "GET", path)
            if status != 200:
                with pytest.raises(CatalogError):
                    getattr(client.activity_discussions, method)(*args)
                continue
            typed = getattr(client.activity_discussions, method)(*args)
            assert status == 200
            assert isinstance(payload, Mapping)
            assert _plain_json(typed.payload) == payload

    async def run_async() -> None:
        async with create_async_client(UDataClientSettings(base_url=ORIGIN)) as client:
            for path, method, args in (
                ("/api/1/activity/?page=1&page_size=20", "activity", (ActivityQuery(),)),
                ("/api/1/discussions/?page=1&page_size=20", "list_discussions", ()),
                ("/api/2/discussions/search/?page=1&page_size=20", "search_discussions", (DiscussionSearchQuery(),)),
            ):
                status, payload, _ = _direct_request("", "GET", path)
                if status != 200:
                    with pytest.raises(CatalogError):
                        await getattr(client.activity_discussions, method)(*args)
                    continue
                operation = getattr(client.activity_discussions, method)(*args)
                typed = await operation
                assert status == 200
                assert isinstance(payload, Mapping)
                assert _plain_json(typed.payload) == payload

    asyncio.run(run_async())


def test_controlled_discussion_lifecycle_matches_raw_routes_and_cleans_up() -> None:
    """Seed one deterministic thread and drive every assigned discussion mutation."""
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled discussion evidence requires UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    title = "evidence discussion"

    def policy(operation: str, target: str) -> MutationPolicy:
        return MutationPolicy(
            destructive=operation.endswith("delete-discussion") or operation.endswith("delete-discussion-comment"),
            confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
            concurrency=ConcurrencyPolicy(overwrite=True),
        )

    def dataset_subject(client: SyncUDataClient) -> dict[str, str]:
        dataset_id = client.datasets.list(DatasetListQuery(page=1, page_size=1)).items[0].id.value
        return {"id": dataset_id, "class": "Dataset"}

    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        subject_id = client.datasets.list(DatasetListQuery(page=1, page_size=1)).items[0].id.value
        created = client.activity_discussions.create_discussion(
            DiscussionCreateInput(
                title=title, comment="evidence opening comment", subject={"id": subject_id, "class": "Dataset"}
            ),
            permissions,
            policy("udata/api-v1.create-discussion", subject_id),
        )
        assert created.record is not None
        discussion_id = str(created.record.payload["id"])
        assert created.receipt.operation == "udata/api-v1.create-discussion"
        try:
            status, raw, _ = _direct_request(token, "GET", f"/api/1/discussions/{discussion_id}/")
            assert status == 200
            assert isinstance(raw, Mapping)
            assert raw["title"] == title
            assert _plain_json(client.activity_discussions.get_discussion(discussion_id).payload) == raw

            commented = client.activity_discussions.comment_discussion(
                discussion_id,
                CommentInput(comment="evidence reply"),
                permissions,
                policy("udata/api-v1.comment-discussion", discussion_id),
            )
            assert commented.receipt.operation == "udata/api-v1.comment-discussion"
            _, after_comment, _ = _direct_request(token, "GET", f"/api/1/discussions/{discussion_id}/")
            assert isinstance(after_comment, Mapping)
            assert len(after_comment["discussion"]) == len(raw["discussion"]) + 1

            updated = client.activity_discussions.update_discussion(
                discussion_id,
                DiscussionUpdateInput(title=f"{title} renamed"),
                permissions,
                policy("udata/api-v1.update-discussion", discussion_id),
            )
            assert updated.record is not None
            assert updated.record.payload["title"] == f"{title} renamed"

            edited = client.activity_discussions.edit_discussion_comment(
                discussion_id,
                "1",
                CommentInput(comment="evidence edited reply"),
                permissions,
                policy("udata/api-v1.edit-discussion-comment", f"{discussion_id}:1"),
            )
            assert edited.record is not None
            assert isinstance(edited.record.payload, Mapping)
            edited_document = _plain_json(edited.record.payload)
            assert isinstance(edited_document, Mapping)
            edited_messages = edited_document["discussion"]
            assert isinstance(edited_messages, list)
            edited_first_reply = edited_messages[1]
            assert isinstance(edited_first_reply, Mapping)
            assert edited_first_reply["content"] == "evidence edited reply"

            comment_deleted = client.activity_discussions.delete_discussion_comment(
                discussion_id,
                "1",
                permissions,
                policy("udata/api-v1.delete-discussion-comment", f"{discussion_id}:1"),
            )
            assert comment_deleted.record is None
            _, after_delete, _ = _direct_request(token, "GET", f"/api/1/discussions/{discussion_id}/")
            assert isinstance(after_delete, Mapping)
            assert len(after_delete["discussion"]) == len(raw["discussion"])
        finally:
            deleted = client.activity_discussions.delete_discussion(
                discussion_id, permissions, policy("udata/api-v1.delete-discussion", discussion_id)
            )
            assert deleted.record is None
        status, _, _ = _direct_request(token, "GET", f"/api/1/discussions/{discussion_id}/")
        assert status == 404

    async def run_async() -> None:
        async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
            dataset_id = (await client.datasets.list(DatasetListQuery(page=1, page_size=1))).items[0].id.value
            created = await client.activity_discussions.create_discussion(
                DiscussionCreateInput(
                    title=title, comment="evidence opening comment", subject={"id": dataset_id, "class": "Dataset"}
                ),
                permissions,
                policy("udata/api-v1.create-discussion", dataset_id),
            )
            assert created.record is not None
            discussion_id = str(created.record.payload["id"])
            try:
                commented = await client.activity_discussions.comment_discussion(
                    discussion_id,
                    CommentInput(comment="evidence reply"),
                    permissions,
                    policy("udata/api-v1.comment-discussion", discussion_id),
                )
                assert commented.receipt.operation == "udata/api-v1.comment-discussion"
                status, raw, _ = _direct_request(token, "GET", f"/api/1/discussions/{discussion_id}/")
                assert status == 200
                assert isinstance(raw, Mapping)
                assert len(raw["discussion"]) == 2
            finally:
                await client.activity_discussions.delete_discussion(
                    discussion_id, permissions, policy("udata/api-v1.delete-discussion", discussion_id)
                )

    asyncio.run(run_async())


def _controlled_reuse_read_cases() -> tuple[tuple[str, str, tuple[object, ...]], ...]:
    """Return the synchronous reuse read cases as (raw path, typed method, typed arguments)."""
    return (
        ("/api/1/reuses/?page=1&page_size=20", "list_reuses", (ReuseListQuery(),)),
        ("/api/1/reuses/recent.atom?page=1&page_size=20", "recent_reuses_atom_feed", (ReuseListQuery(),)),
        ("/api/1/reuses/badges/", "available_reuse_badges", ()),
        ("/api/1/reuses/suggest/?q=a&size=3", "suggest_reuses", (ReuseSuggestQuery(q="a", size=3),)),
        ("/api/1/reuses/types/", "reuse_types", ()),
        ("/api/1/reuses/topics/", "reuse_topics", ()),
        ("/api/2/reuses/?page=1&page_size=20", "list_v2", (ReuseListQuery(),)),
        ("/api/2/reuses/search/?page=1&page_size=50&q=a", "search_v2", (ReuseSearchQuery(q="a"),)),
    )


def _controlled_reuse_async_read_cases() -> tuple[tuple[str, str, tuple[object, ...]], ...]:
    """Return the asynchronous reuse read cases as (raw path, typed method, typed arguments)."""
    return (
        ("/api/1/reuses/?page=1&page_size=20", "list_reuses", (ReuseListQuery(),)),
        ("/api/1/reuses/badges/", "available_reuse_badges", ()),
        ("/api/1/reuses/types/", "reuse_types", ()),
        ("/api/1/reuses/topics/", "reuse_topics", ()),
        ("/api/2/reuses/?page=1&page_size=20", "list_v2", (ReuseListQuery(),)),
    )


def _assert_reuse_read_matches_raw(method: str, payload: object, typed: Any) -> None:
    """Assert one successful reuse read returns the shape its route declares."""
    if method == "recent_reuses_atom_feed":
        assert typed.payload["media_type"] == "application/atom+xml"
        assert typed.payload["size_bytes"] > 0
    elif method in {"reuse_types", "reuse_topics", "suggest_reuses"}:
        assert isinstance(typed, tuple)
    else:
        assert _plain_json(typed.payload) == payload, method


def _assert_reuse_read_failure_agrees(error: CatalogError, status: int, method: str) -> None:
    """Assert one failed reuse read propagated the status of its raw route."""
    assert error.metadata.get("status_code") == status, method


def _exercise_sync_reuse_reads() -> None:
    """Compare every synchronous reuse read route against its raw loopback response."""
    with create_sync_client(UDataClientSettings(base_url=ORIGIN)) as client:
        for path, method, args in _controlled_reuse_read_cases():
            status, payload, _ = _direct_request("", "GET", path, max_bytes=65536)
            if status != 200:
                with pytest.raises(CatalogError) as error:
                    getattr(client.reuses, method)(*args)
                _assert_reuse_read_failure_agrees(error.value, status, method)
                continue
            typed = getattr(client.reuses, method)(*args)
            _assert_reuse_read_matches_raw(method, payload, typed)


async def _exercise_async_reuse_reads() -> None:
    """Compare every asynchronous reuse read route against its raw loopback response."""
    async with create_async_client(UDataClientSettings(base_url=ORIGIN)) as client:
        for path, method, args in _controlled_reuse_async_read_cases():
            status, payload, _ = _direct_request("", "GET", path)
            if status != 200:
                with pytest.raises(CatalogError) as error:
                    await getattr(client.reuses, method)(*args)
                _assert_reuse_read_failure_agrees(error.value, status, method)
                continue
            operation = getattr(client.reuses, method)(*args)
            typed = await operation
            assert status == 200, path
            if method in {"reuse_types", "reuse_topics"}:
                assert isinstance(typed, tuple)
            else:
                assert _plain_json(typed.payload) == payload, method


def test_controlled_reuse_reads_match_raw_routes_in_both_modes() -> None:
    """Every assigned reuse read route returns the same native envelope typed and raw."""
    _exercise_sync_reuse_reads()
    asyncio.run(_exercise_async_reuse_reads())


def test_controlled_reuse_lifecycle_matches_raw_routes_and_cleans_up() -> None:
    """Seed one deterministic reuse and drive every assigned reuse mutation."""
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled reuse evidence requires UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    title = "evidence reuse"

    def policy(operation: str, target: str) -> MutationPolicy:
        return MutationPolicy(
            destructive=operation
            in {
                "udata/api-v1.delete-reuse",
                "udata/api-v1.delete-reuse-badge",
                "udata/api-v1.unfeature-reuse",
                "udata/api-v1.unfollow-reuse",
            },
            confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
            concurrency=ConcurrencyPolicy(overwrite=True),
        )

    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        created = client.reuses.create_reuse(
            ReuseCreateInput(
                title=title,
                description="evidence",
                type="application",
                url=f"https://example.com/reuse/{uuid4().hex[:8]}",
                topic="health",
            ),
            permissions,
            policy("udata/api-v1.create-reuse", title),
        )
        assert created.record is not None
        reuse_id = str(created.record.payload["id"])
        try:
            assert created.receipt.operation == "udata/api-v1.create-reuse"
            status, raw, _ = _direct_request(token, "GET", f"/api/1/reuses/{reuse_id}/")
            assert status == 200
            assert isinstance(raw, Mapping)
            assert raw["title"] == title
            assert _plain_json(client.reuses.get_reuse(reuse_id).payload) == raw

            dataset_id = client.datasets.list(DatasetListQuery(page=1, page_size=1)).items[0].id.value
            linked = client.reuses.reuse_add_dataset(
                reuse_id, dataset_id, permissions, policy("udata/api-v1.reuse-add-dataset", reuse_id)
            )
            assert linked.record is not None
            assert linked.receipt.operation == "udata/api-v1.reuse-add-dataset"

            updated = client.reuses.update_reuse(
                reuse_id,
                ReuseUpdateInput(title=f"{title} renamed"),
                permissions,
                policy("udata/api-v1.update-reuse", reuse_id),
            )
            assert updated.record is not None
            assert updated.record.payload["title"] == f"{title} renamed"

            with pytest.raises(CatalogError):
                client.reuses.add_reuse_badge(
                    reuse_id, "badger", permissions, policy("udata/api-v1.add-reuse-badge", reuse_id)
                )

            featured = client.reuses.feature_reuse(
                reuse_id, permissions, policy("udata/api-v1.feature-reuse", reuse_id)
            )
            assert featured.receipt.operation == "udata/api-v1.feature-reuse"
            assert featured.record is not None
            assert featured.record.payload["featured"] is True

            followed = client.reuses.follow_reuse(reuse_id, permissions, policy("udata/api-v1.follow-reuse", reuse_id))
            assert followed.receipt.operation == "udata/api-v1.follow-reuse"

            followers_page = client.reuses.list_reuse_followers(reuse_id, ReuseFollowersQuery())
            followers_payload = cast(Mapping[str, object], _plain_json(followers_page.payload))
            total = followers_payload["total"]
            assert isinstance(total, int)
            assert total >= 1
        finally:
            try:
                client.reuses.unfollow_reuse(reuse_id, permissions, policy("udata/api-v1.unfollow-reuse", reuse_id))
            except CatalogError:
                pass
            try:
                client.reuses.unfeature_reuse(reuse_id, permissions, policy("udata/api-v1.unfeature-reuse", reuse_id))
            except CatalogError:
                pass
            try:
                client.reuses.delete_reuse_badge(
                    reuse_id, "badger", permissions, policy("udata/api-v1.delete-reuse-badge", reuse_id)
                )
            except CatalogError:
                pass
            deleted = client.reuses.delete_reuse(reuse_id, permissions, policy("udata/api-v1.delete-reuse", reuse_id))
            assert deleted.record is None
        status, _, _ = _direct_request("", "GET", f"/api/1/reuses/{reuse_id}/")
        assert status in {404, 410}

    async def run_async() -> None:
        async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
            created = await client.reuses.create_reuse(
                ReuseCreateInput(
                    title=title,
                    description="evidence",
                    type="application",
                    url=f"https://example.com/reuse/{uuid4().hex[:8]}",
                    topic="health",
                ),
                permissions,
                policy("udata/api-v1.create-reuse", title),
            )
            assert created.record is not None
            reuse_id = str(created.record.payload["id"])
            try:
                dataset_id = (await client.datasets.list(DatasetListQuery(page=1, page_size=1))).items[0].id.value
                linked = await client.reuses.reuse_add_dataset(
                    reuse_id, dataset_id, permissions, policy("udata/api-v1.reuse-add-dataset", reuse_id)
                )
                assert linked.receipt.operation == "udata/api-v1.reuse-add-dataset"
                updated = await client.reuses.update_reuse(
                    reuse_id,
                    ReuseUpdateInput(title=f"{title} renamed"),
                    permissions,
                    policy("udata/api-v1.update-reuse", reuse_id),
                )
                assert updated.record is not None
                assert updated.record.payload["title"] == f"{title} renamed"
            finally:
                await client.reuses.delete_reuse(reuse_id, permissions, policy("udata/api-v1.delete-reuse", reuse_id))

    asyncio.run(run_async())


def _assert_posts_reports_read_matches_raw(method: str, payload: object, typed: Any) -> None:
    """Assert one successful post or report read matches the raw route response."""
    if method == "recent_posts_atom_feed":
        assert typed.payload["media_type"] == "application/atom+xml"
    elif method == "list_reports_reasons":
        assert isinstance(typed, tuple)
    else:
        assert _plain_json(typed.payload) == payload, method


def _assert_sync_posts_reports_read_matches_raw(method: str, payload: object, typed: Any) -> None:
    """Assert one successful synchronous post or report read, including the bounded feed size."""
    _assert_posts_reports_read_matches_raw(method, payload, typed)
    if method == "recent_posts_atom_feed":
        assert typed.payload["size_bytes"] > 0


def _exercise_sync_posts_reports_reads(token: str, cases: tuple[tuple[str, str, tuple[object, ...]], ...]) -> None:
    """Compare every synchronous post and report read route against its raw response."""
    credential = UDataCredential(api_key=token)
    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        for path, method, args in cases:
            status, payload, _ = _direct_request(token, "GET", path, max_bytes=65536)
            if status != 200:
                with pytest.raises(CatalogError) as error:
                    getattr(client.posts_reports, method)(*args)
                assert error.value.metadata.get("status_code") == status, method
                continue
            typed = getattr(client.posts_reports, method)(*args)
            _assert_sync_posts_reports_read_matches_raw(method, payload, typed)


async def _exercise_async_posts_reports_reads(
    token: str, cases: tuple[tuple[str, str, tuple[object, ...]], ...]
) -> None:
    """Compare every asynchronous post and report read route against its raw response."""
    credential = UDataCredential(api_key=token)
    async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        for path, method, args in cases:
            status, payload, _ = _direct_request(token, "GET", path, max_bytes=65536)
            if status != 200:
                with pytest.raises(CatalogError) as error:
                    await getattr(client.posts_reports, method)(*args)
                assert error.value.metadata.get("status_code") == status, method
                continue
            operation = getattr(client.posts_reports, method)(*args)
            typed = await operation
            _assert_posts_reports_read_matches_raw(method, payload, typed)


def test_controlled_posts_reports_reads_match_raw_routes_in_both_modes() -> None:
    """Every assigned post, report, and notification read route is compared to raw."""
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled posts/reports evidence requires UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    from datasluice.connectors.catalog.udata.models.posts_reports import (
        PostListQuery,
        PostSearchQuery,
        ReportQuery,
    )

    cases = (
        ("/api/1/posts/?page=1&page_size=20", "list_posts", (PostListQuery(),)),
        ("/api/1/posts/recent.atom", "recent_posts_atom_feed", ()),
        ("/api/2/posts/search/?page=1&page_size=20", "search_posts", (PostSearchQuery(),)),
        ("/api/1/reports/?page=1&page_size=20", "list_reports", (ReportQuery(),)),
        ("/api/1/reports/reasons/", "list_reports_reasons", ()),
    )
    _exercise_sync_posts_reports_reads(token, cases)
    asyncio.run(_exercise_async_posts_reports_reads(token, cases))


def _posts_reports_mutation_policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    """Return the confirmed overwrite policy the controlled post mutations run under."""
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _invite_evidence_member(token: str, member_token: str) -> str:
    """Invite the disposable member to the seeded evidence organization, returning the invitation id."""
    member_id_response = _direct_request(member_token, "GET", "/api/1/me/")
    assert member_id_response[0] == 200
    assert isinstance(member_id_response[1], Mapping)
    member_id = str(member_id_response[1]["id"])
    status, invitation, _ = _direct_request(
        token,
        "POST",
        "/api/1/organizations/evidence-organization/member/",
        body={"user": member_id, "role": "editor"},
    )
    assert status == 201
    assert isinstance(invitation, Mapping)
    return str(invitation["id"])


def _controlled_member_notifications(member_token: str) -> tuple[object, object, str]:
    """Return the raw unhandled notification page, its items, and the first unhandled notification id."""
    member_status, notifications, _ = _direct_request(
        member_token, "GET", "/api/1/notifications/?page=1&page_size=20&handled=false"
    )
    assert member_status == 200
    assert isinstance(notifications, Mapping)
    notification_items = notifications["data"]
    matching = [item for item in notification_items if isinstance(item, Mapping) and item.get("handled_at") is None]
    assert matching
    return notifications, notification_items, str(matching[0]["id"])


def _cancel_evidence_invitation(token: str, invitation_id: str) -> None:
    """Cancel the disposable evidence organization invitation, tolerating a refusal from the deployment."""
    try:
        _direct_request(
            token,
            "POST",
            f"/api/1/organizations/evidence-organization/membership/{invitation_id}/cancel/",
        )
    except CatalogError:
        pass


def _mark_controlled_notification_read(member_token: str, notification_id: str) -> None:
    """Mark the disposable notification handled again, tolerating a refusal from the deployment."""
    try:
        _direct_request(member_token, "POST", f"/api/1/notifications/{notification_id}/read/", body={})
    except CatalogError:
        pass


def _assert_controlled_post_deleted(post_id: str) -> None:
    """Assert the raw post route reports the deleted controlled post as gone."""
    status, _, _ = _direct_request("", "GET", f"/api/1/posts/{post_id}/")
    assert status in {404, 410}


def _assert_controlled_post_matches_raw(client: SyncUDataClient, token: str, post_id: str, title: str) -> None:
    """Assert the synchronous typed post read returns the same envelope the raw route returned."""
    status, raw, _ = _direct_request(token, "GET", f"/api/1/posts/{post_id}/")
    assert status == 200
    assert isinstance(raw, Mapping)
    assert raw["name"] == title
    assert _plain_json(client.posts_reports.get_post(post_id).payload) == raw


def _exercise_sync_post_publication(client: SyncUDataClient, permissions: EffectivePermissions, post_id: str) -> None:
    """Publish, illustrate, resize, and rename one controlled post over the synchronous client."""
    from datasluice.connectors.catalog.udata.models.posts_reports import PostUpdateInput

    published = client.posts_reports.publish_post(
        post_id, permissions, _posts_reports_mutation_policy("udata/api-v1.publish-post", post_id)
    )
    assert published.record is not None
    assert published.record.payload["published"] is not None
    uploaded = client.posts_reports.post_image(
        post_id,
        _CONTROLLED_PNG,
        "image/png",
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.post-image", post_id),
    )
    assert uploaded.record is not None
    resized = client.posts_reports.resize_post_image(
        post_id,
        _CONTROLLED_PNG,
        "image/png",
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.resize-post-image", post_id),
    )
    assert resized.record is not None

    updated = client.posts_reports.update_post(
        post_id,
        PostUpdateInput(headline="Controlled evidence renamed"),
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.update-post", post_id),
    )
    assert updated.record is not None
    assert updated.record.payload["headline"] == "Controlled evidence renamed"


def _exercise_sync_controlled_report(client: SyncUDataClient, token: str, permissions: EffectivePermissions) -> str:
    """Create, read, and update one controlled dataset report, returning its id."""
    from datasluice.connectors.catalog.udata.models.posts_reports import ReportCreateInput, ReportUpdateInput

    dataset_id = client.datasets.list(DatasetListQuery(page=1, page_size=1)).items[0].id.value
    reported = client.posts_reports.create_report(
        ReportCreateInput(subject={"class": "Dataset", "id": dataset_id}, reason="spam", message="evidence"),
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.create-report", dataset_id),
    )
    assert reported.record is not None
    report_id = str(reported.record.payload["id"])
    status, raw_report, _ = _direct_request(token, "GET", f"/api/1/reports/{report_id}/")
    assert status == 200
    assert isinstance(raw_report, Mapping)
    assert _plain_json(client.posts_reports.get_report(report_id).payload) == raw_report
    updated_report = client.posts_reports.update_report(
        report_id,
        ReportUpdateInput(message="evidence updated"),
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.update-report", report_id),
    )
    assert updated_report.record is not None
    assert updated_report.record.payload["message"] == "evidence updated"
    return report_id


def _exercise_sync_member_notification(
    member_token: str,
    member_permissions: EffectivePermissions,
    notifications: object,
    notification_id: str,
) -> None:
    """Compare and read the disposable member notification over the synchronous client."""
    from datasluice.connectors.catalog.udata.models.posts_reports import NotificationQuery

    member_credential = UDataCredential(api_key=member_token)
    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=member_credential)) as member_client:
        typed_notifications = member_client.posts_reports.list_notifications(
            member_permissions, NotificationQuery(handled=False)
        )
        assert _plain_json(typed_notifications.payload) == notifications

        read = member_client.posts_reports.read_notification(
            notification_id,
            member_permissions,
            _posts_reports_mutation_policy("udata/api-v1.read-notification", notification_id),
        )
    assert read.record is not None
    assert read.record.payload["handled_at"] is not None


def _cleanup_sync_controlled_post_run(
    client: SyncUDataClient,
    token: str,
    permissions: EffectivePermissions,
    member_token: str,
    post_id: str,
    invitation_id: str | None,
    notification_id: str | None,
    report_id: str | None,
) -> None:
    """Cancel, re-handle, dismiss, unpublish, and delete every controlled post target in order."""
    from datasluice.connectors.catalog.udata.models.posts_reports import ReportUpdateInput

    if invitation_id is not None:
        _cancel_evidence_invitation(token, invitation_id)
    if notification_id is not None:
        _mark_controlled_notification_read(member_token, notification_id)
    if report_id is not None:
        try:
            client.posts_reports.update_report(
                report_id,
                ReportUpdateInput(dismissed_at="2026-09-27T00:00:00+00:00"),
                permissions,
                _posts_reports_mutation_policy("udata/api-v1.update-report", report_id),
            )
        except CatalogError:
            pass
    try:
        client.posts_reports.unpublish_post(
            post_id, permissions, _posts_reports_mutation_policy("udata/api-v1.unpublish-post", post_id)
        )
    except CatalogError:
        pass
    deleted = client.posts_reports.delete_post(
        post_id,
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.delete-post", post_id, destructive=True),
    )
    assert deleted.record is None


def _exercise_sync_post_lifecycle(
    token: str,
    member_token: str,
    permissions: EffectivePermissions,
    member_permissions: EffectivePermissions,
    title: str,
) -> None:
    """Create, publish, report, notify, and delete one controlled post over the synchronous client."""
    from datasluice.connectors.catalog.udata.models.posts_reports import PostCreateInput

    credential = UDataCredential(api_key=token)
    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        created = client.posts_reports.create_post(
            PostCreateInput(name=title, headline="Controlled evidence", content="Evidence body", kind="news"),
            permissions,
            _posts_reports_mutation_policy("udata/api-v1.create-post", title),
        )
        assert created.record is not None
        post_id = str(created.record.payload["id"])
        invitation_id: str | None = None
        notification_id: str | None = None
        report_id: str | None = None
        try:
            assert created.receipt.operation == "udata/api-v1.create-post"
            _assert_controlled_post_matches_raw(client, token, post_id, title)
            _exercise_sync_post_publication(client, permissions, post_id)
            report_id = _exercise_sync_controlled_report(client, token, permissions)
            invitation_id = _invite_evidence_member(token, member_token)
            notifications, notification_items, notification_id = _controlled_member_notifications(member_token)
            assert isinstance(notification_items, list)
            _exercise_sync_member_notification(member_token, member_permissions, notifications, notification_id)
        finally:
            _cleanup_sync_controlled_post_run(
                client, token, permissions, member_token, post_id, invitation_id, notification_id, report_id
            )
        _assert_controlled_post_deleted(post_id)


async def _assert_controlled_post_matches_raw_async(client: AsyncUDataClient, token: str, post_id: str) -> None:
    """Assert the asynchronous typed post read returns the same envelope the raw route returned."""
    status, raw, _ = _direct_request(token, "GET", f"/api/1/posts/{post_id}/")
    assert status == 200
    assert isinstance(raw, Mapping)
    assert _plain_json((await client.posts_reports.get_post(post_id)).payload) == raw


async def _exercise_async_post_publication(
    client: AsyncUDataClient, permissions: EffectivePermissions, post_id: str
) -> None:
    """Publish, illustrate, resize, and rename one controlled post over the asynchronous client."""
    from datasluice.connectors.catalog.udata.models.posts_reports import PostUpdateInput

    published = await client.posts_reports.publish_post(
        post_id, permissions, _posts_reports_mutation_policy("udata/api-v1.publish-post", post_id)
    )
    assert published.record is not None
    uploaded = await client.posts_reports.post_image(
        post_id,
        _CONTROLLED_PNG,
        "image/png",
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.post-image", post_id),
    )
    assert uploaded.record is not None
    resized = await client.posts_reports.resize_post_image(
        post_id,
        _CONTROLLED_PNG,
        "image/png",
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.resize-post-image", post_id),
    )
    assert resized.record is not None

    updated = await client.posts_reports.update_post(
        post_id,
        PostUpdateInput(headline="Controlled evidence renamed"),
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.update-post", post_id),
    )
    assert updated.record is not None
    assert updated.record.payload["headline"] == "Controlled evidence renamed"


async def _exercise_async_controlled_report(
    client: AsyncUDataClient, token: str, permissions: EffectivePermissions
) -> str:
    """Create, read, and update one controlled dataset report, returning its id."""
    from datasluice.connectors.catalog.udata.models.posts_reports import ReportCreateInput, ReportUpdateInput

    dataset_id = (await client.datasets.list(DatasetListQuery(page=1, page_size=1))).items[0].id.value
    reported = await client.posts_reports.create_report(
        ReportCreateInput(subject={"class": "Dataset", "id": dataset_id}, reason="spam", message="evidence"),
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.create-report", dataset_id),
    )
    assert reported.record is not None
    report_id = str(reported.record.payload["id"])
    status, raw_report, _ = _direct_request(token, "GET", f"/api/1/reports/{report_id}/")
    assert status == 200
    assert isinstance(raw_report, Mapping)
    assert _plain_json((await client.posts_reports.get_report(report_id)).payload) == raw_report
    updated_report = await client.posts_reports.update_report(
        report_id,
        ReportUpdateInput(message="evidence updated"),
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.update-report", report_id),
    )
    assert updated_report.record is not None
    assert updated_report.record.payload["message"] == "evidence updated"
    return report_id


async def _exercise_async_member_notification(
    member_token: str,
    member_permissions: EffectivePermissions,
    notifications: object,
    notification_id: str,
) -> None:
    """Compare and read the disposable member notification over the asynchronous client."""
    from datasluice.connectors.catalog.udata.models.posts_reports import NotificationQuery

    member_credential = UDataCredential(api_key=member_token)
    async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=member_credential)) as member_client:
        typed_notifications = await member_client.posts_reports.list_notifications(
            member_permissions, NotificationQuery(handled=False)
        )
        assert _plain_json(typed_notifications.payload) == notifications

        read = await member_client.posts_reports.read_notification(
            notification_id,
            member_permissions,
            _posts_reports_mutation_policy("udata/api-v1.read-notification", notification_id),
        )
    assert read.record is not None
    assert read.record.payload["handled_at"] is not None


async def _cleanup_async_controlled_post_run(
    client: AsyncUDataClient,
    token: str,
    permissions: EffectivePermissions,
    post_id: str,
    invitation_id: str | None,
    report_id: str | None,
) -> None:
    """Cancel, dismiss, unpublish, and delete every controlled post target in order."""
    from datasluice.connectors.catalog.udata.models.posts_reports import ReportUpdateInput

    if invitation_id is not None:
        _cancel_evidence_invitation(token, invitation_id)
    if report_id is not None:
        try:
            await client.posts_reports.update_report(
                report_id,
                ReportUpdateInput(dismissed_at="2026-09-27T00:00:00+00:00"),
                permissions,
                _posts_reports_mutation_policy("udata/api-v1.update-report", report_id),
            )
        except CatalogError:
            pass
    try:
        await client.posts_reports.unpublish_post(
            post_id, permissions, _posts_reports_mutation_policy("udata/api-v1.unpublish-post", post_id)
        )
    except CatalogError:
        pass
    deleted = await client.posts_reports.delete_post(
        post_id,
        permissions,
        _posts_reports_mutation_policy("udata/api-v1.delete-post", post_id, destructive=True),
    )
    assert deleted.record is None


async def _exercise_async_post_lifecycle(
    token: str,
    member_token: str,
    permissions: EffectivePermissions,
    member_permissions: EffectivePermissions,
    title: str,
) -> None:
    """Create, publish, report, notify, and delete one controlled post over the asynchronous client."""
    from datasluice.connectors.catalog.udata.models.posts_reports import PostCreateInput

    credential = UDataCredential(api_key=token)
    async with create_async_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        async_name = f"{title} async"
        created = await client.posts_reports.create_post(
            PostCreateInput(name=async_name, headline="Controlled evidence", content="Evidence body", kind="news"),
            permissions,
            _posts_reports_mutation_policy("udata/api-v1.create-post", async_name),
        )
        assert created.record is not None
        post_id = str(created.record.payload["id"])
        report_id: str | None = None
        invitation_id: str | None = None
        try:
            invitation_id = _invite_evidence_member(token, member_token)
            await _assert_controlled_post_matches_raw_async(client, token, post_id)
            await _exercise_async_post_publication(client, permissions, post_id)
            report_id = await _exercise_async_controlled_report(client, token, permissions)
            notifications, _, notification_id = _controlled_member_notifications(member_token)
            await _exercise_async_member_notification(member_token, member_permissions, notifications, notification_id)
        finally:
            await _cleanup_async_controlled_post_run(client, token, permissions, post_id, invitation_id, report_id)
        _assert_controlled_post_deleted(post_id)


def test_controlled_posts_reports_lifecycle_matches_raw_routes_and_cleans_up() -> None:
    """Create, publish, image, report, read, and delete deterministic controlled targets."""
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    member_token = os.environ.get("UDATA_EVIDENCE_MEMBER_TOKEN")
    if not token or not member_token:
        pytest.skip("controlled posts/reports lifecycle requires admin and member disposable tokens")

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    member_credential = UDataCredential(api_key=member_token)
    member_permissions = EffectivePermissions.for_credential(member_credential, platform=CatalogPlatform.UDATA)
    title = "evidence post"

    _exercise_sync_post_lifecycle(token, member_token, permissions, member_permissions, title)
    asyncio.run(_exercise_async_post_lifecycle(token, member_token, permissions, member_permissions, title))
