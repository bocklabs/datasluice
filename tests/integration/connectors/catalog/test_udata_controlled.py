"""Controlled-environment tracer proof against the loopback uData 17.6 stack."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import subprocess
from collections.abc import Callable, Mapping
from importlib import resources
from inspect import isawaitable
from io import BytesIO
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
from datasluice.connectors.catalog.udata.models.datasets import DatasetListQuery, DatasetSuggestQuery
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
from datasluice.connectors.catalog.udata.models.root_profile import SiteMutationResult, SitePatchInput, SiteProfile
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


def _disposable_user_token(user_id: str) -> tuple[str, str]:
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
    if (
        issued.returncode
        or not isinstance(token_id, str)
        or not token_id
        or not isinstance(token, str)
        or not token.startswith("udata_")
    ):
        raise AssertionError("Disposable user token generation failed.")
    return token_id, token


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


def _assert_user_read_matches_raw(method: str, raw: object, typed: object) -> None:
    def digest(value: object) -> str:
        return hashlib.sha256(json.dumps(_plain_json(value), sort_keys=True).encode()).hexdigest()

    if isinstance(typed, UDataPageEnvelope):
        assert isinstance(raw, Mapping)
        raw_items = raw.get("data")
        assert isinstance(raw_items, list)
        assert digest([_record_payload(item) for item in typed.items]) == digest(raw_items), method
        for key in ("page", "page_size", "previous_page", "next_page", "total"):
            if key in raw:
                assert getattr(typed.native_page, key) == raw[key]
    elif isinstance(typed, NativeRecord):
        assert isinstance(raw, Mapping)
        safe = {
            key: value for key, value in raw.items() if key not in {"token", "token_hash", "password", "last_login_at"}
        }
        assert isinstance(raw.get("last_login_at"), str)
        assert isinstance(typed.payload.get("last_login_at"), str)
        assert set(typed.payload) - {"last_login_at"} == set(safe), method
        for key, value in safe.items():
            assert digest(typed.payload[key]) == digest(value), (method, key)
    elif isinstance(typed, MappingRecord):
        assert isinstance(raw, Mapping)
        assert digest(typed.payload) == digest(raw), method
    elif isinstance(typed, tuple):
        raw_items = raw.get("data") if isinstance(raw, Mapping) else raw
        assert isinstance(raw_items, list)
        if typed and isinstance(typed[0], ApiTokenMetadata):
            assert all(set(item.to_dict()) == _TOKEN_SAFE_FIELDS for item in typed)
            assert all(isinstance(item, Mapping) for item in raw_items)
            safe_items = [
                {key: value for key, value in item.items() if key in _TOKEN_SAFE_FIELDS} for item in raw_items
            ]
            typed_items = [
                {key: value for key, value in item.to_dict().items() if key in safe_items[index]}
                for index, item in enumerate(typed)
            ]
        else:
            safe_items = raw_items
            typed_items = [_record_payload(item) for item in typed]
        assert digest(typed_items) == digest(safe_items), method
    else:
        assert digest(typed) == digest(raw), method


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


def test_controlled_user_mutations_match_raw_routes_in_both_modes() -> None:
    admin_token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not admin_token:
        pytest.skip("controlled user mutations require a seeded disposable administrator")
    admin_credential = UDataCredential(api_key=admin_token)
    admin_permissions = EffectivePermissions.for_credential(
        admin_credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    admin_status, admin_user, _ = _direct_request(admin_token, "GET", "/api/1/me/")
    assert admin_status == 200
    assert isinstance(admin_user, Mapping)
    admin_id = admin_user.get("id")
    assert isinstance(admin_id, str)
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jV1sAAAAASUVORK5CYII="
    )

    def policy(name: str, target: str, *, destructive: bool = False) -> MutationPolicy:
        return MutationPolicy(
            destructive=destructive,
            confirmation=ConfirmationPolicy(
                confirmed=True, operation=f"udata/api-v1.{name.replace('_', '-')}", target=target
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        )

    def check(status: int, result: object, name: str, target: str | None = None) -> None:
        assert isinstance(result, (UserMutationResult, ApiTokenCreationResult))
        receipt = result.receipt
        assert receipt.operation == f"udata/api-v1.{name.replace('_', '-')}"
        assert receipt.outcome == "succeeded"
        assert receipt.audit_metadata["status_code"] == status
        if target is not None:
            assert receipt.target.value == target

    async def invoke(service: object, name: str, *args: object) -> object:
        result = getattr(service, name)(*args)
        return await result if isawaitable(result) else result

    async def exercise(async_mode: bool) -> None:
        admin_client = (
            create_async_client(UDataClientSettings(base_url=ORIGIN, credential=admin_credential))
            if async_mode
            else create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=admin_credential))
        )
        organizations: dict[str, str] = {}
        users: dict[tuple[str, str], tuple[str, str]] = {}
        token_ids: list[str] = []
        user_token_ids: list[str] = []
        cleanup_errors: list[str] = []

        async def user_call(user_token: str, name: str, *args: object) -> object:
            client = (
                create_async_client(
                    UDataClientSettings(base_url=ORIGIN, credential=UDataCredential(api_key=user_token))
                )
                if async_mode
                else create_sync_client(
                    UDataClientSettings(base_url=ORIGIN, credential=UDataCredential(api_key=user_token))
                )
            )
            try:
                return await invoke(client.users_tokens, name, *args)
            finally:
                if isinstance(client, AsyncUDataClient):
                    await client.aclose()
                else:
                    assert isinstance(client, SyncUDataClient)
                    client.close()

        try:
            run_id = uuid4().hex
            raw_org_name = f"Raw user evidence {run_id}"
            typed_org_name = f"Typed user evidence {run_id}"
            raw_org_status, raw_org, _ = _direct_request(
                admin_token,
                "POST",
                "/api/1/organizations/",
                body={"name": raw_org_name, "description": "Disposable user invitation evidence"},
            )
            assert raw_org_status == 201
            assert isinstance(raw_org, Mapping)
            raw_org_id = raw_org["id"]
            assert isinstance(raw_org_id, str)
            organizations["raw"] = raw_org_id
            typed_org = await invoke(
                admin_client.organizations_memberships,
                "create_organization",
                OrganizationCreateInput(name=typed_org_name, description="Disposable user invitation evidence"),
                admin_permissions,
                policy("create_organization", typed_org_name),
            )
            assert isinstance(typed_org, OrganizationMutationResult)
            assert typed_org.record is not None
            organizations["typed"] = typed_org.record.id.value
            assert typed_org.receipt.audit_metadata["status_code"] == raw_org_status

            for decision in ("accept", "refuse"):
                run_id = uuid4().hex
                raw_email = f"raw-{run_id}@gmail.com"
                typed_email = f"typed-{run_id}@gmail.com"
                raw_status, raw_user, _ = _direct_request(
                    admin_token,
                    "POST",
                    "/api/1/users/",
                    body={"first_name": "Raw", "last_name": "Evidence", "email": raw_email, "active": True},
                )
                typed = await invoke(
                    admin_client.users_tokens,
                    "create_user",
                    UserCreateInput("Typed", "Evidence", typed_email, fields={"active": True}),
                    admin_permissions,
                    policy("create_user", f"request:{hashlib.sha256(typed_email.encode()).hexdigest()[:24]}"),
                )
                assert raw_status == 201
                assert isinstance(raw_user, Mapping)
                assert isinstance(typed, UserMutationResult)
                assert typed.record is not None
                check(raw_status, typed, "create_user", typed.record.id.value)
                raw_id = raw_user["id"]
                typed_id = typed.record.id.value
                assert isinstance(raw_id, str)
                raw_read_status, raw_created_user, _ = _direct_request(admin_token, "GET", f"/api/1/users/{raw_id}/")
                typed_created_user = await invoke(admin_client.users_tokens, "get_user", typed_id)
                assert raw_read_status == 200
                assert isinstance(raw_created_user, Mapping)
                assert isinstance(typed_created_user, NativeRecord)
                assert raw_created_user.get("email") == raw_email
                assert typed_created_user.payload.get("email") == typed_email
                assert raw_created_user.get("active") is True
                assert typed_created_user.payload.get("active") is True
                raw_user_token_id, raw_user_token = _disposable_user_token(raw_id)
                typed_user_token_id, typed_user_token = _disposable_user_token(typed_id)
                users[(decision, "raw")] = (raw_id, raw_user_token)
                users[(decision, "typed")] = (typed_id, typed_user_token)
                user_token_ids.extend((raw_user_token_id, typed_user_token_id))

            assert len({user_id for user_id, _ in users.values()}) == 4

            raw_id, raw_user_token = users[("accept", "raw")]
            typed_id, typed_user_token = users[("accept", "typed")]
            typed_permissions = EffectivePermissions.for_credential(
                UDataCredential(api_key=typed_user_token), platform=CatalogPlatform.UDATA
            )
            raw_status, raw_updated, _ = _direct_request(
                raw_user_token, "PUT", "/api/1/me/", body={"website": "https://example.org/raw"}
            )
            typed_updated = await user_call(
                typed_user_token,
                "update_me",
                UserUpdateInput({"website": "https://example.org/typed"}),
                typed_permissions,
                policy("update_me", "me"),
            )
            assert raw_status == 200
            assert isinstance(raw_updated, Mapping)
            check(raw_status, typed_updated, "update_me", "me")
            raw_state_status, raw_state, _ = _direct_request(raw_user_token, "GET", "/api/1/me/")
            typed_state = await user_call(typed_user_token, "get_me", typed_permissions)
            assert raw_state_status == 200
            assert isinstance(raw_state, Mapping)
            assert isinstance(typed_state, NativeRecord)
            assert raw_state.get("website") == "https://example.org/raw"
            assert typed_state.payload.get("website") == "https://example.org/typed"

            raw_status, raw_admin_updated, _ = _direct_request(
                admin_token, "PUT", f"/api/1/users/{raw_id}/", body={"about": "raw evidence"}
            )
            typed_admin_updated = await invoke(
                admin_client.users_tokens,
                "update_user",
                typed_id,
                UserUpdateInput({"about": "typed evidence"}),
                admin_permissions,
                policy("update_user", typed_id),
            )
            assert raw_status == 200
            assert isinstance(raw_admin_updated, Mapping)
            check(raw_status, typed_admin_updated, "update_user", typed_id)
            raw_state_status, raw_state, _ = _direct_request(admin_token, "GET", f"/api/1/users/{raw_id}/")
            typed_state = await invoke(admin_client.users_tokens, "get_user", typed_id)
            assert raw_state_status == 200
            assert isinstance(raw_state, Mapping)
            assert isinstance(typed_state, NativeRecord)
            assert raw_state.get("about") == "raw evidence"
            assert typed_state.payload.get("about") == "typed evidence"

            raw_avatar_body, raw_avatar_type = _multipart_body(png, "raw.png", media_type="image/png")
            raw_status, _, _ = _direct_request(
                raw_user_token, "POST", "/api/1/me/avatar/", body=raw_avatar_body, content_type=raw_avatar_type
            )
            typed_avatar = await user_call(
                typed_user_token,
                "my_avatar",
                UserAvatarInput(BytesIO(png), "typed.png", len(png), "image/png"),
                typed_permissions,
                policy("my_avatar", "me"),
            )
            check(raw_status, typed_avatar, "my_avatar", "me")
            avatar_state = _controlled_user_state((raw_id, typed_id))
            assert avatar_state[raw_id]["avatar_present"]
            assert avatar_state[typed_id]["avatar_present"]
            raw_admin_id, _ = users[("refuse", "raw")]
            typed_admin_id, _ = users[("refuse", "typed")]
            raw_status, _, _ = _direct_request(
                admin_token,
                "POST",
                f"/api/1/users/{raw_admin_id}/avatar/",
                body=raw_avatar_body,
                content_type=raw_avatar_type,
            )
            typed_avatar = await invoke(
                admin_client.users_tokens,
                "user_avatar",
                typed_admin_id,
                UserAvatarInput(BytesIO(png), "typed-admin.png", len(png), "image/png"),
                admin_permissions,
                policy("user_avatar", typed_admin_id),
            )
            check(raw_status, typed_avatar, "user_avatar", typed_admin_id)
            avatar_state = _controlled_user_state((raw_admin_id, typed_admin_id))
            assert avatar_state[raw_admin_id]["avatar_present"]
            assert avatar_state[typed_admin_id]["avatar_present"]

            raw_status, _, _ = _direct_request(admin_token, "POST", f"/api/1/users/{raw_id}/followers/")
            typed_follow = await invoke(
                admin_client.users_tokens, "follow_user", typed_id, admin_permissions, policy("follow_user", typed_id)
            )
            check(raw_status, typed_follow, "follow_user", typed_id)
            raw_follower_status, raw_followers, _ = _direct_request(
                admin_token,
                "GET",
                f"/api/1/users/{raw_id}/followers/?page=1&page_size=20",
            )
            typed_followers = await invoke(admin_client.users_tokens, "list_user_followers", typed_id)
            assert raw_follower_status == 200
            assert admin_id in _follower_ids(raw_followers)
            assert isinstance(typed_followers, UDataPageEnvelope)
            assert admin_id in _typed_follower_ids(typed_followers)
            raw_status, _, _ = _direct_request(admin_token, "DELETE", f"/api/1/users/{raw_id}/followers/")
            typed_unfollow = await invoke(
                admin_client.users_tokens,
                "unfollow_user",
                typed_id,
                admin_permissions,
                policy("unfollow_user", typed_id, destructive=True),
            )
            check(raw_status, typed_unfollow, "unfollow_user", typed_id)
            raw_follower_status, raw_followers, _ = _direct_request(
                admin_token,
                "GET",
                f"/api/1/users/{raw_id}/followers/?page=1&page_size=20",
            )
            typed_followers = await invoke(admin_client.users_tokens, "list_user_followers", typed_id)
            assert raw_follower_status == 200
            assert admin_id not in _follower_ids(raw_followers)
            assert isinstance(typed_followers, UDataPageEnvelope)
            assert admin_id not in _typed_follower_ids(typed_followers)

            raw_status, raw_token, _ = _direct_request(
                admin_token, "POST", "/api/1/me/api_tokens/", body={"name": "raw controlled"}
            )
            assert raw_status == 201
            assert isinstance(raw_token, dict)
            raw_token_id = raw_token.get("id")
            assert isinstance(raw_token_id, str)
            raw_token.pop("token", None)
            token_ids.append(raw_token_id)
            typed_token = await invoke(
                admin_client.users_tokens,
                "create_api_token",
                ApiTokenCreateInput(name="typed controlled"),
                admin_permissions,
                policy("create_api_token", "new-api-token"),
            )
            assert isinstance(typed_token, ApiTokenCreationResult)
            check(raw_status, typed_token, "create_api_token", typed_token.metadata.id)
            if not typed_token.secret.reveal_once().startswith("udata_"):
                pytest.fail("Created token did not match the controlled token shape.")
            token_ids.append(typed_token.metadata.id)
            raw_list_status, raw_token_list, _ = _direct_request(admin_token, "GET", "/api/1/me/api_tokens/")
            typed_token_list = await invoke(admin_client.users_tokens, "list_api_tokens", admin_permissions)
            raw_token_rows = raw_token_list.get("data") if isinstance(raw_token_list, Mapping) else raw_token_list
            assert raw_list_status == 200
            assert isinstance(raw_token_rows, list)
            assert isinstance(typed_token_list, tuple)
            raw_active_token_ids = {
                item["id"]
                for item in raw_token_rows
                if isinstance(item, Mapping) and isinstance(item.get("id"), str) and not item.get("revoked_at")
            }
            typed_active_token_ids = {
                item.id for item in typed_token_list if isinstance(item, ApiTokenMetadata) and item.revoked_at is None
            }
            assert {raw_token_id, typed_token.metadata.id} <= raw_active_token_ids
            assert {raw_token_id, typed_token.metadata.id} <= typed_active_token_ids
            raw_status, _, _ = _direct_request(admin_token, "DELETE", f"/api/1/me/api_tokens/{raw_token_id}/")
            typed_revoke = await invoke(
                admin_client.users_tokens,
                "revoke_api_token",
                typed_token.metadata.id,
                admin_permissions,
                policy("revoke_api_token", typed_token.metadata.id, destructive=True),
            )
            check(raw_status, typed_revoke, "revoke_api_token", typed_token.metadata.id)
            raw_list_status, raw_token_list, _ = _direct_request(admin_token, "GET", "/api/1/me/api_tokens/")
            typed_token_list = await invoke(admin_client.users_tokens, "list_api_tokens", admin_permissions)
            raw_token_rows = raw_token_list.get("data") if isinstance(raw_token_list, Mapping) else raw_token_list
            assert raw_list_status == 200
            assert isinstance(raw_token_rows, list)
            assert isinstance(typed_token_list, tuple)
            raw_tokens_by_id = {
                item["id"]: item
                for item in raw_token_rows
                if isinstance(item, Mapping) and isinstance(item.get("id"), str)
            }
            typed_tokens_by_id = {item.id: item for item in typed_token_list if isinstance(item, ApiTokenMetadata)}
            for token_id in (raw_token_id, typed_token.metadata.id):
                raw_state = raw_tokens_by_id.get(token_id)
                typed_state = typed_tokens_by_id.get(token_id)
                assert raw_state is None or raw_state.get("revoked_at") is not None
                assert typed_state is None or typed_state.revoked_at is not None

            for decision in ("refuse", "accept"):
                raw_id, raw_user_token = users[(decision, "raw")]
                typed_id, typed_user_token = users[(decision, "typed")]
                typed_permissions = EffectivePermissions.for_credential(
                    UDataCredential(api_key=typed_user_token), platform=CatalogPlatform.UDATA
                )
                raw_org_status, raw_org_state, _ = _direct_request(
                    admin_token, "GET", f"/api/1/organizations/{organizations['raw']}/"
                )
                assert raw_org_status == 200
                assert isinstance(raw_org_state, Mapping)
                assert not any(member["user"]["id"] == raw_id for member in raw_org_state["members"]), decision
                raw_status, raw_invitation, _ = _direct_request(
                    admin_token,
                    "POST",
                    f"/api/1/organizations/{organizations['raw']}/member/",
                    body={"user": raw_id, "role": "editor"},
                )
                assert raw_status == 201
                typed_invitation = await invoke(
                    admin_client.organizations_memberships,
                    "invite_organization_member",
                    organizations["typed"],
                    OrganizationInvitationInput(user=typed_id, role="editor"),
                    admin_permissions,
                    policy("invite_organization_member", organizations["typed"]),
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
                typed_decision = await user_call(
                    typed_user_token,
                    f"{decision}_org_invitation",
                    typed_invitation_id,
                    typed_permissions,
                    policy(f"{decision}_org_invitation", typed_invitation_id),
                )
                check(raw_status, typed_decision, f"{decision}_org_invitation", typed_invitation_id)
                if decision == "accept":
                    for org_id, user_id in ((organizations["raw"], raw_id), (organizations["typed"], typed_id)):
                        member_status, member_org, _ = _direct_request(
                            admin_token, "GET", f"/api/1/organizations/{org_id}/"
                        )
                        assert member_status == 200
                        assert isinstance(member_org, Mapping)
                        assert any(member["user"]["id"] == user_id for member in member_org["members"])
                else:
                    raw_pending_status, raw_pending, _ = _direct_request(
                        raw_user_token, "GET", "/api/1/me/org_invitations/"
                    )
                    assert raw_pending_status == 200
                    assert isinstance(raw_pending, list)
                    assert not raw_pending
                    typed_pending = await user_call(typed_user_token, "list_org_invitations", typed_permissions)
                    assert not typed_pending

            before_rotation = _controlled_user_state((raw_id, typed_id))
            assert not before_rotation[raw_id]["password_rotation_requested"]
            assert not before_rotation[typed_id]["password_rotation_requested"]
            raw_status, _, _ = _direct_request(admin_token, "POST", f"/api/1/users/{raw_id}/rotate_password/")
            typed_rotated = await invoke(
                admin_client.users_tokens,
                "rotate_user_password",
                typed_id,
                admin_permissions,
                policy("rotate_user_password", typed_id),
            )
            check(raw_status, typed_rotated, "rotate_user_password", typed_id)
            after_rotation = _controlled_user_state((raw_id, typed_id))
            assert after_rotation[raw_id]["password_rotation_requested"]
            assert after_rotation[typed_id]["password_rotation_requested"]

            raw_refuse_id, raw_refuse_token = users[("refuse", "raw")]
            typed_refuse_id, typed_refuse_token = users[("refuse", "typed")]
            typed_refuse_permissions = EffectivePermissions.for_credential(
                UDataCredential(api_key=typed_refuse_token), platform=CatalogPlatform.UDATA
            )
            raw_status, _, _ = _direct_request(raw_refuse_token, "DELETE", "/api/1/me/")
            typed_deleted_me = await user_call(
                typed_refuse_token,
                "delete_me",
                typed_refuse_permissions,
                policy("delete_me", "me", destructive=True),
            )
            check(raw_status, typed_deleted_me, "delete_me", "me")
            raw_accept_id = users[("accept", "raw")][0]
            raw_delete_status, _, _ = _direct_request(
                admin_token,
                "DELETE",
                f"/api/1/users/{raw_accept_id}/?send_legal_notice=false&no_mail=true&delete_comments=false",
            )
            typed_accept_id = users[("accept", "typed")][0]
            typed_deleted_user = await invoke(
                admin_client.users_tokens,
                "delete_user",
                typed_accept_id,
                admin_permissions,
                policy("delete_user", typed_accept_id, destructive=True),
                UserDeleteOptions(no_mail=True),
            )
            check(raw_delete_status, typed_deleted_user, "delete_user", typed_accept_id)
            assert raw_delete_status == 204
            for user_id in (raw_refuse_id, typed_refuse_id, users[("accept", "raw")][0], typed_accept_id):
                deleted_status, deleted_user, _ = _direct_request(admin_token, "GET", f"/api/1/users/{user_id}/")
                assert deleted_status in {200, 404, 410}
                if deleted_status == 200:
                    assert isinstance(deleted_user, Mapping)
                    assert deleted_user.get("active") is False
        finally:
            for token_id in token_ids:
                try:
                    status, _, _ = _direct_request(admin_token, "DELETE", f"/api/1/me/api_tokens/{token_id}/")
                    if status not in {204, 404, 410}:
                        cleanup_errors.append(f"token deletion returned {status}")
                except Exception as error:
                    cleanup_errors.append(f"token deletion raised {type(error).__name__}")
            if user_token_ids:
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
                            *user_token_ids,
                        ],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    if revoked.returncode:
                        cleanup_errors.append("disposable user token revocation failed")
                except Exception as error:
                    cleanup_errors.append(f"disposable user token revocation raised {type(error).__name__}")
            for user_id, _ in users.values():
                try:
                    status, _, _ = _direct_request(
                        admin_token,
                        "DELETE",
                        f"/api/1/users/{user_id}/?send_legal_notice=false&no_mail=true&delete_comments=false",
                    )
                    if status not in {204, 404, 410}:
                        cleanup_errors.append(f"user deletion returned {status}")
                    status, payload, _ = _direct_request(admin_token, "GET", f"/api/1/users/{user_id}/")
                    if status == 200 and (not isinstance(payload, Mapping) or payload.get("active") is not False):
                        cleanup_errors.append("user remains active after cleanup")
                    elif status not in {200, 404, 410}:
                        cleanup_errors.append(f"user cleanup verification returned {status}")
                except Exception as error:
                    cleanup_errors.append(f"user cleanup raised {type(error).__name__}")
            for org_id in organizations.values():
                try:
                    deleted_status, _, _ = _direct_request(admin_token, "DELETE", f"/api/1/organizations/{org_id}/")
                    if deleted_status not in {204, 404, 410}:
                        cleanup_errors.append(f"organization deletion returned {deleted_status}")
                except Exception as error:
                    cleanup_errors.append(f"organization deletion raised {type(error).__name__}")
            if isinstance(admin_client, AsyncUDataClient):
                try:
                    await admin_client.aclose()
                except Exception as error:
                    cleanup_errors.append(f"client close raised {type(error).__name__}")
            else:
                assert isinstance(admin_client, SyncUDataClient)
                try:
                    admin_client.close()
                except Exception as error:
                    cleanup_errors.append(f"client close raised {type(error).__name__}")
            assert not cleanup_errors, "; ".join(cleanup_errors)

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


def test_controlled_organization_mutations_match_raw_routes_in_both_modes() -> None:
    admin_token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    member_token = os.environ.get("UDATA_EVIDENCE_MEMBER_TOKEN")
    organization_admin_token = os.environ.get("UDATA_EVIDENCE_ORGANIZATION_ADMIN_TOKEN")
    if not admin_token or not member_token or not organization_admin_token:
        pytest.skip("controlled organization mutations require disposable admin and member tokens")
    from io import BytesIO

    from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput

    admin_credential = UDataCredential(api_key=admin_token)
    member_credential = UDataCredential(api_key=member_token)
    org_admin_credential = UDataCredential(api_key=organization_admin_token)
    admin_permissions = EffectivePermissions.for_credential(
        admin_credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    member_permissions = EffectivePermissions.for_credential(member_credential, platform=CatalogPlatform.UDATA)
    org_admin_permissions = EffectivePermissions.for_credential(org_admin_credential, platform=CatalogPlatform.UDATA)

    def policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
        return MutationPolicy(
            destructive=destructive,
            confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
            concurrency=ConcurrencyPolicy(overwrite=True),
        )

    def check(raw_status: int, typed: OrganizationMutationResult, operation: str, statuses: set[int]) -> None:
        receipt = typed.receipt
        assert raw_status in statuses
        assert receipt.operation == operation
        assert receipt.outcome == "succeeded"
        assert receipt.audit_metadata["status_code"] == raw_status

    async def invoke(client, method: str, *args):
        result = getattr(client.organizations_memberships, method)(*args)
        return await result if isawaitable(result) else result

    image = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jV1sAAAAASUVORK5CYII="
    )

    async def exercise(admin_client, member_client, org_admin_client, run_id: str) -> None:
        raw_org_id: str | None = None
        typed_org_id: str | None = None
        raw_delete_status: int | None = None
        cleanup_errors: list[Exception] = []

        async def cleanup(
            action: Callable[[], object], *, expected_status: int | None = None, operation: str | None = None
        ) -> object | None:
            try:
                result = action()
                if isawaitable(result):
                    result = await result
            except Exception as error:
                cleanup_errors.append(error)
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
                    cleanup_errors.append(AssertionError("typed organization delete receipt did not match raw delete"))
            return result

        try:
            raw_status, raw_org, _ = _direct_request(
                admin_token,
                "POST",
                "/api/1/organizations/",
                body={"name": f"Raw Org {run_id}", "description": "route differential"},
            )
            assert raw_status == 201
            assert isinstance(raw_org, Mapping)
            assert isinstance(raw_org.get("id"), str)
            raw_org_id = raw_org["id"]
            typed_created = await invoke(
                admin_client,
                "create_organization",
                OrganizationCreateInput(name=f"Typed Org {run_id}", description="route differential"),
                admin_permissions,
                policy("udata/api-v1.create-organization", f"Typed Org {run_id}"),
            )
            check(201, typed_created, "udata/api-v1.create-organization", {201})
            assert typed_created.record is not None
            typed_org_id = typed_created.record.id.value

            direct_status, direct_updated, _ = _direct_request(
                admin_token,
                "PUT",
                f"/api/1/organizations/{raw_org_id}/",
                body={"description": "raw updated"},
            )
            typed_updated = await invoke(
                admin_client,
                "update_organization",
                typed_org_id,
                OrganizationUpdateInput(description="typed updated"),
                admin_permissions,
                policy("udata/api-v1.update-organization", typed_org_id),
            )
            assert isinstance(direct_updated, Mapping)
            assert direct_updated["description"] == "raw updated"
            check(direct_status, typed_updated, "udata/api-v1.update-organization", {200})
            assert typed_updated.record is not None
            assert typed_updated.record.payload["description"] == "typed updated"

            direct_status, direct_request, _ = _direct_request(
                member_token,
                "POST",
                f"/api/1/organizations/{raw_org_id}/membership/",
                body={"comment": "raw join"},
            )
            typed_request = await invoke(
                member_client,
                "membership_request",
                typed_org_id,
                MembershipRequestInput(comment="typed join"),
                member_permissions,
                policy("udata/api-v1.membership-request", typed_org_id),
            )
            check(direct_status, typed_request, "udata/api-v1.membership-request", {200, 201})
            assert isinstance(direct_request, Mapping)
            assert isinstance(typed_request.value, Mapping)
            raw_request_id = direct_request["id"]
            typed_request_id = typed_request.value["id"]
            member_id = direct_request["user"]["id"]
            raw_status, raw_requests, _ = _direct_request(
                admin_token, "GET", f"/api/1/organizations/{raw_org_id}/membership/"
            )
            typed_requests = await invoke(admin_client, "list_membership_requests", typed_org_id, admin_permissions)
            assert raw_status == 200
            assert isinstance(raw_requests, list)
            assert any(item["id"] == raw_request_id for item in raw_requests)
            assert any(item.payload["id"] == typed_request_id for item in typed_requests)

            direct_status, direct_member, _ = _direct_request(
                admin_token,
                "POST",
                f"/api/1/organizations/{raw_org_id}/membership/{raw_request_id}/accept/",
            )
            typed_member = await invoke(
                admin_client,
                "accept_membership",
                typed_org_id,
                typed_request_id,
                admin_permissions,
                policy("udata/api-v1.accept-membership", f"{typed_org_id}/{typed_request_id}"),
            )
            assert isinstance(direct_member, Mapping)
            assert direct_member["user"]["id"] == member_id
            check(direct_status, typed_member, "udata/api-v1.accept-membership", {200})
            assert isinstance(typed_member.value, Mapping)
            assert typed_member.value["user"]["id"] == member_id

            raw_status, raw_role, _ = _direct_request(
                admin_token,
                "PUT",
                f"/api/1/organizations/{raw_org_id}/member/{member_id}/",
                body={"role": "partial_editor"},
            )
            typed_role = await invoke(
                admin_client,
                "update_organization_member",
                typed_org_id,
                member_id,
                OrganizationMemberInput("partial_editor"),
                admin_permissions,
                policy("udata/api-v1.update-organization-member", f"{typed_org_id}/{member_id}"),
            )
            assert isinstance(raw_role, Mapping)
            assert raw_role["role"] == "partial_editor"
            check(raw_status, typed_role, "udata/api-v1.update-organization-member", {200})
            assert isinstance(typed_role.value, Mapping)
            assert typed_role.value["role"] == "partial_editor"
            raw_status, raw_assignments, _ = _direct_request(
                admin_token, "GET", f"/api/1/organizations/{raw_org_id}/assignments/"
            )
            typed_assignments = await invoke(
                admin_client, "list_organization_assignments", typed_org_id, admin_permissions
            )
            assert raw_status == 200
            assert isinstance(raw_assignments, list)
            assert not typed_assignments
            raw_status, raw_synced, _ = _direct_request(
                admin_token,
                "PUT",
                f"/api/1/organizations/{raw_org_id}/member/{member_id}/assignments/",
                body=[],
            )
            typed_synced = await invoke(
                admin_client,
                "sync_member_assignments",
                typed_org_id,
                member_id,
                [],
                admin_permissions,
                policy("udata/api-v1.sync-member-assignments", f"{typed_org_id}/{member_id}"),
            )
            assert raw_status == 200
            assert raw_synced == []
            check(raw_status, typed_synced, "udata/api-v1.sync-member-assignments", {200})
            raw_status, raw_member_delete, _ = _direct_request(
                admin_token,
                "DELETE",
                f"/api/1/organizations/{raw_org_id}/member/{member_id}/",
            )
            typed_member_delete = await invoke(
                admin_client,
                "delete_organization_member",
                typed_org_id,
                member_id,
                admin_permissions,
                policy(
                    "udata/api-v1.delete-organization-member",
                    f"{typed_org_id}/{member_id}",
                    destructive=True,
                ),
            )
            assert raw_status == 204
            assert raw_member_delete is None
            check(raw_status, typed_member_delete, "udata/api-v1.delete-organization-member", {204})

            direct_status, raw_pending, _ = _direct_request(
                organization_admin_token,
                "POST",
                f"/api/1/organizations/{raw_org_id}/membership/",
                body={"comment": "raw refusal"},
            )
            typed_pending = await invoke(
                org_admin_client,
                "membership_request",
                typed_org_id,
                MembershipRequestInput(comment="typed refusal"),
                org_admin_permissions,
                policy("udata/api-v1.membership-request", typed_org_id),
            )
            check(direct_status, typed_pending, "udata/api-v1.membership-request", {201})
            assert isinstance(raw_pending, Mapping)
            assert isinstance(typed_pending.value, Mapping)
            raw_status, raw_refused, _ = _direct_request(
                admin_token,
                "POST",
                f"/api/1/organizations/{raw_org_id}/membership/{raw_pending['id']}/refuse/",
                body={"comment": "raw refused"},
            )
            typed_refused = await invoke(
                admin_client,
                "refuse_membership",
                typed_org_id,
                typed_pending.value["id"],
                OrganizationRefusalInput("typed refused"),
                admin_permissions,
                policy(
                    "udata/api-v1.refuse-membership",
                    f"{typed_org_id}/{typed_pending.value['id']}",
                ),
            )
            assert raw_refused == {}
            check(raw_status, typed_refused, "udata/api-v1.refuse-membership", {200})

            seeded_status, seeded_organization, _ = _direct_request(
                admin_token, "GET", "/api/1/organizations/evidence-organization/"
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
                admin_token,
                "POST",
                f"/api/1/organizations/{raw_org_id}/member/",
                body={"user": invited_user_id, "role": "editor"},
            )
            typed_invitation = await invoke(
                admin_client,
                "invite_organization_member",
                typed_org_id,
                OrganizationInvitationInput(user=invited_user_id, role="editor"),
                admin_permissions,
                policy("udata/api-v1.invite-organization-member", typed_org_id),
            )
            check(direct_status, typed_invitation, "udata/api-v1.invite-organization-member", {201})
            assert isinstance(raw_invitation, Mapping)
            assert isinstance(typed_invitation.value, Mapping)
            assert raw_invitation["user"]["id"] == invited_user_id
            assert typed_invitation.value["user"]["id"] == invited_user_id
            raw_status, raw_cancel, _ = _direct_request(
                admin_token,
                "POST",
                f"/api/1/organizations/{raw_org_id}/membership/{raw_invitation['id']}/cancel/",
            )
            typed_cancel = await invoke(
                admin_client,
                "cancel_membership",
                typed_org_id,
                typed_invitation.value["id"],
                admin_permissions,
                policy(
                    "udata/api-v1.cancel-membership",
                    f"{typed_org_id}/{typed_invitation.value['id']}",
                ),
            )
            assert raw_cancel == {}
            check(raw_status, typed_cancel, "udata/api-v1.cancel-membership", {200})

            direct_status, raw_badge, _ = _direct_request(
                admin_token,
                "POST",
                f"/api/1/organizations/{raw_org_id}/badges/",
                body={"kind": "certified"},
            )
            typed_badge = await invoke(
                admin_client,
                "add_organization_badge",
                typed_org_id,
                "certified",
                admin_permissions,
                policy("udata/api-v1.add-organization-badge", f"{typed_org_id}/certified"),
            )
            assert isinstance(raw_badge, Mapping)
            check(direct_status, typed_badge, "udata/api-v1.add-organization-badge", {200, 201})
            raw_status, raw_badge_delete, _ = _direct_request(
                admin_token, "DELETE", f"/api/1/organizations/{raw_org_id}/badges/certified/"
            )
            typed_badge_delete = await invoke(
                admin_client,
                "delete_organization_badge",
                typed_org_id,
                "certified",
                admin_permissions,
                policy(
                    "udata/api-v1.delete-organization-badge",
                    f"{typed_org_id}/certified",
                    destructive=True,
                ),
            )
            check(raw_status, typed_badge_delete, "udata/api-v1.delete-organization-badge", {200, 204})

            for typed_side, org_id in enumerate((raw_org_id, typed_org_id)):
                # Each upload uses the same tiny valid PNG bytes with an independent filename.
                png = image
                boundary = f"udata-org-logo-{run_id}-{int(typed_side)}"
                logo_name = f"logo-{run_id}-{int(typed_side)}.png"
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
                    admin_token,
                    "POST",
                    f"/api/1/organizations/{org_id}/logo/",
                    body=raw_logo_body,
                    content_type=f"multipart/form-data; boundary={boundary}",
                )
                typed_logo = await invoke(
                    admin_client,
                    "organization_logo",
                    org_id,
                    ResourceUploadInput(BytesIO(png), logo_name, len(png), "image/png"),
                    admin_permissions,
                    policy("udata/api-v1.organization-logo", org_id),
                )
                check(direct_status, typed_logo, "udata/api-v1.organization-logo", {200})
                resize_status, _, _ = _direct_request(
                    admin_token,
                    "PUT",
                    f"/api/1/organizations/{org_id}/logo/",
                    body=raw_logo_body,
                    content_type=f"multipart/form-data; boundary={boundary}",
                )
                typed_resize = await invoke(
                    admin_client,
                    "resize_organization_logo",
                    org_id,
                    ResourceUploadInput(BytesIO(png), logo_name, len(png), "image/png"),
                    admin_permissions,
                    policy("udata/api-v1.resize-organization-logo", org_id),
                )
                check(resize_status, typed_resize, "udata/api-v1.resize-organization-logo", {200})

            for org_id in (raw_org_id, typed_org_id):
                extras_path = f"/api/2/organizations/{org_id}/extras/"
                direct_status, direct_extras, _ = _direct_request(
                    admin_token, "PUT", extras_path, body={"matrix": run_id}
                )
                typed_extras = await invoke(
                    admin_client,
                    "update_organization_extras",
                    org_id,
                    {"matrix": run_id},
                    admin_permissions,
                    policy("udata/api-v2.update-organization-extras", org_id),
                )
                assert direct_extras == {"matrix": run_id}
                check(direct_status, typed_extras, "udata/api-v2.update-organization-extras", {200})
                direct_status, direct_extras, _ = _direct_request(admin_token, "GET", extras_path)
                typed_extras_value = await invoke(admin_client, "get_organization_extras", org_id)
                assert direct_extras == typed_extras_value == {"matrix": run_id}
                assert direct_status == 200
                assert typed_extras_value == {"matrix": run_id}
                direct_status, direct_deleted, _ = _direct_request(admin_token, "DELETE", extras_path, body=["matrix"])
                typed_deleted = await invoke(
                    admin_client,
                    "delete_organization_extras",
                    org_id,
                    ("matrix",),
                    admin_permissions,
                    policy("udata/api-v2.delete-organization-extras", org_id, destructive=True),
                )
                assert direct_deleted is None or isinstance(direct_deleted, Mapping)
                check(direct_status, typed_deleted, "udata/api-v2.delete-organization-extras", {200, 204})

            raw_follow_status, _, _ = _direct_request(
                admin_token, "GET", f"/api/1/organizations/{raw_org_id}/followers/"
            )
            typed_followers = await invoke(admin_client, "list_organization_followers", typed_org_id)
            assert raw_follow_status == 200
            assert isinstance(typed_followers, tuple)
            raw_follow_status, raw_follow, _ = _direct_request(
                admin_token, "POST", f"/api/1/organizations/{raw_org_id}/followers/"
            )
            typed_follow = await invoke(
                admin_client,
                "follow_organization",
                typed_org_id,
                admin_permissions,
                policy("udata/api-v1.follow-organization", typed_org_id),
            )
            assert isinstance(raw_follow, Mapping)
            check(raw_follow_status, typed_follow, "udata/api-v1.follow-organization", {200, 201})
            raw_unfollow_status, raw_unfollow, _ = _direct_request(
                admin_token, "DELETE", f"/api/1/organizations/{raw_org_id}/followers/"
            )
            typed_unfollow = await invoke(
                admin_client,
                "unfollow_organization",
                typed_org_id,
                admin_permissions,
                policy("udata/api-v1.unfollow-organization", typed_org_id, destructive=True),
            )
            assert isinstance(raw_unfollow, Mapping)
            check(raw_unfollow_status, typed_unfollow, "udata/api-v1.unfollow-organization", {200})
        finally:
            if raw_org_id is not None:
                raw_delete_result = await cleanup(
                    lambda: _assert_direct_delete(
                        _direct_request(admin_token, "DELETE", f"/api/1/organizations/{raw_org_id}/")
                    )
                )
                if isinstance(raw_delete_result, int):
                    raw_delete_status = raw_delete_result
                await cleanup(
                    lambda: _assert_dataset_absent(
                        _direct_request(admin_token, "GET", f"/api/1/organizations/{raw_org_id}/")
                    )
                )
            if typed_org_id is not None:
                await cleanup(
                    lambda: invoke(
                        admin_client,
                        "delete_organization",
                        typed_org_id,
                        admin_permissions,
                        policy("udata/api-v1.delete-organization", typed_org_id, destructive=True),
                    ),
                    expected_status=raw_delete_status if raw_delete_status is not None else 204,
                    operation="udata/api-v1.delete-organization",
                )
                await cleanup(
                    lambda: _assert_dataset_absent(
                        _direct_request(admin_token, "GET", f"/api/1/organizations/{typed_org_id}/")
                    )
                )
            assert not cleanup_errors, f"{len(cleanup_errors)} organization route-matrix cleanup operations failed"

    def run_sync_matrix() -> None:
        with (
            create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=admin_credential)) as admin_client,
            create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=member_credential)) as member_client,
            create_sync_client(
                UDataClientSettings(base_url=ORIGIN, credential=org_admin_credential)
            ) as org_admin_client,
        ):
            asyncio.run(exercise(admin_client, member_client, org_admin_client, "sync"))

    async def run_async_matrix() -> None:
        async with (
            create_async_client(UDataClientSettings(base_url=ORIGIN, credential=admin_credential)) as admin_client,
            create_async_client(UDataClientSettings(base_url=ORIGIN, credential=member_credential)) as member_client,
            create_async_client(
                UDataClientSettings(base_url=ORIGIN, credential=org_admin_credential)
            ) as org_admin_client,
        ):
            await exercise(admin_client, member_client, org_admin_client, "async")

    run_sync_matrix()
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


def test_controlled_resource_routes_match_bounded_raw_differential() -> None:
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

    def policy(target: str, operation: str, *, destructive: bool = False) -> MutationPolicy:
        return MutationPolicy(
            destructive=destructive,
            confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
            concurrency=ConcurrencyPolicy(overwrite=True),
        )

    dataset_id: str | None = None
    direct_resource_id: str | None = None
    typed_resource_id: str | None = None
    direct_upload_id: str | None = None
    typed_upload_id: str | None = None
    direct_community_upload_id: str | None = None
    typed_community_upload_id: str | None = None
    direct_community_id: str | None = None
    typed_community_id: str | None = None
    cleanup_errors: list[Exception] = []

    def cleanup(action: Callable[[], object]) -> None:
        try:
            action()
        except Exception as error:
            cleanup_errors.append(error)

    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        try:
            created_dataset = client.datasets.create(
                DatasetCreateInput(title="Raw differential evidence", description="d"),
                permissions,
                policy("Raw differential evidence", "udata/api-v1.create-dataset"),
            )
            assert created_dataset.record is not None
            dataset_id = created_dataset.record.id.value

            status, raw, _ = _direct_request(
                token,
                "POST",
                f"/api/1/datasets/{dataset_id}/resources/",
                body=ResourceCreateInput(title="Raw resource", url="https://example.com/raw.csv").payload(),
            )
            assert status == 201
            assert isinstance(raw, Mapping)
            assert isinstance(raw.get("id"), str)
            direct_resource_id = raw["id"]
            typed_created = client.resources.create(
                dataset_id,
                ResourceCreateInput(title="Typed resource", url="https://example.com/typed.csv"),
                permissions,
                policy(dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-create"),
            )
            assert typed_created.record is not None
            typed_resource_id = typed_created.record.id.value

            raw_status, raw_get, _ = _direct_request(
                token, "GET", f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/"
            )
            typed_get = client.resources.get(dataset_id, direct_resource_id)
            assert raw_status == 200
            assert isinstance(raw_get, Mapping)
            assert raw_get["id"] == typed_get.id.value

            update_body = ResourceUpdateInput({"title": "Raw updated"}).payload()
            raw_status, raw_update, _ = _direct_request(
                token, "PUT", f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/", body=update_body
            )
            typed_update = client.resources.update(
                dataset_id,
                direct_resource_id,
                ResourceUpdateInput({"title": "Typed updated"}),
                permissions,
                policy(direct_resource_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-update"),
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
                token, "PUT", f"/api/1/datasets/{dataset_id}/resources/", body=reorder_body
            )
            typed_reorder = client.resources.reorder(
                dataset_id,
                (ResourceUpdateInput(reorder_body[0]), ResourceUpdateInput(reorder_body[1])),
                permissions,
                policy(dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-reorder"),
            )
            assert raw_status == 200
            assert isinstance(raw_reorder, list)
            assert typed_reorder.records
            assert raw_reorder[0]["id"] == typed_reorder.records[0].id.value

            extras_path = f"/api/2/datasets/{dataset_id}/resources/{direct_resource_id}/extras/"
            raw_status, raw_extras, _ = _direct_request(
                token, "PUT", extras_path, body={"raw": "value", "typed": "value"}
            )
            typed_extras = client.resources.update_extras_v2(
                dataset_id,
                direct_resource_id,
                {"raw": "value", "typed": "value"},
                permissions,
                policy(
                    direct_resource_id,
                    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-extras-update",
                ),
            )
            assert raw_status == 200
            assert raw_extras == {"raw": "value", "typed": "value"}
            assert typed_extras.extras == {"raw": "value", "typed": "value"}
            raw_status, raw_extras, _ = _direct_request(token, "GET", extras_path)
            typed_extras_read = client.resources.get_extras_v2(dataset_id, direct_resource_id)
            assert raw_status == 200
            assert raw_extras == {"raw": "value", "typed": "value"}
            assert typed_extras_read == {"raw": "value", "typed": "value"}
            raw_status, raw_deleted_extras, _ = _direct_request(token, "DELETE", extras_path, body=["raw"])
            typed_deleted_extras = client.resources.delete_extras_v2(
                dataset_id,
                direct_resource_id,
                ("typed",),
                permissions,
                policy(
                    direct_resource_id,
                    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-extras-delete",
                    destructive=True,
                ),
            )
            assert raw_status == 204
            assert raw_deleted_extras is None
            assert typed_deleted_extras.receipt.outcome == "succeeded"

            raw_status, raw_dataset, _ = _direct_request(token, "GET", f"/api/2/datasets/{dataset_id}/")
            typed_dataset = client.resources.get_dataset_v2(dataset_id)
            assert raw_status == 200
            assert isinstance(raw_dataset, Mapping)
            assert raw_dataset["id"] == typed_dataset.id.value
            raw_status, raw_resource_page, _ = _direct_request(token, "GET", f"/api/2/datasets/{dataset_id}/resources/")
            typed_resource_page = client.resources.list_v2(dataset_id)
            assert raw_status == 200
            assert isinstance(raw_resource_page, Mapping)
            assert typed_resource_page.items
            raw_status, raw_resource, _ = _direct_request(
                token, "GET", f"/api/2/datasets/resources/{direct_resource_id}/"
            )
            typed_resource = client.resources.get_v2(direct_resource_id)
            assert raw_status == 200
            assert isinstance(raw_resource, Mapping)
            assert typed_resource.id.value == direct_resource_id
            raw_status, raw_types, _ = _direct_request(token, "GET", "/api/1/datasets/resource_types/")
            typed_types = client.resources.resource_types()
            assert raw_status == 200
            assert isinstance(raw_types, list)
            assert len(raw_types) == len(typed_types)
            raw_status, raw_redirect, raw_headers = _direct_request(
                token, "GET", f"/api/1/datasets/r/{direct_resource_id}"
            )
            assert raw_status == 302
            assert raw_redirect is None
            assert raw_headers.get("location") == client.resources.redirect(direct_resource_id)

            replacement_body, replacement_type = _multipart_body(b"raw", "raw.csv")
            raw_status, raw_replaced, _ = _direct_request(
                token,
                "POST",
                f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/upload/",
                body=replacement_body,
                content_type=replacement_type,
            )
            typed_replaced = client.resources.upload(
                dataset_id,
                ResourceUploadInput(BytesIO(b"typed"), "typed.csv", 5),
                permissions,
                policy(
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
                token, "DELETE", f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/"
            )
            assert raw_status == 204
            assert raw_deleted is None
            direct_resource_id = None

            direct_upload_body, direct_upload_type = _multipart_body(b"raw", "raw-new.csv")
            raw_status, raw_upload, _ = _direct_request(
                token,
                "POST",
                f"/api/1/datasets/{dataset_id}/upload/",
                body=direct_upload_body,
                content_type=direct_upload_type,
            )
            direct_upload_id = raw_upload["id"] if raw_status == 201 and isinstance(raw_upload, Mapping) else None
            typed_upload = client.resources.upload(
                dataset_id,
                ResourceUploadInput(BytesIO(b"typed"), "typed-new.csv", 5),
                permissions,
                policy(dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-new"),
            )
            assert raw_status == 201
            assert typed_upload.record is not None
            assert direct_upload_id is not None
            typed_upload_id = typed_upload.record.id.value

            community_body, community_type = _multipart_body(b"raw", "raw-community.csv")
            raw_status, raw_community_upload, _ = _direct_request(
                token,
                "POST",
                f"/api/1/datasets/{dataset_id}/upload/community/",
                body=community_body,
                content_type=community_type,
            )
            direct_community_upload_id = (
                raw_community_upload["id"] if raw_status == 201 and isinstance(raw_community_upload, Mapping) else None
            )
            typed_community_upload = client.resources.upload_community(
                dataset_id,
                ResourceUploadInput(BytesIO(b"typed"), "typed-community.csv", 5),
                permissions,
                policy(
                    dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-community-new"
                ),
            )
            assert raw_status == 201
            assert typed_community_upload.record is not None
            assert direct_community_upload_id is not None
            typed_community_upload_id = typed_community_upload.record.id.value

            community_create_body = ResourceCreateInput(
                title="Raw community", url="https://example.com/raw-community.csv"
            ).payload() | {"dataset": dataset_id}
            raw_status, raw_community, _ = _direct_request(
                token, "POST", "/api/1/datasets/community_resources/", body=community_create_body
            )
            direct_community_id = (
                raw_community["id"] if raw_status == 201 and isinstance(raw_community, Mapping) else None
            )
            typed_community = client.resources.create_community(
                dataset_id,
                ResourceCreateInput(title="Typed community", url="https://example.com/typed-community.csv"),
                permissions,
                policy(
                    dataset_id, "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-create"
                ),
            )
            assert raw_status == 201
            assert typed_community.record is not None
            assert direct_community_id is not None
            typed_community_id = typed_community.record.id.value

            raw_status, raw_community_get, _ = _direct_request(
                token, "GET", f"/api/1/datasets/community_resources/{direct_community_id}/"
            )
            typed_community_get = client.resources.get_community(direct_community_id)
            assert raw_status == 200
            assert isinstance(raw_community_get, Mapping)
            assert raw_community_get["id"] == typed_community_get.id.value
            assert typed_community_get.id.value == direct_community_id
            raw_status, raw_community_list, _ = _direct_request(
                token, "GET", f"/api/1/datasets/community_resources/?dataset={dataset_id}"
            )
            typed_community_list = client.resources.list_community({"dataset": dataset_id})
            assert raw_status == 200
            assert isinstance(raw_community_list, Mapping)
            assert typed_community_list.items

            community_update_body = ResourceUpdateInput({"title": "Raw community updated"}).payload()
            raw_status, raw_community_update, _ = _direct_request(
                token,
                "PUT",
                f"/api/1/datasets/community_resources/{direct_community_id}/",
                body=community_update_body,
            )
            typed_community_update = client.resources.update_community(
                direct_community_id,
                ResourceUpdateInput({"title": "Typed community updated"}),
                permissions,
                policy(
                    direct_community_id,
                    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-update",
                ),
            )
            assert raw_status == 200
            assert isinstance(raw_community_update, Mapping)
            assert typed_community_update.record is not None

            community_replace_body, community_replace_type = _multipart_body(b"raw", "raw-community-replace.csv")
            raw_status, raw_community_replace, _ = _direct_request(
                token,
                "POST",
                f"/api/1/datasets/community_resources/{direct_community_id}/upload/",
                body=community_replace_body,
                content_type=community_replace_type,
            )
            typed_community_replace = client.resources.reupload_community(
                direct_community_id,
                ResourceUploadInput(BytesIO(b"typed"), "typed-community-replace.csv", 5),
                permissions,
                policy(
                    direct_community_id,
                    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-community-replace",
                    destructive=True,
                ),
            )
            assert raw_status == 200
            assert isinstance(raw_community_replace, Mapping)
            assert typed_community_replace.record is not None

            raw_status, raw_community_deleted, _ = _direct_request(
                token, "DELETE", f"/api/1/datasets/community_resources/{direct_community_id}/"
            )
            assert raw_status == 204
            assert raw_community_deleted is None
            direct_community_id = None
            typed_deleted_community = client.resources.delete_community(
                typed_community_id,
                permissions,
                policy(
                    typed_community_id,
                    "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-delete",
                    destructive=True,
                ),
            )
            assert typed_deleted_community.receipt.outcome == "succeeded"
        finally:
            if direct_upload_id is not None and dataset_id is not None:
                cleanup(
                    lambda: _assert_direct_delete(
                        _direct_request(token, "DELETE", f"/api/1/datasets/{dataset_id}/resources/{direct_upload_id}/")
                    )
                )
            if direct_community_upload_id is not None:
                cleanup(
                    lambda: _assert_direct_delete(
                        _direct_request(
                            token,
                            "DELETE",
                            f"/api/1/datasets/community_resources/{direct_community_upload_id}/",
                        )
                    )
                )
            if typed_upload_id is not None and dataset_id is not None:
                cleanup(
                    lambda: _assert_typed_delete(
                        client.resources.delete(
                            dataset_id,
                            typed_upload_id,
                            permissions,
                            policy(
                                typed_upload_id,
                                "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-delete",
                                destructive=True,
                            ),
                        )
                    ),
                )
            if typed_community_upload_id is not None:
                cleanup(
                    lambda: _assert_typed_delete(
                        client.resources.delete_community(
                            typed_community_upload_id,
                            permissions,
                            policy(
                                typed_community_upload_id,
                                "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-community-delete",
                                destructive=True,
                            ),
                        )
                    ),
                )
            if direct_resource_id is not None and dataset_id is not None:
                cleanup(
                    lambda: _assert_direct_delete(
                        _direct_request(
                            token, "DELETE", f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/"
                        )
                    )
                )
            if typed_resource_id is not None and dataset_id is not None:
                cleanup(
                    lambda: _assert_typed_delete(
                        client.resources.delete(
                            dataset_id,
                            typed_resource_id,
                            permissions,
                            policy(
                                typed_resource_id,
                                "udata/api-v1.dataset-resource-create-update-reorder-upload-delete-delete",
                                destructive=True,
                            ),
                        )
                    ),
                )
            if dataset_id is not None:
                cleanup(
                    lambda: _assert_typed_delete(
                        client.datasets.delete(
                            dataset_id,
                            permissions,
                            DatasetDeleteOptions(),
                            policy(dataset_id, "udata/api-v1.delete-dataset", destructive=True),
                        )
                    )
                )
                cleanup(lambda: _assert_dataset_absent(_direct_request(token, "GET", f"/api/1/datasets/{dataset_id}/")))
            assert not cleanup_errors, f"{len(cleanup_errors)} controlled cleanup operations failed"


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
