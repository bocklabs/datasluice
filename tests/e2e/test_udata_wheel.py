"""Installed-artifact proof for the strict uData tracer slice."""

from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path


def _unpacked_wheel_source(built_wheel: Path, tmp_path: Path) -> Path:
    unpacked = tmp_path / "wheel"
    with zipfile.ZipFile(built_wheel) as archive:
        archive.extractall(unpacked)
    return unpacked


def test_wheel_ships_udata_176_contract_files_and_no_legacy_profile(built_wheel: Path, tmp_path: Path) -> None:
    unpacked = _unpacked_wheel_source(built_wheel, tmp_path)
    package = unpacked / "datasluice" / "connectors" / "catalog" / "udata"
    profiles = unpacked / "datasluice" / "contracts" / "catalog" / "profiles"
    fixtures = unpacked / "datasluice" / "contracts" / "catalog" / "fixtures" / "udata"

    for module in ("settings.py", "clients.py", "probes.py", "mapping.py", "live.py", "factory.py", "connector.py"):
        assert (package / module).is_file(), module
    for module in (
        "models/root_profile.py",
        "wire/root_profile.py",
        "services/root_profile.py",
        "models/resources.py",
        "wire/resources.py",
        "services/resources.py",
        "models/organizations.py",
        "wire/organizations.py",
        "services/organizations_memberships.py",
        "models/users.py",
        "secrets.py",
        "wire/users.py",
        "services/users_tokens.py",
        "models/oauth.py",
        "wire/oauth.py",
        "services/auth_oauth.py",
    ):
        assert (package / module).is_file(), module
    assert (profiles / "udata-17.6.json").is_file()
    assert (fixtures / "root_profile.json").is_file()
    assert (fixtures / "cases.json").is_file()
    assert not (profiles / "udata-17.3.json").exists()

    profile = json.loads((profiles / "udata-17.6.json").read_text(encoding="utf-8"))
    assert profile["profile_version"] == "17.6.0"
    assert profile["platform"] == "udata"


def test_wheel_import_proves_the_tracer_path_from_installed_content(built_wheel: Path, tmp_path: Path) -> None:
    """A fresh interpreter importing only the unpacked wheel runs the full tracer."""
    unpacked = _unpacked_wheel_source(built_wheel, tmp_path)
    script = """
import json, secrets, sys
from io import BytesIO
sys.path.insert(0, sys.argv[1])
import datasluice
assert datasluice.__file__ and datasluice.__file__.startswith(sys.argv[1])
from datasluice.connectors.catalog.udata.clients import create_async_client, create_sync_client, declared_udata_profile
from datasluice.connectors.catalog.udata.models.datasets import DatasetCreateInput
from datasluice.connectors.catalog.udata.models.organizations import OrganizationCreateInput, OrganizationUpdateInput
from datasluice.connectors.catalog.udata.models.resources import ResourceCreateInput, ResourceUploadInput
from datasluice.connectors.catalog.udata.models.oauth import OAuthRevokeRequest, OAuthTokenRequest
from datasluice.connectors.catalog.udata.models.users import ApiTokenCreateInput
from datasluice.connectors.catalog.udata.probes import UDataVersionError
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError
from datasluice.runtime.transport.base import AsyncRuntimeStreamResponse, RuntimeResponse, RuntimeStreamResponse

op_id = next(
    op
    for op in declared_udata_profile().operations
    if op.method == "dataset-list-search-show-create-update-delete"
)

class Transport:
    def __init__(self, version="17.6.0"):
        self.requests = []
        self.close_count = 0
        self.version = version
        self.token_value = secrets.token_urlsafe(36)

    def send(self, request):
        url = request.url
        self.requests.append(url)
        if url.endswith("/api/1/site/"):
            body = json.dumps({"feed_size": 0, "id": "s", "keywords": [], "metrics": {},
                               "title": "uData", "version": self.version}).encode()
            headers = {"Content-Type": "application/json"}
        elif url.endswith("/api/1/me/"):
            body = json.dumps({"id": "wheel-user", "first_name": "Wheel", "last_name": "User"}).encode()
            headers = {"Content-Type": "application/json"}
        elif url.endswith("/api/1/me/api_tokens/") and request.method == "GET":
            body = json.dumps([{"id": "wheel-token-id", "token_prefix": "wheel-prefix"}]).encode()
            headers = {"Content-Type": "application/json"}
        elif url.endswith("/api/1/me/api_tokens/") and request.method == "POST":
            body = json.dumps(
                {"id": "wheel-token-id", "token_prefix": "wheel-prefix", "token": self.token_value}
            ).encode()
            headers = {"Content-Type": "application/json"}
        elif url.endswith("/api/1/me/api_tokens/wheel-token-id/") and request.method == "DELETE":
            body = b""
            headers = {}
        elif "/oauth/token" in url and request.method == "POST":
            body = json.dumps({"access_token": "wheel-access", "token_type": "Bearer", "expires_in": 60}).encode()
            headers = {"Content-Type": "application/json"}
        elif "/oauth/revoke" in url and request.method == "POST":
            body = b""
            headers = {}
        elif "/oauth/client_info" in url or "/oauth/authorize" in url:
            body = b"\\n"
            headers = {"Content-Type": "text/html; charset=utf-8"}
        elif "/oauth/error" in url:
            body = b"\\n"
            headers = {"Content-Type": "text/html; charset=utf-8"}
        elif url.endswith(".csv"):
            body = b"id\\nwheel\\n"
            headers = {"Content-Type": "text/csv"}
        elif "/api/1/organizations/abc/" in url:
            body = json.dumps({"id": "abc", "name": "Wheel organization", "description": "d"}).encode()
            headers = {"Content-Type": "application/json"}
        elif request.method == "POST":
            body = json.dumps(
                {
                    "id": "wheel-created",
                    "name": "Wheel organization",
                    "title": "Wheel dataset",
                    "slug": "wheel-dataset",
                    "description": "d",
                    "private": False,
                }
            ).encode()
            headers = {"Content-Type": "application/json"}
        else:
            body = json.dumps({"data": [{"id": "abc", "title": "T"}], "next_page": None, "page": 1,
                               "page_size": 20, "previous_page": None, "total": 1}).encode()
            headers = {}
        status = 204 if request.method == "DELETE" else 201 if request.method == "POST" else 200
        return RuntimeResponse(status_code=status, headers=headers, body=body)

    def send_stream(self, request):
        response = self.send(request)
        return RuntimeStreamResponse(response.status_code, response.headers, iter((response.body,)), lambda: None)

    def close(self):
        self.close_count += 1


class AsyncTransport(Transport):
    async def send(self, request):
        return Transport.send(self, request)

    async def aclose(self):
        self.close_count += 1

    async def send_stream(self, request):
        response = await self.send(request)

        async def chunks():
            yield response.body

        return AsyncRuntimeStreamResponse(response.status_code, response.headers, chunks(), lambda: None)

transport = Transport()
sync_credential = UDataCredential(api_key="sync-wheel-key")
client = create_sync_client(
    UDataClientSettings(base_url="http://127.0.0.1:5640", sync_transport=transport, credential=sync_credential)
)
assert client.site_version().version == "17.6.0"
root_profile = client.root_profile.get()
assert root_profile.id == "s"
root_export = client.root_profile.datasets_csv()
assert root_export.size_bytes == len(b"id\\nwheel\\n")
envelope = client.datasets_list(
    CatalogOperationRequest(operation_id=op_id, payload={}),
    CatalogOperationGuard(operation_id=op_id),
)
service_page = client.datasets.list()
assert service_page.items[0].id.value == "abc"
organization = client.organizations_memberships.get_organization("abc")
try:
    client.organizations_memberships.get_organization("")
except CatalogValidationError:
    pass
else:
    raise AssertionError("invalid organization id was dispatched")
assert organization.id.value == "abc"
sync_permissions = EffectivePermissions.for_credential(sync_credential, platform=CatalogPlatform.UDATA)
sync_resource = client.resources.create(
    "abc",
    ResourceCreateInput(title="Sync wheel resource", url="https://example.test/sync.csv"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-create",
            target="abc",
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
sync_upload = client.resources.upload(
    "abc",
    ResourceUploadInput(BytesIO(b"abc"), "sync.csv", 3),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-new",
            target="abc",
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert sync_resource.record is not None and sync_upload.record is not None
organization_created = client.organizations_memberships.create_organization(
    OrganizationCreateInput(name="Wheel organization", description="d"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.create-organization", target="Wheel organization"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert organization_created.record is not None
assert organization_created.receipt.operation == "udata/api-v1.create-organization"
assert organization_created.receipt.outcome == "succeeded"
assert organization_created.receipt.audit_metadata["status_code"] == 201
assert client.users_tokens.get_me(sync_permissions).id.value == "wheel-user"
assert client.users_tokens.list_api_tokens(sync_permissions)[0].token_prefix == "wheel-prefix"
created_token = client.users_tokens.create_api_token(
    ApiTokenCreateInput(name="wheel"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.create-api-token", target="new-api-token"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
oauth_token = client.auth_oauth.access_token(
    OAuthTokenRequest(grant_type="client_credentials", client_id="wheel-client", client_secret="wheel-secret"),
    sync_permissions,
)
assert oauth_token.receipt.outcome == "succeeded"
assert oauth_token.token_type == "Bearer"
assert "wheel-access" not in json.dumps(oauth_token.to_dict())
oauth_revoked = client.auth_oauth.revoke_token(
    OAuthRevokeRequest(token="wheel-access"),
    sync_permissions,
    MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/oauth.revoke-token", target="request:revoke_token"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert oauth_revoked.receipt.operation == "udata/oauth.revoke-token"
assert client.auth_oauth.oauth_error().session_gated is True
assert client.auth_oauth.authorize(sync_permissions).session_gated is True
assert created_token.receipt.audit_metadata["status_code"] == 201
assert "token" not in created_token.to_dict()
if not created_token.secret.reveal_once():
    raise AssertionError("installed-wheel token reveal failed")
revoked_token = client.users_tokens.revoke_api_token(
    created_token.metadata.id,
    sync_permissions,
    MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.revoke-api-token", target=created_token.metadata.id
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert revoked_token.receipt.audit_metadata["status_code"] == 204
client.close()

import asyncio
async_transport = AsyncTransport()
async_credential = UDataCredential(api_key="wheel-key")
async_client = create_async_client(
    UDataClientSettings(
        base_url="http://127.0.0.1:5640", async_transport=async_transport, credential=async_credential
    )
)
async def run_async():
    async with async_client as active:
        page = await active.datasets.list()
        root_profile = await active.root_profile.get()
        root_export = await active.root_profile.datasets_csv()
        organization = await active.organizations_memberships.get_organization("abc")
        permissions = EffectivePermissions.for_credential(async_credential, platform=CatalogPlatform.UDATA)
        created = await active.datasets.create(
            DatasetCreateInput(title="Wheel dataset", description="d"),
            permissions=permissions,
            mutation_policy=MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.create-dataset", target="Wheel dataset"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        resource_created = await active.resources.create(
            "abc",
            ResourceCreateInput(title="Wheel resource", url="https://example.test/data.csv"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True,
                    operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-create",
                    target="abc",
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        organization_created = await active.organizations_memberships.create_organization(
            OrganizationCreateInput(name="Async wheel organization", description="d"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.create-organization", target="Async wheel organization"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        user = await active.users_tokens.get_me(permissions)
        token_list = await active.users_tokens.list_api_tokens(permissions)
        token_created = await active.users_tokens.create_api_token(
            ApiTokenCreateInput(name="async wheel"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.create-api-token", target="new-api-token"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        if not token_created.secret.reveal_once():
            raise AssertionError("async installed-wheel token reveal failed")
        token_revoked = await active.users_tokens.revoke_api_token(
            token_created.metadata.id,
            permissions,
            MutationPolicy(
                destructive=True,
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.revoke-api-token", target=token_created.metadata.id
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        assert user.id.value == "wheel-user"
        assert token_list[0].token_prefix == "wheel-prefix"
        assert token_created.receipt.audit_metadata["status_code"] == 201
        assert "token" not in token_created.to_dict()
        assert token_revoked.receipt.audit_metadata["status_code"] == 204
        try:
            await active.organizations_memberships.get_organization("")
        except CatalogValidationError:
            pass
        else:
            raise AssertionError("invalid organization id was dispatched")
        try:
            await active.organizations_memberships.update_organization(
                "invalid/id",
                OrganizationUpdateInput(description="invalid"),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.update-organization", target="invalid/id"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        except CatalogValidationError as error:
            receipt = error.__dict__["mutation_receipt"]
            assert receipt.operation == "udata/api-v1.update-organization"
            assert receipt.outcome == "rejected"
        else:
            raise AssertionError("invalid organization mutation was dispatched")
        return (
            page.items[0].id.value,
            root_profile.id,
            root_export.size_bytes,
            organization,
            created,
            resource_created,
            organization_created,
        )
(
    async_result,
    async_root_id,
    async_export_size,
    organization,
    created,
    resource_created,
    organization_created,
) = asyncio.run(run_async())
assert async_result == "abc"
assert async_root_id == "s"
assert async_export_size == len(b"id\\nwheel\\n")
assert organization.id.value == "abc"
assert created.record.id.value == "wheel-created"
assert created.receipt.outcome == "succeeded"
assert created.receipt.audit_metadata["status_code"] == 201
assert resource_created.record.id.value == "wheel-created"
assert organization_created.record.id.value == "wheel-created"
assert organization_created.receipt.operation == "udata/api-v1.create-organization"
assert organization_created.receipt.outcome == "succeeded"
assert organization_created.receipt.audit_metadata["status_code"] == 201
recorded = [getattr(r, "url", r) for r in transport.requests]
assert recorded == [
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/site/datasets.csv",
    "http://127.0.0.1:5640/api/1/datasets/",
    "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/organizations/abc/",
    "http://127.0.0.1:5640/api/1/datasets/abc/resources/",
    "http://127.0.0.1:5640/api/1/datasets/abc/upload/",
    "http://127.0.0.1:5640/api/1/organizations/",
    "http://127.0.0.1:5640/api/1/me/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/",
    "http://127.0.0.1:5640/oauth/token",
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/oauth/revoke",
    "http://127.0.0.1:5640/oauth/error",
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/oauth/authorize",
    "http://127.0.0.1:5640/api/1/me/api_tokens/wheel-token-id/",
], recorded
assert [getattr(r, "url", r) for r in async_transport.requests] == [
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/site/datasets.csv",
    "http://127.0.0.1:5640/api/1/organizations/abc/",
    "http://127.0.0.1:5640/api/1/datasets/",
    "http://127.0.0.1:5640/api/1/datasets/abc/resources/",
    "http://127.0.0.1:5640/api/1/organizations/",
    "http://127.0.0.1:5640/api/1/me/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/wheel-token-id/",
]
assert transport.close_count == 0
assert envelope.items[0].id.value == "abc"

class MalformedTransport(Transport):
    def send(self, request):
        if request.url.endswith("/api/1/datasets/abc/"):
            self.requests.append(request.url)
            return RuntimeResponse(status_code=200, headers={}, body=b"{")
        return Transport.send(self, request)

malformed = create_sync_client(
    UDataClientSettings(base_url="http://127.0.0.1:5640", sync_transport=MalformedTransport())
)
try:
    malformed.datasets.get("abc")
except NativeCatalogError as error:
    assert error.status_code == 200
else:
    raise AssertionError("malformed installed-wheel response was accepted")
malformed.close()



blocked = create_sync_client(
    UDataClientSettings(base_url="http://127.0.0.1:5640", sync_transport=Transport(version="17.7"))
)
try:
    blocked.datasets_list(
        CatalogOperationRequest(operation_id=op_id, payload={}),
        CatalogOperationGuard(operation_id=op_id),
    )
except UDataVersionError as error:
    assert len(blocked.transport.requests) == 1
else:
    raise AssertionError("version mismatch did not block dispatch")
blocked.close()
print("TRACER_OK")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(unpacked)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=tmp_path,
    )
    assert completed.returncode == 0, completed.stderr
    assert "TRACER_OK" in completed.stdout
