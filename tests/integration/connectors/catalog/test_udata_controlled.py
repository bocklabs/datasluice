"""Controlled-environment tracer proof against the loopback uData 17.6 stack."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable, Mapping
from importlib import resources
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

import pytest

from datasluice.connectors.catalog.udata.clients import (
    _create_controlled_async_client,
    _create_controlled_sync_client,
    create_async_client,
    create_sync_client,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.datasets import DatasetListQuery, DatasetSuggestQuery
from datasluice.connectors.catalog.udata.models.organizations import (
    OrganizationCreateInput,
    OrganizationListQuery,
    OrganizationSuggestQuery,
    OrganizationUpdateInput,
)
from datasluice.connectors.catalog.udata.models.root_profile import SiteMutationResult, SitePatchInput, SiteProfile
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

if os.environ.get("UDATA_EVIDENCE_ORIGIN", "http://127.0.0.1:5640") != "http://127.0.0.1:5640":
    pytest.skip(
        allow_module_level=True,
        reason=(
            "Controlled uData evidence is restricted to the fixed loopback stack "
            "http://127.0.0.1:5640; set UDATA_EVIDENCE_ORIGIN to the stock loopback origin."
        ),
    )

ORIGIN = "http://127.0.0.1:5640"
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
    token: str, method: str, path: str, *, body: object | None = None, content_type: str = "application/json"
) -> tuple[int, object, dict[str, str]]:
    data = None if body is None else body if isinstance(body, bytes) else json.dumps(body).encode()
    headers = {"X-API-KEY": token}
    if data is not None:
        headers["Content-Type"] = content_type
    request = Request(f"{ORIGIN}{path}", data=data, headers=headers, method=method)
    try:
        response = build_opener(_DirectNoRedirect()).open(request, timeout=10)
    except HTTPError as error:
        response = error
    with response:
        payload_bytes = response.read(8193)
        assert len(payload_bytes) <= 8192
        media_type = response.headers.get_content_type()
        payload = json.loads(payload_bytes) if payload_bytes and media_type == "application/json" else None
        status = response.status
        assert type(status) is int
        return status, payload, {key.lower(): value for key, value in response.headers.items()}


def _assert_direct_delete(result: tuple[int, object, dict[str, str]]) -> None:
    status, payload, _ = result
    assert status == 204 and payload is None


def _assert_typed_delete(result: object) -> None:
    receipt = getattr(result, "receipt", None)
    assert receipt is not None and receipt.outcome == "succeeded"
    assert receipt.audit_metadata["status_code"] == 204


def _assert_dataset_absent(result: tuple[int, object, dict[str, str]]) -> None:
    status, payload, _ = result
    assert status in {404, 410} or (
        status == 200 and isinstance(payload, Mapping) and isinstance(payload.get("deleted"), str)
    )


def _multipart_body(content: bytes, file_name: str) -> tuple[bytes, str]:
    boundary = "datasluice-evidence-boundary"
    body = b"\r\n".join(
        (
            f"--{boundary}".encode(),
            f'Content-Disposition: form-data; name="file"; filename="{file_name}"'.encode(),
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
            assert status == 200 and isinstance(payload, Mapping) and payload["description"] == "Task 04-04 updated"
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
            assert status == 200 and isinstance(payload, Mapping) and payload["id"] == typed.id.value
            status, payload, _ = _direct_request(token, "GET", "/api/1/organizations/?page=1&page_size=20")
            page = await client.organizations_memberships.list_organizations(
                OrganizationListQuery(page=1, page_size=20)
            )
            assert status == 200 and isinstance(payload, Mapping) and page.items
            assert {item["id"] for item in payload["data"]} >= {item.id.value for item in page.items}

    asyncio.run(run())


def test_controlled_organization_read_matrix_matches_raw_routes() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled organization evidence requires UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    credential = UDataCredential(api_key=token)
    organization_id = "evidence-organization"
    with create_sync_client(UDataClientSettings(base_url=ORIGIN, credential=credential)) as client:
        reads = (
            (
                f"/api/1/organizations/{organization_id}/datasets.csv",
                lambda: client.organizations_memberships.organization_datasets_csv(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/dataservices.csv",
                lambda: client.organizations_memberships.organization_dataservices_csv(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/discussions.csv",
                lambda: client.organizations_memberships.organization_discussions_csv(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/datasets-resources.csv",
                lambda: client.organizations_memberships.organization_datasets_resources_csv(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/catalog",
                lambda: client.organizations_memberships.rdf_organization(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/catalog.ttl",
                lambda: client.organizations_memberships.rdf_organization_format(organization_id, "ttl"),
            ),
            ("/api/1/organizations/badges/", client.organizations_memberships.available_organization_badges),
            (
                f"/api/1/organizations/{organization_id}/contacts/?page=1&page_size=20",
                lambda: client.organizations_memberships.get_organization_contact_point(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/contacts/suggest/?q=ev&size=10",
                lambda: client.organizations_memberships.suggest_org_contact_points(
                    organization_id, OrganizationSuggestQuery("ev")
                ),
            ),
            (
                f"/api/1/organizations/{organization_id}/membership/",
                lambda: client.organizations_memberships.list_membership_requests(
                    organization_id,
                    EffectivePermissions.for_credential(
                        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
                    ),
                ),
            ),
            (
                f"/api/1/organizations/{organization_id}/assignments/",
                lambda: client.organizations_memberships.list_organization_assignments(
                    organization_id,
                    EffectivePermissions.for_credential(
                        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
                    ),
                ),
            ),
            (
                "/api/1/organizations/suggest/?q=ev&size=10",
                lambda: client.organizations_memberships.suggest_organizations(OrganizationSuggestQuery("ev")),
            ),
            (
                f"/api/1/organizations/{organization_id}/datasets/?page=1&page_size=20",
                lambda: client.organizations_memberships.list_organization_datasets(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/reuses/",
                lambda: client.organizations_memberships.list_organization_reuses(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/discussions/",
                lambda: client.organizations_memberships.list_organization_discussions(organization_id),
            ),
            ("/api/1/organizations/roles/", client.organizations_memberships.org_roles),
            ("/api/2/organizations/search/?page=1&page_size=20", client.organizations_memberships.search_organizations),
            (
                f"/api/2/organizations/{organization_id}/extras/",
                lambda: client.organizations_memberships.get_organization_extras(organization_id),
            ),
            (
                f"/api/1/organizations/{organization_id}/followers/",
                lambda: client.organizations_memberships.list_organization_followers(organization_id),
            ),
        )
        for path, typed_call in reads:
            status, _, _ = _direct_request(token, "GET", path)
            assert status in {200, 302}
            typed_call()


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
            assert status == 201 and isinstance(raw, Mapping) and isinstance(raw.get("id"), str)
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
            assert raw_status == 200 and isinstance(raw_get, Mapping) and raw_get["id"] == typed_get.id.value

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
            assert raw_status == 200 and isinstance(raw_update, Mapping) and typed_update.record is not None
            assert raw_update["id"] == typed_update.record.id.value == direct_resource_id

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
            assert raw_status == 200 and isinstance(raw_reorder, list) and typed_reorder.records
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
            assert raw_status == 200 and raw_extras == typed_extras.extras == {"raw": "value", "typed": "value"}
            raw_status, raw_extras, _ = _direct_request(token, "GET", extras_path)
            typed_extras_read = client.resources.get_extras_v2(dataset_id, direct_resource_id)
            assert raw_status == 200 and raw_extras == typed_extras_read == {"raw": "value", "typed": "value"}
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
            assert (
                raw_status == 204 and raw_deleted_extras is None and typed_deleted_extras.receipt.outcome == "succeeded"
            )

            raw_status, raw_dataset, _ = _direct_request(token, "GET", f"/api/2/datasets/{dataset_id}/")
            typed_dataset = client.resources.get_dataset_v2(dataset_id)
            assert (
                raw_status == 200 and isinstance(raw_dataset, Mapping) and raw_dataset["id"] == typed_dataset.id.value
            )
            raw_status, raw_resource_page, _ = _direct_request(token, "GET", f"/api/2/datasets/{dataset_id}/resources/")
            typed_resource_page = client.resources.list_v2(dataset_id)
            assert raw_status == 200 and isinstance(raw_resource_page, Mapping) and typed_resource_page.items
            raw_status, raw_resource, _ = _direct_request(
                token, "GET", f"/api/2/datasets/resources/{direct_resource_id}/"
            )
            typed_resource = client.resources.get_v2(direct_resource_id)
            assert (
                raw_status == 200
                and isinstance(raw_resource, Mapping)
                and typed_resource.id.value == direct_resource_id
            )
            raw_status, raw_types, _ = _direct_request(token, "GET", "/api/1/datasets/resource_types/")
            typed_types = client.resources.resource_types()
            assert raw_status == 200 and isinstance(raw_types, list) and len(raw_types) == len(typed_types)
            raw_status, raw_redirect, raw_headers = _direct_request(
                token, "GET", f"/api/1/datasets/r/{direct_resource_id}"
            )
            assert (
                raw_status == 302
                and raw_redirect is None
                and raw_headers.get("location") == client.resources.redirect(direct_resource_id)
            )

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
            assert raw_status == 200 and isinstance(raw_replaced, Mapping) and typed_replaced.record is not None
            assert raw_replaced["id"] == typed_replaced.record.id.value == direct_resource_id

            raw_status, raw_deleted, _ = _direct_request(
                token, "DELETE", f"/api/1/datasets/{dataset_id}/resources/{direct_resource_id}/"
            )
            assert raw_status == 204 and raw_deleted is None
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
            assert raw_status == 201 and typed_upload.record is not None and direct_upload_id is not None
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
            assert (
                raw_status == 201
                and typed_community_upload.record is not None
                and direct_community_upload_id is not None
            )
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
            assert raw_status == 201 and typed_community.record is not None and direct_community_id is not None
            typed_community_id = typed_community.record.id.value

            raw_status, raw_community_get, _ = _direct_request(
                token, "GET", f"/api/1/datasets/community_resources/{direct_community_id}/"
            )
            typed_community_get = client.resources.get_community(direct_community_id)
            assert raw_status == 200 and isinstance(raw_community_get, Mapping)
            assert raw_community_get["id"] == typed_community_get.id.value == direct_community_id
            raw_status, raw_community_list, _ = _direct_request(
                token, "GET", f"/api/1/datasets/community_resources/?dataset={dataset_id}"
            )
            typed_community_list = client.resources.list_community({"dataset": dataset_id})
            assert raw_status == 200 and isinstance(raw_community_list, Mapping) and typed_community_list.items

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
            assert (
                raw_status == 200
                and isinstance(raw_community_update, Mapping)
                and typed_community_update.record is not None
            )

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
            assert raw_status == 200 and isinstance(raw_community_replace, Mapping)
            assert typed_community_replace.record is not None

            raw_status, raw_community_deleted, _ = _direct_request(
                token, "DELETE", f"/api/1/datasets/community_resources/{direct_community_id}/"
            )
            assert raw_status == 204 and raw_community_deleted is None
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
