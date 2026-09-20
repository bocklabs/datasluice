"""Controlled-environment tracer proof against the loopback uData 17.6 stack."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
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
from datasluice.connectors.catalog.udata.models.root_profile import SiteMutationResult, SitePatchInput, SiteProfile
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogPlatform

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
