"""Controlled-environment tracer proof against the loopback uData 17.6 stack."""

from __future__ import annotations

import os
import subprocess
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse

import pytest

from datasluice.connectors.catalog.udata.clients import (
    create_async_client,
    create_sync_client,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.datasets import DatasetListQuery, DatasetSuggestQuery
from datasluice.connectors.catalog.udata.models.root_profile import ControlledStackAttestation, SitePatchInput
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.connectors.catalog.udata.wire.root_profile import ROOT_OPERATIONS
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogPlatform

_ORIGINAL_ORIGIN = os.environ.get("UDATA_EVIDENCE_ORIGIN", "http://127.0.0.1:5640")
_parsed = urlparse(_ORIGINAL_ORIGIN)
if _parsed.scheme != "http" or _parsed.hostname != "127.0.0.1" or _parsed.port != 5640:
    raise SystemExit(
        "Controlled uData evidence is restricted to the fixed loopback stack "
        "http://127.0.0.1:5640; set UDATA_EVIDENCE_ORIGIN to the stock loopback origin."
    )

ORIGIN = "http://127.0.0.1:5640"
_REPO_ROOT = Path(__file__).resolve().parents[4]
_COMPOSE_FILE = _REPO_ROOT / "dev" / "udata-evidence" / "compose.yaml"
_ENV_FILE = _COMPOSE_FILE.with_name(".env")
_EXPECTED_UDATA_COMMIT = "0546582058d84706812a1c37387576efc4e5ad1f"
_EXPECTED_IMAGE_DIGESTS = (
    "mongo@sha256:d3d7c7fbbbb18f61baac3f8d13f0834c28a0e000cae444691def321d568abe47",
    "redis@sha256:28bd5e15c3674c48a472a3dd475ba446d0a3cd876e7addb988b5840a286b2256",
    "elasticsearch/elasticsearch@sha256:5496dd095a610571a02c362cd5f60ddd29a2cac5225d52f953241a5189871356",
    "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e",
    "axllent/mailpit@sha256:fa9d90f91a042f92cc28cf6dc4c75c6d57ac693b2737cdd30a6bfd9879838bbf",
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


def _compose_read(*args: str) -> str:
    completed = subprocess.run(
        ["docker", "compose", "--env-file", str(_ENV_FILE), "-f", str(_COMPOSE_FILE), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        pytest.fail("controlled stack identity could not be verified")
    return completed.stdout.strip()


def _verify_controlled_stack_identity() -> ControlledStackAttestation:
    nonce = os.environ.get("UDATA_EVIDENCE_STACK_NONCE")
    if not nonce or not _ENV_FILE.is_file():
        pytest.fail("controlled mutations require a stack nonce and dev/udata-evidence/.env")
    services = _compose_read("ps", "--status", "running", "--services").splitlines()
    expected_services = {"udata", "mongo", "redis", "search", "storage", "mailpit"}
    if len(services) != len(expected_services) or set(services) != expected_services:
        pytest.fail("controlled stack identity does not match the approved compose services")
    if _compose_read("port", "udata", "7000") != "127.0.0.1:5640":
        pytest.fail("controlled uData service is not bound to the fixed loopback port")
    if _compose_read("exec", "-T", "udata", "git", "-C", "/opt/udata", "rev-parse", "HEAD") != _EXPECTED_UDATA_COMMIT:
        pytest.fail("controlled uData service is not built from the approved source commit")
    if _compose_read("exec", "-T", "udata", "printenv", "UDATA_EVIDENCE_STACK_NONCE") != nonce:
        pytest.fail("controlled uData service nonce does not match the caller-selected stack")
    compose_text = _COMPOSE_FILE.read_text(encoding="utf-8")
    if any(digest not in compose_text for digest in _EXPECTED_IMAGE_DIGESTS):
        pytest.fail("controlled compose file is missing an approved dependency digest")
    dockerfile_text = _COMPOSE_FILE.with_name("Dockerfile").read_text(encoding="utf-8")
    if (
        "ghcr.io/astral-sh/uv:0.12.5@sha256:e85be844203885286c60ffad8a858d48afb6c5a5c237ca0e67f12e74b8f174b1"
        not in dockerfile_text
        or "python:3.13-slim-bookworm@sha256:b6bd71b0dd3811ddbcbc523ec2965fd1e1bcfdf7a20ab24679273d3bee726129"
        not in dockerfile_text
        or f"UDATA_COMMIT={_EXPECTED_UDATA_COMMIT}" not in dockerfile_text
    ):
        pytest.fail("controlled uData build is missing an approved image or source digest")
    return ControlledStackAttestation._from_verified_values(
        origin=ORIGIN,
        source_commit=_EXPECTED_UDATA_COMMIT,
        compose_sha256=sha256(compose_text.encode()).hexdigest(),
        dockerfile_sha256=sha256(dockerfile_text.encode()).hexdigest(),
        image_digests=_EXPECTED_IMAGE_DIGESTS,
        nonce=nonce,
        site_id=os.environ.get("UDATA_EVIDENCE_SITE_ID", "default"),
    )


def test_controlled_stack_proves_exact_version_then_one_dataset_read() -> None:
    settings = UDataClientSettings(base_url=ORIGIN)
    with create_sync_client(settings) as client:
        assert client.site_version().version == "17.6.0"
        envelope = client.datasets_list(
            CatalogOperationRequest(operation_id=_FAMILY_OPERATION_ID, payload={"page": 1, "page_size": 5}),
            CatalogOperationGuard(operation_id=_FAMILY_OPERATION_ID),
        )

    assert envelope.page is not None
    assert envelope.page.total_items is not None and envelope.page.total_items >= 0
    for record in envelope.items:
        assert record.id.value


def test_controlled_stack_proves_dataset_family_reads() -> None:
    settings = UDataClientSettings(base_url=ORIGIN)
    with create_sync_client(settings) as client:
        page = client.datasets.list(DatasetListQuery(page=1, page_size=3))
        suggestions = client.datasets.suggest(DatasetSuggestQuery(q="evidence", size=3))
        v2_page = client.datasets.list_v2(DatasetListQuery(page=1, page_size=3))

    assert page.page is not None and page.page.total_items is not None
    assert isinstance(suggestions, tuple)
    assert v2_page.page is not None


def test_controlled_stack_proves_authenticated_dataset_mutation_chain() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled mutations require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    attestation = _verify_controlled_stack_identity()
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
    settings = UDataClientSettings(base_url=ORIGIN, credential=credential, controlled_stack_attestation=attestation)
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
                        ),
                    )
                    cleanup_outcome = cleanup_result.receipt
                except Exception as error:
                    cleanup_error = error

    assert cleanup_error is None, f"controlled cleanup failed for {dataset_id}: {cleanup_error}"
    assert cleanup_outcome is not None
    assert cleanup_outcome.audit_metadata["status_code"] == 204


def test_controlled_stack_proves_site_patch_is_confirmed_and_receipt_bearing() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled mutations require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    attestation = _verify_controlled_stack_identity()
    from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
    from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy

    credential = UDataCredential(api_key=token)
    permissions = EffectivePermissions.for_credential(
        credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
    )
    with create_sync_client(
        UDataClientSettings(base_url=ORIGIN, credential=credential, controlled_stack_attestation=attestation)
    ) as client:
        before = client.root_profile.get()
        result = client.root_profile.set_site(
            SitePatchInput(title=before.title),
            permissions=permissions,
            mutation_policy=MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True,
                    operation=ROOT_OPERATIONS["set_site"],
                    target=before.site_id,
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )

    assert result.profile is not None and result.profile.title == before.title
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
    assert total is not None and total >= 0
