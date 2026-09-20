"""Bounded uData upload failure behavior."""

from __future__ import annotations

import asyncio
import json
from io import BytesIO

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import ForbiddenError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse


class _InterruptingTransport:
    def __init__(self) -> None:
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.url.endswith("/api/1/site/"):
            body = json.dumps(
                {"id": "site", "title": "uData", "version": "17.6.0", "feed_size": 0, "keywords": [], "metrics": {}}
            ).encode()
            return RuntimeResponse(200, {"Content-Type": "application/json"}, body)
        assert len(request.files) == 1
        data = request.files[0].data
        assert not isinstance(data, bytes)
        data.read(1)
        raise OSError("upload source stopped after dispatch")

    def close(self) -> None:
        self.close_count += 1


class _CancellingTransport:
    def __init__(self) -> None:
        self.close_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.url.endswith("/api/1/site/"):
            return _InterruptingTransport().send(request)
        assert len(request.files) == 1
        data = request.files[0].data
        assert not isinstance(data, bytes)
        data.read(1)
        raise asyncio.CancelledError

    async def aclose(self) -> None:
        self.close_count += 1


def test_mid_stream_failure_is_ambiguous_and_closes_borrowed_source() -> None:
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    transport = _InterruptingTransport()
    source = BytesIO(b"abc")
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )
    policy = MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete",
            target="dataset",
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )

    with client, pytest.raises(OSError) as raised:
        client.resources.upload("dataset", ResourceUploadInput(source, "data.csv", 3), permissions, policy)

    assert source.closed
    assert transport.close_count == 0
    assert raised.value.__dict__["mutation_receipt"].outcome == "ambiguous"


def test_upload_policy_rejection_closes_source_without_dispatch() -> None:
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    transport = _InterruptingTransport()
    source = BytesIO(b"abc")
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )

    with client, pytest.raises(ForbiddenError) as raised:
        client.resources.upload("dataset", ResourceUploadInput(source, "data.csv", 3), permissions)

    assert source.closed
    assert transport.close_count == 0
    assert raised.value.__dict__["mutation_receipt"].outcome == "rejected"


def test_async_upload_cancellation_closes_source_and_records_cancelled() -> None:
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    transport = _CancellingTransport()
    source = BytesIO(b"abc")
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )
    policy = MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete",
            target="dataset",
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )

    async def run() -> BaseException:
        async with client:
            with pytest.raises(asyncio.CancelledError) as raised:
                await client.resources.upload(
                    "dataset", ResourceUploadInput(source, "data.csv", 3), permissions, policy
                )
        return raised.value

    error = asyncio.run(run())
    assert source.closed
    assert transport.close_count == 0
    assert error.__dict__["mutation_receipt"].outcome == "cancelled"
