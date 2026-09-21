"""Bounded uData upload failure behavior."""

from __future__ import annotations

import asyncio
import json
from io import BytesIO

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput
from datasluice.connectors.catalog.udata.wire import resources as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError
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


class _CloseFailingSource(BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self._failed = False

    def close(self) -> None:
        if not self._failed:
            self._failed = True
            raise OSError("source close failed")
        super().close()


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
            operation=wire.UPLOAD_NEW_OPERATION,
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


def test_invalid_upload_id_closes_source_and_attaches_rejection_receipt() -> None:
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    source = BytesIO(b"abc")
    client = SyncUDataClient(
        _InterruptingTransport(),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )
    policy = MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation=wire.UPLOAD_REPLACE_OPERATION, target="resource/id"),
        concurrency=ConcurrencyPolicy(overwrite=True),
        destructive=True,
    )

    with client, pytest.raises(Exception) as raised:
        client.resources.upload(
            "dataset",
            ResourceUploadInput(source, "data.csv", 3),
            permissions,
            policy,
            resource_id="resource/id",
        )

    assert source.closed
    assert raised.value.__dict__["mutation_receipt"].outcome == "rejected"


def test_async_invalid_community_upload_id_keeps_route_receipt_identity() -> None:
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    source = BytesIO(b"abc")
    client = AsyncUDataClient(
        _CancellingTransport(),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )
    policy = MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation=wire.UPLOAD_COMMUNITY_REPLACE_OPERATION,
            target="resource/id",
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
        destructive=True,
    )

    async def run() -> CatalogValidationError:
        async with client:
            with pytest.raises(CatalogValidationError) as raised:
                await client.resources.reupload_community(
                    "resource/id", ResourceUploadInput(source, "data.csv", 3), permissions, policy
                )
        return raised.value

    error = asyncio.run(run())
    assert source.closed
    assert error.__dict__["mutation_receipt"].operation == wire.UPLOAD_COMMUNITY_REPLACE_OPERATION


def test_upload_close_failure_preserves_the_primary_rejection_receipt() -> None:
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    client = SyncUDataClient(
        _InterruptingTransport(),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )

    with client, pytest.raises(Exception) as raised:
        client.resources.upload(
            "dataset",
            ResourceUploadInput(_CloseFailingSource(b"abc"), "data.csv", 3),
            permissions,
        )

    assert raised.value.__dict__["mutation_receipt"].outcome == "rejected"


def test_upload_malformed_2xx_is_ambiguous_and_307_is_not_replayed() -> None:
    class Responses(_InterruptingTransport):
        def __init__(self) -> None:
            super().__init__()
            self.upload_calls = 0

        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            if request.url.endswith("/api/1/site/"):
                return super().send(request)
            if request.url.endswith("/api/1/datasets/dataset/upload/"):
                self.upload_calls += 1
                if self.upload_calls == 1:
                    return RuntimeResponse(200, {"Content-Type": "application/json"}, b"{")
                return RuntimeResponse(307, {"Location": "http://other.test/upload"}, b"")
            return RuntimeResponse(307, {"Location": "http://other.test/upload"}, b"")

    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    policy = MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation=wire.UPLOAD_NEW_OPERATION, target="dataset"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )
    client = SyncUDataClient(
        Responses(),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )

    with client:
        with pytest.raises(Exception) as raised:
            client.resources.upload("dataset", ResourceUploadInput(BytesIO(b"abc"), "data.csv", 3), permissions, policy)
        with pytest.raises(Exception) as redirected:
            client.resources.upload(
                "dataset",
                ResourceUploadInput(BytesIO(b"abc"), "data.csv", 3),
                permissions,
                policy,
            )
    assert raised.value.__dict__["mutation_receipt"].outcome == "ambiguous"
    assert redirected.value.__dict__["mutation_receipt"].outcome == "ambiguous"


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
            operation=wire.UPLOAD_NEW_OPERATION,
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
