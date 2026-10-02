"""Bounded uData upload failure behavior."""

from __future__ import annotations

import asyncio
import json
from io import BytesIO
from typing import Any, cast

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.resources import (
    UPLOAD_STREAM_OPERATION,
    MidStreamUploadError,
    ResourceUploadInput,
    UploadDeadlineExceeded,
)
from datasluice.connectors.catalog.udata.wire import resources as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import (
    ConcurrencyPolicy,
    ConfirmationPolicy,
    IdempotencyPolicy,
    MutationPolicy,
)
from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
)
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure


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

    with client, pytest.raises(CatalogValidationError) as raised:
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

    with client, pytest.raises(ForbiddenError) as raised:
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
    transport = Responses()
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )
    with client:
        with pytest.raises(NativeCatalogError) as raised:
            client.resources.upload("dataset", ResourceUploadInput(BytesIO(b"abc"), "data.csv", 3), permissions, policy)
        with pytest.raises(CatalogValidationError) as redirected:
            client.resources.upload(
                "dataset",
                ResourceUploadInput(BytesIO(b"abc"), "data.csv", 3),
                permissions,
                policy,
            )
    assert raised.value.__dict__["mutation_receipt"].outcome == "ambiguous"
    assert redirected.value.__dict__["mutation_receipt"].outcome == "ambiguous"
    assert transport.upload_calls == 2


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


def test_retry_opted_in_upload_is_attempted_once_because_the_stream_is_one_shot() -> None:
    """A one-shot upload part is never replayed, even when the caller opted into retries."""
    credential = UDataCredential(api_key="local-test-key")
    upload_calls = 0

    class MidStreamReset(_InterruptingTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            nonlocal upload_calls
            if request.url.endswith("/api/1/site/"):
                return super().send(request)
            upload_calls += 1
            data = request.files[0].data
            assert not isinstance(data, bytes)
            data.read(1)
            raise TransportFailure("connection reset mid upload")

    client = SyncUDataClient(
        MidStreamReset(),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        max_attempts=3,
        owns_transport=False,
    )
    upload = ResourceUploadInput(BytesIO(b"abc"), "data.csv", 3)

    with client, pytest.raises(TransportFailure):
        client._dataset_call(
            method="POST",
            path="/api/1/datasets/dataset/upload/",
            owning_operation=wire.UPLOAD_NEW_OPERATION,
            idempotency_policy=IdempotencyPolicy(explicit_retry_opt_in=True),
            files=(upload.part(),),
        )

    upload.close()
    assert upload_calls == 1


def test_async_retry_opted_in_upload_is_attempted_once_because_the_stream_is_one_shot() -> None:
    credential = UDataCredential(api_key="local-test-key")
    upload_calls = 0

    class AsyncMidStreamReset(_CancellingTransport):
        async def send(self, request: RuntimeRequest) -> RuntimeResponse:
            nonlocal upload_calls
            if request.url.endswith("/api/1/site/"):
                return _InterruptingTransport().send(request)
            upload_calls += 1
            data = request.files[0].data
            assert not isinstance(data, bytes)
            data.read(1)
            raise TransportFailure("connection reset mid upload")

    client = AsyncUDataClient(
        AsyncMidStreamReset(),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        max_attempts=3,
        owns_transport=False,
    )
    upload = ResourceUploadInput(BytesIO(b"abc"), "data.csv", 3)

    async def run() -> None:
        async with client:
            with pytest.raises(TransportFailure):
                await client._dataset_call_async(
                    method="POST",
                    path="/api/1/datasets/dataset/upload/",
                    owning_operation=wire.UPLOAD_NEW_OPERATION,
                    idempotency_policy=IdempotencyPolicy(explicit_retry_opt_in=True),
                    files=(upload.part(),),
                )

    asyncio.run(run())
    upload.close()
    assert upload_calls == 1


class _ManualClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


class _TickingSource(BytesIO):
    def __init__(self, value: bytes, clock: _ManualClock, tick: float) -> None:
        super().__init__(value)
        self._clock = clock
        self._tick = float(tick)
        self.reads = 0
        self.close_calls = 0

    def read(self, size: int | None = -1) -> bytes:
        chunk = super().read(1)
        self._clock.advance(self._tick)
        self.reads += 1
        return chunk

    def close(self) -> None:
        self.close_calls += 1
        super().close()


class _DrainingTransport:
    """Drain the multipart stream exactly as a transport would, then answer the route."""

    def __init__(self) -> None:
        self.close_count = 0
        self.delivered = 0
        self.upload_calls = 0

    def _site(self) -> RuntimeResponse:
        body = json.dumps(
            {"id": "site", "title": "uData", "version": "17.6.0", "feed_size": 0, "keywords": [], "metrics": {}}
        ).encode()
        return RuntimeResponse(200, {"Content-Type": "application/json"}, body)

    def _drain(self, request: RuntimeRequest) -> RuntimeResponse:
        self.upload_calls += 1
        assert len(request.files) == 1
        data = request.files[0].data
        assert not isinstance(data, bytes)
        while chunk := data.read(1):
            self.delivered += len(chunk)
        return RuntimeResponse(200, {"Content-Type": "application/json"}, json.dumps({"id": "resource"}).encode())

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.url.endswith("/api/1/site/"):
            return self._site()
        return self._drain(request)

    def close(self) -> None:
        self.close_count += 1


class _AsyncCancellingDrainTransport:
    """Drain the multipart stream, then cancel mid upload."""

    def __init__(self, after_bytes: int) -> None:
        self.close_count = 0
        self.delivered = 0
        self.upload_calls = 0
        self._limit = after_bytes

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.url.endswith("/api/1/site/"):
            body = json.dumps(
                {"id": "site", "title": "uData", "version": "17.6.0", "feed_size": 0, "keywords": [], "metrics": {}}
            ).encode()
            return RuntimeResponse(200, {"Content-Type": "application/json"}, body)
        self.upload_calls += 1
        assert len(request.files) == 1
        data = request.files[0].data
        assert not isinstance(data, bytes)
        while chunk := data.read(1):
            self.delivered += len(chunk)
            if self.delivered >= self._limit:
                raise asyncio.CancelledError
        return RuntimeResponse(200, {"Content-Type": "application/json"}, json.dumps({"id": "resource"}).encode())

    async def aclose(self) -> None:
        self.close_count += 1


def _upload_policy() -> MutationPolicy:
    return MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation=wire.UPLOAD_REPLACE_OPERATION, target="resource"),
        concurrency=ConcurrencyPolicy(overwrite=True),
        destructive=True,
    )


def _budget(total: float) -> TimeBudget:
    return TimeBudget(connect=1.0, read=1.0, write=1.0, total=total)


def test_upload_input_rejects_an_untyped_budget_and_a_non_callable_clock() -> None:
    with pytest.raises(TypeError, match="uData upload time budgets must use TimeBudget"):
        ResourceUploadInput(BytesIO(b"a"), "data.csv", 1, budget=cast(Any, object()))
    with pytest.raises(TypeError, match="uData upload sources require a monotonic clock callable"):
        ResourceUploadInput(BytesIO(b"a"), "data.csv", 1, clock=cast(Any, "now"))


def test_upload_source_exactly_at_its_byte_ceiling_streams_every_byte_and_reports_end_of_file() -> None:
    """A source whose length equals the ceiling is neither truncated nor rejected."""
    source = BytesIO(b"abcd")
    upload = ResourceUploadInput(source, "data.csv", 4, budget=_budget(1.0))
    part = upload.part()

    assert not isinstance(part.data, bytes)
    assert part.data.read() == b"abcd"
    assert part.data.read() == b""
    upload.close()
    assert source.closed


def test_upload_source_over_its_byte_ceiling_fails_on_bytes_and_not_on_the_deadline() -> None:
    clock = _ManualClock()
    source = _TickingSource(b"abcd", clock, 0.25)
    upload = ResourceUploadInput(source, "data.csv", 3, budget=_budget(4.0), clock=clock)

    with pytest.raises(MidStreamUploadError, match="byte limit") as raised:
        part = upload.part()
        assert not isinstance(part.data, bytes)
        while part.data.read():
            pass

    assert not isinstance(raised.value, UploadDeadlineExceeded)
    assert source.tell() == 4
    assert clock.now == 1.0


def test_streaming_deadline_takes_precedence_over_the_byte_ceiling_once_it_is_exhausted() -> None:
    """The boundary check runs before any byte is consumed past an exhausted deadline."""
    clock = _ManualClock()
    source = _TickingSource(b"abcd", clock, 0.0)
    upload = ResourceUploadInput(source, "data.csv", 3, budget=_budget(1.0), clock=clock)
    clock.advance(10.0)

    with pytest.raises(UploadDeadlineExceeded) as raised:
        part = upload.part()
        assert not isinstance(part.data, bytes)
        while part.data.read():
            pass

    assert source.tell() == 0
    assert source.reads == 0
    assert raised.value.budget_seconds == 1.0
    assert raised.value.elapsed_seconds == 10.0
    assert isinstance(raised.value.__cause__, BudgetExhaustedError)
    assert raised.value.__cause__.operation == UPLOAD_STREAM_OPERATION
    assert raised.value.__cause__.platform == CatalogPlatform.UDATA.value


def test_streaming_deadline_stops_a_slow_drip_that_stays_under_the_byte_ceiling() -> None:
    """The byte ceiling does not bound wall-clock time, so the streaming boundary does."""
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    clock = _ManualClock()
    source = _TickingSource(b"x" * 64, clock, 1.0)
    transport = _DrainingTransport()
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )

    with client, pytest.raises(UploadDeadlineExceeded) as raised:
        client.resources.upload(
            "dataset",
            ResourceUploadInput(source, "data.csv", 64, budget=_budget(2.0), clock=clock),
            permissions,
            _upload_policy(),
            resource_id="resource",
        )

    assert transport.delivered == 2
    assert transport.delivered < 64
    assert source.close_calls == 1
    assert transport.close_count == 0
    assert raised.value.budget_seconds == 2.0
    assert raised.value.__dict__["mutation_receipt"].outcome == "ambiguous"
    assert "data.csv" not in str(raised.value)
    assert "x" * 64 not in str(raised.value)


def test_streaming_deadline_never_rejects_a_source_that_reaches_its_byte_ceiling() -> None:
    """A completed-but-slow upload settles as succeeded instead of a false deadline failure."""
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    clock = _ManualClock()
    source = _TickingSource(b"abcd", clock, 1.0)
    transport = _DrainingTransport()
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )

    with client:
        result = client.resources.upload(
            "dataset",
            ResourceUploadInput(source, "data.csv", 4, budget=_budget(4.0), clock=clock),
            permissions,
            _upload_policy(),
            resource_id="resource",
        )

    assert clock.now == 5.0
    assert transport.delivered == 4
    assert source.close_calls == 1
    assert result.receipt.outcome == "succeeded"


def test_streaming_deadline_failure_still_closes_the_borrowed_source_exactly_once() -> None:
    clock = _ManualClock()
    source = _TickingSource(b"abcd", clock, 0.0)
    upload = ResourceUploadInput(source, "data.csv", 3, budget=_budget(1.0), clock=clock)
    clock.advance(10.0)

    with pytest.raises(UploadDeadlineExceeded):
        part = upload.part()
        assert not isinstance(part.data, bytes)
        part.data.read()

    upload.close()
    upload.close()
    assert source.close_calls == 1
    with pytest.raises(ValueError, match="is closed"):
        part.data.read()
    with pytest.raises(ValueError, match="cannot be reused"):
        upload.part()


def test_async_cancellation_mid_drip_closes_once_and_is_not_reported_as_the_deadline() -> None:
    credential = UDataCredential(api_key="local-test-key")
    permissions = EffectivePermissions.for_credential(credential, platform=CatalogPlatform.UDATA)
    clock = _ManualClock()
    source = _TickingSource(b"x" * 64, clock, 0.5)
    transport = _AsyncCancellingDrainTransport(after_bytes=3)
    client = AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=credential,
        owns_transport=False,
    )

    async def run() -> asyncio.CancelledError:
        async with client:
            with pytest.raises(asyncio.CancelledError) as raised:
                await client.resources.upload(
                    "dataset",
                    ResourceUploadInput(source, "data.csv", 64, budget=_budget(30.0), clock=clock),
                    permissions,
                    _upload_policy(),
                    resource_id="resource",
                )
        return raised.value

    error = asyncio.run(run())
    assert transport.delivered == 3
    assert source.close_calls == 1
    assert transport.close_count == 0
    assert error.__dict__["mutation_receipt"].outcome == "cancelled"
