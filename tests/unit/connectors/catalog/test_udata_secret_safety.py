"""Token failure paths retain only bounded safe metadata."""

from __future__ import annotations

import hmac
import logging
import secrets
from io import BytesIO

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.users import (
    ApiTokenCreateInput,
    ApiTokenCreationResult,
    UserAvatarInput,
)
from datasluice.connectors.catalog.udata.services import users_tokens as users_tokens_service
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogNotFoundError,
    CatalogRateLimitError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
)
from datasluice.runtime.events import EventEmitter, ListSink, LoggingSink
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.unit.connectors.catalog.test_udata_users_tokens import (
    CREDENTIAL,
    ORIGIN,
    PERMISSIONS,
    _AsyncRouter,
    _policy,
    _Router,
    _routes,
)


@pytest.mark.parametrize("async_mode", [False, True])
def test_non_finite_user_response_raises_native_error_with_operation(async_mode: bool) -> None:
    import asyncio

    routes = _routes(("GET", "/api/1/users/person/", 200, {"id": "person", "score": float("nan")}))
    if async_mode:
        async_client = AsyncUDataClient(_AsyncRouter(routes), declared_udata_profile(), origin=ORIGIN)

        async def read_user() -> None:
            async with async_client:
                await async_client.users_tokens.get_user("person")

        with pytest.raises(NativeCatalogError) as raised:
            asyncio.run(read_user())
    else:
        sync_client = SyncUDataClient(_Router(routes), declared_udata_profile(), origin=ORIGIN)
        with sync_client, pytest.raises(NativeCatalogError) as raised:
            sync_client.users_tokens.get_user("person")
    assert raised.value.operation == "udata/api-v1.get-user"


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        ("Invalid API token", "invalid"),
        ("Revoked API token", "revoked"),
        ("Expired API token", "expired"),
        ("Inactive user", "inactive-user"),
    ],
)
def test_pinned_authentication_failures_have_distinct_safe_reason_codes(message: str, reason: str) -> None:
    router = _Router(_routes(("GET", "/api/1/me/api_tokens/", 401, {"message": message})))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client, pytest.raises(UnauthenticatedError) as raised:
        client.users_tokens.list_api_tokens(PERMISSIONS)
    assert raised.value.metadata["reason_code"] == reason
    assert raised.value.capability_state == "unauthorized"


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (400, CatalogValidationError),
        (403, ForbiddenError),
        (404, CatalogNotFoundError),
        (410, CatalogConflictError),
        (429, CatalogRateLimitError),
        (503, CatalogUnavailableError),
    ],
)
def test_user_family_http_failures_keep_their_safe_error_type(status: int, error_type: type[Exception]) -> None:
    router = _Router(_routes(("GET", "/api/1/users/person/", status, {"message": "failure"})))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN)
    with client, pytest.raises(error_type):
        client.users_tokens.get_user("person")


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b'{"message": ["Invalid API token"]}', id="list-message"),
        pytest.param(b'{"message": {"text": "Invalid API token"}}', id="object-message"),
        pytest.param(b"not-json", id="non-json"),
        pytest.param(b"x" * 513, id="oversized"),
    ],
)
def test_malformed_authentication_failures_stay_unauthenticated_without_reason_code(body: bytes) -> None:
    class RawBodyRouter(_Router):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            if request.url.endswith("/api/1/me/api_tokens/"):
                self.requests.append(request)
                return RuntimeResponse(401, {"Content-Type": "application/json"}, body)
            return super().send(request)

    client = SyncUDataClient(RawBodyRouter(_routes()), declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client, pytest.raises(UnauthenticatedError) as raised:
        client.users_tokens.list_api_tokens(PERMISSIONS)
    assert "reason_code" not in (raised.value.metadata or {})
    assert raised.value.capability_state == "unauthorized"


def test_malformed_created_token_cannot_survive_in_exception_or_receipt() -> None:
    plaintext = secrets.token_urlsafe(36)
    router = _Router(
        _routes(
            (
                "POST",
                "/api/1/me/api_tokens/",
                201,
                {"id": "token-id", "token_prefix": "", "token": plaintext},
            ),
            ("DELETE", "/api/1/me/api_tokens/token-id/", 204, None),
        )
    )
    events = ListSink()
    client = SyncUDataClient(
        router,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=CREDENTIAL,
        emitter=EventEmitter(sinks=(events,)),
    )
    with client:
        with pytest.raises(CatalogValidationError) as raised:
            client.users_tokens.create_api_token(
                ApiTokenCreateInput(), PERMISSIONS, _policy("create_api_token", "new-api-token")
            )
        error = raised.value
        receipt = error.__dict__["mutation_receipt"]
        assert isinstance(receipt, MutationReceipt)
        assert receipt.target.value == "token-id"
        if any(plaintext in repr(value) for value in (error, error.metadata, receipt)):
            pytest.fail("One-time token entered a retained failure value.")
        traceback = error.__traceback__
        while traceback is not None:
            if str(traceback.tb_frame.f_globals.get("__name__", "")).startswith("datasluice"):
                if plaintext in repr(traceback.tb_frame.f_locals):
                    pytest.fail("One-time token entered a connector failure frame.")
            traceback = traceback.tb_next
        revoked = client.users_tokens.revoke_api_token(
            receipt.target.value,
            PERMISSIONS,
            _policy("revoke_api_token", receipt.target.value, destructive=True),
        )
        assert revoked.receipt.target.value == receipt.target.value
        assert router.requests[-1].url == f"{ORIGIN}/api/1/me/api_tokens/token-id/"
    operation_events = [
        event.outcome for event in events.events if event.operation_id == "udata/api-v1.create-api-token"
    ]
    assert operation_events == ["failed"]


@pytest.mark.parametrize("exception_type", [KeyboardInterrupt, SystemExit], ids=["keyboard-interrupt", "system-exit"])
def test_interruption_after_token_response_redacts_traceback_locals(
    monkeypatch: pytest.MonkeyPatch, exception_type: type[BaseException]
) -> None:
    plaintext = secrets.token_urlsafe(36)
    router = _Router(
        _routes(
            (
                "POST",
                "/api/1/me/api_tokens/",
                201,
                {"id": "token-id", "token_prefix": "safe-prefix", "token": plaintext},
            ),
            ("DELETE", "/api/1/me/api_tokens/token-id/", 204, None),
        )
    )
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    original_response = users_tokens_service.RuntimeResponse

    def interrupt_before_response_redaction(**kwargs: object) -> object:
        monkeypatch.setattr(users_tokens_service, "RuntimeResponse", original_response)
        raise exception_type()

    monkeypatch.setattr(users_tokens_service, "RuntimeResponse", interrupt_before_response_redaction)
    with client:
        with pytest.raises(exception_type) as raised:
            client.users_tokens.create_api_token(
                ApiTokenCreateInput(), PERMISSIONS, _policy("create_api_token", "new-api-token")
            )
        traceback = raised.value.__traceback__
        while traceback is not None:
            frame = traceback.tb_frame
            if str(frame.f_globals.get("__name__", "")).startswith("datasluice"):
                if plaintext in repr(frame.f_locals):
                    pytest.fail("Interrupted token plaintext remained in a connector traceback frame.")
                response = frame.f_locals.get("response")
                if isinstance(response, RuntimeResponse):
                    assert response.body == b""
            traceback = traceback.tb_next
        receipt = raised.value.__dict__["mutation_receipt"]
        assert isinstance(receipt, MutationReceipt)
        assert receipt.outcome == "ambiguous"
        assert receipt.target.value == "token-id"
        revoked = client.users_tokens.revoke_api_token(
            receipt.target.value,
            PERMISSIONS,
            _policy("revoke_api_token", receipt.target.value, destructive=True),
        )
        assert revoked.receipt.target.value == receipt.target.value
        assert router.requests[-1].url == f"{ORIGIN}/api/1/me/api_tokens/token-id/"


@pytest.mark.parametrize("exception_type", [KeyboardInterrupt, SystemExit], ids=["keyboard-interrupt", "system-exit"])
def test_async_interruption_after_token_response_redacts_traceback_locals(
    monkeypatch: pytest.MonkeyPatch, exception_type: type[BaseException]
) -> None:
    import asyncio

    plaintext = secrets.token_urlsafe(36)
    router = _AsyncRouter(
        _routes(
            (
                "POST",
                "/api/1/me/api_tokens/",
                201,
                {"id": "token-id", "token_prefix": "safe-prefix", "token": plaintext},
            ),
            ("DELETE", "/api/1/me/api_tokens/token-id/", 204, None),
        )
    )
    client = AsyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    original_response = users_tokens_service.RuntimeResponse

    def interrupt_before_response_redaction(**kwargs: object) -> object:
        monkeypatch.setattr(users_tokens_service, "RuntimeResponse", original_response)
        raise exception_type()

    monkeypatch.setattr(users_tokens_service, "RuntimeResponse", interrupt_before_response_redaction)

    async def run() -> None:
        async with client:
            with pytest.raises(exception_type) as raised:
                await client.users_tokens.create_api_token(
                    ApiTokenCreateInput(), PERMISSIONS, _policy("create_api_token", "new-api-token")
                )
            traceback = raised.value.__traceback__
            while traceback is not None:
                frame = traceback.tb_frame
                if str(frame.f_globals.get("__name__", "")).startswith("datasluice"):
                    if plaintext in repr(frame.f_locals):
                        pytest.fail("Interrupted token plaintext remained in an async connector traceback frame.")
                    response = frame.f_locals.get("response")
                    if isinstance(response, RuntimeResponse):
                        assert response.body == b""
                traceback = traceback.tb_next
            receipt = raised.value.__dict__["mutation_receipt"]
            assert isinstance(receipt, MutationReceipt)
            assert receipt.outcome == "ambiguous"
            assert receipt.target.value == "token-id"
            revoked = await client.users_tokens.revoke_api_token(
                receipt.target.value,
                PERMISSIONS,
                _policy("revoke_api_token", receipt.target.value, destructive=True),
            )
            assert revoked.receipt.target.value == receipt.target.value
            assert router.requests[-1].url == f"{ORIGIN}/api/1/me/api_tokens/token-id/"

    asyncio.run(run())


def test_interruption_after_token_decode_discards_reveal_once_result(monkeypatch: pytest.MonkeyPatch) -> None:
    plaintext = secrets.token_urlsafe(36)
    router = _Router(
        _routes(
            (
                "POST",
                "/api/1/me/api_tokens/",
                201,
                {"id": "token-id", "token_prefix": "safe-prefix", "token": plaintext},
            )
        )
    )
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    shaped_results: list[ApiTokenCreationResult] = []
    parse_mutation = users_tokens_service._shape_mutation
    emit = client._emit

    def capture_result(payload: object, receipt: object, name: str) -> object:
        result = parse_mutation(payload, receipt, name)
        if isinstance(result, ApiTokenCreationResult):
            shaped_results.append(result)
        return result

    def interrupt_after_decode(owning_id: OperationId | str, outcome: str, **metadata: object) -> None:
        if str(owning_id) == "udata/api-v1.create-api-token" and outcome == "succeeded":
            raise KeyboardInterrupt
        emit(owning_id, outcome, **metadata)

    monkeypatch.setattr(users_tokens_service, "_shape_mutation", capture_result)
    monkeypatch.setattr(client, "_emit", interrupt_after_decode)
    with client, pytest.raises(KeyboardInterrupt) as raised:
        client.users_tokens.create_api_token(
            ApiTokenCreateInput(), PERMISSIONS, _policy("create_api_token", "new-api-token")
        )
    assert len(shaped_results) == 1
    with pytest.raises(RuntimeError):
        shaped_results[0].secret.reveal_once()
    receipt = raised.value.__dict__["mutation_receipt"]
    assert isinstance(receipt, MutationReceipt)
    assert receipt.outcome == "ambiguous"
    assert receipt.target.value == "token-id"
    traceback = raised.value.__traceback__
    while traceback is not None:
        if str(traceback.tb_frame.f_globals.get("__name__", "")).startswith("datasluice"):
            if plaintext in repr(traceback.tb_frame.f_locals):
                pytest.fail("Interrupted token plaintext remained in a connector traceback frame.")
        traceback = traceback.tb_next


def test_created_token_never_enters_events_or_logs(caplog: pytest.LogCaptureFixture) -> None:
    plaintext = secrets.token_urlsafe(36)
    router = _Router(
        _routes(
            (
                "POST",
                "/api/1/me/api_tokens/",
                201,
                {"id": "token-id", "token_prefix": "safe-prefix", "token": plaintext},
            )
        )
    )
    events = ListSink()
    emitter = EventEmitter(sinks=(events, LoggingSink()))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL, emitter=emitter)
    with caplog.at_level(logging.INFO, logger="datasluice.runtime.events"), client:
        result = client.users_tokens.create_api_token(
            ApiTokenCreateInput(), PERMISSIONS, _policy("create_api_token", "new-api-token")
        )
    operation_events = [
        event.outcome for event in events.events if event.operation_id == "udata/api-v1.create-api-token"
    ]
    assert operation_events == ["succeeded"]
    if plaintext in repr(events.events) or plaintext in caplog.text or plaintext in repr(result.receipt):
        pytest.fail("A one-time token entered an event, log, or receipt.")
    if not hmac.compare_digest(result.secret.reveal_once(), plaintext):
        pytest.fail("A one-time token did not match its reveal-once value.")


class _CloseFault(BytesIO):
    def close(self) -> None:
        super().close()
        raise OSError("Avatar source close failed.")


def test_avatar_close_failure_carries_the_successful_mutation_receipt() -> None:
    router = _Router(_routes(("POST", "/api/1/me/avatar/", 200, {"image": "avatar-path"})))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    upload = UserAvatarInput(_CloseFault(b"image"), "avatar.png", max_upload_bytes=16, content_type="image/png")
    with client, pytest.raises(OSError) as raised:
        client.users_tokens.my_avatar(upload, PERMISSIONS, _policy("my_avatar", "me"))
    receipt = raised.value.__dict__["mutation_receipt"]
    assert isinstance(receipt, MutationReceipt)
    assert receipt.outcome == "succeeded"
    assert receipt.operation == "udata/api-v1.my-avatar"
