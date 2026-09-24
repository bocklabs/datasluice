"""Token failure paths retain only bounded safe metadata."""

from __future__ import annotations

import hmac
import logging
import secrets
from io import BytesIO

import pytest

from datasluice.connectors.catalog.udata.clients import SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.users import ApiTokenCreateInput, UserAvatarInput
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogNotFoundError,
    CatalogRateLimitError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    UnauthenticatedError,
)
from datasluice.runtime.events import EventEmitter, ListSink, LoggingSink
from tests.unit.connectors.catalog.test_udata_users_tokens import (
    CREDENTIAL,
    ORIGIN,
    PERMISSIONS,
    _policy,
    _Router,
    _routes,
)


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


def test_malformed_created_token_cannot_survive_in_exception_or_receipt() -> None:
    plaintext = secrets.token_urlsafe(36)
    router = _Router(_routes(("POST", "/api/1/me/api_tokens/", 201, {"token": plaintext})))
    events = ListSink()
    client = SyncUDataClient(
        router,
        declared_udata_profile(),
        origin=ORIGIN,
        credentials=CREDENTIAL,
        emitter=EventEmitter(sinks=(events,)),
    )
    with client, pytest.raises(CatalogValidationError) as raised:
        client.users_tokens.create_api_token(
            ApiTokenCreateInput(), PERMISSIONS, _policy("create_api_token", "new-api-token")
        )
    error = raised.value
    if any(plaintext in repr(value) for value in (error, error.metadata, error.__dict__["mutation_receipt"])):
        pytest.fail("One-time token entered a retained failure value.")
    traceback = error.__traceback__
    while traceback is not None:
        if "/src/datasluice/" in traceback.tb_frame.f_code.co_filename:
            if plaintext in repr(traceback.tb_frame.f_locals):
                pytest.fail("One-time token entered a connector failure frame.")
        traceback = traceback.tb_next
    operation_events = [
        event.outcome for event in events.events if event.operation_id == "udata/api-v1.create-api-token"
    ]
    assert operation_events == ["failed"]


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
