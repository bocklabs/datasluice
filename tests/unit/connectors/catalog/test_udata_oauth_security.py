from __future__ import annotations

import asyncio
import copy
import pickle
from dataclasses import asdict, replace
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qsl

import pytest

from datasluice.connectors.catalog.udata.models.oauth import (
    OAuthAuthorizeDecision,
    OAuthRevokeRequest,
    OAuthTokenRequest,
    OAuthTokenResult,
    begin_authorization_code,
)
from datasluice.connectors.catalog.udata.secrets import UDataOAuthSecret
from datasluice.connectors.catalog.udata.services.auth_oauth import AsyncAuthOAuthService, SyncAuthOAuthService
from datasluice.connectors.catalog.udata.wire import oauth as wire
from datasluice.domain.catalog.auth import OAuthFlow, SecretValue
from datasluice.errors.catalog import BudgetExhaustedError, CatalogError, CatalogValidationError
from datasluice.runtime.transport.base import RuntimeResponse, TransportError
from tests.helpers.udata_test_support import (
    async_client,
    declared_routes,
    html_async_route_table,
    html_sync_route_table,
    sync_client,
)
from tests.unit.connectors.catalog.test_udata_auth_oauth import CREDENTIAL, PERMISSIONS, _policy

if TYPE_CHECKING:
    from collections.abc import Callable

    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
    from datasluice.domain.catalog.receipts import MutationReceipt
    from tests.helpers.udata_test_support import AsyncRouteRouter, SyncRouteRouter


ORIGIN = "https://catalog.example.test"
CALLBACK = "https://app.example.test/callback?registered=1"


def _descriptor() -> OAuthFlow:
    return OAuthFlow.authorization_code(ORIGIN + "/oauth/authorize", ORIGIN + "/oauth/token", "client", CALLBACK)


@pytest.mark.parametrize("state", [None, "", "attacker-state"])
def test_exchange_requires_callback_state_at_request_creation(state: object) -> None:
    binding = begin_authorization_code(_descriptor())
    typed_state = cast("str", state)
    with pytest.raises(CatalogValidationError):
        binding.token_request("code", state=typed_state)


def test_direct_code_grant_cannot_bypass_binding_or_change_bound_fields() -> None:
    binding = begin_authorization_code(_descriptor())
    with pytest.raises(ValueError, match="binding"):
        OAuthTokenRequest("authorization_code", "client", code="code", code_verifier="verifier")
    request = binding.token_request("code", state=binding.expected_state)
    with pytest.raises(ValueError, match="binding"):
        replace(request, client_id="attacker")
    with pytest.raises(ValueError, match="binding"):
        replace(request, redirect_uri="https://attacker.test/callback")
    with pytest.raises(ValueError, match="binding"):
        replace(request, code_verifier="wrong")


@pytest.mark.parametrize(
    "redirect",
    [
        "https://app.example.test/callback#fragment",
        "https://app.example.test/callback#",
        "https://app.example.test/callback\n",
        "https://app.example.test:bad/callback",
        "https://app.example.test/callback?state=predefined",
    ],
)
def test_binding_rejects_unsafe_registered_redirects(redirect: str) -> None:
    descriptor = replace(_descriptor(), redirect_uri=redirect)
    with pytest.raises(ValueError):
        begin_authorization_code(descriptor)


@pytest.mark.parametrize(
    "location",
    [
        "https://attacker.test/callback?registered=1&state=expected&code=code",
        "https://app.example.test/other?registered=1&state=expected&code=code",
        CALLBACK + "&state=attacker&code=code",
        CALLBACK + "&state=expected&state=expected&code=code",
        CALLBACK + "&state=expected&code=code#fragment",
        "https://app.example.test/callback?registered=2&state=expected&code=code",
        CALLBACK + "&code=code",
    ],
)
def test_callback_checks_exact_endpoint_query_and_single_state(location: str) -> None:
    binding = begin_authorization_code(_descriptor(), state="expected")
    with pytest.raises((ValueError, CatalogValidationError)):
        binding.validate_callback_redirect(location)
    binding.validate_callback_redirect(CALLBACK + "&state=expected&code=code")


def test_consent_cannot_forward_unbound_state_or_redirect() -> None:
    binding = begin_authorization_code(_descriptor(), state="expected")
    with pytest.raises(ValueError, match="binding"):
        OAuthAuthorizeDecision(True, "expected", CALLBACK)
    with pytest.raises(CatalogValidationError):
        OAuthAuthorizeDecision(True, "attacker", CALLBACK, authorization=binding)
    with pytest.raises(ValueError, match="binding"):
        OAuthAuthorizeDecision(True, "expected", "https://attacker.test/callback", authorization=binding)
    assert (
        OAuthAuthorizeDecision(True, "expected", CALLBACK, authorization=binding).form_fields()["state"] == "expected"
    )


@pytest.mark.parametrize("grant", ["client_credentials", "password", "refresh_token", "authorization_code"])
def test_form_defers_all_secret_reveals_until_its_single_send(monkeypatch: pytest.MonkeyPatch, grant: str) -> None:
    binding = begin_authorization_code(_descriptor(), state="expected")
    requests = {
        "client_credentials": OAuthTokenRequest("client_credentials", "client", client_secret="secret"),
        "password": OAuthTokenRequest("password", "client", username="user", password="secret"),
        "refresh_token": OAuthTokenRequest("refresh_token", "client", refresh_token="secret"),
        "authorization_code": binding.token_request("secret", state="expected"),
    }
    request = requests[grant]
    reveals: list[UDataOAuthSecret] = []
    reveal = UDataOAuthSecret.reveal

    def record_reveal(secret: UDataOAuthSecret) -> str:
        reveals.append(secret)
        return reveal(secret)

    monkeypatch.setattr(UDataOAuthSecret, "reveal", record_reveal)
    form = wire.build_request("access_token", request)[3]
    assert isinstance(form, wire.OAuthForm)
    assert reveals == []
    assert "'secret'" not in repr(request.form_fields())
    encoded = form()
    assert reveals
    assert "secret" in dict(parse_qsl(encoded.decode())).values()
    with pytest.raises(RuntimeError):
        form()


@pytest.mark.parametrize("copy_value", [copy.copy, copy.deepcopy, pickle.dumps, asdict])
def test_oauth_request_secrets_block_implicit_serialization(copy_value: Callable[[object], object]) -> None:
    request = OAuthTokenRequest("password", "client", username="user", password="sensitive-password")
    with pytest.raises(TypeError):
        copy_value(request.password if copy_value is not asdict else request)


def test_caller_secret_holder_is_not_revealed_to_construct_or_discard_form(monkeypatch: pytest.MonkeyPatch) -> None:
    original = SecretValue.reveal
    reveals: list[SecretValue] = []

    def reveal(secret: SecretValue) -> str:
        reveals.append(secret)
        return original(secret)

    secret = SecretValue("caller-secret")
    monkeypatch.setattr(SecretValue, "reveal", reveal)
    request = OAuthTokenRequest("refresh_token", "client", refresh_token=secret)
    form = wire.build_request("access_token", request)[3]
    assert isinstance(form, wire.OAuthForm)
    assert reveals == []
    assert dict(parse_qsl(form().decode()))["refresh_token"] == "caller-secret"
    assert reveals == [secret]
    second_form = wire.build_request("access_token", request)[3]
    assert isinstance(second_form, wire.OAuthForm)
    second_form.discard()
    assert reveals == [secret]


@pytest.mark.parametrize("async_mode", [False, True])
def test_shared_client_guard_refuses_secret_reveal_before_dispatch(
    monkeypatch: pytest.MonkeyPatch, async_mode: bool
) -> None:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
    from tests.unit.connectors.catalog.test_udata_controlled_only_dispatch import _AsyncRouter, _Router

    request = OAuthRevokeRequest("private-refresh")
    reveals: list[UDataOAuthSecret] = []
    original = UDataOAuthSecret.reveal

    def reveal(secret: UDataOAuthSecret) -> str:
        reveals.append(secret)
        return original(secret)

    monkeypatch.setattr(UDataOAuthSecret, "reveal", reveal)
    router = _AsyncRouter() if async_mode else _Router()
    policy = _policy("revoke_token", "request:revoke_token", destructive=True)
    if async_mode:
        assert isinstance(router, _AsyncRouter)

        async def run() -> None:
            async with AsyncUDataClient(
                router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL
            ) as client:
                with pytest.raises(CatalogValidationError, match="controlled local deployment"):
                    await client.auth_oauth.revoke_token(request, PERMISSIONS, policy)

        asyncio.run(run())
    else:
        assert isinstance(router, _Router)
        with (
            SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client,
            pytest.raises(CatalogValidationError, match="controlled local deployment"),
        ):
            client.auth_oauth.revoke_token(request, PERMISSIONS, policy)
    assert reveals == []
    assert all(item.url.endswith("/api/1/site/") for item in router.requests)


@pytest.mark.parametrize("async_mode", [False, True])
def test_shared_client_never_retries_a_refresh_grant(async_mode: bool) -> None:
    routes = declared_routes(("POST", "/oauth/token", 503, {"error": "temporarily_unavailable"}))
    request = OAuthTokenRequest("refresh_token", "client", refresh_token="old-refresh")
    router = html_async_route_table(routes) if async_mode else html_sync_route_table(routes)
    if async_mode:

        async def run() -> None:
            async with async_client(cast("AsyncRouteRouter", router), CREDENTIAL) as client:
                with pytest.raises(CatalogError):
                    await client.auth_oauth.access_token(request, PERMISSIONS)

        asyncio.run(run())
    else:
        with sync_client(cast("SyncRouteRouter", router), CREDENTIAL) as client, pytest.raises(CatalogError):
            client.auth_oauth.access_token(request, PERMISSIONS)
    assert len([item for item in router.requests if item.url.endswith("/oauth/token")]) == 1


class _Client:
    _origin = ORIGIN

    def __init__(self, failure: BaseException | None = None, *, cancel_before_send: bool = False) -> None:
        self.failure = failure
        self.cancel_before_send = cancel_before_send
        self.forms: list[wire.OAuthForm] = []
        self.events: list[str] = []
        self.sends = 0
        self.response_payload: dict[str, object] = {}
        self.location: str | None = None
        self.fail_success_event = False

    def _resolved_credential(self) -> object:
        return CREDENTIAL

    async def _resolved_credential_async(self) -> object:
        return CREDENTIAL

    def _emit(self, _operation: str, outcome: str) -> None:
        if outcome == "succeeded" and self.fail_success_event:
            raise RuntimeError("The event sink failed.")
        self.events.append(outcome)

    def _dataset_call(self, **kwargs: object) -> tuple[int, object, RuntimeResponse]:
        form = kwargs["form_body"]
        assert isinstance(form, wire.OAuthForm)
        self.forms.append(form)
        assert kwargs["redirect_mode"] is True
        if self.cancel_before_send:
            assert self.failure is not None
            raise self.failure
        form()
        self.sends += 1
        if self.failure is not None:
            raise self.failure
        self.response_payload = {
            "token_type": "Bearer",
            "access_token": "new-access",
            "refresh_token": "rotated-refresh",
        }
        headers = {"Content-Type": "application/json"}
        if self.location is not None:
            headers["Location"] = self.location
        return (
            302 if self.location is not None else 200,
            self.response_payload,
            RuntimeResponse(302 if self.location is not None else 200, headers, b"secret-body"),
        )

    async def _dataset_call_async(self, **kwargs: object) -> tuple[int, object, RuntimeResponse]:
        return self._dataset_call(**kwargs)


@pytest.mark.parametrize("async_mode", [False, True])
def test_code_exchange_cannot_send_to_an_unbound_catalog(async_mode: bool) -> None:
    client = _Client()
    client._origin = "https://attacker.test"
    binding = begin_authorization_code(_descriptor())
    body = binding.token_request("code", state=binding.expected_state)
    if async_mode:
        coroutine = AsyncAuthOAuthService(cast("AsyncUDataClient", client)).access_token(body, PERMISSIONS)
        with pytest.raises(CatalogValidationError, match="endpoint") as excinfo:
            asyncio.run(coroutine)
    else:
        service = SyncAuthOAuthService(cast("SyncUDataClient", client))
        with pytest.raises(CatalogValidationError, match="endpoint") as excinfo:
            service.access_token(body, PERMISSIONS)
    assert client.forms == []
    assert cast("MutationReceipt", excinfo.value.__dict__["mutation_receipt"]).outcome == "rejected"


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("before_send", [False, True])
@pytest.mark.parametrize(
    "failure_type", [TransportError, BudgetExhaustedError, asyncio.CancelledError, KeyboardInterrupt, SystemExit]
)
def test_refresh_failures_discard_form_and_preserve_caller_credential(
    async_mode: bool, before_send: bool, failure_type: type[BaseException]
) -> None:
    if not async_mode and failure_type is asyncio.CancelledError:
        pytest.skip("Cancellation applies to the asynchronous service.")
    failure = (
        failure_type("interrupted")
        if failure_type is not BudgetExhaustedError
        else BudgetExhaustedError(
            "The budget expired.",
            operation=wire.OPERATIONS["access_token"],
            platform="udata",
            safe_action="Retry with a new budget.",
            elapsed_seconds=1.0,
            budget_seconds=1.0,
        )
    )
    client = _Client(failure, cancel_before_send=before_send)
    body = OAuthTokenRequest("refresh_token", "client", refresh_token="old-refresh")
    if async_mode:
        coroutine = AsyncAuthOAuthService(cast("AsyncUDataClient", client)).access_token(body, PERMISSIONS)
        with pytest.raises(failure_type) as raised:
            asyncio.run(coroutine)
    else:
        service = SyncAuthOAuthService(cast("SyncUDataClient", client))
        with pytest.raises(failure_type) as raised:
            service.access_token(body, PERMISSIONS)
    assert client.sends == (0 if before_send else 1)
    assert len(client.forms) == 1
    with pytest.raises(RuntimeError):
        client.forms[0]()
    assert isinstance(body.refresh_token, SecretValue)
    assert body.refresh_token.reveal() == "old-refresh"
    receipt = cast("MutationReceipt", raised.value.__dict__["mutation_receipt"])
    assert receipt.outcome == (
        "cancelled"
        if failure_type is asyncio.CancelledError or (failure_type is KeyboardInterrupt and before_send)
        else "ambiguous"
        if not before_send and failure_type in (TransportError, BudgetExhaustedError, KeyboardInterrupt, SystemExit)
        else "failed"
    )
    assert client.events == [
        "cancelled"
        if failure_type is asyncio.CancelledError
        else "budget_exhausted"
        if failure_type is BudgetExhaustedError
        else "failed"
    ]
    assert "old-refresh" not in repr(receipt)


@pytest.mark.parametrize("async_mode", [False, True])
def test_refresh_success_returns_rotation_without_mutating_old_credential(async_mode: bool) -> None:
    client = _Client()
    body = OAuthTokenRequest("refresh_token", "client", refresh_token="old-refresh")
    result = (
        asyncio.run(AsyncAuthOAuthService(cast("AsyncUDataClient", client)).access_token(body, PERMISSIONS))
        if async_mode
        else SyncAuthOAuthService(cast("SyncUDataClient", client)).access_token(body, PERMISSIONS)
    )
    assert isinstance(result.refresh_token, SecretValue)
    assert result.refresh_token.reveal() == "rotated-refresh"
    assert isinstance(body.refresh_token, SecretValue)
    assert body.refresh_token.reveal() == "old-refresh"
    assert client.response_payload == {}
    assert client.events == ["succeeded"]


@pytest.mark.parametrize("async_mode", [False, True])
def test_interruption_after_decode_discards_unreturned_tokens(async_mode: bool) -> None:
    client = _Client()
    decoded: list[OAuthTokenResult] = []
    service = (
        AsyncAuthOAuthService(cast("AsyncUDataClient", client))
        if async_mode
        else SyncAuthOAuthService(cast("SyncUDataClient", client))
    )

    def decode(status: int, payload: object, _media: str) -> OAuthTokenResult:
        from datasluice.connectors.catalog.udata.services.auth_oauth import _success_receipt

        result = wire.parse_token(
            "access_token", payload, _success_receipt(wire.OPERATIONS["access_token"], "self", None, status)
        )
        decoded.append(result)
        return result

    client.fail_success_event = True
    body = OAuthTokenRequest("client_credentials", "client")
    if isinstance(service, AsyncAuthOAuthService):
        coroutine = service._write_async("access_token", body, PERMISSIONS, None, decode)
        with pytest.raises(RuntimeError):
            asyncio.run(coroutine)
    else:
        with pytest.raises(RuntimeError):
            service._write("access_token", body, PERMISSIONS, None, decode)
    assert client.response_payload == {}
    assert len(decoded) == 1
    for secret in (decoded[0].access_token, decoded[0].refresh_token):
        assert isinstance(secret, UDataOAuthSecret)
        with pytest.raises(RuntimeError):
            secret.reveal()


def test_revocation_form_is_deferred_and_discardable() -> None:
    form = wire.build_request("revoke_token", OAuthRevokeRequest("token", client_secret="secret"))[3]
    assert isinstance(form, wire.OAuthForm)
    form.discard()
    with pytest.raises(RuntimeError):
        form()


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("valid_redirect", [False, True])
def test_consent_checks_callback_redirect_without_following_it(async_mode: bool, valid_redirect: bool) -> None:
    client = _Client()
    binding = begin_authorization_code(_descriptor(), state="expected")
    client.location = (
        CALLBACK + "&state=expected&code=private-code"
        if valid_redirect
        else "https://attacker.test/callback?state=expected&code=private-code"
    )
    body = OAuthAuthorizeDecision(True, "expected", CALLBACK, authorization=binding)

    def run() -> None:
        if async_mode:
            asyncio.run(
                AsyncAuthOAuthService(cast("AsyncUDataClient", client)).authorize_post(
                    body, PERMISSIONS, _policy("authorize_post", "self")
                )
            )
        else:
            SyncAuthOAuthService(cast("SyncUDataClient", client)).authorize_post(
                body, PERMISSIONS, _policy("authorize_post", "self")
            )

    if valid_redirect:
        run()
        assert client.events == ["succeeded"]
    else:
        with pytest.raises(ValueError, match="redirect"):
            run()
        assert client.events == ["failed"]
    assert client.sends == 1
    assert client.response_payload == {}


def test_real_task_cancellation_releases_pending_form_without_sending() -> None:
    entered = asyncio.Event()

    class WaitingClient(_Client):
        async def _dataset_call_async(self, **kwargs: object) -> tuple[int, object, RuntimeResponse]:
            form = kwargs["form_body"]
            assert isinstance(form, wire.OAuthForm)
            self.forms.append(form)
            entered.set()
            await asyncio.Future[None]()
            raise AssertionError("Cancelled requests cannot resume.")

    async def run() -> None:
        client = WaitingClient()
        body = OAuthTokenRequest("refresh_token", "client", refresh_token="old-refresh")
        task = asyncio.create_task(
            AsyncAuthOAuthService(cast("AsyncUDataClient", client)).access_token(body, PERMISSIONS)
        )
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.sends == 0
        assert client.events == ["cancelled"]
        assert task.done()
        with pytest.raises(RuntimeError):
            client.forms[0]()

    asyncio.run(run())


@pytest.mark.parametrize("invalid", [{"token_type": "MAC"}, {"expires_in": -1}, {"refresh_token": ""}])
def test_malformed_token_response_removes_all_plaintext(invalid: dict[str, object]) -> None:
    from datasluice.connectors.catalog.udata.services.auth_oauth import _success_receipt

    payload = {"token_type": "Bearer", "access_token": "private-access", "refresh_token": "private-refresh", **invalid}
    receipt = _success_receipt(wire.OPERATIONS["access_token"], "self", None, 200)
    with pytest.raises(CatalogValidationError) as raised:
        wire.parse_token("access_token", payload, receipt)
    assert payload == {}
    traceback = raised.value.__traceback__
    while traceback is not None:
        if str(traceback.tb_frame.f_globals.get("__name__", "")).startswith("datasluice"):
            assert "private-access" not in repr(traceback.tb_frame.f_locals)
            assert "private-refresh" not in repr(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next
