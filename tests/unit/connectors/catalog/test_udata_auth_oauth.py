from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
import json
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, parse_qsl, urlsplit

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
from datasluice.connectors.catalog.udata.models.oauth import (
    OAuthAuthorizeDecision,
    OAuthClientRequest,
    OAuthConsentOutcome,
    OAuthConsentSummary,
    OAuthErrorDocument,
    OAuthRevokeRequest,
    OAuthTokenRequest,
    OAuthTokenResult,
    begin_authorization_code,
)
from datasluice.connectors.catalog.udata.wire import oauth as wire
from datasluice.domain.catalog.auth import EffectivePermissions, OAuthFlow, SecretValue, UDataCredential
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import CatalogError, CatalogValidationError
from datasluice.runtime.events import EventEmitter, ListSink
from datasluice.runtime.transport.base import RedirectPolicy, RuntimeResponse
from tests.helpers.udata_oauth_checks import assert_oauth_mutation_async, assert_oauth_mutation_sync
from tests.helpers.udata_test_support import (
    UDATA_ORIGIN,
    RouteRecorder,
    assert_ambiguous_mutation_receipt,
    async_client,
    declared_routes,
    html_async_route_table,
    html_sync_route_table,
    mutation_policy,
    sync_client,
)

if TYPE_CHECKING:
    from datasluice.domain.catalog.safety import MutationPolicy
    from datasluice.runtime.transport.base import RuntimeRequest

ORIGIN = UDATA_ORIGIN
_JSON = "application/json"
SITE = {"id": "site", "title": "uData", "version": "17.6.0"}
CREDENTIAL = UDataCredential(api_key="unit-credential")
PERMISSIONS = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA)

# The six stock /oauth route-methods from udata/api/oauth2.py at v17.6.0.
ASSIGNED_ROUTES = (
    ("access_token", "POST", "/oauth/token"),
    ("revoke_token", "POST", "/oauth/revoke"),
    ("client_info", "GET", "/oauth/client_info"),
    ("authorize", "GET", "/oauth/authorize"),
    ("authorize_post", "POST", "/oauth/authorize"),
    ("oauth_error", "GET", "/oauth/error"),
)


def _policy(name: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    return mutation_policy(wire.OPERATIONS[name], target, destructive=destructive)


def _form(request: RuntimeRequest) -> dict[str, str]:
    assert request.body is not None
    return dict(parse_qsl(request.body.decode()))


def test_auth_oauth_contract_exposes_every_assigned_method_in_both_modes() -> None:
    assert hasattr(SyncUDataClient, "auth_oauth")
    assert hasattr(AsyncUDataClient, "auth_oauth")
    module = importlib.import_module("datasluice.connectors.catalog.udata.services.auth_oauth")
    expected = {name for name, _, _ in ASSIGNED_ROUTES}
    assert expected <= set(dir(module.SyncAuthOAuthService))
    assert expected <= set(dir(module.AsyncAuthOAuthService))


_FORM_SAMPLES = {
    "access_token": OAuthTokenRequest(grant_type="client_credentials", client_id="cid", client_secret="secret"),
    "revoke_token": OAuthRevokeRequest(token="opaque"),
    "authorize_post": OAuthAuthorizeDecision(accept=True),
}


_QUERY_SAMPLES = {
    "client_info": OAuthClientRequest(client_id="client-id"),
    "authorize": OAuthClientRequest(client_id="client-id", response_type="code", scope="default", state="xyz"),
}


@pytest.mark.parametrize(("name", "method", "path"), ASSIGNED_ROUTES)
def test_every_assigned_oauth_route_has_exact_verb_and_path(name: str, method: str, path: str) -> None:
    request = wire.build_request(name, _FORM_SAMPLES.get(name) or _QUERY_SAMPLES.get(name))
    assert request[0] == method
    assert request[1].split("?")[0] == path
    assert (request[3] is not None) == (name in _FORM_SAMPLES)


def test_query_routes_send_the_parameters_stock_requires() -> None:
    """Stock GET /oauth/authorize reads client_id and response_type from the query."""
    method, path, _headers, body = wire.build_request(
        "authorize", OAuthClientRequest(client_id="client-id", response_type="code", scope="default", state="xyz")
    )
    assert (method, body) == ("GET", None)
    assert dict(parse_qsl(path.partition("?")[2])) == {
        "client_id": "client-id",
        "response_type": "code",
        "scope": "default",
        "state": "xyz",
    }
    _method, client_path, _headers, _body = wire.build_request("client_info", OAuthClientRequest(client_id="client-id"))
    assert dict(parse_qsl(client_path.partition("?")[2])) == {"client_id": "client-id"}


def test_query_routes_refuse_a_request_stock_would_reject() -> None:
    with pytest.raises(ValueError):
        wire.build_request("authorize", OAuthClientRequest(client_id=None))
    with pytest.raises(ValueError):
        wire.build_request("client_info", OAuthClientRequest(client_id=None))


def test_oauth_token_route_sends_exact_form_without_cookie_or_session_auth() -> None:
    router = html_sync_route_table(
        declared_routes(("POST", "/oauth/token", 200, {"access_token": "opaque", "token_type": "Bearer"}), site=SITE)
    )
    client = sync_client(router, CREDENTIAL)
    token = OAuthTokenRequest(grant_type="client_credentials", client_id="client-id", client_secret="secret-value")
    with client:
        result = client.auth_oauth.access_token(token, PERMISSIONS)
    request = router.requests[-1]
    assert (request.method, request.url) == ("POST", f"{ORIGIN}/oauth/token")
    assert _form(request) == {
        "grant_type": "client_credentials",
        "client_id": "client-id",
        "client_secret": "secret-value",
    }
    assert "X-API-KEY" not in request.headers
    assert "Cookie" not in request.headers
    assert result.token_type == "Bearer"
    assert isinstance(result.access_token, SecretValue)
    assert result.access_token.reveal() == "opaque"
    assert str(result.access_token) == "***"


def test_oauth_revoke_route_sends_exact_form_and_records_target() -> None:
    router = html_sync_route_table(declared_routes(("POST", "/oauth/revoke", 200, None), site=SITE))
    client = sync_client(router, CREDENTIAL)
    revoke = OAuthRevokeRequest(token="opaque-access-token", token_type_hint="access_token")
    with client:
        result = client.auth_oauth.revoke_token(
            revoke, PERMISSIONS, _policy("revoke_token", "request:revoke_token", destructive=True)
        )
    request = router.requests[-1]
    assert (request.method, request.url) == ("POST", f"{ORIGIN}/oauth/revoke")
    assert _form(request) == {"token": "opaque-access-token", "token_type_hint": "access_token"}
    receipt = result.receipt
    assert receipt.operation == wire.OPERATIONS["revoke_token"]
    assert receipt.target.value == "request:revoke_token"
    assert receipt.outcome == "succeeded"
    assert "opaque-access-token" not in repr(receipt) + json.dumps(receipt.to_dict())


def test_oauth_client_info_reports_the_stock_session_gate_without_fabricating_consent() -> None:
    """Stock guards this route with login_required, so an API key gets no consent body."""
    router = html_sync_route_table(
        declared_routes(("GET", "/oauth/client_info?client_id=client-id", 401, None), site=SITE)
    )
    client = sync_client(router, CREDENTIAL)
    with client, pytest.raises(CatalogError):
        client.auth_oauth.client_info(OAuthClientRequest(client_id="client-id"), PERMISSIONS)
    assert router.requests[-1].url == f"{ORIGIN}/oauth/client_info?client_id=client-id"


def test_oauth_consent_summary_decodes_the_stock_document_when_one_is_returned() -> None:
    summary = wire.parse_consent_summary(
        "client_info", {"client": {"name": "Portal"}, "scopes": ["default"]}, 200, "application/json"
    )
    assert summary.client_name == "Portal"
    assert summary.scopes == ("default",)
    assert summary.to_dict() == {"session_gated": False, "client": {"name": "Portal"}, "scopes": ["default"]}
    assert "client_secret" not in json.dumps(summary.to_dict())


def test_oauth_consent_summary_keeps_only_status_for_a_session_gated_reply() -> None:
    summary = wire.parse_consent_summary("authorize", None, 302, "text/html; charset=utf-8")
    assert summary.session_gated is True
    assert summary.to_dict() == {"session_gated": True, "status_code": 302, "media_type": "text/html"}


@pytest.mark.parametrize("name", ["client_info", "authorize"])
@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_oauth_login_redirect_is_status_metadata_not_a_consent_document(
    monkeypatch: pytest.MonkeyPatch, name: str, async_mode: bool, status: int
) -> None:
    response_for = RouteRecorder.response_for

    def response_with_location(router: RouteRecorder, request: RuntimeRequest) -> RuntimeResponse:
        response = response_for(router, request)
        if response.status_code == status:
            return RuntimeResponse(status, {**response.headers, "Location": "/login"}, response.body)
        return response

    monkeypatch.setattr(RouteRecorder, "response_for", response_with_location)
    query = OAuthClientRequest(client_id="client-id")
    path = wire.build_request(name, query)[1]
    routes = declared_routes(("GET", path, status, {"message": "login-required"}), site=SITE)
    if async_mode:
        router = html_async_route_table(routes)

        async def run() -> OAuthConsentSummary:
            async with async_client(router, CREDENTIAL) as client:
                return await getattr(client.auth_oauth, name)(query, PERMISSIONS)

        result = asyncio.run(run())
        assert len([request for request in router.requests if "/oauth/" in request.url]) == 1
        assert "X-API-KEY" not in router.requests[-1].headers
        assert router.requests[-1].redirect_policy is RedirectPolicy.NO_FOLLOW
    else:
        sync_router = html_sync_route_table(routes)
        with sync_client(sync_router, CREDENTIAL) as client:
            result = getattr(client.auth_oauth, name)(query, PERMISSIONS)
        assert len([request for request in sync_router.requests if "/oauth/" in request.url]) == 1
        assert "X-API-KEY" not in sync_router.requests[-1].headers
        assert sync_router.requests[-1].redirect_policy is RedirectPolicy.NO_FOLLOW
    assert result.to_dict() == {"session_gated": True, "status_code": status, "media_type": "application/json"}


@pytest.mark.parametrize("status", [400, 401, 403, 404])
@pytest.mark.parametrize("async_mode", [False, True])
def test_oauth_read_error_keeps_http_status_instead_of_becoming_session_metadata(status: int, async_mode: bool) -> None:
    routes = declared_routes(
        ("GET", "/oauth/authorize?client_id=client-id&response_type=code", status, {"message": "rejected"}), site=SITE
    )
    with pytest.raises(CatalogError) as raised:
        if async_mode:

            async def run() -> None:
                async with async_client(html_async_route_table(routes), CREDENTIAL) as client:
                    await client.auth_oauth.authorize(OAuthClientRequest(client_id="client-id"), PERMISSIONS)

            asyncio.run(run())
        else:
            with sync_client(html_sync_route_table(routes), CREDENTIAL) as client:
                client.auth_oauth.authorize(OAuthClientRequest(client_id="client-id"), PERMISSIONS)
    assert raised.value.metadata["status_code"] == status


def test_oauth_authorize_get_sends_the_query_and_no_form_body() -> None:
    router = html_sync_route_table(
        declared_routes(("GET", "/oauth/authorize?client_id=client-id&response_type=code", 200, None), site=SITE)
    )
    client = sync_client(router, CREDENTIAL)
    with client:
        summary = client.auth_oauth.authorize(OAuthClientRequest(client_id="client-id"), PERMISSIONS)
    assert router.requests[-1].method == "GET"
    assert router.requests[-1].url == f"{ORIGIN}/oauth/authorize?client_id=client-id&response_type=code"
    assert router.requests[-1].body is None
    assert summary.session_gated is True


def test_oauth_error_route_is_an_exact_public_html_read() -> None:
    """The stock route answers the public error page, and keeps no body."""
    router = html_sync_route_table(declared_routes(("GET", "/oauth/error", 200, None), site=SITE))
    client = sync_client(router, CREDENTIAL)
    with client:
        outcome = client.auth_oauth.oauth_error()
    request = router.requests[-1]
    assert (request.method, request.url) == ("GET", f"{ORIGIN}/oauth/error")
    # Stock serves this page as HTML, so the assertion exercises real semantics
    # rather than echoing a router constant.
    assert outcome.to_dict() == {"session_gated": True, "status_code": 200, "media_type": "text/html"}


@pytest.mark.parametrize("accept", [True, False])
def test_authorize_post_reports_the_observed_outcome_not_a_fabricated_consent(accept: bool) -> None:
    """The consent POST must not report "authorized" for a declined decision."""
    router = html_sync_route_table(declared_routes(("POST", "/oauth/authorize", 200, None), site=SITE))
    with sync_client(router, CREDENTIAL) as client:
        outcome = client.auth_oauth.authorize_post(
            OAuthAuthorizeDecision(accept=accept),
            PERMISSIONS,
            _policy("authorize_post", "self"),
        )
    assert isinstance(outcome, OAuthConsentOutcome)
    assert outcome.accepted is accept
    assert outcome.status_code == 200
    assert outcome.media_type == "text/html"
    assert _form(router.requests[-1]) == ({"accept": "y"} if accept else {"decline": "y"})


def test_authorize_post_outcomes_stay_distinguishable() -> None:
    """Accept and decline must never collapse into one reported summary."""

    def run(accept: bool) -> dict[str, object]:
        router = html_sync_route_table(declared_routes(("POST", "/oauth/authorize", 200, None), site=SITE))
        with sync_client(router, CREDENTIAL) as client:
            return client.auth_oauth.authorize_post(
                OAuthAuthorizeDecision(accept=accept), PERMISSIONS, _policy("authorize_post", "self")
            ).to_dict()

    assert run(True)["accepted"] is True
    assert run(False)["accepted"] is False


def test_form_bodies_keep_their_declared_media_type() -> None:
    """A form body must never be relabelled as JSON by the shared header seam."""
    router = html_sync_route_table(
        declared_routes(("POST", "/oauth/token", 200, {"access_token": "opaque", "token_type": "Bearer"}), site=SITE)
    )
    with sync_client(router, CREDENTIAL) as client:
        client.auth_oauth.access_token(OAuthTokenRequest(grant_type="client_credentials", client_id="c"), PERMISSIONS)
    assert router.requests[-1].headers["Content-Type"] == wire.FORM_MEDIA_TYPE


def test_oauth_error_returns_the_stock_missing_template_status() -> None:
    """The 17.6.0 image ships no api/oauth_error.html, so stock answers 500.

    The typed surface exists to report that page, so the status must be returned rather than raised past the model.
    """
    router = html_sync_route_table(declared_routes(("GET", "/oauth/error", 500, None), site=SITE))
    with sync_client(router, CREDENTIAL) as client:
        outcome = client.auth_oauth.oauth_error()
    assert outcome.to_dict() == {"session_gated": True, "status_code": 500, "media_type": "text/html"}

    async def run() -> OAuthErrorDocument:
        async with async_client(
            html_async_route_table(declared_routes(("GET", "/oauth/error", 500, None), site=SITE)), CREDENTIAL
        ) as client:
            return await client.auth_oauth.oauth_error()

    assert asyncio.run(run()).status_code == 500


def test_oauth_error_still_raises_on_a_status_outside_the_modelled_page() -> None:
    """A status the route does not model must not be laundered into a document."""
    router = html_sync_route_table(declared_routes(("GET", "/oauth/error", 502, None), site=SITE))
    with (
        sync_client(router, CREDENTIAL) as client,
        pytest.raises(CatalogError),
    ):
        client.auth_oauth.oauth_error()


def test_oauth_error_route_never_sends_the_api_key() -> None:
    """The public error page needs no credential, so the API key must not reach it."""
    routes = declared_routes(("GET", "/oauth/error", 200, None), site=SITE)
    sync_router = html_sync_route_table(routes)
    with sync_client(sync_router, CREDENTIAL) as client:
        client.auth_oauth.oauth_error()
    assert "X-API-KEY" not in sync_router.requests[-1].headers

    router = html_async_route_table(routes)

    async def run() -> None:
        async with async_client(router, CREDENTIAL) as client:
            await client.auth_oauth.oauth_error()

    asyncio.run(run())
    assert "X-API-KEY" not in router.requests[-1].headers


def test_client_info_and_authorize_never_send_the_api_key() -> None:
    """Login-required browser GETs cannot be authenticated by an API key."""
    routes = declared_routes(
        ("GET", "/oauth/client_info?client_id=client-id", 401, None),
        ("GET", "/oauth/authorize?client_id=client-id&response_type=code", 401, None),
        site=SITE,
    )
    router = html_sync_route_table(routes)
    with sync_client(router, CREDENTIAL) as client:
        for name in ("client_info", "authorize"):
            with pytest.raises(CatalogError):
                getattr(client.auth_oauth, name)(OAuthClientRequest(client_id="client-id"), PERMISSIONS)
    oauth_requests = [request for request in router.requests if request.url.startswith(f"{ORIGIN}/oauth/")]
    assert {request.url.split("/oauth/")[1].partition("?")[0] for request in oauth_requests} == {
        "client_info",
        "authorize",
    }
    assert all("X-API-KEY" not in request.headers for request in oauth_requests)


def test_only_the_browser_consent_post_may_send_the_api_key() -> None:
    """The family contract: only the browser consent POST authenticates with the uData API key."""
    assert {name for name in wire.OPERATIONS if wire.sends_credential(name)} == {"authorize_post"}


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"scopes": ["default"]}, id="missing-client"),
        pytest.param({"client": {}, "scopes": ["default"]}, id="missing-client-name"),
        pytest.param({"client": {"name": "Portal"}}, id="missing-scopes"),
        pytest.param({"client": {"name": "Portal"}, "scopes": []}, id="empty-scopes"),
        pytest.param({"client": {"name": "Portal"}, "scopes": "default"}, id="scopes-not-a-list"),
    ],
)
def test_malformed_oauth_documents_fail_with_route_identity(payload: dict[str, object]) -> None:
    with pytest.raises(CatalogValidationError) as raised:
        wire.parse_consent_summary("client_info", payload, 200, "application/json")
    assert raised.value.operation == wire.OPERATIONS["client_info"]


def test_invalid_oauth_inputs_fail_before_any_dispatch() -> None:
    router = html_sync_route_table(
        declared_routes(("POST", "/oauth/token", 200, {"access_token": "opaque"}), site=SITE)
    )
    client = sync_client(router, CREDENTIAL)
    with client, pytest.raises(ValueError):
        client.auth_oauth.access_token(OAuthTokenRequest(grant_type="", client_id="c"), PERMISSIONS)
    with client, pytest.raises(ValueError):
        client.auth_oauth.access_token(
            OAuthTokenRequest(grant_type="client_credentials", client_id="c", scope=""), PERMISSIONS
        )
    assert router.requests == [], router.requests


def test_authorization_code_grant_requires_the_pkce_verifier_before_dispatch() -> None:
    """Stock 17.6.0 registers the code grant with ``CodeChallenge(required=True)``."""
    router = html_sync_route_table(
        declared_routes(("POST", "/oauth/token", 200, {"access_token": "opaque"}), site=SITE)
    )
    client = sync_client(router, CREDENTIAL)
    with client, pytest.raises(ValueError, match="code_verifier"):
        client.auth_oauth.access_token(
            OAuthTokenRequest(grant_type="authorization_code", client_id="client-id", code="code-1"), PERMISSIONS
        )
    assert router.requests == [], router.requests


def test_authorization_code_binding_binds_the_state_and_pkce_verifier_end_to_end() -> None:
    """The shared runtime flow binds the redirect state and the S256 verifier for one round trip."""
    descriptor = OAuthFlow.authorization_code(
        "https://portal.example.test/oauth/authorize",
        "https://portal.example.test/oauth/token",
        "client-id",
        "https://app.example.test/callback",
    )
    binding = begin_authorization_code(descriptor)
    query = parse_qs(urlsplit(binding.authorization_url).query)

    assert query["state"] == [binding.expected_state]
    assert query["code_challenge_method"] == ["S256"]
    assert query["response_type"] == ["code"]
    assert query["redirect_uri"] == ["https://app.example.test/callback"]
    assert "plain" not in binding.authorization_url

    binding.validate_callback_state(binding.expected_state)
    for observed in ("attacker-state", ""):
        with pytest.raises(CatalogValidationError):
            binding.validate_callback_state(observed)

    encoded = wire.build_request("access_token", binding.token_request("code-1", state=binding.expected_state))[3]
    assert encoded is not None
    form = dict(parse_qsl(encoded().decode()))
    assert form["grant_type"] == "authorization_code"
    assert form["code"] == "code-1"
    assert form["redirect_uri"] == "https://app.example.test/callback"
    challenge = base64.urlsafe_b64encode(hashlib.sha256(form["code_verifier"].encode()).digest()).rstrip(b"=").decode()
    assert challenge == query["code_challenge"][0]


def test_async_oauth_service_matches_sync_wire_exactly() -> None:
    routes = declared_routes(
        ("POST", "/oauth/token", 200, {"access_token": "opaque", "token_type": "Bearer"}),
        ("GET", "/oauth/client_info?client_id=client-id", 200, {"client": {"name": "Portal"}, "scopes": ["default"]}),
        site=SITE,
    )
    token = OAuthTokenRequest(grant_type="client_credentials", client_id="client-id", client_secret="secret-value")
    sync_router = html_sync_route_table(routes)
    with sync_client(sync_router, CREDENTIAL) as client:
        sync_token = client.auth_oauth.access_token(token, PERMISSIONS)
        sync_info = client.auth_oauth.client_info(OAuthClientRequest(client_id="client-id"), PERMISSIONS)

    async_router = html_async_route_table(routes)

    async def run() -> tuple[OAuthTokenResult, OAuthConsentSummary]:
        async with async_client(async_router, CREDENTIAL) as client:
            return await client.auth_oauth.access_token(token, PERMISSIONS), await client.auth_oauth.client_info(
                OAuthClientRequest(client_id="client-id"), PERMISSIONS
            )

    async_token, async_info = asyncio.run(run())
    assert async_token.to_dict() == sync_token.to_dict()
    assert async_info.to_dict() == sync_info.to_dict()
    assert [(request.method, request.url, request.body) for request in sync_router.requests] == [
        (request.method, request.url, request.body) for request in async_router.requests
    ]


@pytest.mark.parametrize("name", ["client_info", "authorize"])
def test_malformed_json_on_query_reads_fails_with_operation_identity(name: str) -> None:
    path = wire.build_request(name, OAuthClientRequest(client_id="client-id"))[1]
    router = html_sync_route_table(declared_routes(("GET", path, 200, b"{"), site=SITE))
    client = sync_client(router, CREDENTIAL)
    with client, pytest.raises(CatalogValidationError) as raised:
        getattr(client.auth_oauth, name)(OAuthClientRequest(client_id="client-id"), PERMISSIONS)
    assert raised.value.operation == wire.OPERATIONS[name]

    async def run() -> None:
        async with async_client(
            html_async_route_table(declared_routes(("GET", path, 200, b"{"), site=SITE)), CREDENTIAL
        ) as async_client_:
            with pytest.raises(CatalogValidationError):
                await getattr(async_client_.auth_oauth, name)(OAuthClientRequest(client_id="client-id"), PERMISSIONS)

    asyncio.run(run())


@pytest.mark.parametrize(
    "document",
    [
        pytest.param({"access_token": "opaque"}, id="missing-token-type"),
        pytest.param({"token_type": "Bearer"}, id="missing-access-token"),
        pytest.param({"token_type": "MAC", "access_token": "opaque"}, id="invalid-token-type"),
        pytest.param({"token_type": "Bearer", "access_token": ""}, id="empty-access-token"),
        pytest.param({"token_type": "Bearer", "access_token": "opaque", "scope": ""}, id="empty-scope"),
        pytest.param(
            {"token_type": "Bearer", "access_token": "opaque", "refresh_token": ""},
            id="empty-refresh-token",
        ),
    ],
)
def test_token_success_requires_typed_metadata(document: dict[str, object]) -> None:
    with pytest.raises(CatalogValidationError) as raised:
        wire.parse_token(
            "access_token",
            document,
            MutationReceipt(
                operation=wire.OPERATIONS["access_token"],
                outcome="succeeded",
                target=CatalogId(CatalogPlatform.UDATA, ResourceKind("oauth-token"), "self"),
            ),
        )
    assert raised.value.operation == wire.OPERATIONS["access_token"]


_DECODE_FAILURE_CASES = (
    pytest.param(
        "access_token",
        OAuthTokenRequest(grant_type="client_credentials", client_id="c"),
        {"token_type": "Bearer", "unexpected": "document"},
        id="access-token",
    ),
    pytest.param(
        "revoke_token",
        OAuthRevokeRequest(token="opaque"),
        {"unexpected": "document"},
        id="revoke-token",
    ),
)


@pytest.mark.parametrize(("name", "body", "payload"), _DECODE_FAILURE_CASES)
def test_post_dispatch_token_decode_failure_keeps_receipt_and_failed_event(
    name: str, body: OAuthTokenRequest | OAuthRevokeRequest, payload: dict[str, object]
) -> None:
    path = "/oauth/token" if name == "access_token" else "/oauth/revoke"
    events = ListSink()
    router = html_sync_route_table(declared_routes(("POST", path, 200, payload), site=SITE))
    client = sync_client(router, CREDENTIAL, emitter=EventEmitter(sinks=(events,)))
    policy = None if name == "access_token" else _policy(name, f"request:{name}", destructive=True)
    with client, pytest.raises(CatalogValidationError) as raised:
        if isinstance(body, OAuthTokenRequest):
            client.auth_oauth.access_token(body, PERMISSIONS)
        else:
            client.auth_oauth.revoke_token(body, PERMISSIONS, policy)
    assert_ambiguous_mutation_receipt(
        raised.value,
        raised.value.__dict__["mutation_receipt"],
        events,
        operation=wire.OPERATIONS[name],
        target=f"request:{name}",
        status_code=200,
        secret="unexpected",
    )


@pytest.mark.parametrize(("name", "body", "payload"), _DECODE_FAILURE_CASES)
def test_async_post_dispatch_token_decode_failure_keeps_receipt_and_failed_event(
    name: str, body: OAuthTokenRequest | OAuthRevokeRequest, payload: dict[str, object]
) -> None:
    path = "/oauth/token" if name == "access_token" else "/oauth/revoke"
    events = ListSink()
    router = html_async_route_table(declared_routes(("POST", path, 200, payload), site=SITE))
    policy = None if name == "access_token" else _policy(name, f"request:{name}", destructive=True)

    async def run() -> tuple[CatalogValidationError, MutationReceipt]:
        async with async_client(router, CREDENTIAL, emitter=EventEmitter(sinks=(events,))) as client:
            with pytest.raises(CatalogValidationError) as raised:
                if isinstance(body, OAuthTokenRequest):
                    await client.auth_oauth.access_token(body, PERMISSIONS)
                else:
                    await client.auth_oauth.revoke_token(body, PERMISSIONS, policy)
            return raised.value, raised.value.__dict__["mutation_receipt"]

    error, receipt = asyncio.run(run())
    assert_ambiguous_mutation_receipt(
        error,
        receipt,
        events,
        operation=wire.OPERATIONS[name],
        target=f"request:{name}",
        status_code=200,
        secret="unexpected",
    )


def test_offline_oauth_mutation_helpers_accept_empty_revocation_success_in_both_modes() -> None:
    raw = (200, None, {"content-type": _JSON})
    body = OAuthRevokeRequest(token="opaque")
    revoke_routes = declared_routes(("POST", "/oauth/revoke", 200, None), site=SITE)
    with sync_client(html_sync_route_table(revoke_routes), CREDENTIAL) as client:
        assert_oauth_mutation_sync(client, "revoke_token", raw, body, PERMISSIONS)

    async def run() -> None:
        async with async_client(
            html_async_route_table(declared_routes(("POST", "/oauth/revoke", 200, None), site=SITE)), CREDENTIAL
        ) as client:
            await assert_oauth_mutation_async(client, "revoke_token", raw, body, PERMISSIONS)

    asyncio.run(run())


def test_oauth_secrets_never_enter_retained_results_or_reprs() -> None:
    router = html_sync_route_table(
        declared_routes(
            (
                "POST",
                "/oauth/token",
                200,
                {"token_type": "Bearer", "access_token": "opaque-secret-token", "expires_in": 60},
            ),
            site=SITE,
        )
    )
    client = sync_client(router, CREDENTIAL)
    token = OAuthTokenRequest(grant_type="client_credentials", client_id="client-id", client_secret="secret-value")
    with client:
        result = client.auth_oauth.access_token(token, PERMISSIONS)
    if any(
        "opaque-secret-token" in representation
        for representation in (repr(result), str(result), json.dumps(result.to_dict()))
    ):
        pytest.fail("The OAuth access token entered a retained representation.")
    assert result.expires_in == 60
