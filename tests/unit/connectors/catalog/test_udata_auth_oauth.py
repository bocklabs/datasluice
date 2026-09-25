"""Pinned OAuth and authentication route contract for stock uData 17.6.0.

Expectations are derived from the independent upstream oracle
``udata/api/oauth2.py`` at tag ``v17.6.0``, not from production helpers.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from urllib.parse import parse_qsl

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.oauth import (
    OAuthAuthorizeDecision,
    OAuthClientRequest,
    OAuthConsentOutcome,
    OAuthConsentSummary,
    OAuthErrorDocument,
    OAuthRevokeRequest,
    OAuthTokenRequest,
    OAuthTokenResult,
)
from datasluice.connectors.catalog.udata.wire import oauth as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogError, CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

ORIGIN = "http://127.0.0.1:5640"
_JSON = "application/json"
_HTML = "text/html; charset=utf-8"
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


class _Router:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, object]]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        status, payload = self.routes[(request.method, request.url)]
        # None models the stock RFC 7009 empty 200 body.
        body = b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(
            status_code=status, headers={"Content-Type": _HTML if payload is None else _JSON}, body=body
        )

    def close(self) -> None:
        return None


class _AsyncRouter:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, object]]) -> None:
        self._sync = _Router(routes)
        self.requests = self._sync.requests

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self._sync.send(request)

    async def aclose(self) -> None:
        return None


def _routes(*items: tuple[str, str, int, object]) -> dict[tuple[str, str], tuple[int, object]]:
    routes = {("GET", f"{ORIGIN}/api/1/site/"): (200, SITE)}
    routes.update({(method, f"{ORIGIN}{path}"): (status, body) for method, path, status, body in items})
    return routes


def _policy(name: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=wire.OPERATIONS[name], target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


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
    router = _Router(_routes(("POST", "/oauth/token", 200, {"access_token": "opaque", "token_type": "Bearer"})))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
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
    assert result.access_token == "opaque"


def test_oauth_revoke_route_sends_exact_form_and_records_target() -> None:
    router = _Router(_routes(("POST", "/oauth/revoke", 200, None)))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    revoke = OAuthRevokeRequest(token="opaque-access-token", token_type_hint="access_token")
    with client:
        client.auth_oauth.revoke_token(
            revoke, PERMISSIONS, _policy("revoke_token", "request:revoke_token", destructive=True)
        )
    request = router.requests[-1]
    assert (request.method, request.url) == ("POST", f"{ORIGIN}/oauth/revoke")
    assert _form(request) == {"token": "opaque-access-token", "token_type_hint": "access_token"}


def test_oauth_client_info_reports_the_stock_session_gate_without_fabricating_consent() -> None:
    """Stock guards this route with login_required, so an API key gets no consent body."""
    router = _Router(_routes(("GET", "/oauth/client_info?client_id=client-id", 401, None)))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
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


def test_oauth_authorize_get_sends_the_query_and_no_form_body() -> None:
    router = _Router(_routes(("GET", "/oauth/authorize?client_id=client-id&response_type=code", 200, None)))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client:
        summary = client.auth_oauth.authorize(OAuthClientRequest(client_id="client-id"), PERMISSIONS)
    assert router.requests[-1].method == "GET"
    assert router.requests[-1].url == f"{ORIGIN}/oauth/authorize?client_id=client-id&response_type=code"
    assert router.requests[-1].body is None
    assert summary.session_gated is True


def test_oauth_error_route_is_an_exact_public_html_read() -> None:
    """The stock route answers the public error page, and keeps no body."""
    router = _Router(_routes(("GET", "/oauth/error", 200, None)))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
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
    router = _Router(_routes(("POST", "/oauth/authorize", 200, None)))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
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
        router = _Router(_routes(("POST", "/oauth/authorize", 200, None)))
        with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
            return client.auth_oauth.authorize_post(
                OAuthAuthorizeDecision(accept=accept), PERMISSIONS, _policy("authorize_post", "self")
            ).to_dict()

    assert run(True)["accepted"] is True
    assert run(False)["accepted"] is False


def test_form_bodies_keep_their_declared_media_type() -> None:
    """A form body must never be relabelled as JSON by the shared header seam."""
    router = _Router(_routes(("POST", "/oauth/token", 200, {"access_token": "opaque", "token_type": "Bearer"})))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        client.auth_oauth.access_token(OAuthTokenRequest(grant_type="client_credentials", client_id="c"), PERMISSIONS)
    assert router.requests[-1].headers["Content-Type"] == wire.FORM_MEDIA_TYPE


def test_oauth_error_returns_the_stock_missing_template_status() -> None:
    """The 17.6.0 image ships no api/oauth_error.html, so stock answers 500.

    The typed surface exists to report that page, so the status must be returned
    rather than raised past the model.
    """
    router = _Router(_routes(("GET", "/oauth/error", 500, None)))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        outcome = client.auth_oauth.oauth_error()
    assert outcome.to_dict() == {"session_gated": True, "status_code": 500, "media_type": "text/html"}

    async def run() -> OAuthErrorDocument:
        async with AsyncUDataClient(
            _AsyncRouter(_routes(("GET", "/oauth/error", 500, None))),
            declared_udata_profile(),
            origin=ORIGIN,
            credentials=CREDENTIAL,
        ) as client:
            return await client.auth_oauth.oauth_error()

    assert asyncio.run(run()).status_code == 500


def test_oauth_error_still_raises_on_a_status_outside_the_modelled_page() -> None:
    """A status the route does not model must not be laundered into a document."""
    router = _Router(_routes(("GET", "/oauth/error", 502, None)))
    with (
        SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client,
        pytest.raises(CatalogError),
    ):
        client.auth_oauth.oauth_error()


def test_oauth_error_route_never_sends_the_api_key() -> None:
    """The public error page needs no credential, so the API key must not reach it."""
    routes = _routes(("GET", "/oauth/error", 200, None))
    sync_router = _Router(routes)
    with SyncUDataClient(sync_router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        client.auth_oauth.oauth_error()
    assert "X-API-KEY" not in sync_router.requests[-1].headers

    router = _AsyncRouter(routes)

    async def run() -> None:
        async with AsyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
            await client.auth_oauth.oauth_error()

    asyncio.run(run())
    assert "X-API-KEY" not in router.requests[-1].headers


def test_session_gated_reads_never_send_the_api_key() -> None:
    """client_info and authorize are browser-session routes; the key cannot authorize them."""
    routes = _routes(
        ("GET", "/oauth/client_info?client_id=client-id", 401, None),
        ("GET", "/oauth/authorize?client_id=client-id&response_type=code", 401, None),
    )
    router = _Router(routes)
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
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
    """The family contract: exactly one route authenticates with the uData API key."""
    assert {name for name in wire.OPERATIONS if wire.sends_credential(name)} == {"authorize_post"}


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"scopes": ["default"]}, id="missing-client"),
        pytest.param({"client": {}, "scopes": ["default"]}, id="missing-client-name"),
        pytest.param({"client": {"name": "Portal"}}, id="missing-scopes"),
        pytest.param({"client": {"name": "Portal"}, "scopes": "default"}, id="scopes-not-a-list"),
    ],
)
def test_malformed_oauth_documents_fail_with_route_identity(payload: dict[str, object]) -> None:
    with pytest.raises(CatalogValidationError):
        wire.parse_consent_summary("client_info", payload, 200, "application/json")


def test_invalid_oauth_inputs_fail_before_any_dispatch() -> None:
    router = _Router(_routes(("POST", "/oauth/token", 200, {"access_token": "opaque"})))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    with client, pytest.raises(ValueError):
        client.auth_oauth.access_token(OAuthTokenRequest(grant_type="", client_id="c"), PERMISSIONS)
    with client, pytest.raises(ValueError):
        client.auth_oauth.access_token(
            OAuthTokenRequest(grant_type="client_credentials", client_id="c", scope=""), PERMISSIONS
        )
    assert router.requests == [], router.requests


def test_async_oauth_service_matches_sync_wire_exactly() -> None:
    routes = _routes(
        ("POST", "/oauth/token", 200, {"access_token": "opaque", "token_type": "Bearer"}),
        ("GET", "/oauth/client_info?client_id=client-id", 200, {"client": {"name": "Portal"}, "scopes": ["default"]}),
    )
    token = OAuthTokenRequest(grant_type="client_credentials", client_id="client-id", client_secret="secret-value")
    sync_router = _Router(routes)
    with SyncUDataClient(sync_router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        sync_token = client.auth_oauth.access_token(token, PERMISSIONS)
        sync_info = client.auth_oauth.client_info(OAuthClientRequest(client_id="client-id"), PERMISSIONS)

    async_router = _AsyncRouter(routes)

    async def run() -> tuple[OAuthTokenResult, OAuthConsentSummary]:
        async with AsyncUDataClient(
            async_router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL
        ) as client:
            return await client.auth_oauth.access_token(token, PERMISSIONS), await client.auth_oauth.client_info(
                OAuthClientRequest(client_id="client-id"), PERMISSIONS
            )

    async_token, async_info = asyncio.run(run())
    assert async_token.to_dict() == sync_token.to_dict()
    assert async_info.to_dict() == sync_info.to_dict()
    assert [(request.method, request.url, request.body) for request in sync_router.requests] == [
        (request.method, request.url, request.body) for request in async_router.requests
    ]


def test_oauth_secrets_never_enter_retained_results_or_reprs() -> None:
    router = _Router(_routes(("POST", "/oauth/token", 200, {"access_token": "opaque-secret-token", "expires_in": 60})))
    client = SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL)
    token = OAuthTokenRequest(grant_type="client_credentials", client_id="client-id", client_secret="secret-value")
    with client:
        result = client.auth_oauth.access_token(token, PERMISSIONS)
    if any(
        "opaque-secret-token" in representation
        for representation in (repr(result), str(result), json.dumps(result.to_dict()))
    ):
        pytest.fail("The OAuth access token entered a retained representation.")
    assert result.expires_in == 60
