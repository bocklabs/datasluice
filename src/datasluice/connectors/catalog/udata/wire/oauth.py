from __future__ import annotations

from collections.abc import Mapping
from threading import Lock
from typing import TYPE_CHECKING, cast
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.oauth import (
    TOKEN_TYPES,
    OAuthAuthorizeDecision,
    OAuthClientRequest,
    OAuthConsentSummary,
    OAuthErrorDocument,
    OAuthRevokeRequest,
    OAuthTokenRequest,
    OAuthTokenResult,
)
from datasluice.connectors.catalog.udata.secrets import UDataOAuthSecret, oauth_secret
from datasluice.domain.catalog.auth import SecretValue
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.errors.catalog import CatalogValidationError

if TYPE_CHECKING:
    from datasluice.domain.catalog.receipts import MutationReceipt

PLATFORM = CatalogPlatform.UDATA
FORM_MEDIA_TYPE = "application/x-www-form-urlencoded"
_SCHEMA_ACTION = "Verify the response against the pinned OAuth schema."

OPERATIONS = {
    "access_token": "udata/oauth.access-token",
    "revoke_token": "udata/oauth.revoke-token",
    "client_info": "udata/oauth.client-info",
    "authorize": "udata/oauth.authorize",
    "authorize_post": "udata/oauth.authorize-post",
    "oauth_error": "udata/oauth.oauth-error",
}


class OAuthForm:
    """Encode one form at transport send and reject a second send."""

    __slots__ = ("_body", "_lock", "_send_started")

    def __init__(self, body: OAuthTokenRequest | OAuthRevokeRequest | OAuthAuthorizeDecision) -> None:
        self._body: OAuthTokenRequest | OAuthRevokeRequest | OAuthAuthorizeDecision | None = body
        self._lock = Lock()
        self._send_started = False

    @property
    def send_started(self) -> bool:
        return self._send_started

    def __call__(self) -> bytes:
        with self._lock:
            body, self._body = self._body, None
            if body is not None:
                self._send_started = True
        if body is None:
            raise RuntimeError("The uData OAuth form was already sent or discarded.")
        fields = body.form_fields()
        revealed: dict[str, str] = {}
        value: str | SecretValue | None = None
        try:
            for name, value in fields.items():
                revealed[name] = value.reveal() if isinstance(value, SecretValue) else value
            return urlencode(revealed).encode()
        finally:
            revealed.clear()
            fields.clear()
            value = None

    def discard(self) -> None:
        with self._lock:
            self._body = None

    def __reduce__(self) -> tuple[object, ...]:
        raise TypeError("uData OAuth forms cannot be serialized or copied.")


type Request = tuple[str, str, dict[str, str], OAuthForm | None]

_PATHS = {
    "access_token": "/oauth/token",
    "revoke_token": "/oauth/revoke",
    "client_info": "/oauth/client_info",
    "authorize": "/oauth/authorize",
    "authorize_post": "/oauth/authorize",
    "oauth_error": "/oauth/error",
}
_METHODS = dict.fromkeys(("access_token", "revoke_token", "authorize_post"), "POST")
_METHODS.update(dict.fromkeys(("client_info", "authorize", "oauth_error"), "GET"))
_FORM_BODIES = frozenset({"access_token", "revoke_token", "authorize_post"})

_CREDENTIAL_ROUTES = frozenset({"authorize_post"})

_ERROR_PAGE_STATUSES = frozenset({200, 404, 500})


def is_error_page(status_code: int) -> bool:
    """Return whether a status is the stock /oauth/error page rather than a fault."""
    return status_code in _ERROR_PAGE_STATUSES


def _invalid(name: str, detail: str, action: str) -> CatalogValidationError:
    return CatalogValidationError(
        f"The uData OAuth response {detail}.",
        operation=OPERATIONS[name],
        platform=PLATFORM.value,
        safe_action=action,
    )


def _document(name: str, payload: object) -> Mapping[str, object]:
    if not isinstance(payload, Mapping):
        raise _invalid(name, "must be a JSON object", _SCHEMA_ACTION)
    return payload


def _optional_text(name: str, payload: Mapping[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is not None and (not isinstance(value, str) or not value):
        raise _invalid(name, f"has an invalid {key}", _SCHEMA_ACTION)
    return value


_QUERY_ROUTES = frozenset({"client_info", "authorize"})
_FORM_BODY_TYPES: dict[str, type[OAuthTokenRequest | OAuthRevokeRequest | OAuthAuthorizeDecision]] = {
    "access_token": OAuthTokenRequest,
    "revoke_token": OAuthRevokeRequest,
    "authorize_post": OAuthAuthorizeDecision,
}


def build_request(name: str, body: object = None) -> Request:
    """Build one documented OAuth request from its named stock route."""
    if name not in OPERATIONS:
        raise ValueError("The uData OAuth route is not assigned to this family.")
    method = _METHODS[name]
    path = _PATHS[name]
    if name in _QUERY_ROUTES:
        if not isinstance(body, OAuthClientRequest):
            raise ValueError(f"uData OAuth {name} requires its typed query body.")
        return (
            method,
            f"{path}?{urlencode(body.query_fields(name))}",
            {"Accept": "application/json" if name == "client_info" else "*/*"},
            None,
        )
    if name not in _FORM_BODIES:
        return method, path, {}, None
    expected = _FORM_BODY_TYPES[name]
    if not isinstance(body, expected):
        raise ValueError(f"uData OAuth {name} requires its typed {expected.__name__} body.")
    return method, path, {"Content-Type": FORM_MEDIA_TYPE, "Accept": "application/json"}, OAuthForm(body)


def sends_credential(name: str) -> bool:
    """Return whether this stock route may carry the uData API-key header.

    This is the family's single credential-transmission contract, derived from the route's declared authentication
    mechanism rather than a per-call-site opt-in, so no dispatch site can forget to apply it.
    """
    return name in _CREDENTIAL_ROUTES


def parse_token(name: str, payload: object, receipt: MutationReceipt) -> OAuthTokenResult:
    """Decode one stock token response without retaining its secrets."""
    document = payload if isinstance(payload, dict) else dict(_document(name, payload))
    access_token: UDataOAuthSecret | None = None
    refresh_token: UDataOAuthSecret | None = None
    try:
        access_token = _take_secret(name, document, "access_token", required=True)
        refresh_token = _take_secret(name, document, "refresh_token")
        token_type = document.get("token_type")
        if not isinstance(token_type, str) or token_type not in TOKEN_TYPES:
            raise _invalid(name, "has an invalid token type", _SCHEMA_ACTION)
        expires_in = document.get("expires_in")
        if expires_in is not None and (type(expires_in) is not int or expires_in < 0):
            raise _invalid(name, "has an invalid expiry", _SCHEMA_ACTION)
        return OAuthTokenResult(
            receipt=receipt,
            token_type=token_type,
            expires_in=expires_in,
            scope=_optional_text(name, document, "scope"),
            access_token=access_token,
            refresh_token=refresh_token,
        )
    except BaseException:
        for secret in (access_token, refresh_token):
            if secret is not None:
                secret.discard()
        raise
    finally:
        document.clear()


def _take_secret(
    name: str, document: dict[str, object], key: str, *, required: bool = False
) -> UDataOAuthSecret | None:
    value = document.pop(key, None)
    try:
        if value is None and not required:
            return None
        if not isinstance(value, str) or not value:
            raise _invalid(name, f"has an invalid {key}", _SCHEMA_ACTION)
        return oauth_secret(value)
    finally:
        value = None


def parse_consent_summary(name: str, payload: object, status_code: int, media_type: str) -> OAuthConsentSummary:
    """Decode the stock consent document, or report a session-gated reply as-is.

    The stock routes are ``login_required``: without a browser session they answer a redirect or an unauthorized page
    rather than a consent JSON body.
    """
    if not isinstance(media_type, str) or not media_type:
        raise _invalid(name, "omitted its media type", _SCHEMA_ACTION)
    if payload is None or status_code in {301, 302, 303, 307, 308}:
        return OAuthConsentSummary(
            session_gated=True,
            status_code=status_code,
            media_type=media_type.split(";", 1)[0].strip().lower(),
        )
    document = _document(name, payload)
    client = document.get("client")
    if not isinstance(client, Mapping) or not isinstance(client.get("name"), str) or not client["name"]:
        raise _invalid(name, "omitted its client name", _SCHEMA_ACTION)
    scopes = document.get("scopes")
    if not isinstance(scopes, list) or not scopes or not all(isinstance(scope, str) and scope for scope in scopes):
        raise _invalid(name, "omitted its scopes", _SCHEMA_ACTION)
    return OAuthConsentSummary(client_name=client["name"], scopes=tuple(cast("list[str]", scopes)))


def parse_error_document(name: str, status_code: int, media_type: str) -> OAuthErrorDocument:
    """Type the bounded public /oauth/error HTML page metadata."""
    if not isinstance(media_type, str) or not media_type:
        raise _invalid(name, "omitted its media type", _SCHEMA_ACTION)
    return OAuthErrorDocument(status_code=status_code, media_type=media_type.split(";", 1)[0].strip().lower())
