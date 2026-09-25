"""Exact stock uData /oauth route, form, and error semantics.

Expectations are transcribed from the independent upstream oracle
``udata/api/oauth2.py`` at tag ``v17.6.0``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.oauth import (
    OAuthAuthorizeDecision,
    OAuthConsentSummary,
    OAuthErrorDocument,
    OAuthRevokeRequest,
    OAuthTokenRequest,
    OAuthTokenResult,
)
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = CatalogPlatform.UDATA
FORM_MEDIA_TYPE = "application/x-www-form-urlencoded"

# Operation identities map the pinned COVERAGE inventory rows 263-268. The two
# authorize verbs are separate dispatchable identities, matching how every other
# multi-verb stock route in this connector is named.
OPERATIONS = {
    "access_token": "udata/oauth.access-token",
    "revoke_token": "udata/oauth.revoke-token",
    "client_info": "udata/oauth.client-info",
    "authorize": "udata/oauth.authorize",
    "authorize_post": "udata/oauth.authorize-post",
    "oauth_error": "udata/oauth.oauth-error",
}

type Request = tuple[str, str, dict[str, str], bytes | None]

_PATHS = {
    "access_token": "/oauth/token",
    "revoke_token": "/oauth/revoke",
    "client_info": "/oauth/client_info",
    "authorize": "/oauth/authorize",
    "authorize_post": "/oauth/authorize",
    "oauth_error": "/oauth/error",
}
_METHODS = {name: "POST" for name in ("access_token", "revoke_token", "authorize_post")}
_METHODS.update({name: "GET" for name in ("client_info", "authorize", "oauth_error")})
_FORM_BODIES = frozenset({"access_token", "revoke_token", "authorize_post"})
_MUTATIONS = frozenset({"access_token", "revoke_token", "authorize_post"})


def _invalid(name: str, detail: str, action: str) -> CatalogValidationError:
    return CatalogValidationError(
        f"The uData OAuth response {detail}.",
        operation=OPERATIONS[name],
        platform=PLATFORM.value,
        safe_action=action,
    )


def _document(name: str, payload: object) -> Mapping[str, object]:
    if not isinstance(payload, Mapping):
        raise _invalid(name, "must be a JSON object", "Verify the response against the pinned OAuth schema.")
    return payload


def _optional_text(name: str, payload: Mapping[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is not None and not isinstance(value, str):
        raise _invalid(name, f"has an invalid {key}", "Verify the response against the pinned OAuth schema.")
    return value


def build_request(name: str, body: object = None) -> Request:
    """Build one documented OAuth request from its named stock route."""
    if name not in OPERATIONS:
        raise ValueError("The uData OAuth route is not assigned to this family.")
    method = _METHODS[name]
    path = _PATHS[name]
    if name not in _FORM_BODIES:
        return method, path, {}, None
    if not isinstance(body, (OAuthTokenRequest, OAuthRevokeRequest, OAuthAuthorizeDecision)):
        raise ValueError(f"uData OAuth {name} requires its typed form body.")
    encoded = urlencode(body.form_fields()).encode()
    return method, path, {"Content-Type": FORM_MEDIA_TYPE, "Accept": "application/json"}, encoded


def is_mutation(name: str) -> bool:
    """Return whether this stock route changes server-side OAuth state."""
    return name in _MUTATIONS


def requires_credential(name: str) -> bool:
    """Return whether the stock route reads or changes a credential-bound resource."""
    return name != "oauth_error"


def parse_token(name: str, payload: object, receipt: MutationReceipt) -> OAuthTokenResult:
    """Decode one stock token response without retaining its secrets."""
    document = _document(name, payload)
    token_type = document.get("token_type", "Bearer")
    if not isinstance(token_type, str) or not token_type:
        raise _invalid(name, "omitted its token type", "Verify the response against the pinned OAuth schema.")
    expires_in = document.get("expires_in")
    if expires_in is not None and (type(expires_in) is not int or expires_in < 0):
        raise _invalid(name, "has an invalid expiry", "Verify the response against the pinned OAuth schema.")
    return OAuthTokenResult(
        receipt=receipt,
        token_type=token_type,
        expires_in=expires_in,
        scope=_optional_text(name, document, "scope"),
        access_token=_optional_text(name, document, "access_token"),
        refresh_token=_optional_text(name, document, "refresh_token"),
    )


def parse_consent_summary(name: str, payload: object, status_code: int, media_type: str) -> OAuthConsentSummary:
    """Decode the stock consent document, or report a session-gated reply as-is.

    The stock routes are ``login_required``: without a browser session they
    answer a redirect or an unauthorized page rather than a consent JSON body.
    """
    if not isinstance(media_type, str) or not media_type:
        raise _invalid(name, "omitted its media type", "Verify the response against the pinned OAuth schema.")
    if payload is None:
        return OAuthConsentSummary(
            session_gated=True,
            status_code=status_code,
            media_type=media_type.split(";", 1)[0].strip().lower(),
        )
    document = _document(name, payload)
    client = document.get("client")
    if not isinstance(client, Mapping) or not isinstance(client.get("name"), str) or not client["name"]:
        raise _invalid(name, "omitted its client name", "Verify the response against the pinned OAuth schema.")
    scopes = document.get("scopes")
    if not isinstance(scopes, list) or not all(isinstance(scope, str) and scope for scope in scopes):
        raise _invalid(name, "omitted its scopes", "Verify the response against the pinned OAuth schema.")
    return OAuthConsentSummary(client_name=client["name"], scopes=tuple(cast("list[str]", scopes)))


def parse_error_document(name: str, status_code: int, media_type: str) -> OAuthErrorDocument:
    """Type the bounded public /oauth/error HTML page metadata."""
    if not isinstance(media_type, str) or not media_type:
        raise _invalid(name, "omitted its media type", "Verify the response against the pinned OAuth schema.")
    return OAuthErrorDocument(status_code=status_code, media_type=media_type.split(";", 1)[0].strip().lower())
