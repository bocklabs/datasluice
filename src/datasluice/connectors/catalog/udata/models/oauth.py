"""Immutable OAuth request and result records for the stock uData /oauth routes."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from datasluice.domain.catalog.receipts import MutationReceipt

# The four grants uData 17.6.0 registers on its AuthorizationServer.
GRANT_TYPES = frozenset({"authorization_code", "client_credentials", "password", "refresh_token"})
TOKEN_TYPE_HINTS = frozenset({"access_token", "refresh_token"})
TOKEN_TYPES = frozenset({"Bearer"})
_SCOPE = re.compile(r"[A-Za-z0-9_.:-]+(?: [A-Za-z0-9_.:-]+)*")


def _text(value: object, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"uData OAuth {name} must be a non-empty string.")


def _safe_url(value: str, name: str) -> tuple[str, str]:
    try:
        parts = urlsplit(value)
    except ValueError:
        raise ValueError(f"uData OAuth {name} must be a valid absolute URI.") from None
    if parts.scheme not in {"http", "https"} or not parts.netloc or parts.username or parts.password:
        raise ValueError(f"uData OAuth {name} must be an absolute HTTP(S) URI without credentials.")
    return parts.scheme, parts.netloc


@dataclass(frozen=True, slots=True)
class OAuthTokenRequest:
    """One RFC 6749 token-endpoint form body with exact key omission."""

    grant_type: str
    client_id: str
    client_secret: str | None = field(default=None, repr=False)
    code: str | None = field(default=None, repr=False)
    refresh_token: str | None = field(default=None, repr=False)
    username: str | None = field(default=None, repr=False)
    password: str | None = field(default=None, repr=False)
    scope: str | None = None
    code_verifier: str | None = field(default=None, repr=False)
    redirect_uri: str | None = None

    def __post_init__(self) -> None:
        if self.grant_type not in GRANT_TYPES:
            raise ValueError(f"uData OAuth grant_type must be one of {sorted(GRANT_TYPES)}.")
        _text(self.client_id, "client_id")
        if self.scope is not None and (not isinstance(self.scope, str) or _SCOPE.fullmatch(self.scope) is None):
            raise ValueError("uData OAuth scope must be a space-delimited list of RFC 6749 scope tokens.")
        for name in ("client_secret", "code", "refresh_token", "username", "password", "code_verifier"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"uData OAuth {name} must be a string or null.")
        if self.grant_type == "refresh_token" and not self.refresh_token:
            raise ValueError("The uData refresh_token grant requires a refresh token.")
        if self.grant_type == "password" and not (self.username and self.password):
            raise ValueError("The uData password grant requires a username and password.")
        if self.grant_type == "authorization_code" and not self.code:
            raise ValueError("The uData authorization_code grant requires a code.")

    def form_fields(self) -> dict[str, str]:
        """Return the exact form keys the stock token endpoint reads, in wire order."""
        fields = {"grant_type": self.grant_type, "client_id": self.client_id}
        for name in (
            "client_secret",
            "code",
            "refresh_token",
            "username",
            "password",
            "scope",
            "code_verifier",
            "redirect_uri",
        ):
            value = getattr(self, name)
            if value is not None:
                fields[name] = value
        return fields


@dataclass(frozen=True, slots=True)
class OAuthRevokeRequest:
    """One RFC 7009 revocation form body without the optional client hint."""

    token: str = field(repr=False)
    token_type_hint: str | None = None
    client_id: str | None = None
    client_secret: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _text(self.token, "token")
        if self.token_type_hint is not None and self.token_type_hint not in TOKEN_TYPE_HINTS:
            raise ValueError(f"uData OAuth token_type_hint must be one of {sorted(TOKEN_TYPE_HINTS)}.")
        for name in ("client_id", "client_secret"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"uData OAuth {name} must be a string or null.")

    def form_fields(self) -> dict[str, str]:
        """Return the exact revocation form keys, omitting the absent optional hint."""
        fields = {"token": self.token}
        for name in ("token_type_hint", "client_id", "client_secret"):
            value = getattr(self, name)
            if value is not None:
                fields[name] = value
        return fields


@dataclass(frozen=True, slots=True)
class OAuthClientRequest:
    """The query parameters stock uData reads on the GET /oauth routes.

    ``client_info`` requires the token presented by the browser session, and
    ``authorize`` requires a client identifier and a response type; without
    them stock answers an error page instead of the requested document.
    """

    client_id: str | None = None
    response_type: str = "code"
    scope: str | None = None
    state: str | None = None

    def __post_init__(self) -> None:
        if self.client_id is not None:
            _text(self.client_id, "client_id")
        _text(self.response_type, "response_type")
        if self.scope is not None and (not isinstance(self.scope, str) or _SCOPE.fullmatch(self.scope) is None):
            raise ValueError("uData OAuth scope must be a space-delimited list of RFC 6749 scope tokens.")
        if self.state is not None and (not isinstance(self.state, str) or not self.state):
            raise ValueError("uData OAuth state must be a non-empty string or null.")

    def query_fields(self, name: str) -> dict[str, str]:
        """Return the exact query keys the stock route reads, omitting absent ones."""
        if name == "client_info":
            if self.client_id is None:
                raise ValueError("The uData client_info route requires a client_id.")
            return {"client_id": self.client_id}
        if self.client_id is None:
            raise ValueError("The uData authorize route requires a client_id.")
        fields = {"client_id": self.client_id, "response_type": self.response_type}
        for key in ("scope", "state"):
            value = getattr(self, key)
            if value is not None:
                fields[key] = value
        return fields


@dataclass(frozen=True, slots=True)
class OAuthAuthorizeDecision:
    """Consent outcome for the stock authorize POST form."""

    accept: bool = False
    state: str | None = None
    redirect_uri: str | None = None

    def __post_init__(self) -> None:
        if type(self.accept) is not bool:
            raise ValueError("uData OAuth accept must be a boolean.")
        for name in ("state", "redirect_uri"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f"uData OAuth {name} must be a non-empty string or null.")
        if self.redirect_uri is not None:
            _safe_url(self.redirect_uri, "redirect_uri")

    def form_fields(self) -> dict[str, str]:
        """Return the consent form the stock authorize POST reads."""
        fields = {"accept": "y"} if self.accept else {"decline": "y"}
        for name in ("state", "redirect_uri"):
            value = getattr(self, name)
            if value is not None:
                fields[name] = value
        return fields


@dataclass(frozen=True, slots=True)
class OAuthTokenResult:
    """Allowlisted token metadata; the access token never enters retained state."""

    receipt: MutationReceipt
    token_type: str
    expires_in: int | None = None
    scope: str | None = None
    access_token: str | None = field(default=None, repr=False)
    refresh_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.token_type not in TOKEN_TYPES:
            raise ValueError(f"uData OAuth token_type must be one of {sorted(TOKEN_TYPES)}.")
        if self.expires_in is not None and (type(self.expires_in) is not int or self.expires_in < 0):
            raise ValueError("uData OAuth expires_in must be a non-negative integer or null.")
        for name in ("scope", "access_token", "refresh_token"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"uData OAuth {name} must be a string or null.")

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "scope": self.scope,
            "has_access_token": self.access_token is not None,
            "has_refresh_token": self.refresh_token is not None,
        }


@dataclass(frozen=True, slots=True)
class OAuthConsentSummary:
    """The stock consent document, or the raw status of a browser-only page.

    Stock uData guards ``client_info`` and ``authorize`` with ``login_required``,
    so they answer a browser session, not an API key. This connector never
    fabricates cookie or session authentication: a session-gated reply is
    reported as its status and media type instead of an invented consent body.
    """

    client_name: str | None = None
    scopes: tuple[str, ...] | None = None
    status_code: int | None = None
    media_type: str | None = None
    session_gated: bool = False

    def __post_init__(self) -> None:
        if self.session_gated:
            if type(self.status_code) is not int:
                raise ValueError("A session-gated uData OAuth reply must carry its status code.")
            _text(self.media_type, "media type")
            return
        _text(self.client_name, "client name")
        if not self.scopes or not all(isinstance(scope, str) and scope for scope in self.scopes):
            raise ValueError("uData OAuth consent scopes must be non-empty strings.")

    def to_dict(self) -> dict[str, object]:
        if self.session_gated or self.scopes is None:
            return {"session_gated": True, "status_code": self.status_code, "media_type": self.media_type}
        return {"session_gated": False, "client": {"name": self.client_name}, "scopes": list(self.scopes)}


@dataclass(frozen=True, slots=True)
class OAuthErrorDocument:
    """The bounded metadata of the stock /oauth/error HTML page.

    The stock route renders ``api/oauth_error.html`` and never returns a JSON
    body, so only the status and media type are typed here. The rendered bytes
    are deliberately not retained.
    """

    status_code: int
    media_type: str
    session_gated: bool = True

    def __post_init__(self) -> None:
        if type(self.status_code) is not int or self.status_code < 100 or self.status_code > 599:
            raise ValueError("uData OAuth error status must be a valid HTTP status code.")
        _text(self.media_type, "media type")

    def to_dict(self) -> dict[str, object]:
        return {"session_gated": self.session_gated, "status_code": self.status_code, "media_type": self.media_type}
