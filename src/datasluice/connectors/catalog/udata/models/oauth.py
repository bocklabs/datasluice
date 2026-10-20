"""Immutable OAuth request and result records for the stock uData /oauth routes."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urlsplit

from datasluice.connectors.catalog.udata.secrets import oauth_secret
from datasluice.domain.catalog.auth import OAuthFlow, SecretValue
from datasluice.runtime.oauth import AuthorizationCodeFlow

if TYPE_CHECKING:
    from datasluice.domain.catalog.receipts import MutationReceipt

GRANT_TYPES = frozenset({"authorization_code", "client_credentials", "password", "refresh_token"})
TOKEN_TYPE_HINTS = frozenset({"access_token", "refresh_token"})
TOKEN_TYPES = frozenset({"Bearer"})
_MEDIA_TYPE = "media type"
_SCOPE = re.compile(r"[A-Za-z0-9_.:-]+(?: [A-Za-z0-9_.:-]+)*")


def _text(value: object, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"uData OAuth {name} must be a non-empty string.")


def _validate_url(value: str, name: str) -> None:
    parts = urlsplit("")
    try:
        _text(value, name)
        try:
            parts = urlsplit(value)
            _ = parts.port
        except ValueError:
            raise ValueError(f"uData OAuth {name} must be a valid absolute URI.") from None
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or "#" in value
            or "\\" in value
            or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in value)
        ):
            raise ValueError(f"uData OAuth {name} must be an absolute HTTPS URI without credentials or a fragment.")
    finally:
        parts = urlsplit("")
        value = ""


def _validate_scope_and_username(scope: str | None, username: str | None) -> None:
    if scope is not None and (not isinstance(scope, str) or _SCOPE.fullmatch(scope) is None):
        raise ValueError("uData OAuth scope must be a space-delimited list of RFC 6749 scope tokens.")
    if username is not None and not isinstance(username, str):
        raise ValueError("uData OAuth username must be a string or null.")


@dataclass(frozen=True, slots=True)
class OAuthTokenRequest:
    """One RFC 6749 token-endpoint form body with exact key omission."""

    grant_type: str
    client_id: str
    client_secret: SecretValue | str | None = field(default=None, repr=False)
    code: SecretValue | str | None = field(default=None, repr=False)
    refresh_token: SecretValue | str | None = field(default=None, repr=False)
    username: str | None = field(default=None, repr=False)
    password: SecretValue | str | None = field(default=None, repr=False)
    scope: str | None = None
    code_verifier: SecretValue | str | None = field(default=None, repr=False)
    redirect_uri: str | None = None
    authorization: OAuthAuthorizationBinding | None = field(default=None, repr=False, kw_only=True)
    state: str | None = field(default=None, repr=False, kw_only=True)

    def __post_init__(self) -> None:
        if self.grant_type not in GRANT_TYPES:
            raise ValueError(f"uData OAuth grant_type must be one of {sorted(GRANT_TYPES)}.")
        _text(self.client_id, "client_id")
        _validate_scope_and_username(self.scope, self.username)
        self._wrap_secrets()
        self._require_grant_fields()
        if self.redirect_uri is not None:
            _validate_url(self.redirect_uri, "redirect_uri")
        if self.grant_type == "authorization_code":
            self._validate_code_exchange()

    def _wrap_secrets(self) -> None:
        for name in ("client_secret", "refresh_token", "password", "code", "code_verifier"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, oauth_secret(value))

    def _require_grant_fields(self) -> None:
        required = {
            "refresh_token": ("refresh_token",),
            "password": ("username", "password"),
            "authorization_code": ("code", "code_verifier"),
        }.get(self.grant_type, ())
        if any(not getattr(self, name) for name in required):
            raise ValueError(f"The uData {self.grant_type} grant requires {', '.join(required)}.")

    def _validate_code_exchange(self) -> None:
        if not isinstance(self.authorization, OAuthAuthorizationBinding):
            raise ValueError("The uData authorization_code grant requires an authorization binding.")
        self.authorization.validate_callback_state(self.state)
        if (
            self.client_id != self.authorization._descriptor.client_id
            or self.redirect_uri != self.authorization._descriptor.redirect_uri
            or self.code_verifier is not self.authorization._verifier
        ):
            raise ValueError("The uData code exchange must match its authorization binding.")

    def form_fields(self) -> dict[str, str | SecretValue]:
        """Return form fields without revealing secret values."""
        fields: dict[str, str | SecretValue] = {"grant_type": self.grant_type, "client_id": self.client_id}
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
class OAuthAuthorizationBinding:
    """One browser authorization-code round trip over the shared OAuth runtime.

    The exchange requires the observed callback state and the bound redirect URI.
    """

    _descriptor: OAuthFlow = field(repr=False)
    _flow: AuthorizationCodeFlow = field(repr=False)
    _verifier: SecretValue = field(repr=False)

    @property
    def authorization_url(self) -> str:
        return self._flow.authorization_url()

    @property
    def expected_state(self) -> str:
        return self._flow.expected_state

    def validate_callback_state(self, observed_state: object) -> None:
        """Require the authorization redirect to echo the bound state exactly."""
        self._flow.validate_state(observed_state)

    def validate_callback_redirect(self, location: str) -> None:
        """Require the registered callback endpoint and exactly one matching state."""
        observed = urlsplit("")
        parameters: list[tuple[str, str]] = []
        retained: list[tuple[str, str]] = []
        try:
            _validate_url(location, "callback redirect")
            expected = urlsplit(self._descriptor.redirect_uri or "")
            observed = urlsplit(location)
            if (observed.scheme, observed.netloc, observed.path) != (expected.scheme, expected.netloc, expected.path):
                raise ValueError("The uData callback redirect must match its authorization binding.")
            parameters = parse_qsl(observed.query, keep_blank_values=True)
            states = [value for key, value in parameters if key == "state"]
            if len(states) != 1:
                raise ValueError("The uData callback redirect must contain exactly one state.")
            self.validate_callback_state(states[0])
            retained = [
                (key, value) for key, value in parameters if key not in {"state", "code", "error", "error_description"}
            ]
            if retained != parse_qsl(expected.query, keep_blank_values=True):
                raise ValueError("The uData callback redirect query must match its authorization binding.")
        finally:
            observed = urlsplit("")
            parameters.clear()
            retained.clear()
            location = ""

    def token_request(self, code: str, *, state: str) -> OAuthTokenRequest:
        """Build the typed code exchange carrying the bound verifier and redirect URI."""
        try:
            return OAuthTokenRequest(
                grant_type="authorization_code",
                client_id=self._descriptor.client_id,
                code=oauth_secret(code),
                code_verifier=self._verifier,
                redirect_uri=self._descriptor.redirect_uri,
                authorization=self,
                state=state,
            )
        finally:
            code = ""


def begin_authorization_code(descriptor: OAuthFlow, *, state: str | None = None) -> OAuthAuthorizationBinding:
    """Bind one uData browser authorization-code round trip over the shared OAuth runtime."""
    if not isinstance(descriptor, OAuthFlow):
        raise TypeError("uData authorization requires an OAuthFlow.")
    for name in ("authorization_url", "token_url"):
        value = getattr(descriptor, name)
        _validate_url(value, name)
        if urlsplit(value).query:
            raise ValueError("uData OAuth endpoint URLs cannot contain a query.")
    if descriptor.redirect_uri is None:
        raise ValueError("uData authorization requires an explicit redirect URI.")
    _validate_url(descriptor.redirect_uri, "redirect_uri")
    if any(
        key in {"state", "code", "error", "error_description"}
        for key, _ in parse_qsl(urlsplit(descriptor.redirect_uri).query)
    ):
        raise ValueError("The uData redirect URI cannot contain OAuth response parameters.")
    flow = AuthorizationCodeFlow(descriptor, state=state)
    return OAuthAuthorizationBinding(_descriptor=descriptor, _flow=flow, _verifier=oauth_secret(flow.verifier))


@dataclass(frozen=True, slots=True)
class OAuthRevokeRequest:
    """One RFC 7009 revocation form body without the optional client hint."""

    token: SecretValue | str = field(repr=False)
    token_type_hint: str | None = None
    client_id: str | None = None
    client_secret: SecretValue | str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "token", oauth_secret(self.token))
        if self.token_type_hint is not None and self.token_type_hint not in TOKEN_TYPE_HINTS:
            raise ValueError(f"uData OAuth token_type_hint must be one of {sorted(TOKEN_TYPE_HINTS)}.")
        if self.client_id is not None and not isinstance(self.client_id, str):
            raise ValueError("uData OAuth client_id must be a string or null.")
        if self.client_secret is not None:
            object.__setattr__(self, "client_secret", oauth_secret(self.client_secret))

    def form_fields(self) -> dict[str, str | SecretValue]:
        """Return revocation fields without revealing secret values."""
        fields: dict[str, str | SecretValue] = {"token": self.token}
        for name in ("token_type_hint", "client_id", "client_secret"):
            value = getattr(self, name)
            if value is not None:
                fields[name] = value
        return fields


@dataclass(frozen=True, slots=True)
class OAuthClientRequest:
    """The query parameters stock uData reads on the GET /oauth routes.

    ``client_info`` requires the token presented by the browser session, and ``authorize`` requires a client identifier
    and a response type; without them stock answers an error page instead of the requested document.
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
    authorization: OAuthAuthorizationBinding | None = field(default=None, repr=False, kw_only=True)

    def __post_init__(self) -> None:
        if type(self.accept) is not bool:
            raise ValueError("uData OAuth accept must be a boolean.")
        for name in ("state", "redirect_uri"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f"uData OAuth {name} must be a non-empty string or null.")
        if self.redirect_uri is not None:
            _validate_url(self.redirect_uri, "redirect_uri")
        if self.state is not None or self.redirect_uri is not None or self.authorization is not None:
            if not isinstance(self.authorization, OAuthAuthorizationBinding):
                raise ValueError("The uData consent state and redirect require an authorization binding.")
            self.authorization.validate_callback_state(self.state)
            if self.redirect_uri != self.authorization._descriptor.redirect_uri:
                raise ValueError("The uData consent redirect must match its authorization binding.")

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
    """Token metadata with secret holders that block implicit serialization."""

    receipt: MutationReceipt
    token_type: str
    expires_in: int | None = None
    scope: str | None = None
    access_token: SecretValue | str | None = field(default=None, repr=False)
    refresh_token: SecretValue | str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.token_type not in TOKEN_TYPES:
            raise ValueError(f"uData OAuth token_type must be one of {sorted(TOKEN_TYPES)}.")
        if self.expires_in is not None and (type(self.expires_in) is not int or self.expires_in < 0):
            raise ValueError("uData OAuth expires_in must be a non-negative integer or null.")
        if self.scope is not None:
            _text(self.scope, "scope")
        for name in ("access_token", "refresh_token"):
            value = getattr(self, name)
            if value is not None:
                value = oauth_secret(value)
                object.__setattr__(self, name, value)

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
            _text(self.media_type, _MEDIA_TYPE)
            return
        _text(self.client_name, "client name")
        if not self.scopes or not all(isinstance(scope, str) and scope for scope in self.scopes):
            raise ValueError("uData OAuth consent scopes must be non-empty strings.")

    def to_dict(self) -> dict[str, object]:
        if self.session_gated or self.scopes is None:
            return {"session_gated": True, "status_code": self.status_code, "media_type": self.media_type}
        return {"session_gated": False, "client": {"name": self.client_name}, "scopes": list(self.scopes)}


@dataclass(frozen=True, slots=True)
class OAuthConsentOutcome:
    """The observed result of one consent decision, never a fabricated consent.

    Stock answers a redirect or a confirmation page rather than a consent document, and never reports which decision the
    user made. The outcome therefore carries only what the deployment actually returned, so an accept and a decline stay
    distinguishable to the caller.
    """

    accepted: bool
    status_code: int
    media_type: str

    def __post_init__(self) -> None:
        if type(self.accepted) is not bool:
            raise ValueError("uData OAuth consent outcome must record a boolean decision.")
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise ValueError("uData OAuth consent outcome must carry a valid HTTP status code.")
        _text(self.media_type, _MEDIA_TYPE)

    def to_dict(self) -> dict[str, object]:
        return {
            "accepted": self.accepted,
            "status_code": self.status_code,
            "media_type": self.media_type,
        }


@dataclass(frozen=True, slots=True)
class OAuthErrorDocument:
    """The bounded metadata of the stock /oauth/error HTML page.

    The stock route renders ``api/oauth_error.html`` and never returns a JSON body, so only the status and media type
    are typed here. The rendered bytes are deliberately not retained.
    """

    status_code: int
    media_type: str
    session_gated: bool = True

    def __post_init__(self) -> None:
        if type(self.status_code) is not int or self.status_code < 100 or self.status_code > 599:
            raise ValueError("uData OAuth error status must be a valid HTTP status code.")
        _text(self.media_type, _MEDIA_TYPE)

    def to_dict(self) -> dict[str, object]:
        return {"session_gated": self.session_gated, "status_code": self.status_code, "media_type": self.media_type}
