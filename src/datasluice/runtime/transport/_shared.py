from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from datasluice.domain import CredentialScope
from datasluice.runtime.transport.base import (
    RedirectPolicy,
    RuntimeRequest,
    TransportFailure,
    drop_body_transfer_headers,
    redirect_method_and_body,
    strip_sensitive_redirect_headers,
)

ALLOWED_REDIRECT_SCHEMES = frozenset({"http", "https"})
_CREDENTIAL_PARTS = (
    "api_key",
    "apikey",
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
    "authorization",
    "signature",
)


def _retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(0.0, (parsed - datetime.now(UTC)).total_seconds())


def _origin(url: str) -> tuple[str, str, int | None]:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        return "", "", None
    return parsed.scheme.lower(), parsed.hostname or "", port or (443 if parsed.scheme == "https" else 80)


def _redacted_redirect_url(url: str) -> str:
    """Render *url* for exception surfaces with credential-shaped query params removed."""
    try:
        parsed = urlsplit(url)
        query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if not any(part in key.lower() for part in _CREDENTIAL_PARTS)
            ]
        )
    except ValueError:
        return "<unparseable-redirect-target>"
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _retains_credentials(scope: CredentialScope | None, current_url: str, next_url: str) -> bool:
    if scope is None:
        return _origin(current_url) == _origin(next_url)
    scheme, host, _ = _origin(next_url)
    return scope.send_on_redirect and scheme in scope.allowed_schemes and host in scope.allowed_hosts


def _enforce_body_limit(size: int, max_bytes: int) -> None:
    if size > max_bytes:
        raise TransportFailure("The catalog response exceeds its configured byte limit.")


def _next_redirect_request(
    request: RuntimeRequest,
    status: int,
    next_url: str,
    credential_scope: CredentialScope | None,
    label: str,
) -> RuntimeRequest:
    headers = dict(request.headers)
    if not _retains_credentials(credential_scope, request.url, next_url):
        headers = strip_sensitive_redirect_headers(headers)
    method, body, files = redirect_method_and_body(request.method, status, request.body, request.files)
    if (body is not None or files) and _origin(request.url) != _origin(next_url):
        raise TransportFailure(
            f"{label} refused to relay a request body to a different redirect origin "
            f"{_redacted_redirect_url(next_url)!r}."
        )
    if body is None and not files:
        headers = drop_body_transfer_headers(headers)
    return RuntimeRequest(
        method=method,
        url=next_url,
        headers=headers,
        body=body,
        files=files,
        redirect_policy=request.redirect_policy,
        max_response_bytes=request.max_response_bytes,
    )


def _follow_location(request: RuntimeRequest, response: Any) -> str | None:
    if request.redirect_policy is RedirectPolicy.NO_FOLLOW or not response.is_redirect:
        return None
    return response.headers.get("location")
