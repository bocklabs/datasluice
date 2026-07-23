"""HTTP client with retry, rate-limiting, and authentication support."""

from __future__ import annotations

import email.utils
import json as json_module
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from datasluice.auth import NoAuth
from datasluice.config.defaults import DEFAULT_TIMEOUT
from datasluice.exceptions import PortalError, RateLimitError, RetryableHTTPError
from datasluice.logging import get_logger
from datasluice.transport.rate_limit import RateLimiter
from datasluice.transport.redirect import CredentialAwareRedirectHandler
from datasluice.transport.retry import RetryPolicy, with_retry
from datasluice.transport.user_agent import build_user_agent

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth
    from datasluice.domain import CredentialScope

logger = get_logger("transport.http")


def _parse_retry_after(raw: str | None) -> float | None:
    """Parse a ``Retry-After`` header into a delay in seconds.

    Supports both delta-seconds and HTTP-date formats. Returns ``None`` when the
    value is missing or cannot be parsed.
    """
    if raw is None:
        return None
    text = raw.strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        parsed_date = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed_date is None:
        return None
    if parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=UTC)
    delay = (parsed_date - datetime.now(UTC)).total_seconds()
    return max(delay, 0.0)


def _truncate_body(body: Any, limit: int = 200) -> str:
    """Render *body* as text, truncating to *limit* characters."""
    if isinstance(body, bytes):
        text = body.decode("utf-8", errors="replace")
    else:
        text = str(body)
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


class HttpClient:
    """Thin HTTP client wrapping :mod:`urllib` with auth, retry, and rate-limiting.

    Args:
        auth: Authentication strategy (defaults to :class:`NoAuth`).
        timeout: Request timeout in seconds.
        retry_policy: Retry configuration.
        rate_limiter: Optional rate limiter.
        user_agent: Custom User-Agent string.
        credential_scope: Optional host-scoped credential policy applied to
            redirects. When omitted, any cross-host redirect strips credentials
            (zero-config safety).
    """

    def __init__(
        self,
        *,
        auth: BaseAuth | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        retry_policy: RetryPolicy | None = None,
        rate_limiter: RateLimiter | None = None,
        user_agent: str | None = None,
        credential_scope: CredentialScope | None = None,
    ) -> None:
        self.auth = auth or NoAuth()
        self.timeout = timeout
        self.retry_policy = retry_policy or RetryPolicy()
        self.rate_limiter = rate_limiter
        self.user_agent = user_agent or build_user_agent()
        self._credential_scope = credential_scope
        self._opener = urllib.request.build_opener(CredentialAwareRedirectHandler(self._credential_scope))

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> bytes:
        """Perform an HTTP request and return the raw response body.

        Raises:
            PortalError: On non-2xx responses.
            RateLimitError: On HTTP 429.
            RetryableHTTPError: On HTTP 5xx responses.
        """
        headers = dict(headers or {})
        headers.setdefault("User-Agent", self.user_agent)

        if params:
            headers, params = self.auth.apply(headers, params)
        else:
            headers, _ = self.auth.apply(headers, {})

        if params:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{urllib.parse.urlencode(params)}"

        def _do_request() -> bytes:
            if self.rate_limiter:
                self.rate_limiter.acquire()
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            logger.debug("%s %s", method, url)
            try:
                with self._opener.open(req, timeout=self.timeout) as resp:
                    return resp.read()
            except urllib.error.HTTPError as exc:
                raw = exc.read()
                try:
                    parsed: dict[str, Any] | str = json_module.loads(raw)
                except (ValueError, TypeError):
                    parsed = raw.decode("utf-8", errors="replace")
                if exc.code == 429:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    raise RateLimitError(f"Rate limited by {url}", retry_after=_parse_retry_after(retry_after)) from exc
                if exc.code >= 500:
                    raise RetryableHTTPError(
                        f"HTTP {exc.code} from {url}: {_truncate_body(parsed)}", status_code=exc.code
                    ) from exc
                raise PortalError(f"HTTP {exc.code} from {url}: {_truncate_body(parsed)}") from exc
            except urllib.error.URLError as exc:
                raise PortalError(f"Connection error for {url}: {exc.reason}") from exc

        return with_retry(_do_request, self.retry_policy)

    def get_text(self, url: str, **kwargs: Any) -> str:
        """GET *url* and return the response as text."""
        return self.request(url, method="GET", **kwargs).decode("utf-8")

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        """GET *url* and return the response as parsed JSON."""
        data = self.request(url, method="GET", **kwargs)
        result = json_module.loads(data)
        return result if isinstance(result, dict) else {"data": result}

    def download(self, url: str, **kwargs: Any) -> bytes:
        """GET *url* and return the raw bytes (for file downloads)."""
        return self.request(url, method="GET", **kwargs)
