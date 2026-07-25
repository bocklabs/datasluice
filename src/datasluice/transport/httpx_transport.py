"""httpx-backed HTTP transport satisfying the Transport + StreamingTransport Protocols.

Replaces urllib as the default transport (when the ``http`` extra is installed)
while preserving the Phase 1 security semantics verbatim:

* ``follow_redirects=False`` plus a manual redirect loop applies the
  ``CredentialScope`` policy per hop (SEC-01/SEC-02) — httpx ``event_hooks``
  cannot express the same-host-but-not-in-scope stripping case (Pattern 1).
* Each per-attempt call is wrapped in the existing ``with_retry`` /
  ``RetryPolicy`` (SEC-05). httpx native transport-level retries are
  intentionally NOT used (D-P3-08).
* 401/403 against a ``HostCredentialProvider`` evicts and refreshes exactly
  once (D-P3-15); a ``_refreshed`` flag survives ``with_retry`` attempts to
  prevent an eviction loop.

All httpx imports are lazy (inside ``__init__`` / methods) so bare installs
without the ``http`` extra never import httpx at module load (D-P3-01).
"""

from __future__ import annotations

import importlib
import json as json_module
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urlparse

from datasluice.auth import NoAuth
from datasluice.config.defaults import DEFAULT_TIMEOUT
from datasluice.exceptions import PortalError, RateLimitError, RetryableHTTPError
from datasluice.logging import get_logger
from datasluice.transport.http_client import _parse_retry_after, _truncate_body
from datasluice.transport.rate_limit import RateLimiter
from datasluice.transport.redirect import SENSITIVE_HEADERS
from datasluice.transport.retry import RetryPolicy, with_retry
from datasluice.transport.user_agent import build_user_agent

if TYPE_CHECKING:
    from collections.abc import Iterator

    import httpx

    from datasluice.auth import BaseAuth
    from datasluice.domain import CredentialScope

logger = get_logger("transport.httpx")


def _host_credential_provider_type() -> type[Any] | None:
    """Lazily resolve ``HostCredentialProvider`` (plan 03-04), returning ``None`` if its module has not landed.

    This keeps the 401/403 eviction path correct-by-design even when 03-04
    ships later in the same wave: the ``isinstance`` capability check simply
    evaluates to ``False`` until the type is importable, and the request falls
    through to the normal fail-fast ``PortalError`` mapping.

    Resolved via ``importlib.import_module`` (rather than a static ``import``)
    because the sibling module genuinely does not exist until 03-04 lands — a
    static import would be flagged ``unresolved-import`` by ``ty`` during this
    plan's commit.
    """

    try:
        module = importlib.import_module("datasluice.credentials.host_provider")
    except ImportError:
        return None
    return getattr(module, "HostCredentialProvider", None)


class StreamResponse:
    """Backend-agnostic streaming response wrapper (D-P3-07).

    Iterable for byte chunks (via httpx ``iter_raw`` so gzip pass-through
    decoding surprises are avoided — Phase 4 compression decorators own
    decompression), and exposes a ``headers`` dict so callers can capture
    ``ETag`` / ``Last-Modified`` for cache revalidation (D-P3-12).
    """

    def __init__(self, httpx_response: httpx.Response) -> None:
        self._response = httpx_response
        self.headers: httpx.Headers = httpx_response.headers

    def __iter__(self) -> Iterator[bytes]:
        yield from self._response.iter_raw()

    def close(self) -> None:
        """Release the underlying httpx response."""
        self._response.close()


class HttpxTransport:
    """HTTP transport backed by httpx, satisfying Transport + StreamingTransport.

    Drives redirects manually so the Phase 1 ``CredentialScope`` policy is
    applied per hop, and wraps each per-attempt call in the existing
    ``with_retry`` / ``RetryPolicy``. Construct one instance and reuse it
    across many requests (the underlying ``httpx.Client`` is thread-safe and
    pools connections).
    """

    def __init__(
        self,
        *,
        auth: BaseAuth | None = None,
        credential_provider: Any | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        retry_policy: RetryPolicy | None = None,
        rate_limiter: RateLimiter | None = None,
        user_agent: str | None = None,
        credential_scope: CredentialScope | None = None,
        max_redirects: int = 20,
    ) -> None:
        import httpx  # lazy (D-P3-01)

        self.auth = auth or NoAuth()
        self._credential_provider = credential_provider
        self.timeout = timeout
        self.retry_policy = retry_policy or RetryPolicy()
        self.rate_limiter = rate_limiter
        self.user_agent = user_agent or build_user_agent()
        self._credential_scope = credential_scope
        self._max_redirects = max_redirects
        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout),
            follow_redirects=False,
            max_redirects=max_redirects,
        )

    def _should_strip_authorization(self, old_url: str, new_url: str) -> bool:
        """Return whether sensitive headers must be stripped on this redirect hop.

        Mirrors Phase 1 ``CredentialAwareRedirectHandler``: under an explicit
        ``CredentialScope`` the new host/scheme must both be allowed (and
        ``send_on_redirect`` true); under zero-config, any cross-host hop or
        ``https`` → ``http`` downgrade strips. ``https`` → ``http`` always
        strips regardless of scope (SEC-01/SEC-02).
        """

        old_parsed = urlparse(old_url)
        new_parsed = urlparse(new_url)
        scheme_downgrade = old_parsed.scheme == "https" and new_parsed.scheme == "http"
        scope = self._credential_scope
        if scope is not None:
            return (
                scheme_downgrade
                or new_parsed.hostname not in scope.allowed_hosts
                or new_parsed.scheme not in scope.allowed_schemes
                or not scope.send_on_redirect
            )
        return scheme_downgrade or old_parsed.hostname != new_parsed.hostname

    def _send_with_redirects(
        self,
        url: str,
        method: str,
        headers: dict[str, str],
        body: bytes | None,
    ) -> httpx.Response:
        """Drive the manual redirect loop, stripping sensitive headers per hop (Pattern 1)."""

        current_url = url
        req = self._client.build_request(method, current_url, headers=headers, content=body)
        response = self._client.send(req, follow_redirects=False)
        for _ in range(self._max_redirects):
            if not response.has_redirect_location:
                break
            next_request = response.next_request
            if next_request is None:
                break
            next_url = str(next_request.url)
            if self._should_strip_authorization(current_url, next_url):
                for header_name in list(next_request.headers.keys()):
                    if header_name.lower() in SENSITIVE_HEADERS:
                        del next_request.headers[header_name]
                logger.debug("Stripped sensitive headers on redirect to %s", next_url)
            current_url = next_url
            response = self._client.send(next_request, follow_redirects=False)
        return response

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
            PortalError: On non-2xx 4xx responses (or 401/403 after refresh).
            RateLimitError: On HTTP 429.
            RetryableHTTPError: On HTTP 5xx responses.
        """

        request_headers = dict(headers or {})
        request_headers.setdefault("User-Agent", self.user_agent)

        if params:
            request_headers, params = self.auth.apply(request_headers, params)
        else:
            request_headers, _ = self.auth.apply(request_headers, {})

        if params:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{urlencode(params)}"

        refreshed: list[bool] = [False]
        credential_provider = self._credential_provider

        def _do_request() -> bytes:
            if self.rate_limiter:
                self.rate_limiter.acquire()
            logger.debug("%s %s", method, url)
            response = self._send_with_redirects(url, method, request_headers, body)
            status = response.status_code

            if status in (401, 403):
                provider_type = _host_credential_provider_type()
                if (
                    provider_type is not None
                    and credential_provider is not None
                    and isinstance(credential_provider, provider_type)
                    and not refreshed[0]
                ):
                    refreshed[0] = True
                    host = urlparse(url).hostname or ""
                    credential_provider.evict(host)
                    new_auth = credential_provider.resolve(host)
                    applied, _ = new_auth.apply(dict(request_headers), {})
                    request_headers.clear()
                    request_headers.update(applied)
                    response = self._send_with_redirects(url, method, request_headers, body)
                    status = response.status_code
                    if status in (401, 403):
                        response.read()
                        raise PortalError(
                            f"HTTP {status} from {url} after credential refresh: {_truncate_body(response.content)}"
                        )

            response.read()
            if status == 429:
                raise RateLimitError(
                    f"Rate limited by {url}",
                    retry_after=_parse_retry_after(response.headers.get("Retry-After")),
                )
            if status >= 500:
                raise RetryableHTTPError(
                    f"HTTP {status} from {url}: {_truncate_body(response.content)}",
                    status_code=status,
                )
            if status >= 400:
                raise PortalError(f"HTTP {status} from {url}: {_truncate_body(response.content)}")
            return response.content

        return with_retry(_do_request, self.retry_policy)

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        """GET *url* and return the response as parsed JSON (non-dicts wrapped under ``"data"``)."""

        data = self.request(url, method="GET", **kwargs)
        result = json_module.loads(data)
        return result if isinstance(result, dict) else {"data": result}

    def download(self, url: str, **kwargs: Any) -> bytes:
        """GET *url* and return the raw bytes (for file downloads)."""

        return self.request(url, method="GET", **kwargs)

    @contextmanager
    def stream(self, url: str, **kwargs: Any) -> Iterator[StreamResponse]:
        """Yield a :class:`StreamResponse` for streaming the response body (D-P3-06/D-P3-07).

        Does NOT wrap in ``with_retry`` — resumable streaming reads are Phase 4's
        concern (via checkpoint state). Maps >=400 status codes to the existing
        exception hierarchy before yielding so callers never see an error
        response as a successful stream.
        """

        headers = dict(kwargs.pop("headers", None) or {})
        headers.setdefault("User-Agent", self.user_agent)
        headers, _ = self.auth.apply(headers, {})
        with self._client.stream("GET", url, headers=headers) as resp:
            if resp.status_code >= 400:
                resp.read()
                if resp.status_code == 429:
                    raise RateLimitError(
                        f"Rate limited by {url}",
                        retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
                    )
                if resp.status_code >= 500:
                    raise RetryableHTTPError(
                        f"HTTP {resp.status_code} from {url}: {_truncate_body(resp.content)}",
                        status_code=resp.status_code,
                    )
                raise PortalError(f"HTTP {resp.status_code} from {url}: {_truncate_body(resp.content)}")
            yield StreamResponse(resp)
