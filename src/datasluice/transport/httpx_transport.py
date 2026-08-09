"""httpx-backed HTTP transport satisfying the Transport + StreamingTransport Protocols.

Replaces urllib as the default transport (when the ``http`` extra is installed)
while preserving the security semantics verbatim:

* ``follow_redirects=False`` plus a manual redirect loop applies the
  ``CredentialScope`` policy per hop — httpx ``event_hooks``
  cannot express the same-host-but-not-in-scope stripping case (Pattern 1).
* Each per-attempt call is wrapped in the existing ``with_retry`` /
  ``RetryPolicy``. httpx native transport-level retries are
  intentionally NOT used.
* 401/403 against a ``HostCredentialProvider`` evicts and refreshes exactly
  once; a ``_refreshed`` flag survives ``with_retry`` attempts to
  prevent an eviction loop.

All httpx imports are lazy (inside ``__init__`` / methods) so bare installs
without the ``http`` extra never import httpx at module load.
"""

from __future__ import annotations

import importlib
import json as json_module
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urlparse

from datasluice._uri import sanitize_uri
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
    from datasluice.ports.transport import ConditionalFetchResult

logger = get_logger("transport.httpx")


def _default_port(scheme: str | None) -> int | None:
    """Return the IANA default port for *scheme*, or ``None`` when unknown."""
    if scheme == "https":
        return 443
    if scheme == "http":
        return 80
    return None


def _effective_origin(parsed: Any) -> tuple[str, str, int | None]:
    """Return ``(scheme, hostname, effective_port)`` for a parsed URL.

    ``effective_port`` falls back to the IANA default for the scheme so a
    redirect from ``https://host:443`` to ``https://host`` is treated as the
    same origin, while ``https://host`` to ``https://host:8443`` is not.
    """
    scheme = (parsed.scheme or "").lower()
    try:
        port = parsed.port
    except ValueError:
        port = None
    if port is None:
        port = _default_port(scheme)
    return scheme, parsed.hostname or "", port


def _url_with_params(url: str, params: dict[str, Any]) -> str:
    """Return *url* with *params* appended (preserving any existing query).

    Used to rebuild auth-applied URLs from a base URL plus refreshed auth
    params.
    """
    if not params:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{urlencode(params, doseq=True)}"


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
    """Backend-agnostic streaming response wrapper.

    Iterable for byte chunks (via httpx ``iter_raw`` so gzip pass-through
    decoding surprises are avoided — compression decorators own
    decompression), and exposes a ``headers`` dict so callers can capture
    ``ETag`` / ``Last-Modified`` for cache revalidation.
    """

    def __init__(self, httpx_response: httpx.Response) -> None:
        self._response = httpx_response
        self.headers: httpx.Headers = httpx_response.headers

    def __iter__(self) -> Iterator[bytes]:
        yield from self._response.iter_raw()

    def close(self) -> None:
        """Release the underlying httpx response."""
        self._response.close()


@contextmanager
def _stream_response(response: httpx.Response) -> Iterator[StreamResponse]:
    """Yield a :class:`StreamResponse` and deterministically close its response."""
    wrapped = StreamResponse(response)
    try:
        yield wrapped
    finally:
        wrapped.close()


class HttpxTransport:
    """HTTP transport backed by httpx, satisfying Transport + StreamingTransport.

    Drives redirects manually so the ``CredentialScope`` policy is
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
        import httpx  # lazy

        self.auth = auth or NoAuth()
        self._credential_provider = credential_provider
        self.timeout = timeout
        self.retry_policy = retry_policy or RetryPolicy()
        self.rate_limiter = rate_limiter
        self.user_agent = user_agent or build_user_agent()
        self._credential_scope = credential_scope
        self._max_redirects = max_redirects
        self._closed = False
        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout),
            follow_redirects=False,
        )

    def close(self) -> None:
        """Close the pooled HTTP client exactly once."""
        if self._closed:
            return
        self._closed = True
        self._client.close()

    def __enter__(self) -> HttpxTransport:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _should_strip_authorization(self, old_url: str, new_url: str) -> bool:
        """Return whether sensitive headers must be stripped on this redirect hop.

        Mirrors ``CredentialAwareRedirectHandler``: under an explicit
        ``CredentialScope`` the new host/scheme must both be allowed (and
        ``send_on_redirect`` true); under zero-config, any cross-host hop or
        ``https`` → ``http`` downgrade strips. ``https`` → ``http`` always
        strips regardless of scope.
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
        # Compare scheme + hostname + effective port so a redirect from
        # https://host:443 to https://host:8443 (same hostname, different port)
        # strips Authorization — the original hostname-only check leaked
        # credentials to a different service on the same host.
        return scheme_downgrade or _effective_origin(old_parsed) != _effective_origin(new_parsed)

    def _send_with_redirects(
        self,
        url: str,
        method: str,
        headers: dict[str, str],
        body: bytes | None,
        *,
        stream: bool = False,
    ) -> httpx.Response:
        """Drive the manual redirect loop, stripping sensitive headers per hop (Pattern 1)."""

        current_url = url
        req = self._client.build_request(method, current_url, headers=headers, content=body)
        response = self._send_translating(req, stream=stream)
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
                logger.debug("Stripped sensitive headers on redirect to %s", sanitize_uri(next_url))
            current_url = next_url
            response.close()
            response = self._send_translating(next_request, stream=stream)
        if response.has_redirect_location:
            response.close()
            raise PortalError(f"Redirect limit {self._max_redirects} exceeded for {url}")
        return response

    def _send_translating(self, request: httpx.Request, *, stream: bool) -> httpx.Response:
        """Send a request, translating httpx network errors into DataSluice exceptions.

        httpx ``TransportError``/``TimeoutException`` are not ``OSError`` subclasses, so without
        this wrapper they would neither be retried by ``RetryPolicy`` nor surface as a
        ``PortalError`` across the port boundary.
        """
        import httpx  # lazy

        try:
            return self._client.send(request, follow_redirects=False, stream=stream)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise RetryableHTTPError(f"Transient httpx failure: {exc}") from exc
        except httpx.HTTPError as exc:
            raise PortalError(f"httpx request failed: {exc}") from exc

    def _try_credential_refresh(
        self,
        *,
        response: httpx.Response,
        base_url: str,
        method: str,
        headers: dict[str, str],
        body: bytes | None,
        status: int,
        refreshed: bool,
        params_box: list[dict[str, Any]],
        stream: bool = False,
    ) -> tuple[httpx.Response, int, bool]:
        """Evict and re-apply credentials for a 401/403 exactly once.

                Re-issues the request against the refreshed credential when the
                transport is bound to a :class:`HostCredentialProvider` and a refresh
                has not already occurred. The rejected *response* is closed BEFORE the
                retry so the connection it occupies is released back to the pool
        . *headers* and *params_box* are mutated in place: the refreshed
                auth's header credentials overwrite *headers* and its query credentials
                overwrite the entries in ``params_box[0]``, then the request URL is
                rebuilt from *base_url* with the refreshed params so stale query
                credentials cannot survive into the retry. Returns
                ``(response, status, refreshed)``. When no refresh is applicable the
                original *response* and *status* are returned unchanged.
        """
        if status not in (401, 403) or refreshed:
            return response, status, False
        provider_type = _host_credential_provider_type()
        credential_provider = self._credential_provider
        if provider_type is None or credential_provider is None or not isinstance(credential_provider, provider_type):
            return response, status, False
        host = urlparse(base_url).hostname or ""
        credential_provider.evict(host)
        new_auth = credential_provider.resolve(host)
        applied_headers, applied_params = new_auth.apply(dict(headers), params_box[0])
        headers.clear()
        headers.update(applied_headers)
        params_box[0] = applied_params
        refreshed_url = _url_with_params(base_url, applied_params)
        # Close the rejected response before issuing the retry so the original
        # connection is released; otherwise a single-connection client pool
        # raises PoolTimeout when the retry tries to acquire a second slot
        # while the streamed 401/403 body remains unread.
        try:
            response.close()
        except Exception:
            logger.debug("Ignored error closing rejected 401/403 response before credential refresh", exc_info=True)
        refreshed_response = self._send_with_redirects(refreshed_url, method, headers, body, stream=stream)
        return refreshed_response, refreshed_response.status_code, True

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

        base_url = url
        request_headers = dict(headers or {})
        request_headers.setdefault("User-Agent", self.user_agent)
        # Always preserve the auth-applied params: the previous code
        # discarded query credentials when the caller supplied no params of
        # its own, so APIKeyAuth(in_query=True) sent /x instead of /x?api_key=...
        request_headers, auth_params = self.auth.apply(request_headers, params or {})
        params_box: list[dict[str, Any]] = [auth_params]

        refreshed: list[bool] = [False]

        def _do_request() -> bytes:
            if self.rate_limiter:
                self.rate_limiter.acquire()
            current_url = _url_with_params(base_url, params_box[0])
            display_url = sanitize_uri(current_url)
            logger.debug("%s %s", method, display_url)
            response = self._send_with_redirects(current_url, method, request_headers, body)
            status = response.status_code

            if status in (401, 403):
                refreshed_response, status, did_refresh = self._try_credential_refresh(
                    response=response,
                    base_url=base_url,
                    method=method,
                    headers=request_headers,
                    body=body,
                    status=status,
                    refreshed=refreshed[0],
                    params_box=params_box,
                )
                refreshed[0] = refreshed[0] or did_refresh
                response = refreshed_response
                current_url = _url_with_params(base_url, params_box[0])
                display_url = sanitize_uri(current_url)
                if did_refresh and status in (401, 403):
                    response.read()
                    raise PortalError(
                        f"HTTP {status} from {display_url} after credential refresh: {_truncate_body(response.content)}"
                    )

            response.read()
            if status == 429:
                raise RateLimitError(
                    f"Rate limited by {display_url}",
                    retry_after=_parse_retry_after(response.headers.get("Retry-After")),
                )
            if status >= 500:
                raise RetryableHTTPError(
                    f"HTTP {status} from {display_url}: {_truncate_body(response.content)}",
                    status_code=status,
                )
            if status >= 400:
                raise PortalError(f"HTTP {status} from {display_url}: {_truncate_body(response.content)}")
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

    def conditional_fetch(
        self,
        url: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
    ) -> ConditionalFetchResult:
        """Fetch *url* conditionally, surfacing 304 as a normal result.

        Conditional validators are opaque server-provided strings and are sent
        verbatim. Redirects use the existing manual security loop: auth headers
        may be stripped, while conditional headers survive hops. Does not use
        ``with_retry`` — resumable reads belong to the sync layer, matching
        :meth:`stream`.
        """
        from datasluice.ports.transport import ConditionalFetchResult

        base_url = url
        headers: dict[str, str] = {}
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match
        if if_modified_since is not None:
            headers["If-Modified-Since"] = if_modified_since
        headers.setdefault("User-Agent", self.user_agent)
        headers, auth_params = self.auth.apply(headers, {})
        params_box: list[dict[str, Any]] = [auth_params]
        current_url = _url_with_params(base_url, auth_params)
        display_url = sanitize_uri(current_url)

        response = self._send_with_redirects(current_url, "GET", headers, None, stream=True)
        status = response.status_code

        if status in (401, 403):
            response, status, _did_refresh = self._try_credential_refresh(
                response=response,
                base_url=base_url,
                method="GET",
                headers=headers,
                body=None,
                status=status,
                refreshed=False,
                params_box=params_box,
                stream=True,
            )
            current_url = _url_with_params(base_url, params_box[0])
            display_url = sanitize_uri(current_url)

        response_headers = response.headers

        if status == 304:
            response.read()
            response.close()
            return ConditionalFetchResult(status, response_headers, None)
        if status >= 400:
            response.read()
            response.close()
            if status == 429:
                raise RateLimitError(
                    f"Rate limited by {display_url}",
                    retry_after=_parse_retry_after(response_headers.get("Retry-After")),
                )
            if status >= 500:
                raise RetryableHTTPError(
                    f"HTTP {status} from {display_url}: {_truncate_body(response.content)}",
                    status_code=status,
                )
            raise PortalError(f"HTTP {status} from {display_url}: {_truncate_body(response.content)}")
        return ConditionalFetchResult(status, response_headers, _stream_response(response))

    @contextmanager
    def stream(self, url: str, **kwargs: Any) -> Iterator[StreamResponse]:
        """Yield a :class:`StreamResponse` for streaming the response body.

        Does NOT wrap in ``with_retry`` — resumable streaming reads are the
        caller's concern (via checkpoint state). Maps >=400 status codes to the existing
        exception hierarchy before yielding so callers never see an error
        response as a successful stream.
        """

        base_url = url
        headers = dict(kwargs.pop("headers", None) or {})
        headers.setdefault("User-Agent", self.user_agent)
        headers, auth_params = self.auth.apply(headers, {})
        params_box: list[dict[str, Any]] = [auth_params]
        current_url = _url_with_params(base_url, auth_params)
        display_url = sanitize_uri(current_url)
        response = self._send_with_redirects(current_url, "GET", headers, None, stream=True)
        # Track the actual final response (which may change after a refresh) so
        # the finally block closes the response the caller actually consumed —
        # the original code closed the rejected 401/403 response instead of the
        # refreshed one, leaking the refreshed connection.
        resp = response
        try:
            if resp.status_code in (401, 403):
                refreshed_response, status, did_refresh = self._try_credential_refresh(
                    response=resp,
                    base_url=base_url,
                    method="GET",
                    headers=headers,
                    body=None,
                    status=resp.status_code,
                    refreshed=False,
                    params_box=params_box,
                    stream=True,
                )
                resp = refreshed_response
                current_url = _url_with_params(base_url, params_box[0])
                display_url = sanitize_uri(current_url)
                if did_refresh and resp.status_code in (401, 403):
                    resp.read()
                    raise PortalError(
                        f"HTTP {resp.status_code} from {display_url} after credential refresh: "
                        f"{_truncate_body(resp.content)}"
                    )
            if resp.status_code >= 400:
                resp.read()
                if resp.status_code == 429:
                    raise RateLimitError(
                        f"Rate limited by {display_url}",
                        retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
                    )
                if resp.status_code >= 500:
                    raise RetryableHTTPError(
                        f"HTTP {resp.status_code} from {display_url}: {_truncate_body(resp.content)}",
                        status_code=resp.status_code,
                    )
                raise PortalError(f"HTTP {resp.status_code} from {display_url}: {_truncate_body(resp.content)}")
            yield StreamResponse(resp)
        finally:
            resp.close()
