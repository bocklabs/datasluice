"""Validate each redirect target before a data-plane HTTP request."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urljoin

from datasluice._uri import sanitize_uri, validate_http_target
from datasluice.runtime.transport.base import RuntimeResponse, RuntimeStreamResponse

if TYPE_CHECKING:
    from collections.abc import Callable

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_MAX_HOPS = 10


def follow_redirects[T: (RuntimeResponse, RuntimeStreamResponse)](
    url: str,
    *,
    allow_private: bool,
    label: str,
    error: type[Exception],
    send: Callable[[str], T],
    on_discard: Callable[[T], None] | None = None,
) -> T:
    """Send a GET request through at most ten validated redirects.

    Args:
        url: Initial request URL.
        allow_private: Permit private and loopback targets.
        label: Operation name for error messages.
        error: Exception class for invalid targets, failed requests, or too many redirects.
        send: Send one HTTP request and return its response.
        on_discard: Close each discarded response before the next request or error.

    Returns:
        The final successful (2xx) response.
    """
    for hop in range(_MAX_HOPS + 1):
        try:
            validate_http_target(url, allow_private=allow_private, resolve=True)
        except ValueError as exc:
            raise error(f"{label} target is invalid ({exc})") from None
        response = send(url)
        if 200 <= response.status_code < 300:
            return response
        headers = {key.lower(): value for key, value in response.headers.items()}
        if on_discard is not None:
            on_discard(response)
        if response.status_code not in _REDIRECT_STATUSES or "location" not in headers:
            raise error(f"{label} for {sanitize_uri(url)!r} returned HTTP {response.status_code}")
        if hop == _MAX_HOPS:
            raise error(f"{label} for {sanitize_uri(url)!r} exceeded its redirect limit")
        try:
            url = urljoin(url, headers["location"])
        except ValueError:
            raise error(f"{label} for {sanitize_uri(url)!r} returned an unusable redirect Location") from None
    raise AssertionError("unreachable")
