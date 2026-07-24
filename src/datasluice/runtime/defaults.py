"""Default transport factory for the DataSluiceSession composition root.

Replaces the former ``DataSluice._build_transport`` method by reading the
``DEFAULT_*`` constants from :mod:`datasluice.config.defaults` directly (D-14)
instead of the removed ``Settings`` dataclass.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.auth import NoAuth
from datasluice.config.defaults import DEFAULT_RATE_LIMIT, DEFAULT_RETRIES, DEFAULT_TIMEOUT
from datasluice.transport import HttpClient, RateLimiter, RetryPolicy
from datasluice.transport.user_agent import build_user_agent

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth


def create_default_transport(auth: BaseAuth | None = None) -> HttpClient:
    """Construct a default :class:`HttpClient` from ``DEFAULT_*`` constants.

    Args:
        auth: Optional auth strategy; defaults to :class:`NoAuth` when omitted.

    Returns:
        A configured :class:`HttpClient` instance.
    """
    rate_limiter = RateLimiter(requests_per_second=DEFAULT_RATE_LIMIT) if DEFAULT_RATE_LIMIT else None
    retry_policy = RetryPolicy(max_attempts=DEFAULT_RETRIES)
    return HttpClient(
        auth=auth or NoAuth(),
        timeout=DEFAULT_TIMEOUT,
        retry_policy=retry_policy,
        rate_limiter=rate_limiter,
        user_agent=build_user_agent(),
    )
