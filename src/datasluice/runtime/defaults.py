"""Default transport factory for the DataSluiceSession composition root.

Replaces the former ``DataSluice._build_transport`` method by reading the
``DEFAULT_*`` constants from :mod:`datasluice.config.defaults` directly
instead of the removed ``Settings`` dataclass.

: the factory now picks :class:`HttpxTransport` when httpx is
importable and falls back to the urllib :class:`HttpClient` for bare installs
. The return type widens to the :class:`Transport` port so callers
treat either backend uniformly.
"""

from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING

from datasluice.auth import NoAuth
from datasluice.config.defaults import DEFAULT_RATE_LIMIT, DEFAULT_RETRIES, DEFAULT_TIMEOUT
from datasluice.logging import get_logger
from datasluice.transport import HttpClient, RateLimiter, RetryPolicy
from datasluice.transport.user_agent import build_user_agent

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth
    from datasluice.ports import CredentialProvider, Transport

logger = get_logger("runtime.defaults")


def create_default_transport(
    auth: BaseAuth | None = None,
    *,
    credential_provider: CredentialProvider | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    rate_limit: float | None = DEFAULT_RATE_LIMIT,
) -> Transport:
    """Construct a default transport from ``DEFAULT_*`` constants.

    Picks :class:`HttpxTransport` when httpx is importable, falling
    back to the urllib :class:`HttpClient` for bare installs. The
    httpx availability check uses ``importlib.util.find_spec`` so the module is
    never eagerly imported at this call site (keeps bare installs clean).

    Args:
        auth: Optional auth strategy; defaults to :class:`NoAuth` when omitted.
        credential_provider: Optional credential provider forwarded to
            :class:`HttpxTransport` for 401/403 eviction.
        timeout: Request timeout in seconds.
        retries: Maximum number of attempts for the retry policy.
        rate_limit: Optional requests-per-second cap (``None`` disables).

    Returns:
        A transport instance satisfying the :class:`Transport` port.
    """

    retry_policy = RetryPolicy(max_attempts=retries)
    rate_limiter = RateLimiter(requests_per_second=rate_limit) if rate_limit else None
    if importlib.util.find_spec("httpx") is None:
        if credential_provider is not None:
            logger.debug(
                "httpx unavailable: falling back to urllib HttpClient, which does not support "
                "credential_provider eviction"
            )
        return HttpClient(
            auth=auth or NoAuth(),
            timeout=timeout,
            retry_policy=retry_policy,
            rate_limiter=rate_limiter,
            user_agent=build_user_agent(),
        )
    from datasluice.transport.httpx_transport import HttpxTransport

    return HttpxTransport(
        auth=auth or NoAuth(),
        credential_provider=credential_provider,
        timeout=timeout,
        retry_policy=retry_policy,
        rate_limiter=rate_limiter,
        user_agent=build_user_agent(),
    )
