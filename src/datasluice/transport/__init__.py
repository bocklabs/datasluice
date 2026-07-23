"""Transport layer: HTTP client, retry, rate-limiting, and pagination."""

from datasluice.transport.http_client import HttpClient
from datasluice.transport.pagination import PaginationConfig, paginate
from datasluice.transport.rate_limit import RateLimiter
from datasluice.transport.redirect import CredentialAwareRedirectHandler
from datasluice.transport.retry import RetryPolicy, with_retry
from datasluice.transport.user_agent import build_user_agent

__all__ = [
    "HttpClient",
    "RetryPolicy",
    "with_retry",
    "RateLimiter",
    "PaginationConfig",
    "paginate",
    "build_user_agent",
    "CredentialAwareRedirectHandler",
]
