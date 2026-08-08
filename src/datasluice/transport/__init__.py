"""Transport layer: HTTP client, retry, rate-limiting, and pagination.

``HttpxTransport`` and ``StreamResponse`` are exported lazily via PEP 562
module-level attribute resolution so that bare installs without the ``http``
extra never import httpx at module load (D-P3-01). Eager imports of the
urllib ``HttpClient`` and the retry/rate-limit/pagination helpers are
unaffected.
"""

from datasluice.transport.http_client import HttpClient
from datasluice.transport.pagination import PaginationConfig, paginate
from datasluice.transport.rate_limit import RateLimiter
from datasluice.transport.redirect import CredentialAwareRedirectHandler
from datasluice.transport.retry import RetryPolicy, with_retry
from datasluice.transport.user_agent import build_user_agent

__all__ = [
    "CredentialAwareRedirectHandler",
    "HttpClient",
    "HttpxTransport",
    "PaginationConfig",
    "RateLimiter",
    "RetryPolicy",
    "StreamResponse",
    "build_user_agent",
    "paginate",
    "with_retry",
]


def __getattr__(name: str):  # type: ignore[no-untyped-def]
    """Lazily resolve httpx-backed symbols on first attribute access (PEP 562)."""

    if name == "HttpxTransport":
        from datasluice.transport.httpx_transport import HttpxTransport

        return HttpxTransport
    if name == "StreamResponse":
        from datasluice.transport.httpx_transport import StreamResponse

        return StreamResponse
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
