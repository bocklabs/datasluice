"""Transport port Protocols for HTTP-like request execution.

Two narrow runtime-checkable Protocol classes live here, co-located per RESEARCH
Open Question 2 (co-location keeps the streaming contract visible next to the
buffered one). They are deliberately separate so a transport can implement
buffered-only ``Transport`` without advertising streaming capability, letting
Phase 4 ``ResourceReader`` probe ``isinstance(transport, StreamingTransport)``
without depending on any backend-specific types (D-P3-06/D-P3-07).
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Transport(Protocol):
    """Transport boundary Protocol satisfied structurally by HTTP clients."""

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> bytes: ...

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]: ...

    def download(self, url: str, **kwargs: Any) -> bytes: ...


@runtime_checkable
class StreamingTransport(Protocol):
    """Streaming transport boundary Protocol (D-P3-06/D-P3-07).

    `stream(url)` returns a context manager whose target object is both:

    * **iterable** — yielding `bytes` chunks of the response body, consumed by
      Phase 4 download-to-cache / ResourceReader paths; and
    * exposes a ``headers: dict[str, str]`` attribute so callers can capture
      ``ETag`` / ``Last-Modified`` for cache revalidation (D-P3-12).

    The yielded object is intentionally typed as ``Any`` at the Protocol level:
    the concrete wrapper (e.g. ``StreamResponse``) lives behind the transport
    boundary and must not leak into the port, keeping the port backend-agnostic
    and free of any httpx imports (D-P3-07).
    """

    def stream(self, url: str, **kwargs: Any) -> AbstractContextManager[Any]: ...
