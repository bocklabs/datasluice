"""Transport port Protocols for HTTP-like request execution.

Three narrow runtime-checkable Protocol classes live here, co-located per
RESEARCH Open Question 2 (co-location keeps streaming and conditional-fetch
contracts visible next to the buffered one). They are deliberately separate so
a transport advertises only capabilities it actually implements, letting
callers probe with ``isinstance`` without backend-specific types
.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
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
    """Streaming transport boundary Protocol.

    `stream(url)` returns a context manager whose target object is both:

    * **iterable** — yielding `bytes` chunks of the response body, consumed by
      download-to-cache / ResourceReader paths; and
    * exposes a ``headers: dict[str, str]`` attribute so callers can capture
      ``ETag`` / ``Last-Modified`` for cache revalidation.

    The yielded object is intentionally typed as ``Any`` at the Protocol level:
    the concrete wrapper (e.g. ``StreamResponse``) lives behind the transport
    boundary and must not leak into the port, keeping the port backend-agnostic
    and free of any httpx imports.
    """

    def stream(self, url: str, **kwargs: Any) -> AbstractContextManager[Any]: ...


@dataclass(frozen=True)
class ConditionalFetchResult:
    """Result of a conditional resource fetch.

        A 304 Not Modified response is a normal healthy outcome represented by
        ``status_code == 304`` and ``stream is None`` — never an exception. On a
        successful fetch, ``stream`` is a context manager yielding the same
        backend-agnostic iterable-byte wrapper as :class:`StreamingTransport`.
        ``headers`` remains ``Any`` so the port stays free of httpx imports
    .
    """

    status_code: int
    headers: Any
    stream: AbstractContextManager[Any] | None


@runtime_checkable
class ConditionalTransport(Protocol):
    """Optional transport capability for ETag/Last-Modified conditional GETs.

    Implementations surface 304 as :class:`ConditionalFetchResult` rather
    than hiding the status behind a bytes-only response. The urllib fallback
    deliberately does not implement this Protocol.
    """

    def conditional_fetch(
        self,
        url: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
    ) -> ConditionalFetchResult: ...
