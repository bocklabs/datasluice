"""Transport port Protocol for HTTP-like request execution."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Transport(Protocol):
    """Transport boundary Protocol satisfied structurally by HTTP clients."""

    def request(self, url: str, *, method: str = "GET", **kwargs: Any) -> bytes: ...

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]: ...

    def download(self, url: str, **kwargs: Any) -> bytes: ...
