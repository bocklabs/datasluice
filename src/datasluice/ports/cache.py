"""Cache port Protocol for content-addressed byte caching."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class CachePort(Protocol):
    """Boundary protocol for a simple key/byte cache."""

    def get(self, key: str) -> bytes | None: ...

    def put(self, key: str, data: bytes) -> None: ...

    def delete(self, key: str) -> None: ...
