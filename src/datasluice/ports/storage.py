"""Storage port Protocol returning URI references."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class StoragePort(Protocol):
    """Boundary protocol for byte storage addressed by path/URI strings."""

    def write(self, data: bytes, path: str) -> str: ...

    def read(self, path: str) -> bytes: ...

    def exists(self, path: str) -> bool: ...
