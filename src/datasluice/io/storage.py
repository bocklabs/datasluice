"""Storage abstraction for reading and writing resource files.

Currently supports the local filesystem.  This protocol allows future
backends (S3, GCS, etc.) to be plugged in without changing the download
pipeline.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from datasluice.io.local import ensure_dir, save_bytes


class Storage(ABC):
    """Abstract storage backend."""

    @abstractmethod
    def write(self, data: bytes, key: str) -> str:
        """Persist *data* under *key* and return the storage URI/path."""

    @abstractmethod
    def read(self, key: str) -> bytes:
        """Read and return the bytes stored under *key*."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return ``True`` if *key* exists in storage."""


class LocalStorage(Storage):
    """Local-filesystem storage backend.

    Args:
        base_dir: Root directory for stored files.
    """

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = ensure_dir(base_dir)

    def write(self, data: bytes, key: str) -> str:
        path = self.base_dir / key
        return str(save_bytes(data, path))

    def read(self, key: str) -> bytes:
        path = self.base_dir / key
        return path.read_bytes()

    def exists(self, key: str) -> bool:
        return (self.base_dir / key).exists()
