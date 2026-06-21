"""Simple file-based cache for downloaded resources."""

from __future__ import annotations

import os
import time
from pathlib import Path

from datasluice.logging import get_logger

logger = get_logger("io.cache")


class FileCache:
    """A time-based file cache.

    Args:
        cache_dir: Directory to store cached files.
        ttl: Time-to-live in seconds; entries older than this are evicted.
    """

    def __init__(self, cache_dir: str | Path, ttl: int = 3600) -> None:
        self.cache_dir = Path(cache_dir)
        self.ttl = ttl
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _key_path(self, key: str) -> Path:
        safe = key.replace("/", "_").replace(":", "_")
        return self.cache_dir / safe

    def get(self, key: str) -> bytes | None:
        """Return cached bytes for *key*, or ``None`` if missing/expired."""
        path = self._key_path(key)
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > self.ttl:
            logger.debug("Cache miss (expired): %s", key)
            return None
        logger.debug("Cache hit: %s", key)
        return path.read_bytes()

    def put(self, key: str, data: bytes) -> Path:
        """Store *data* under *key* and return the cache file path."""
        path = self._key_path(key)
        path.write_bytes(data)
        os.chmod(path, 0o644)
        return path

    def has(self, key: str) -> bool:
        """Return ``True`` if *key* is cached and not expired."""
        return self.get(key) is not None

    def clear(self) -> None:
        """Remove all entries from the cache."""
        for entry in self.cache_dir.iterdir():
            if entry.is_file():
                entry.unlink()
