from __future__ import annotations

import hashlib
import os
import secrets
import sqlite3
import time
from typing import TYPE_CHECKING, Any

from datasluice.config.defaults import DEFAULT_CACHE_TTL
from datasluice.exceptions import DownloadError
from datasluice.io.filesystem import safe_remove
from datasluice.logging import get_logger

if TYPE_CHECKING:
    import fsspec

_SQL_BEGIN_IMMEDIATE = "BEGIN IMMEDIATE"

logger = get_logger("io.content_cache")

STALE_WRITING_THRESHOLD_SECONDS: float = 300.0
SWEEP_INTERVAL_SECONDS: float = 60.0


class ContentCache:
    """Content-addressed cache with a SQLite WAL metadata index.

    Satisfies :class:`datasluice.ports.cache.CachePort` (get/put/delete) plus
    the sidecar :meth:`put_with_metadata` / :meth:`get_metadata` pair for
    callers that capture ETag/Last-Modified for conditional GETs.

    Args:
        cache_dir: Directory holding ``cache.db`` and the SHA-256-named content
            files. A trailing ``/`` is stripped.
        ttl: Time-to-live in seconds; entries whose ``fetched_at + ttl`` is in
            the past are reported as cache misses. Defaults to
            :data:`DEFAULT_CACHE_TTL`.
        fs: Optional fsspec ``AbstractFileSystem`` for *cache_dir*. When
            omitted, one is constructed lazily via
            :func:`datasluice.io.filesystem.open_filesystem`.
    """

    def __init__(self, cache_dir: str, ttl: int = DEFAULT_CACHE_TTL, fs: Any | None = None) -> None:
        self.cache_dir = cache_dir.rstrip("/")
        self.ttl = ttl
        if fs is not None:
            self._fs: fsspec.AbstractFileSystem = fs
        else:
            from datasluice.io.filesystem import open_filesystem

            self._fs = open_filesystem(f"file://{self.cache_dir}")
        self._fs.makedirs(self.cache_dir, exist_ok=True)
        self._db_path = f"{self.cache_dir}/cache.db"
        self._last_sweep = 0.0
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        """Open a fresh autocommit connection with a busy timeout."""
        conn = sqlite3.connect(self._db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _init_db(self) -> None:
        """Create the schema (idempotent) and enable WAL on the database file."""
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cache ("
                "sha256 TEXT PRIMARY KEY, "
                "url TEXT NOT NULL, "
                "etag TEXT, "
                "last_modified TEXT, "
                "fetched_at REAL NOT NULL, "
                "content_length INTEGER, "
                "status TEXT NOT NULL DEFAULT 'writing'"
                ")"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status_fetched ON cache(status, fetched_at)")
        finally:
            conn.close()

    def _sha(self, key: str) -> str:
        """Return the SHA-256 hexdigest of *key*."""
        return hashlib.sha256(key.encode()).hexdigest()

    def _content_path(self, sha: str) -> str:
        """Return the absolute content-file path for a SHA-256 digest."""
        return f"{self.cache_dir}/{sha}"

    def get(self, key: str) -> bytes | None:
        """Return cached bytes for *key*, or ``None`` on miss / expiry / writing."""
        sha = self._sha(key)
        content_path = self._content_path(sha)
        self._maybe_sweep()
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT etag, last_modified, fetched_at, status FROM cache WHERE sha256=?",
                (sha,),
            ).fetchone()
            if row is None:
                logger.debug("Cache miss (no row): %s", key)
                return None
            _etag, _last_modified, fetched_at, status = row
            if status == "writing":
                logger.debug("Cache miss (writing): %s", key)
                return None
            if fetched_at + self.ttl < time.time():
                logger.debug("Cache miss (expired): %s", key)
                return None
            try:
                data = self._fs.cat_file(content_path)
            except FileNotFoundError:
                logger.debug("Cache miss (orphaned metadata, no content file): %s", key)
                return None
            logger.debug("Cache hit: %s", key)
            return data
        finally:
            conn.close()

    def put(self, key: str, data: bytes) -> None:
        """Store *data* under *key*."""
        self._put_internal(key, data, etag=None, last_modified=None)

    def put_with_metadata(
        self,
        key: str,
        data: bytes,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> None:
        """Store *data* under *key* with ETag/Last-Modified sidecar metadata.

        Callers capture the headers after a fetch and store them alongside the bytes so later conditional GETs can
        revalidate against the origin.
        """
        self._put_internal(key, data, etag=etag, last_modified=last_modified)

    def _put_internal(
        self,
        key: str,
        data: bytes,
        etag: str | None,
        last_modified: str | None,
    ) -> None:
        """Serialize staging, publication, and metadata updates with other writers."""
        sha = self._sha(key)
        content_path = self._content_path(sha)
        tmp_path = f"{self.cache_dir}/.{sha}.tmp.{os.getpid()}.{secrets.token_hex(8)}"
        fetched_at = time.time()

        try:
            self._fs.pipe_file(tmp_path, data)
        except Exception as exc:
            safe_remove(self._fs, tmp_path)
            raise DownloadError(f"Failed to publish cache content for key {key!r}") from exc

        conn = self._connect()
        try:
            conn.execute(_SQL_BEGIN_IMMEDIATE)
            conn.execute(
                "INSERT OR REPLACE INTO cache "
                "(sha256, url, etag, last_modified, fetched_at, content_length, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'ready')",
                (sha, key, etag, last_modified, fetched_at, len(data)),
            )
            self._fs.mv(tmp_path, content_path)
            conn.execute("COMMIT")
        except Exception as exc:
            if conn.in_transaction:
                conn.rollback()
            safe_remove(self._fs, tmp_path)
            raise DownloadError(f"Failed to publish cache content for key {key!r}") from exc
        finally:
            conn.close()

        self._maybe_sweep()

    def delete(self, key: str) -> None:
        """Remove the entry for *key* (CachePort requirement)."""
        sha = self._sha(key)
        content_path = self._content_path(sha)
        conn = self._connect()
        try:
            conn.execute(_SQL_BEGIN_IMMEDIATE)
            conn.execute("DELETE FROM cache WHERE sha256=?", (sha,))
            conn.execute("COMMIT")
        finally:
            conn.close()
        if self._fs.exists(content_path):
            self._fs.rm(content_path)

    def get_metadata(self, key: str) -> dict[str, Any] | None:
        """Return the sidecar metadata for *key*, or ``None`` if missing/writing."""
        sha = self._sha(key)
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT etag, last_modified, fetched_at, content_length, status FROM cache WHERE sha256=?",
                (sha,),
            ).fetchone()
            if row is None:
                return None
            etag, last_modified, fetched_at, content_length, status = row
            if status == "writing" or fetched_at + self.ttl < time.time():
                return None
            return {
                "etag": etag,
                "last_modified": last_modified,
                "fetched_at": fetched_at,
                "content_length": content_length,
            }
        finally:
            conn.close()

    def _maybe_sweep(self) -> None:
        """Sweep stale ``writing`` rows, expired rows, and orphaned content files, at most once per interval.

        Sweep failures are logged at DEBUG and never propagate — a sweep bug must never break ``get``/``put``.
        """
        now = time.time()
        if now - self._last_sweep < SWEEP_INTERVAL_SECONDS:
            return
        self._last_sweep = now
        try:
            conn = self._connect()
            try:
                conn.execute(_SQL_BEGIN_IMMEDIATE)
                conn.execute(
                    "DELETE FROM cache WHERE (status='writing' AND fetched_at + ? < ?)"
                    " OR (status='ready' AND fetched_at + ? < ?)",
                    (STALE_WRITING_THRESHOLD_SECONDS, time.time(), self.ttl, time.time()),
                )
                known = {row[0] for row in conn.execute("SELECT sha256 FROM cache")}
                for entry in self._fs.ls(self.cache_dir):
                    base = entry.rsplit("/", 1)[-1]
                    if len(base) == 64 and all(c in "0123456789abcdef" for c in base) and base not in known:
                        safe_remove(self._fs, entry)
                conn.execute("COMMIT")
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("Sweep failed (non-fatal): %s", exc)
