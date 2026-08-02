"""Content-addressed cache backed by a SQLite WAL index + content files (INFRA-03).

The :class:`ContentCache` keeps downloaded resource bytes in content files under
*cache_dir* (one file per SHA-256 key), and a small metadata index in
``cache_dir/cache.db`` (SQLite WAL mode). Writes are two-phase to prevent torn
reads under concurrency:

1. ``INSERT`` a metadata row with ``status='writing'`` (under ``BEGIN IMMEDIATE``).
2. Write the content bytes to a unique temp file, then ``mv`` it into place
   (POSIX-atomic on local backends; ``COPY+DELETE`` on remote — acceptable
   because content-addressed keys never previously existed).
3. ``UPDATE`` the row to ``status='ready'`` (under ``BEGIN IMMEDIATE``).

Readers ``SELECT WHERE status='ready'`` and skip any in-flight ``writing`` rows
(D-P3-11). A lazy sweep on every ``get``/``put`` removes ``writing`` rows older
than :data:`STALE_WRITING_THRESHOLD_SECONDS` and orphaned content files with no
metadata row (D-P3-13), so crashed writers self-heal on the next access.

SQLite connections are short-lived: each method opens a fresh connection via
:meth:`ContentCache._connect`, uses it for one transaction, and lets the
connection fall out of scope (sqlite3 closes on GC). No global connection is
held, so there is nothing to ``close()``.
"""

from __future__ import annotations

import hashlib
import os
import random
import sqlite3
import time
from typing import TYPE_CHECKING, Any

from datasluice.config.defaults import DEFAULT_CACHE_TTL
from datasluice.exceptions import DownloadError
from datasluice.io.filesystem import safe_remove
from datasluice.logging import get_logger

if TYPE_CHECKING:
    import fsspec

logger = get_logger("io.content_cache")

STALE_WRITING_THRESHOLD_SECONDS: float = 300.0


class ContentCache:
    """Content-addressed cache with a SQLite WAL metadata index (INFRA-03).

    Satisfies :class:`datasluice.ports.cache.CachePort` (get/put/delete) plus
    the sidecar :meth:`put_with_metadata` / :meth:`get_metadata` pair that the
    Phase 4 download-to-cache path uses to capture ETag/Last-Modified for
    Phase 7 conditional GETs (D-P3-12, SYNC-06 enabler).

    Args:
        cache_dir: Directory holding ``cache.db`` and the SHA-256-named content
            files. A trailing ``/`` is stripped.
        ttl: Time-to-live in seconds; entries whose ``fetched_at + ttl`` is in
            the past are reported as cache misses. Defaults to
            :data:`DEFAULT_CACHE_TTL`.
        fs: Optional fsspec ``AbstractFileSystem`` for *cache_dir*. When
            omitted, one is constructed lazily via
            :func:`datasluice.io.filesystem.open_filesystem` (local default,
            D-P3-10).
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
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        """Open a fresh autocommit connection with WAL + busy_timeout set."""
        conn = sqlite3.connect(self._db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _init_db(self) -> None:
        """Create the schema (idempotent) and enable WAL on the database file."""
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
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
        """Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)."""
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
        """Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12)."""
        self._put_internal(key, data, etag=None, last_modified=None)

    def put_with_metadata(
        self,
        key: str,
        data: bytes,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> None:
        """Store *data* under *key* with ETag/Last-Modified sidecar (D-P3-12).

        Phase 4's download-to-cache path calls this after capturing headers via
        :meth:`StreamingTransport.stream`. Phase 7 (SYNC-06) reads the sidecar
        for conditional GETs.
        """
        self._put_internal(key, data, etag=etag, last_modified=last_modified)

    def _put_internal(
        self,
        key: str,
        data: bytes,
        etag: str | None,
        last_modified: str | None,
    ) -> None:
        """Two-phase atomic write (RESEARCH Pattern 3, D-P3-11)."""
        sha = self._sha(key)
        content_path = self._content_path(sha)
        tmp_path = f"{self.cache_dir}/.{sha}.tmp.{os.getpid()}.{random.randint(0, 1 << 32)}"
        fetched_at = time.time()

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT OR REPLACE INTO cache "
                "(sha256, url, etag, last_modified, fetched_at, content_length, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'writing')",
                (sha, key, etag, last_modified, fetched_at, len(data)),
            )
            conn.execute("COMMIT")
        finally:
            conn.close()

        try:
            self._fs.pipe_file(tmp_path, data)
            self._fs.mv(tmp_path, content_path)
        except OSError as exc:
            rollback = self._connect()
            try:
                rollback.execute("BEGIN IMMEDIATE")
                rollback.execute(
                    "DELETE FROM cache WHERE sha256=? AND status='writing'",
                    (sha,),
                )
                rollback.execute("COMMIT")
            finally:
                rollback.close()
            safe_remove(self._fs, tmp_path)
            raise DownloadError(f"Failed to write content for key {key!r}: {exc}") from exc

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE cache SET status='ready' WHERE sha256=?", (sha,))
            conn.execute("COMMIT")
        finally:
            conn.close()

        self._maybe_sweep()

    def delete(self, key: str) -> None:
        """Remove the entry for *key* (CachePort requirement)."""
        sha = self._sha(key)
        content_path = self._content_path(sha)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM cache WHERE sha256=?", (sha,))
            conn.execute("COMMIT")
        finally:
            conn.close()
        if self._fs.exists(content_path):
            self._fs.rm(content_path)

    def get_metadata(self, key: str) -> dict[str, Any] | None:
        """Return the sidecar metadata for *key*, or ``None`` if missing/writing.

        Phase 7 (SYNC-06) reads this for conditional GETs.
        """
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
            if status == "writing":
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
        """Lazily sweep stale ``writing`` rows and orphaned content files (D-P3-13).

        Sweep failures are logged at DEBUG and never propagate — a sweep bug
        must never break ``get``/``put``.
        """
        try:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "DELETE FROM cache WHERE status='writing' AND fetched_at + ? < ?",
                    (STALE_WRITING_THRESHOLD_SECONDS, time.time()),
                )
                conn.execute("COMMIT")
                known = {row[0] for row in conn.execute("SELECT sha256 FROM cache")}
            finally:
                conn.close()

            try:
                entries = self._fs.ls(self.cache_dir)
            except Exception:
                entries = []
            for entry in entries:
                base = entry.rsplit("/", 1)[-1]
                if len(base) == 64 and all(c in "0123456789abcdef" for c in base) and base not in known:
                    try:
                        self._fs.rm(entry)
                    except Exception:
                        logger.debug("Sweep: could not remove orphaned content file %s", entry)
        except Exception as exc:
            logger.debug("Sweep failed (non-fatal): %s", exc)
