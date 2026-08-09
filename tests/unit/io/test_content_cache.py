"""ContentCache behavior tests.

Covers:
- CachePort Protocol conformance
- put/get/delete round-trip + missing-key semantics
- SHA-256 hexdigest keying
- ETag/Last-Modified sidecar round-trip
- Readers skip status='writing' rows
- Lazy sweep removes stale 'writing' rows + orphaned content files
- concurrency matrix:
    * N=10 threads, distinct keys -> all puts succeed, all gets correct
    * N=10 threads, SAME key -> exactly one payload wins, no corruption
    * N=5 writers + N=5 readers -> never observe torn reads
    * Writer crash mid-two-phase -> metadata rolled back, DownloadError raised
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import sqlite3
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from datasluice.exceptions import DownloadError
from datasluice.io.content_cache import STALE_WRITING_THRESHOLD_SECONDS, ContentCache
from datasluice.ports.cache import CachePort


def test_content_cache_satisfies_cache_port_protocol(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    assert isinstance(cache, CachePort)


def test_put_get_round_trip(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    assert cache.get("k1") is None
    cache.put("k1", b"payload")
    assert cache.get("k1") == b"payload"


def test_get_missing_returns_none(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    assert cache.get("never-seen") is None


def test_put_overwrite_returns_latest(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    cache.put("k1", b"first")
    cache.put("k1", b"second")
    assert cache.get("k1") == b"second"


def test_put_empty_payload_round_trips(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    cache.put("empty", b"")
    assert cache.get("empty") == b""


def test_delete_removes_entry(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    cache.put("k1", b"x")
    assert cache.get("k1") == b"x"
    cache.delete("k1")
    assert cache.get("k1") is None


def test_key_is_sha256_hexdigest(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    key = "https://example.com/data.csv"
    sha = cache._sha(key)
    assert sha == hashlib.sha256(b"https://example.com/data.csv").hexdigest()
    assert len(sha) == 64


def test_key_is_deterministic(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    assert cache._sha("k1") == cache._sha("k1")
    assert cache._sha("k1") != cache._sha("k2")


def test_key_has_no_path_separators(tmp_path: Path) -> None:
    """SHA-256 destroys path structure (regression from FileCache)."""
    cache = ContentCache(str(tmp_path / "cache"))
    for key in ["https://x.io/a/b/c.csv", "http://h:8080/p?q=1", "../../etc/passwd"]:
        sha = cache._sha(key)
        assert "/" not in sha
        assert ":" not in sha
        assert "\\" not in sha


def test_etag_sidecar_stored_and_retrieved(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    cache.put_with_metadata(
        "k",
        b"data",
        etag='"abc"',
        last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
    )
    metadata = cache.get_metadata("k")
    assert metadata is not None
    assert metadata["etag"] == '"abc"'
    assert metadata["last_modified"] == "Wed, 01 Jan 2025 00:00:00 GMT"
    assert metadata["content_length"] == 4


def test_get_returns_none_for_writing_status(tmp_path: Path) -> None:
    """Readers skip rows with status='writing'."""
    cache = ContentCache(str(tmp_path / "cache"))
    sha = cache._sha("k")
    conn = sqlite3.connect(cache._db_path)
    try:
        conn.execute(
            "INSERT INTO cache (sha256, url, etag, last_modified, fetched_at, content_length, status) "
            "VALUES (?, ?, NULL, NULL, ?, 4, 'writing')",
            (sha, "k", time.time()),
        )
        conn.commit()
    finally:
        conn.close()
    assert cache.get("k") is None


def test_lazy_sweep_removes_stale_writing(tmp_path: Path) -> None:
    """Stale 'writing' rows are swept on access."""
    cache = ContentCache(str(tmp_path / "cache"))
    conn = sqlite3.connect(cache._db_path)
    try:
        conn.execute(
            "INSERT INTO cache (sha256, url, etag, last_modified, fetched_at, content_length, status) "
            "VALUES (?, ?, NULL, NULL, ?, 0, 'writing')",
            ("stale-sha", "http://x", time.time() - STALE_WRITING_THRESHOLD_SECONDS - 60),
        )
        conn.commit()
    finally:
        conn.close()
    cache.get("any-other-key")
    conn = sqlite3.connect(cache._db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM cache WHERE sha256='stale-sha'").fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_lazy_sweep_skips_fresh_writing(tmp_path: Path) -> None:
    """A 'writing' row newer than the threshold is NOT swept (writer may still be active)."""
    cache = ContentCache(str(tmp_path / "cache"))
    conn = sqlite3.connect(cache._db_path)
    try:
        conn.execute(
            "INSERT INTO cache (sha256, url, etag, last_modified, fetched_at, content_length, status) "
            "VALUES (?, ?, NULL, NULL, ?, 0, 'writing')",
            ("fresh-sha", "http://y", time.time()),
        )
        conn.commit()
    finally:
        conn.close()
    cache.get("any-other-key")
    conn = sqlite3.connect(cache._db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM cache WHERE sha256='fresh-sha'").fetchone()[0]
    finally:
        conn.close()
    assert count == 1


def test_lazy_sweep_removes_orphaned_content_files(tmp_path: Path) -> None:
    """Content files with no metadata row are swept."""
    cache = ContentCache(str(tmp_path / "cache"))
    orphan_sha = "a" * 64
    orphan_path = f"{cache.cache_dir}/{orphan_sha}"
    cache._fs.pipe_file(orphan_path, b"orphan")
    assert cache._fs.exists(orphan_path)
    cache.get("any-key")
    assert not cache._fs.exists(orphan_path)


def test_concurrent_puts_distinct_keys_no_corruption(tmp_path: Path) -> None:
    """N=10 threads writing distinct keys -> all succeed, no OperationalError."""
    cache = ContentCache(str(tmp_path / "cache"))
    payloads = {f"key{i}": f"data-{i}".encode() for i in range(10)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(cache.put, key, data) for key, data in payloads.items()]
        concurrent.futures.wait(futures)
        for future in futures:
            assert future.exception() is None, f"put raised: {future.exception()}"

    for key, expected in payloads.items():
        actual = cache.get(key)
        assert actual == expected, f"key={key}: expected {expected!r}, got {actual!r}"


def test_concurrent_puts_same_key_one_wins(tmp_path: Path) -> None:
    """N=10 threads writing the SAME key -> exactly one payload wins, no corruption."""
    cache = ContentCache(str(tmp_path / "cache"))
    payloads = [f"data-{i}".encode() for i in range(10)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(cache.put, "same-key", payload) for payload in payloads]
        concurrent.futures.wait(futures)
        for future in futures:
            assert future.exception() is None, f"put raised: {future.exception()}"

    result = cache.get("same-key")
    assert result is not None
    assert result in payloads, f"corrupted result: {result!r}"


def test_concurrent_writers_and_readers_no_torn_reads(tmp_path: Path) -> None:
    """N=5 writers + N=5 readers -> readers never observe partial bytes."""
    cache = ContentCache(str(tmp_path / "cache"))
    keys = [f"mix-key-{i}" for i in range(20)]
    expected = {key: (b"x" * (i + 1) * 100) for i, key in enumerate(keys)}
    for key, data in expected.items():
        cache.put(key, data)

    writer_errors: list[BaseException] = []
    reader_violations: list[str] = []

    def writer() -> None:
        try:
            for _ in range(20):
                for key, data in expected.items():
                    cache.put(key, data + b"-rewrite")
        except BaseException as exc:  # noqa: BLE001
            writer_errors.append(exc)

    def reader() -> None:
        try:
            for _ in range(50):
                for key in keys:
                    actual = cache.get(key)
                    if actual is None:
                        continue
                    if actual not in (expected[key], expected[key] + b"-rewrite"):
                        reader_violations.append(f"torn read for {key}: len={len(actual)}")
        except BaseException as exc:  # noqa: BLE001
            reader_violations.append(f"reader raised: {exc!r}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        writer_futures = [executor.submit(writer) for _ in range(5)]
        reader_futures = [executor.submit(reader) for _ in range(5)]
        concurrent.futures.wait(writer_futures + reader_futures)

    assert writer_errors == [], f"writer errors: {writer_errors}"
    assert reader_violations == [], f"torn-read violations: {reader_violations}"


def test_writer_crash_mid_two_phase_rolls_back_metadata(tmp_path: Path) -> None:
    """Crash during a (content write) rolls back the 'writing' metadata row."""
    cache = ContentCache(str(tmp_path / "cache"))

    with patch.object(cache._fs, "pipe_file", side_effect=OSError("disk full")):
        with pytest.raises(DownloadError):
            cache.put("k", b"data")

    conn = sqlite3.connect(cache._db_path)
    try:
        row = conn.execute("SELECT status FROM cache WHERE sha256=?", (cache._sha("k"),)).fetchone()
    finally:
        conn.close()
    assert row is None, f"metadata row should be rolled back, got: {row}"

    assert cache.get("k") is None


def test_mv_failure_cleans_orphan_temp_file(tmp_path: Path) -> None:
    """An ``mv`` failure after ``pipe_file`` succeeds must not leave a temp-file orphan."""
    cache = ContentCache(str(tmp_path / "cache"))
    original_pipe = cache._fs.pipe_file

    with (
        patch.object(cache._fs, "pipe_file", wraps=original_pipe),
        patch.object(cache._fs, "mv", side_effect=OSError("injected move failure")),
    ):
        with pytest.raises(DownloadError):
            cache.put("k", b"data")

    entries = cache._fs.find(cache.cache_dir)
    orphans = [path for path in entries if ".tmp." in path.rsplit("/", 1)[-1]]
    assert orphans == [], f"orphan temp files left behind: {orphans}"


def test_metadata_returns_none_for_writing_status(tmp_path: Path) -> None:
    """get_metadata returns None for in-flight 'writing' rows."""
    cache = ContentCache(str(tmp_path / "cache"))
    sha = cache._sha("k")
    conn = sqlite3.connect(cache._db_path)
    try:
        conn.execute(
            "INSERT INTO cache (sha256, url, etag, last_modified, fetched_at, content_length, status) "
            "VALUES (?, ?, NULL, NULL, ?, 0, 'writing')",
            (sha, "k", time.time()),
        )
        conn.commit()
    finally:
        conn.close()
    assert cache.get_metadata("k") is None


def test_metadata_returns_none_for_missing_key(tmp_path: Path) -> None:
    cache = ContentCache(str(tmp_path / "cache"))
    assert cache.get_metadata("never") is None


def test_put_with_metadata_round_trips_through_get(tmp_path: Path) -> None:
    """Content stored via put_with_metadata is readable via the plain CachePort.get."""
    cache = ContentCache(str(tmp_path / "cache"))
    cache.put_with_metadata("k", b"body", etag='"e"', last_modified="Wed, 01 Jan 2025 00:00:00 GMT")
    assert cache.get("k") == b"body"


def test_ttl_expired_returns_none(tmp_path: Path) -> None:
    """Carry-forward of FileCache TTL semantics."""
    cache = ContentCache(str(tmp_path / "cache"), ttl=1)
    cache.put("k", b"v")
    sha = cache._sha("k")
    conn = sqlite3.connect(cache._db_path)
    try:
        conn.execute("UPDATE cache SET fetched_at=? WHERE sha256=?", (time.time() - 3600, sha))
        conn.commit()
    finally:
        conn.close()
    assert cache.get("k") is None
