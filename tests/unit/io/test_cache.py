"""SHA-256 cache key regression tests for FileCache (SEC-06)."""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path

from datasluice.io.cache import FileCache


def test_key_path_is_sha256_hexdigest(tmp_path: Path) -> None:
    cache = FileCache(tmp_path / "cache")
    key = "https://example.com/data.csv"
    path = cache._key_path(key)
    assert path.name == hashlib.sha256(key.encode()).hexdigest()
    assert len(path.name) == 64


def test_key_path_has_no_path_separators(tmp_path: Path) -> None:
    cache = FileCache(tmp_path / "cache")
    for key in ["https://x.io/a/b/c.csv", "http://h:8080/p?q=1", "../../etc/passwd"]:
        name = cache._key_path(key).name
        assert "/" not in name
        assert ":" not in name
        assert "\\" not in name


def test_key_path_is_deterministic(tmp_path: Path) -> None:
    cache = FileCache(tmp_path / "cache")
    assert cache._key_path("k1") == cache._key_path("k1")


def test_different_keys_map_to_different_paths(tmp_path: Path) -> None:
    cache = FileCache(tmp_path / "cache")
    assert cache._key_path("k1") != cache._key_path("k2")


def test_put_get_round_trip(tmp_path: Path) -> None:
    cache = FileCache(tmp_path / "cache", ttl=3600)
    assert cache.get("k1") is None
    cache.put("k1", b"payload")
    assert cache.get("k1") == b"payload"
    assert cache.has("k1")


def test_clear_evicts(tmp_path: Path) -> None:
    cache = FileCache(tmp_path / "cache", ttl=3600)
    cache.put("k1", b"x")
    assert cache.has("k1")
    cache.clear()
    assert not cache.has("k1")


def test_expired_entry_returns_none(tmp_path: Path) -> None:
    cache = FileCache(tmp_path / "cache", ttl=1)
    cache.put("k1", b"x")
    path = cache._key_path("k1")
    old = time.time() - 3600
    os.utime(path, (old, old))
    assert cache.get("k1") is None
    assert not cache.has("k1")
