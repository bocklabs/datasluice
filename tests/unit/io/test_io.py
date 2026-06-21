"""Unit tests for IO utilities (checksums, cache, local, storage)."""

from __future__ import annotations

from pathlib import Path

import pytest

from datasluice.exceptions import ChecksumMismatchError
from datasluice.io import compute_sha256, ensure_dir, safe_filename, verify_checksum
from datasluice.io.cache import FileCache
from datasluice.io.storage import LocalStorage


def test_compute_sha256(tmp_path: Path) -> None:
    f = tmp_path / "test.txt"
    f.write_text("hello")
    h = compute_sha256(f)
    assert isinstance(h, str)
    assert len(h) == 64


def test_verify_checksum_ok(tmp_path: Path) -> None:
    f = tmp_path / "test.txt"
    f.write_text("hello")
    expected = compute_sha256(f)
    assert verify_checksum(f, expected) is True


def test_verify_checksum_mismatch(tmp_path: Path) -> None:
    f = tmp_path / "test.txt"
    f.write_text("hello")
    with pytest.raises(ChecksumMismatchError):
        verify_checksum(f, "0" * 64)


def test_safe_filename() -> None:
    assert safe_filename("hello world.txt") == "hello world.txt"
    assert "etcpasswd" in safe_filename("../../etc/passwd")


def test_ensure_dir(tmp_path: Path) -> None:
    d = ensure_dir(tmp_path / "a" / "b")
    assert d.exists()
    assert d.is_dir()


def test_file_cache(tmp_path: Path) -> None:
    cache = FileCache(tmp_path / "cache", ttl=3600)
    assert cache.get("key1") is None
    cache.put("key1", b"data")
    assert cache.get("key1") == b"data"
    assert cache.has("key1")
    cache.clear()
    assert not cache.has("key1")


def test_local_storage(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path / "store")
    assert not storage.exists("file1")
    storage.write(b"content", "file1")
    assert storage.exists("file1")
    assert storage.read("file1") == b"content"
