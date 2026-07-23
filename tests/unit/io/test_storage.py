"""Unit tests for LocalStorage path-traversal containment."""

from __future__ import annotations

from pathlib import Path

import pytest

from datasluice.exceptions import DownloadError
from datasluice.io.storage import LocalStorage


def test_storage_rejects_path_traversal(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path / "store")
    with pytest.raises(DownloadError):
        storage.write(b"data", "../../etc/passwd")


def test_storage_rejects_dotdot_segment(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path / "store")
    with pytest.raises(DownloadError):
        storage.write(b"data", "subdir/../../escape.txt")


def test_storage_writes_normal_key(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path / "store")
    path = storage.write(b"data", "normal_file.txt")
    assert Path(path).is_file()
    assert Path(path).resolve().is_relative_to((tmp_path / "store").resolve())


def test_storage_writes_nested_key(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path / "store")
    storage.write(b"data", "subdir/file.txt")
    assert storage.exists("subdir/file.txt")
    assert storage.read("subdir/file.txt") == b"data"


def test_storage_round_trip(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path / "store")
    assert not storage.exists("file1")
    storage.write(b"content", "file1")
    assert storage.exists("file1")
    assert storage.read("file1") == b"content"
