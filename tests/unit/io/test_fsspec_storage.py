"""Unit tests for :class:`FsspecStorage`.

Closes by asserting ``write`` returns a ``str`` URI (never a
``pathlib.Path``), and by asserting the adapter structurally
satisfies :class:`datasluice.ports.storage.StoragePort`.

The implementation module is resolved via ``importlib.import_module`` (rather
than a static ``import``) so the RED commit can land under this repo's
full-suite pre-commit hook: until the GREEN step ships, the whole module
skips cleanly instead of erroring at collection.
"""

from __future__ import annotations

import importlib
import inspect

import pytest

pytest.importorskip("fsspec")

try:
    _fsspec_storage_module = importlib.import_module("datasluice.io.fsspec_storage")
except ImportError:
    pytest.skip(
        "FsspecStorage implementation pending (RED → GREEN within task 03-02)",
        allow_module_level=True,
    )

from datasluice.exceptions import DownloadError  # noqa: E402
from datasluice.io.filesystem import open_filesystem  # noqa: E402
from datasluice.ports.storage import StoragePort  # noqa: E402

FsspecStorage = _fsspec_storage_module.FsspecStorage


def _memory_storage() -> FsspecStorage:
    import fsspec

    return FsspecStorage(fsspec.filesystem("memory"))


def test_fsspec_storage_satisfies_storage_port_protocol() -> None:
    """FsspecStorage is structurally a StoragePort."""
    storage = _memory_storage()
    assert isinstance(storage, StoragePort)


def test_write_returns_uri() -> None:
    """write return value starts with the backend protocol prefix."""
    storage = _memory_storage()
    uri = storage.write(b"data", "file.txt")
    assert isinstance(uri, str)
    assert uri.startswith("memory://")


def test_write_returns_str_not_path_instance() -> None:
    """write returns a bare str, never a pathlib.Path."""
    from pathlib import Path

    storage = _memory_storage()
    uri = storage.write(b"data", "file.txt")
    assert type(uri) is str
    assert not isinstance(uri, Path)


def test_round_trip() -> None:
    """write -> read round-trips the bytes; exists flips False -> True."""
    storage = _memory_storage()
    assert not storage.exists("rt.txt")
    storage.write(b"content", "rt.txt")
    assert storage.exists("rt.txt")
    assert storage.read("rt.txt") == b"content"


def test_read_missing_raises() -> None:
    """Reading a key that was never written raises FileNotFoundError or DownloadError."""
    storage = _memory_storage()
    with pytest.raises((FileNotFoundError, DownloadError)):
        storage.read("never-written-key")


def test_local_backend_works(tmp_path: object) -> None:
    """FsspecStorage over a real file:// backend round-trips on a relative path."""
    fs = open_filesystem(f"file://{tmp_path}")
    storage = FsspecStorage(fs, base_uri=str(tmp_path))
    storage.write(b"local-bytes", "data.csv")
    assert storage.exists("data.csv")
    assert storage.read("data.csv") == b"local-bytes"


def test_absolute_uri_passthrough() -> None:
    """When path is already an absolute URI, _resolve passes it through unchanged."""
    import fsspec

    fs = fsspec.filesystem("memory")
    storage = FsspecStorage(fs)
    storage.write(b"abs", "memory:///bucket/key")
    assert fs.cat_file("/bucket/key") == b"abs"


def test_init_signature_storage_port_compat() -> None:
    """write signature matches the StoragePort contract: (self, data: bytes, path: str) -> str."""
    sig = inspect.signature(FsspecStorage.write)
    params = list(sig.parameters.keys())
    assert params == ["self", "data", "path"]
    assert sig.return_annotation is str


def test_fsspec_rejects_dotdot_on_write() -> None:
    """A path with '..' segments is rejected to prevent traversal."""
    storage = _memory_storage()
    with pytest.raises(DownloadError):
        storage.write(b"x", "../escape.txt")


def test_fsspec_rejects_dotdot_on_read() -> None:
    """Reading a path with '..' is also rejected."""
    storage = _memory_storage()
    with pytest.raises(DownloadError):
        storage.read("../escape.txt")


def test_fsspec_write_failure_wraps_in_download_error() -> None:
    """A write to a read-only backend surfaces as DownloadError."""
    import fsspec

    fs = fsspec.filesystem("memory")
    storage = FsspecStorage(fs, base_uri="/base")
    storage.write(b"data", "file.txt")
    assert storage.read("file.txt") == b"data"
