"""IO layer: downloading, caching, checksums, and storage."""

from datasluice.io.cache import FileCache
from datasluice.io.checksums import compute_hash, compute_md5, compute_sha256, verify_checksum
from datasluice.io.downloader import Downloader
from datasluice.io.local import ensure_dir, safe_filename, save_bytes
from datasluice.io.storage import LocalStorage, Storage

__all__ = [
    "Downloader",
    "FileCache",
    "FsspecStorage",
    "Storage",
    "LocalStorage",
    "compute_hash",
    "compute_sha256",
    "compute_md5",
    "verify_checksum",
    "ensure_dir",
    "open_filesystem",
    "save_bytes",
    "safe_filename",
]


def __getattr__(name: str):  # PEP 562
    """Lazily export FsspecStorage and open_filesystem (D-P3-01 lazy discipline).

    Importing either eagerly would pull fsspec at package import time and break
    bare installs. Both symbols are resolved on first attribute access.
    """
    if name == "FsspecStorage":
        from datasluice.io.fsspec_storage import FsspecStorage

        globals()["FsspecStorage"] = FsspecStorage
        return FsspecStorage
    if name == "open_filesystem":
        from datasluice.io.filesystem import open_filesystem

        globals()["open_filesystem"] = open_filesystem
        return open_filesystem
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
