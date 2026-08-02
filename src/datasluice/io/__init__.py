"""IO layer: downloading, caching, checksums, and storage."""

from datasluice.io.cache import FileCache
from datasluice.io.checksums import compute_hash, compute_md5, compute_sha256, verify_checksum
from datasluice.io.downloader import Downloader
from datasluice.io.local import ensure_dir, safe_filename, save_bytes
from datasluice.io.storage import LocalStorage, Storage

__all__ = [
    "ContentCache",
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
    "safe_remove",
    "save_bytes",
    "safe_filename",
]


def __getattr__(name: str):  # PEP 562
    """Lazily export ContentCache, FsspecStorage, and open_filesystem (D-P3-01 lazy discipline).

    Importing any of these eagerly would pull optional deps (fsspec for the
    storage pair; sqlite3 is stdlib so ContentCache is cheap, but the lazy
    discipline keeps the io surface uniform) at package import time and break
    bare installs. All three symbols are resolved on first attribute access.
    """
    if name == "ContentCache":
        from datasluice.io.content_cache import ContentCache

        globals()["ContentCache"] = ContentCache
        return ContentCache
    if name == "FsspecStorage":
        from datasluice.io.fsspec_storage import FsspecStorage

        globals()["FsspecStorage"] = FsspecStorage
        return FsspecStorage
    if name == "open_filesystem":
        from datasluice.io.filesystem import open_filesystem

        globals()["open_filesystem"] = open_filesystem
        return open_filesystem
    if name == "safe_remove":
        from datasluice.io.filesystem import safe_remove

        globals()["safe_remove"] = safe_remove
        return safe_remove
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
