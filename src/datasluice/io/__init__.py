"""IO layer: downloading, caching, checksums, and storage."""

from datasluice.io.cache import FileCache
from datasluice.io.checksums import compute_hash, compute_md5, compute_sha256, verify_checksum
from datasluice.io.downloader import Downloader
from datasluice.io.local import ensure_dir, safe_filename, save_bytes
from datasluice.io.storage import LocalStorage, Storage

__all__ = [
    "Downloader",
    "FileCache",
    "Storage",
    "LocalStorage",
    "compute_hash",
    "compute_sha256",
    "compute_md5",
    "verify_checksum",
    "ensure_dir",
    "save_bytes",
    "safe_filename",
]
