"""Local filesystem helpers for saving downloaded resources."""

from __future__ import annotations

import os
from pathlib import Path

from datasluice.exceptions import DownloadError


def ensure_dir(path: str | Path) -> Path:
    """Create *path* (and parents) if it does not exist; return as :class:`Path`."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def save_bytes(data: bytes, dest: str | Path, filename: str | None = None) -> Path:
    """Write *data* to *dest* / *filename* and return the file path.

    Raises:
        DownloadError: If the destination is not writable.
    """
    dest_path = Path(dest)
    if dest_path.is_dir() or (not dest_path.exists() and filename):
        ensure_dir(dest_path)
        if not filename:
            raise DownloadError("filename is required when dest is a directory")
        full_path = dest_path / filename
    else:
        ensure_dir(dest_path.parent)
        full_path = dest_path

    try:
        full_path.write_bytes(data)
        os.chmod(full_path, 0o644)
    except OSError as exc:
        raise DownloadError(f"Failed to write {full_path}: {exc}") from exc

    return full_path


def safe_filename(name: str) -> str:
    """Sanitise *name* for use as a filename."""
    return "".join(c for c in name if c.isalnum() or c in "-_. ")
