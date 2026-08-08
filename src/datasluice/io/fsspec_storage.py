"""fsspec-backed storage adapter satisfying :class:`StoragePort` (INFRA-02, CORR-05).

Closes CORR-05 by returning URI strings (never ``pathlib.Path``) from every
write. Wraps an :class:`fsspec.AbstractFileSystem` instance so local, S3, GCS,
Azure and HTTP backends share one adapter. All paths are URI strings.
"""

from typing import Any

from datasluice.exceptions import DownloadError
from datasluice.logging import get_logger

logger = get_logger("io.fsspec")

_ABSOLUTE_URI_PREFIXES = ("s3://", "gs://", "az://", "abfs://", "file://", "http://", "https://", "memory://")


class FsspecStorage:
    """Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort`` (CORR-05).

    All paths are URI strings (never ``pathlib.Path``). Absolute URIs
    (``s3://bucket/key``, ``gs://bucket/obj``) pass through ``_resolve``
    unchanged; bare paths are joined to *base_uri* when one is configured.

    Args:
        fs: An fsspec ``AbstractFileSystem`` instance (e.g. from
            :func:`datasluice.io.filesystem.open_filesystem`).
        base_uri: Optional base joined to bare path arguments. Trailing
            slashes are stripped.
    """

    def __init__(self, fs: Any, base_uri: str = "") -> None:
        self._fs = fs
        self._base_uri = base_uri.rstrip("/") if base_uri else ""

    def write(self, data: bytes, path: str) -> str:
        """Persist *data* under *path* and return the resulting URI string.

        Args:
            data: Bytes to persist.
            path: Bare path (joined to *base_uri*) or an absolute URI.

        Returns:
            The URI at which the bytes were written.

        Raises:
            DownloadError: If the underlying fsspec write fails.
        """
        resolved = self._resolve(path)
        try:
            self._fs.pipe_file(resolved, data)
        except Exception as exc:
            raise DownloadError(f"Failed to write {path}: {exc}") from exc
        return self._as_uri(resolved)

    def read(self, path: str) -> bytes:
        """Read and return the bytes stored under *path*."""
        return self._fs.cat_file(self._resolve(path))

    def exists(self, path: str) -> bool:
        """Return ``True`` if *path* exists in this filesystem."""
        return bool(self._fs.exists(self._resolve(path)))

    def _resolve(self, path: str) -> str:
        """Return the backend-native path for *path*.

        Absolute URIs pass through unchanged; bare paths are joined to
        *base_uri* when set, otherwise returned as-is. Parent-directory
        (``..``) segments are rejected to prevent path traversal on
        ``file://``/local backends (cloud object keys are flat, so ``..`` is a
        no-op there but rejected uniformly for safety).
        """
        if path.startswith(_ABSOLUTE_URI_PREFIXES):
            return path
        if _has_parent_segments(path):
            raise DownloadError(f"Path traversal detected: {path!r} contains '..' segments")
        if self._base_uri:
            return f"{self._base_uri}/{path.lstrip('/')}"
        return path

    def _as_uri(self, path: str) -> str:
        """Reconstruct a URI string for *path* (CORR-05)."""
        if "://" in path:
            return path
        protocol = getattr(self._fs, "protocol", "file")
        if isinstance(protocol, (list, tuple)):
            protocol = protocol[0]
        leading_slash = "/" if path.startswith("/") else ""
        return f"{protocol}://{leading_slash}{path.lstrip('/')}"


def _has_parent_segments(path: str) -> bool:
    """Return True if *path* contains a ``..`` path segment."""
    return any(seg == ".." for seg in path.replace("\\", "/").split("/"))
