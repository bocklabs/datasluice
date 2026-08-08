"""Centralised filesystem factory (INFRA-05).

All fsspec backend instantiation flows through :func:`open_filesystem` so that
S3 / GCS / Azure / HTTP / local dispatch is resolved in exactly one place via
``fsspec.core.url_to_fs``. fsspec is imported lazily inside the function body
so ``datasluice`` stays importable on a bare install (D-P3-01).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import fsspec


def open_filesystem(uri: str, credentials: dict[str, Any] | None = None) -> fsspec.AbstractFileSystem:
    """Instantiate the correct fsspec backend for *uri* (INFRA-05, D-P3-20).

    Delegates to ``fsspec.core.url_to_fs`` so the fsspec registry handles every
    supported protocol (``file``, ``memory``, ``s3``, ``gs``, ``az``, ``abfs``,
    ``http``, ``https``, …) uniformly.

    Credential precedence (D-P3-19):

        1. Explicit *credentials* dict — forwarded to fsspec as
           ``**storage_options`` (e.g. ``{"key": ..., "secret": ...}`` for S3,
           ``{"token": ...}`` for GCS).
        2. URI-embedded credentials (e.g. ``s3://AKIA:SECRET@bucket``) —
           extracted by fsspec.
        3. Environment variables / config files / IAM — resolved by the
           per-backend fsspec resolver.

    Args:
        uri: Fully-qualified URI (``s3://bucket/key``, ``file:///tmp/x``,
            ``gs://bucket/obj``, ``memory://``, …).
        credentials: Per-protocol ``storage_options``. ``None`` (the default)
            forwards an empty dict so fsspec falls back to backend defaults.

    Returns:
        An :class:`fsspec.AbstractFileSystem` instance for the URI's protocol.

    Raises:
        ImportError: If fsspec is not installed. Install with
            ``pip install datasluice[storage]`` (D-P3-01 zero-config fallback).
    """
    try:
        import fsspec
    except ImportError as exc:
        raise ImportError("open_filesystem requires 'fsspec'. Install with: pip install datasluice[storage]") from exc

    fs, _path = fsspec.core.url_to_fs(uri, **(credentials or {}))
    return fs


def safe_remove(fs: Any, path: str) -> None:
    """Best-effort removal of *path* on *fs*; ignore absence and secondary OSError.

    Used by atomic-publish paths (temp-file + ``mv``) to clean up an orphaned
    temporary file after a failed ``pipe_file``/``mv``. A missing path and any
    OSError raised while removing are swallowed so the original failure (which
    is re-raised by the caller) is never masked by a cleanup error.
    """
    try:
        if fs.exists(path):
            fs.rm(path)
    except OSError:
        pass
