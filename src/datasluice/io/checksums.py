"""Checksum (hash) computation and verification."""

from __future__ import annotations

import hashlib
from pathlib import Path

from datasluice.exceptions import ChecksumMismatchError

CHUNK_SIZE = 64 * 1024


def compute_hash(file_path: str | Path, algorithm: str = "sha256") -> str:
    """Compute the hex digest of *file_path* using *algorithm*."""
    hasher = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_sha256(file_path: str | Path) -> str:
    """Return the SHA-256 hex digest of *file_path*."""
    return compute_hash(file_path, "sha256")


def compute_md5(file_path: str | Path) -> str:
    """Return the MD5 hex digest of *file_path*."""
    return compute_hash(file_path, "md5")


def verify_checksum(file_path: str | Path, expected: str, algorithm: str = "sha256") -> bool:
    """Verify that *file_path* matches *expected* hash.

    Args:
        file_path: Path to the file to check.
        expected: Expected hex digest.
        algorithm: Hash algorithm name.

    Returns:
        ``True`` if the computed hash matches.

    Raises:
        ChecksumMismatchError: If the checksums differ.
    """
    actual = compute_hash(file_path, algorithm)
    if actual.lower() != expected.lower():
        raise ChecksumMismatchError(
            f"Checksum mismatch for {file_path} ({algorithm})",
            expected=expected,
            actual=actual,
        )
    return True
