"""Incremental sync state stores: file-backed and in-memory ``StateStore`` impls (SYNC-02).

The :class:`datasluice.ports.state_store.StateStore` Protocol (Phase 2) is the
contract; this module provides its two concrete implementations. Both stores
are dep-free at import time (stdlib + fsspec, which is an installed infra dep),
per D-P7-29's lazy data-plane discipline — pyarrow/dlt are never imported here.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from datasluice.exceptions import StateStoreError, SyncStateConflictError
from datasluice.logging import get_logger

if TYPE_CHECKING:
    from datasluice.domain import SyncState

logger = get_logger("sync.state_store")

_UNSET = object()


class FileStateStore:
    """Durable :class:`StateStore` persisting :class:`SyncState` as versioned JSON files (D-P7-01/03).

    State is written to one SHA-256-hexdigest-named JSON file per key
    (SEC-06 carry-forward: keys never appear as raw filenames, so path
    traversal is impossible by construction). Writes are atomic (temp file +
    ``fs.mv`` rename, mirroring the content-cache discipline) and protected by
    detection-only optimistic CAS (re-read + hash-compare before the rename,
    D-P7-27 — portable across fsspec backends, where no advisory file lock
    exists). The durable format is a versioned envelope
    ``{"schema_version": 1, "key": ..., "state": {...}}`` so future
    :class:`SyncState` evolution can migrate old files on read (D-P7-03).

    Attributes:
        base_uri: URI (``file://``, ``s3://``, …) of the directory holding the
            state files. A trailing ``/`` is stripped. URIs, never Paths
            (CORR-05). This store is caller-provided (D-P7-02).
        fs: Optional fsspec ``AbstractFileSystem``. When omitted, one is
            constructed via :func:`datasluice.io.filesystem.open_filesystem`.
    """

    def __init__(self, base_uri: str, *, fs: Any | None = None) -> None:
        self._base = base_uri.rstrip("/")
        if fs is not None:
            self._fs = fs
        else:
            from datasluice.io.filesystem import open_filesystem

            self._fs = open_filesystem(base_uri)
        self._fs.makedirs(self._base, exist_ok=True)

    def _state_path(self, key: str) -> str:
        """Return the SHA-256-hexdigest (.json) path for *key* (T-07-03 mitigation)."""
        digest = hashlib.sha256(key.encode()).hexdigest()
        return f"{self._base}/{digest}.json"

    def get(self, key: str) -> SyncState | None:
        """Load the :class:`SyncState` for *key*, or ``None`` if absent.

        Raises:
            StateStoreError: if the state file is corrupt (bad JSON), or its
                envelope ``schema_version`` is unsupported. Never silently
                treats corrupt state as "no state" (staleness is worse than a
                loud failure — D-P7-03, T-07-01).
        """
        path = self._state_path(key)
        try:
            raw = self._fs.cat_file(path)
        except (FileNotFoundError, OSError):
            return None
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise StateStoreError(f"Corrupt state file {path!r}: {exc}") from exc
        if not isinstance(envelope, dict) or envelope.get("schema_version") != 1:
            version = envelope.get("schema_version") if isinstance(envelope, dict) else None
            raise StateStoreError(f"Unsupported state file schema_version {version!r} at {path!r}; expected 1")
        s = envelope.get("state") or {}
        from datasluice.domain import SyncState

        return SyncState(
            cursor=dict(s.get("cursor", {})),
            partitions=dict(s.get("partitions", {})),
            last_synced_at=s.get("last_synced_at"),
            extra=dict(s.get("extra", {})),
        )

    def read_raw(self, key: str) -> bytes | None:
        """Return the raw envelope bytes for *key* (or ``None`` if absent) for CAS compare-and-swap.

        Callers pass the returned bytes back into :meth:`put(..., expected_prior=...)`
        so a concurrent writer's intervening commit is detected instead of
        silently overwritten (D-P7-27).
        """
        path = self._state_path(key)
        try:
            return self._fs.cat_file(path)
        except (FileNotFoundError, OSError):
            return None

    def put(self, key: str, state: SyncState, *, expected_prior: bytes | None | object = _UNSET) -> None:
        """Persist *state* under *key* via an atomic, optionally CAS-protected write.

        Atomic write: ``pipe_file(tmp)`` + ``mv(tmp, final)`` — a reader never
        observes torn JSON. Optimistic CAS (D-P7-27): when *expected_prior* is
        provided (the raw bytes previously read from :meth:`read_raw`, or
        ``None`` for "expected absent"), the current on-disk bytes are
        hash-compared immediately before the rename; a mismatch means a
        concurrent writer committed in between and a
        :class:`SyncStateConflictError` is raised rather than silently
        overwriting. Detection-only CAS (content-hash re-read) is used because
        fsspec backends expose no portable conditional-write/etag (RESEARCH
        Pitfall 5). Omit *expected_prior* for an unconditional write.

        Args:
            key: The sync-state key (resource-id-scoped, per Area 1).
            state: The :class:`SyncState` to persist (watermark strings, never
                secrets/signed URLs — T-07-02).
            expected_prior: Raw bytes from :meth:`read_raw` (``None`` = "expected
                absent"). Omit for an unconditional write.

        Raises:
            SyncStateConflictError: if the on-disk content no longer matches
                *expected_prior* (CAS lost a race).
            StateStoreError: on any write I/O failure.
        """
        path = self._state_path(key)
        payload = json.dumps(
            {"schema_version": 1, "key": key, "state": asdict(state)},
            sort_keys=True,
        ).encode()

        if expected_prior is not _UNSET:
            actual_prior = self.read_raw(key)
            expected_bytes = expected_prior if isinstance(expected_prior, bytes) else b""
            actual_bytes = actual_prior if actual_prior is not None else b""
            expected_sha = hashlib.sha256(expected_bytes).hexdigest()
            actual_sha = hashlib.sha256(actual_bytes).hexdigest()
            if actual_sha != expected_sha:
                raise SyncStateConflictError(
                    f"State for key {key!r} changed since last read (CAS mismatch); re-read and re-apply"
                )

        tmp_path = f"{self._base}/.{self._sha256_bytes(payload)}.tmp.{os.getpid()}.{random.randint(0, 1 << 32)}"
        try:
            self._fs.pipe_file(tmp_path, payload)
            self._fs.mv(tmp_path, path)
        except OSError as exc:
            raise StateStoreError(f"Failed to write state file {path!r}: {exc}") from exc

    def delete(self, key: str) -> None:
        """Remove the state file for *key*; a missing file is tolerated (idempotent)."""
        path = self._state_path(key)
        try:
            self._fs.rm(path)
        except (FileNotFoundError, OSError):
            logger.debug("delete: state file absent (tolerated): %s", path)

    @staticmethod
    def _sha256_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()


class InMemoryStateStore:
    """Ephemeral in-process :class:`StateStore` backed by a plain dict (D-P7-02).

    State dies with the process — the canonical default for tests and
    dry-runs. Implements the Protocol exactly (get/put/delete) with no
    persistence obligation.
    """

    def __init__(self) -> None:
        self._store: dict[str, SyncState] = {}

    def get(self, key: str) -> SyncState | None:
        """Return the :class:`SyncState` for *key*, or ``None`` if absent."""
        return self._store.get(key)

    def put(self, key: str, state: SyncState) -> None:
        """Store *state* under *key* (last-writer-wins; ephemeral)."""
        self._store[key] = state

    def delete(self, key: str) -> None:
        """Remove *key* if present; a missing key is tolerated."""
        self._store.pop(key, None)
