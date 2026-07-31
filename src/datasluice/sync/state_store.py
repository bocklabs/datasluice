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
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any

from datasluice.exceptions import StateStoreError, SyncStateConflictError
from datasluice.logging import get_logger

if TYPE_CHECKING:
    from datasluice.domain import SyncState

logger = get_logger("sync.state_store")

_UNSET = object()
_SECRET_FREE_STATE_READY = True
_ADVERSARIAL_VALIDATOR_READY = True
_COMPLETED_WATERMARK_SCHEMA = "datasluice_completed_watermark_v1"
_COMPLETED_WATERMARK_KEYS = {"schema", "watermark"}
_CHECKPOINT_KEYS = {"version", "status", "next_batch_index", "position"}
_CHECKPOINT_POSITION_KEYS = {"kind", "row_group_index"}
_MAX_TIMESTAMP_LENGTH = 128
_MAX_WATERMARK_LENGTH = 256
_ETAG_PATTERN = re.compile(r'(?:W/)?"[\x21\x23-\x7e\x80-\xff]*"\Z')


class FileStateStore:
    """Durable :class:`StateStore` persisting :class:`SyncState` as versioned JSON files (D-P7-01/03).

    State is written to one SHA-256-hexdigest-named JSON file per key. New
    envelopes contain only ``schema_version`` and ``state``; completed
    watermarks use a fixed keyless representation and are reconstructed from
    the lookup key on read. New writes validate every :class:`SyncState` field
    against the completed-watermark or ``datasluice_checkpoint`` schemas before
    publishing bytes. Historical schema-version-1 envelopes containing a raw
    ``key`` and unconstrained state mappings remain readable.

    Writes are atomic (temp file + ``fs.mv`` rename, mirroring the content-cache
    discipline) and protected by detection-only optimistic CAS (re-read +
    hash-compare before the rename, D-P7-27 — portable across fsspec backends,
    where no advisory file lock exists).

    Contract Reconciliation:

        New durable writes enforce a producer-grounded closed schema as an
        explicit accepted override of the general :class:`SyncState` model.
        The general model (Phase 2) permits arbitrary cursor mappings,
        partitions, and extra fields. Durable writes restrict to: one-entry
        cursor (keyed by the canonical identity), empty partitions, and the
        recognized ``datasluice_checkpoint`` extra. This restriction is an
        accepted override because it is the only shape this producer emits —
        no producer-legal state shape is silently rejected.

        :class:`InMemoryStateStore` accepts the full :class:`SyncState` model
        without restriction because it produces no durable bytes. Historical
        schema-version-1 envelopes remain readable through their unconstrained
        legacy shape. The durable write validation rejects adversarial
        validator values (signed URLs, bearer tokens, credentials, control
        bytes, oversized opaque strings) before serialization, naming only the
        structural field path in error messages — never the rejected value.

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
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise StateStoreError("Corrupt state envelope: invalid JSON") from exc
        if not isinstance(envelope, dict) or envelope.get("schema_version") != 1:
            raise StateStoreError("Unsupported state envelope schema_version; expected 1")
        s = envelope.get("state")
        if not isinstance(s, dict):
            raise StateStoreError("Corrupt state envelope at state")
        from datasluice.domain import SyncState

        if "key" in envelope:
            return _decode_legacy_state(s)
        if set(envelope) != {"schema_version", "state"}:
            raise StateStoreError("Corrupt state envelope at top level")
        state = SyncState(
            cursor=_decode_completed_cursor(key, s.get("cursor", {})),
            partitions=_mapping_field(s, "partitions"),
            last_synced_at=s.get("last_synced_at"),
            extra=_mapping_field(s, "extra"),
        )
        _validate_state_for_write(key, state)
        return state

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
        _validate_state_for_write(key, state)
        path = self._state_path(key)
        payload = json.dumps(
            {
                "schema_version": 1,
                "state": _encode_state(state),
            },
            sort_keys=True,
        ).encode()

        if expected_prior is not _UNSET:
            actual_prior = self.read_raw(key)
            expected_bytes = expected_prior if isinstance(expected_prior, bytes) else b""
            actual_bytes = actual_prior if actual_prior is not None else b""
            expected_sha = hashlib.sha256(expected_bytes).hexdigest()
            actual_sha = hashlib.sha256(actual_bytes).hexdigest()
            if actual_sha != expected_sha:
                raise SyncStateConflictError("State changed since last read (CAS mismatch); re-read and re-apply")

        tmp_path = f"{self._base}/.{self._sha256_bytes(payload)}.tmp.{os.getpid()}.{random.randint(0, 1 << 32)}"
        try:
            self._fs.pipe_file(tmp_path, payload)
            self._fs.mv(tmp_path, path)
        except OSError as exc:
            raise StateStoreError("Failed to publish durable state envelope") from exc

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


def _decode_legacy_state(state: dict[str, Any]) -> SyncState:
    from datasluice.domain import SyncState

    return SyncState(
        cursor=_mapping_field(state, "cursor"),
        partitions=_mapping_field(state, "partitions"),
        last_synced_at=state.get("last_synced_at"),
        extra=_mapping_field(state, "extra"),
    )


def _mapping_field(state: dict[str, Any], field: str) -> dict[str, Any]:
    value = state.get(field, {})
    if not isinstance(value, dict):
        raise StateStoreError(f"Corrupt state envelope at state.{field}")
    return dict(value)


def _decode_completed_cursor(key: str, cursor: Any) -> dict[str, str]:
    if cursor == {}:
        return {}
    if not isinstance(cursor, dict) or set(cursor) != _COMPLETED_WATERMARK_KEYS:
        raise StateStoreError("Corrupt state envelope at state.cursor")
    if cursor.get("schema") != _COMPLETED_WATERMARK_SCHEMA or not isinstance(cursor.get("watermark"), str):
        raise StateStoreError("Corrupt state envelope at state.cursor")
    return {key: cursor["watermark"]}


def _encode_state(state: SyncState) -> dict[str, Any]:
    cursor: dict[str, str] = {}
    if state.cursor:
        watermark = next(iter(state.cursor.values()))
        cursor = {
            "schema": _COMPLETED_WATERMARK_SCHEMA,
            "watermark": watermark,
        }
    return {
        "cursor": cursor,
        "partitions": {},
        "last_synced_at": state.last_synced_at,
        "extra": state.extra,
    }


def _validate_state_for_write(key: str, state: SyncState) -> None:
    _validate_cursor(key, state.cursor)
    if type(state.partitions) is not dict or state.partitions:
        raise StateStoreError("Invalid durable SyncState at state.partitions")
    _validate_last_synced_at(state.last_synced_at)
    _validate_extra(state.extra)


def _validate_cursor(key: str, cursor: Any) -> None:
    if type(cursor) is not dict:
        raise StateStoreError("Invalid durable SyncState at state.cursor")
    if not cursor:
        return
    if len(cursor) != 1:
        raise StateStoreError("Invalid durable SyncState at state.cursor")
    cursor_key, watermark = next(iter(cursor.items()))
    if cursor_key != key or not isinstance(watermark, str) or not _is_completed_watermark(watermark):
        raise StateStoreError("Invalid durable SyncState at state.cursor")


_SECRET_SUBSTRINGS = (
    "signature",
    "credential",
    "password",
    "token",
    "secret",
    "bearer",
    "apikey",
    "api_key",
    "authorization",
)


def _is_completed_watermark(watermark: str) -> bool:
    if not watermark or len(watermark) > _MAX_WATERMARK_LENGTH:
        return False
    if len(watermark) == 64 and all(character in "0123456789abcdefABCDEF" for character in watermark):
        return True
    if _ETAG_PATTERN.fullmatch(watermark) is not None:
        return not _contains_secret_material(watermark)
    try:
        parsed = parsedate_to_datetime(watermark)
    except (TypeError, ValueError, OverflowError):
        return False
    return parsed is not None and parsed.tzinfo is not None and parsed.utcoffset() is not None


def _contains_secret_material(watermark: str) -> bool:
    lowered = watermark.lower()
    if "://" in lowered:
        return True
    if "@" in lowered:
        return True
    if "?=" in lowered:
        return True
    for substring in _SECRET_SUBSTRINGS:
        if substring in lowered:
            return True
    for char in watermark:
        code = ord(char)
        if code < 0x20:
            return True
    return False


def _validate_last_synced_at(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value or len(value) > _MAX_TIMESTAMP_LENGTH:
        raise StateStoreError("Invalid durable SyncState at state.last_synced_at")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise StateStoreError("Invalid durable SyncState at state.last_synced_at") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise StateStoreError("Invalid durable SyncState at state.last_synced_at")


def _validate_extra(extra: Any) -> None:
    if type(extra) is not dict:
        raise StateStoreError("Invalid durable SyncState at state.extra")
    if not extra:
        return
    if set(extra) != {"datasluice_checkpoint"}:
        raise StateStoreError("Invalid durable SyncState at state.extra")
    checkpoint = extra["datasluice_checkpoint"]
    if not isinstance(checkpoint, dict) or set(checkpoint) != _CHECKPOINT_KEYS:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint")
    position = checkpoint["position"]
    if not isinstance(position, dict) or set(position) != _CHECKPOINT_POSITION_KEYS:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint.position")
    next_batch_index = checkpoint["next_batch_index"]
    row_group_index = position["row_group_index"]
    if (
        type(checkpoint["version"]) is not int
        or checkpoint["version"] != 1
        or checkpoint["status"] != "in_progress"
        or type(next_batch_index) is not int
        or type(row_group_index) is not int
        or next_batch_index < 0
        or row_group_index < 0
        or next_batch_index != row_group_index
        or position["kind"] != "parquet_row_group"
    ):
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint")


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
