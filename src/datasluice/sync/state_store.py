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
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any

from datasluice._uri import sanitize_uri
from datasluice.exceptions import StateStoreError, SyncStateConflictError
from datasluice.io.filesystem import safe_remove
from datasluice.logging import get_logger

if TYPE_CHECKING:
    from datasluice.domain import SyncState

logger = get_logger("sync.state_store")

_GLOBAL_LOCKS: dict[str, threading.RLock] = {}
_GLOBAL_LOCKS_USERS: dict[str, int] = {}
_GLOBAL_LOCKS_GUARD = threading.Lock()

_UNSET = object()
_SECRET_FREE_STATE_READY = True
_ADVERSARIAL_VALIDATOR_READY = True
_ATOMIC_CAS_READY = True
_COMPLETED_WATERMARK_SCHEMA = "datasluice_completed_watermark_v1"
_COMPLETED_WATERMARK_KEYS = {"schema", "watermark"}
_COMPLETED_ARTIFACT_KEYS = {"destination_identity", "destination_size", "destination_checksum"}
_LEGACY_COMPLETED_ARTIFACT_KEYS = {"destination_uri", "destination_size", "destination_checksum"}
_CHECKPOINT_KEYS = {"version", "status", "next_batch_index", "position"}
_CHECKPOINT_POSITION_KEYS = {"kind", "row_group_index"}
_MAX_TIMESTAMP_LENGTH = 128
_MAX_WATERMARK_LENGTH = 256
_MAX_DESTINATION_URI_LENGTH = 4096
_ETAG_PATTERN = re.compile(r'(?:W/)?"[\x21\x23-\x7e\x80-\xff]*"\Z')

# fsspec backends whose ``mv`` is a true atomic rename (CR-11). On these
# backends the per-key threading lock makes the compare-read and the rename
# indivisible within a process, so CAS is preventive same-process. On any other
# backend (generic copy-then-remove ``mv``) CAS is detection-only — a
# cross-process observer may briefly see no file — and a warning is logged.
_ATOMIC_MV_BACKENDS: frozenset[str] = frozenset({"file", "local", "memory"})


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
    discipline). Compare-and-swap (CAS) is provided through the additive
    :class:`datasluice.ports.state_store.AtomicStateStore` capability Protocol:
    :meth:`read_version` returns the raw envelope bytes for CAS comparison and
    :meth:`conditional_put` writes only when the on-disk version matches a
    caller-supplied ``expected_prior``. A per-key :class:`threading.Lock`
    (``_locks``) serializes the compare-read and the atomic-move so they are
    indivisible within a single process — two barrier-synchronized
    expected-absent writers cannot both succeed; the loser raises
    :class:`SyncStateConflictError` (D-P7-27, CR-02).

    Backend atomicity is declared via :data:`_ATOMIC_MV_BACKENDS`. On backends
    whose ``mv`` is a true atomic rename (local ``file``/, ``memory``) the
    per-key lock makes CAS *preventive* same-process. On non-atomic backends
    (generic copy-then-remove ``mv`` on remote object stores) CAS is
    *detection-only* — a cross-process observer may briefly see no file — and a
    warning is logged; the per-key lock still provides same-process safety
    (CR-11, 01-5, 06-7).

    Backend errors are discriminated: only :class:`FileNotFoundError` maps to
    absent state (``None``). :class:`PermissionError`, :class:`TimeoutError`,
    and other :class:`OSError` subclasses surface as
    :class:`StateStoreError` and are never swallowed as missing state (CR-03).

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

    @property
    def _is_atomic_mv(self) -> bool:
        """Whether this backend's ``mv`` is a true atomic rename (CR-11)."""
        protocol = self._fs.protocol
        if isinstance(protocol, str):
            return protocol in _ATOMIC_MV_BACKENDS
        return any(entry in _ATOMIC_MV_BACKENDS for entry in protocol)

    def _lock_scope(self, key: str) -> str:
        """Return the process-global lock scope for *key* on this store (CR-02).

        Two :class:`FileStateStore` instances that target the same backend
        protocol and the same base URI produce the same scope for the same
        *key*, so their per-key locks coordinate even when the instances do
        not share Python identity. Backends with different protocols never
        share storage even when paths collide, so the protocol is part of the
        scope.
        """
        protocol = self._fs.protocol
        if isinstance(protocol, str):
            protocol_str = protocol
        else:
            protocol_str = "+".join(sorted(protocol))
        return f"{protocol_str}::{self._base}::{self._state_path(key)}"

    @contextmanager
    def key_lock(self, key: str) -> Iterator[threading.RLock]:
        """Hold the per-key lock for *key* so callers serialize a multi-step transaction (CR-03).

        Returns a re-entrant lock scoped by backend+base URI+state path, so a
        caller that holds this lock around (materialize artifact + CAS state)
        serializes publication end-to-end against any other writer for the same
        key, including writers using a separate :class:`FileStateStore`
        instance (CR-02). Re-entrancy lets the same thread call
        :meth:`conditional_put` (which acquires the same lock) inside this
        context without deadlock.
        """
        with self._key_lock_held(key) as lock:
            yield lock

    @contextmanager
    def _key_lock_held(self, key: str) -> Iterator[threading.RLock]:
        """Acquire (lazily creating) the process-global per-key lock, tracking users (CR-02)."""
        scope = self._lock_scope(key)
        with _GLOBAL_LOCKS_GUARD:
            lock = _GLOBAL_LOCKS.get(scope)
            if lock is None:
                lock = threading.RLock()
                _GLOBAL_LOCKS[scope] = lock
            _GLOBAL_LOCKS_USERS[scope] = _GLOBAL_LOCKS_USERS.get(scope, 0) + 1
        try:
            with lock:
                yield lock
        finally:
            with _GLOBAL_LOCKS_GUARD:
                remaining = _GLOBAL_LOCKS_USERS.get(scope, 0) - 1
                if remaining > 0:
                    _GLOBAL_LOCKS_USERS[scope] = remaining
                else:
                    _GLOBAL_LOCKS_USERS.pop(scope, None)
                    _GLOBAL_LOCKS.pop(scope, None)

    def _state_path(self, key: str) -> str:
        """Return the SHA-256-hexdigest (.json) path for *key* (T-07-03 mitigation)."""
        digest = hashlib.sha256(key.encode()).hexdigest()
        return f"{self._base}/{digest}.json"

    def get(self, key: str) -> SyncState | None:
        """Load the :class:`SyncState` for *key*, or ``None`` if absent.

        Raises:
            StateStoreError: if the state file is corrupt (bad JSON), its
                envelope ``schema_version`` is unsupported, or a backend error
                other than :class:`FileNotFoundError` occurs (permission,
                timeout, I/O). Never silently treats a backend failure as "no
                state" (CR-03) — staleness is worse than a loud failure (D-P7-03).
        """
        raw = self.read_raw(key)
        if raw is None:
            return None
        return self._decode_envelope(key, raw)

    def get_with_version(self, key: str) -> tuple[SyncState | None, bytes | None]:
        """Atomically load the state and its CAS version from one backend read (CR-01).

        Returns ``(state, version)`` where ``version`` is the raw envelope
        bytes read for *key* (or ``None`` if absent) and ``state`` is the
        decoded :class:`SyncState` (or ``None``). Both values are derived from
        one :meth:`read_raw` call, so no intervening writer can split the
        state used for sync decisions from the version used as the CAS
        ``expected_prior``. Pass the returned ``version`` directly into the
        next :meth:`conditional_put`; do not call :meth:`read_version`
        separately between read and write.
        """
        raw = self.read_raw(key)
        if raw is None:
            return None, None
        return self._decode_envelope(key, raw), raw

    def _decode_envelope(self, key: str, raw: bytes) -> SyncState:
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
            if envelope["key"] != key:
                raise StateStoreError("Legacy state envelope key does not match lookup key")
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

        Raises:
            StateStoreError: on a backend error other than
                :class:`FileNotFoundError` (CR-03).
        """
        path = self._state_path(key)
        try:
            return self._fs.cat_file(path)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise StateStoreError(f"Failed to read state for key at {sanitize_uri(path)}") from exc

    def read_version(self, key: str) -> bytes | None:
        """Return the raw envelope bytes for *key*, or ``None`` if absent (:class:`AtomicStateStore`).

        Alias for :meth:`read_raw`. Callers pass the returned bytes back into
        :meth:`conditional_put` as ``expected_prior`` so a concurrent writer's
        intervening commit is detected instead of silently overwritten (D-P7-27).
        """
        return self.read_raw(key)

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

        Note: the compare-read and rename in this method are NOT guarded by the
        per-key lock — callers needing same-process indivisibility between two
        writers must use :meth:`conditional_put` instead. This method remains
        for unconditional writes and backward compatibility.

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
        payload = _serialize_state(state)

        if expected_prior is not _UNSET:
            actual_prior = self.read_raw(key)
            expected_bytes = expected_prior if isinstance(expected_prior, bytes) else b""
            actual_bytes = actual_prior if actual_prior is not None else b""
            expected_sha = hashlib.sha256(expected_bytes).hexdigest()
            actual_sha = hashlib.sha256(actual_bytes).hexdigest()
            if actual_sha != expected_sha:
                raise SyncStateConflictError("State changed since last read (CAS mismatch); re-read and re-apply")

        self._publish(key, payload)

    def conditional_put(self, key: str, state: SyncState, expected_prior: bytes | None) -> bytes:
        """Persist *state* under *key* only if the current version matches ``expected_prior``.

        The per-key threading lock (``_locks``) is held across the version check
        and the atomic write, making them indivisible within a single process.
        Two same-process writers that both pass the version check cannot both
        commit; the loser raises :class:`SyncStateConflictError` (CR-02).

        On non-atomic-``mv`` backends (see :data:`_ATOMIC_MV_BACKENDS`) a
        warning is logged and the write proceeds — CAS is detection-only
        cross-process, but the per-key lock still serializes same-process
        writers (CR-11, 01-5, 06-7).

        Args:
            key: The sync-state key.
            state: The :class:`SyncState` to persist.
            expected_prior: Raw bytes from :meth:`read_version` (``None`` =
                "expected absent").

        Returns:
            The committed envelope bytes (the new CAS version). Callers that
            chain another :meth:`conditional_put` MUST pass the returned bytes
            as the next ``expected_prior`` rather than re-reading the version
            separately (CR-01: re-reading after the write opens a TOCTOU gap
            where an interloper can commit between this return and the next
            expected_prior).

        Raises:
            SyncStateConflictError: if the on-disk version does not match
                ``expected_prior`` (CAS lost a race).
            StateStoreError: on any write I/O failure.
        """
        if not self._is_atomic_mv:
            logger.warning(
                "conditional_put on non-atomic-mv backend (%s): CAS is detection-only cross-process",
                self._fs.protocol,
            )
        _validate_state_for_write(key, state)
        payload = _serialize_state(state)
        with self._key_lock_held(key) as _lock:
            actual_prior = self.read_raw(key)
            expected_bytes = expected_prior if isinstance(expected_prior, bytes) else b""
            actual_bytes = actual_prior if actual_prior is not None else b""
            expected_sha = hashlib.sha256(expected_bytes).hexdigest()
            actual_sha = hashlib.sha256(actual_bytes).hexdigest()
            if actual_sha != expected_sha:
                raise SyncStateConflictError("State changed since last read (CAS mismatch); re-read and re-apply")
            self._publish(key, payload)
        return payload

    def delete(self, key: str) -> None:
        """Remove the state file for *key*; a missing file is tolerated (idempotent).

        Raises:
            StateStoreError: on a backend error other than
                :class:`FileNotFoundError` (CR-03).
        """
        path = self._state_path(key)
        try:
            self._fs.rm(path)
        except FileNotFoundError:
            logger.debug("delete: state file absent (tolerated): %s", sanitize_uri(path))
        except OSError as exc:
            raise StateStoreError(f"Failed to delete state for key at {sanitize_uri(path)}") from exc

    def _publish(self, key: str, payload: bytes) -> None:
        """Write ``payload`` to the key's path via temp-file + atomic rename."""
        path = self._state_path(key)
        tmp_path = f"{self._base}/.{self._sha256_bytes(payload)}.tmp.{os.getpid()}.{random.randint(0, 1 << 32)}"
        try:
            self._fs.pipe_file(tmp_path, payload)
            self._fs.mv(tmp_path, path)
        except OSError as exc:
            safe_remove(self._fs, tmp_path)
            raise StateStoreError("Failed to publish durable state envelope") from exc

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


def _serialize_state(state: SyncState) -> bytes:
    """Serialize *state* into the canonical sorted-key envelope bytes for durable publish."""
    return json.dumps(
        {
            "schema_version": 1,
            "state": _encode_state(state),
        },
        sort_keys=True,
    ).encode()


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
    if set(extra) == {"datasluice_completed_artifact"}:
        _validate_completed_artifact(extra["datasluice_completed_artifact"])
        return
    if set(extra) != {"datasluice_checkpoint"}:
        raise StateStoreError("Invalid durable SyncState at state.extra")
    checkpoint = extra["datasluice_checkpoint"]
    if not isinstance(checkpoint, dict):
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint")
    version = checkpoint.get("version")
    if version == 2:
        _validate_checkpoint_v2(checkpoint)
    elif version == 3:
        _validate_checkpoint_v3(checkpoint)
    elif version == 1:
        _validate_checkpoint_v1(checkpoint)
    else:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint.version")


def _validate_checkpoint_v2(checkpoint: dict[str, Any]) -> None:
    if set(checkpoint) != {"version", "status", "next_batch_index", "position", "source_version"}:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint")
    position = checkpoint["position"]
    if not isinstance(position, dict) or set(position) != _CHECKPOINT_POSITION_KEYS:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint.position")
    next_batch_index = checkpoint["next_batch_index"]
    row_group_index = position["row_group_index"]
    source_version = checkpoint["source_version"]
    if (
        type(next_batch_index) is not int
        or type(row_group_index) is not int
        or next_batch_index < 0
        or row_group_index < 0
        or position["kind"] != "parquet_row_group"
        or checkpoint["status"] != "in_progress"
        or not _is_source_version(source_version)
    ):
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint")


def _validate_checkpoint_v3(checkpoint: dict[str, Any]) -> None:
    if set(checkpoint) != {
        "version",
        "status",
        "next_batch_index",
        "position",
        "source_version",
        "destination_identity",
    }:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint")
    position = checkpoint["position"]
    if not isinstance(position, dict) or set(position) != _CHECKPOINT_POSITION_KEYS:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint.position")
    next_batch_index = checkpoint["next_batch_index"]
    row_group_index = position["row_group_index"]
    if (
        type(next_batch_index) is not int
        or type(row_group_index) is not int
        or next_batch_index < 0
        or row_group_index < 0
        or position["kind"] != "parquet_row_group"
        or checkpoint["status"] != "in_progress"
        or not _is_source_version(checkpoint["source_version"])
        or not _is_sha256(checkpoint["destination_identity"])
    ):
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint")


def _validate_completed_artifact(artifact: Any) -> None:
    if not isinstance(artifact, dict):
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_completed_artifact")
    if set(artifact) not in (_COMPLETED_ARTIFACT_KEYS, _LEGACY_COMPLETED_ARTIFACT_KEYS):
        try:
            from datasluice.domain import Artifact

            Artifact.from_dict(artifact)
        except Exception as exc:
            raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_completed_artifact") from exc
        return
    destination_size = artifact["destination_size"]
    destination_checksum = artifact["destination_checksum"]
    if set(artifact) == _COMPLETED_ARTIFACT_KEYS:
        destination_identity = artifact["destination_identity"]
        valid_destination = _is_sha256(destination_identity)
    else:
        destination_uri = artifact["destination_uri"]
        valid_destination = _is_safe_destination_uri(destination_uri)
    if (
        not valid_destination
        or type(destination_size) is not int
        or destination_size < 0
        or not isinstance(destination_checksum, str)
        or not _is_source_version(destination_checksum)
    ):
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_completed_artifact")


def _is_safe_destination_uri(value: Any) -> bool:
    if not isinstance(value, str) or not value or len(value) > _MAX_DESTINATION_URI_LENGTH:
        return False
    if any(ord(character) < 0x20 for character in value):
        return False
    try:
        from urllib.parse import urlsplit

        parts = urlsplit(value)
        if parts.username is not None or parts.password is not None or parts.query or parts.fragment:
            return False
    except ValueError:
        return False
    return True


def _is_source_version(value: Any) -> bool:
    return value is None or _is_sha256(value)


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdefABCDEF" for character in value)
    )


def _validate_checkpoint_v1(checkpoint: dict[str, Any]) -> None:
    if set(checkpoint) != _CHECKPOINT_KEYS:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint")
    position = checkpoint["position"]
    if not isinstance(position, dict) or set(position) != _CHECKPOINT_POSITION_KEYS:
        raise StateStoreError("Invalid durable SyncState at state.extra.datasluice_checkpoint.position")
    next_batch_index = checkpoint["next_batch_index"]
    row_group_index = position["row_group_index"]
    if (
        type(next_batch_index) is not int
        or type(row_group_index) is not int
        or next_batch_index < 0
        or row_group_index < 0
        or next_batch_index != row_group_index
        or position["kind"] != "parquet_row_group"
        or checkpoint["status"] != "in_progress"
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
