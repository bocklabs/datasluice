"""Checkpointed resource synchronization and per-resource outcomes."""

from __future__ import annotations

import io
from collections.abc import Iterable, Iterator
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import TYPE_CHECKING, Any

from datasluice.exceptions import DataSluiceError
from datasluice.logging import get_logger
from datasluice.ports import ResponseAwareReader
from datasluice.runtime.transport.base import RuntimeRequest
from datasluice.sync._identity import canonical_destination_identity, canonical_identity, validate_unique_identities

if TYPE_CHECKING:
    from datasluice.domain import Artifact, SyncState

LegacyArtifactRecord = tuple[str, str, int, str]

logger = get_logger("sync.sync")

_CONDITIONAL_SYNC_READY = True
_RESPONSE_AWARE_READER_READY = True
_WITHIN_RESOURCE_RESUME_READY = True
_FAILURE_BOUNDARY_READY = True
_ARTIFACT_HEALTH_READY = True


@dataclass(frozen=True)
class SyncOutcome:
    """Describe the result of synchronizing one resource."""

    resource: Any
    action: str
    record: Artifact | None = None
    state_key: str | None = None


def _resource_transaction(state_store: Any, key: str, *, is_atomic: bool) -> Any:
    """Return a context manager that serializes a per-resource sync transaction.

    For :class:`FileStateStore` (and any AtomicStateStore that exposes
    ``key_lock``), the per-key lock is held across the entire
    read-materialize-CAS-cleanup sequence so two writers for the same resource
    cannot interleave their artifact publication with their state CAS — the
    loser's CAS sees the winner's committed version and aborts before
    publishing. For stores without ``key_lock`` (e.g. external AtomicStateStore
    implementors that did not opt in), a no-op context manager is returned so
    behavior matches today's per-call CAS.
    """
    if is_atomic and hasattr(state_store, "key_lock"):
        return state_store.key_lock(key)
    return nullcontext()


@dataclass(slots=True)
class _SyncContext:
    prior: Any
    prior_version: bytes | None
    checkpoint: Any | None
    stable_source_version: str | None
    prior_artifact: Artifact | LegacyArtifactRecord | None
    destination_was_healthy: bool
    watermark: str | None
    checkpointed_kind: bool
    outcome: SyncOutcome | None = None


def _resume_outcome(
    resource: Any, prior: Any, key: str, destination_uri: str, *, resume: bool
) -> tuple[Artifact | LegacyArtifactRecord | None, SyncOutcome | None]:
    from datasluice.domain import Artifact
    from datasluice.sync.materialize import destination_health

    completed = _completed_artifact_record(prior, resource, destination_uri) if prior is not None else None
    if (
        resume
        and isinstance(completed, Artifact)
        and destination_health(resource, completed, destination_uri=destination_uri)
    ):
        return completed, SyncOutcome(resource, action="resumed", record=completed, state_key=key)
    return completed, None


def _load_sync_context(
    resource: Any, state_store: Any, key: str, destination_uri: str, *, is_atomic: bool, resume: bool
) -> _SyncContext:
    from datasluice.sync.materialize import destination_health

    if is_atomic:
        prior, prior_version = state_store.get_with_version(key)
    else:
        prior = state_store.get(key)
        prior_version = None
    checkpoint = _decode_checkpoint(prior) if prior is not None else None
    completed, outcome = _resume_outcome(resource, prior, key, destination_uri, resume=resume and checkpoint is None)
    if outcome is not None:
        return _SyncContext(prior, prior_version, None, None, completed, True, prior.cursor.get(key), False, outcome)
    kind = resource.access.kind if resource.access is not None else "http_download"
    checkpointed_kind = (resource.format or "").upper() == "PARQUET" and kind in {"local_file", "object_storage"}
    stable_source_version = _compute_source_version(resource) if checkpointed_kind else None
    if checkpoint is not None and _checkpoint_is_stale(checkpoint, stable_source_version, destination_uri):
        logger.warning("Checkpoint for resource %r is stale; discarding it and restarting", resource.id)
        checkpoint = None
        prior = None
        prior_version = None
    watermark = prior.cursor.get(key) if prior is not None else None
    prior_artifact = _completed_artifact_record(prior, resource, destination_uri)
    healthy = prior_artifact is not None and destination_health(
        resource, prior_artifact, destination_uri=destination_uri
    )
    return _SyncContext(
        prior, prior_version, checkpoint, stable_source_version, prior_artifact, healthy, watermark, checkpointed_kind
    )


def _checkpoint_is_stale(checkpoint: Any, source_version: str | None, destination_uri: str) -> bool:
    source_changed = (
        source_version is not None
        and checkpoint.source_version is not None
        and source_version != checkpoint.source_version
    )
    destination_changed = (
        checkpoint.destination_identity is not None
        and checkpoint.destination_identity != canonical_destination_identity(destination_uri)
    )
    return source_changed or destination_changed


def _conditional_reader(
    resource: Any,
    reader: Any,
    transport: Any | None,
    destination_uri: str,
    key: str,
    context: _SyncContext,
) -> tuple[Any, str | None, SyncOutcome | None]:
    from datasluice.domain import Artifact
    from datasluice.sync.materialize import destination_health

    kind = resource.access.kind if resource.access is not None else "http_download"
    url = getattr(resource.access, "url", None) or resource.url
    if kind != "http_download" or url is None or transport is None or not isinstance(reader, ResponseAwareReader):
        return reader, None, None
    if context.watermark is not None and _looks_like_sha256(context.watermark):
        return reader, None, None
    etag, last_modified = _conditional_validators(context.watermark)
    headers = {
        name: value
        for name, value in {"If-None-Match": etag, "If-Modified-Since": last_modified}.items()
        if value is not None
    }
    response = transport.send(RuntimeRequest(method="GET", url=url, headers=headers))
    if response.status_code == 304:
        completed = _completed_artifact_record(context.prior, resource, destination_uri)
        if isinstance(completed, Artifact) and destination_health(resource, completed, destination_uri=destination_uri):
            return reader, None, SyncOutcome(resource, action="skipped-unchanged", record=completed, state_key=key)
        return reader, None, None
    if not 200 <= response.status_code < 300:
        raise DataSluiceError(f"Conditional fetch for resource {resource.id!r} returned HTTP {response.status_code}")
    response_stream = _BufferedResponseStream(response.body)
    try:
        handed_stream = reader.open_response(resource, response_stream, headers=response.headers)
    except BaseException as exc:
        response_stream.__exit__(type(exc), exc, exc.__traceback__)
        raise
    return _SingleStreamReader(handed_stream), _preferred_watermark(response.headers), None


def _persist_batch_state(
    cursor: Any,
    *,
    state_store: Any,
    state_key: str,
    prior_version_box: list[bytes | None],
    source_version: str | None,
    destination_uri: str,
    is_atomic: bool,
) -> None:
    state = _in_progress_state(cursor, source_version, destination_uri)
    if is_atomic:
        prior_version_box[0] = state_store.conditional_put(state_key, state, prior_version_box[0])
    else:
        state_store.put(state_key, state)


def _materialize_resource(
    resource: Any,
    reader: Any,
    state_store: Any,
    destination_uri: str,
    key: str,
    context: _SyncContext,
    *,
    resume: bool,
    is_atomic: bool,
) -> tuple[Artifact, str, bool, list[bytes | None]]:
    from datasluice.data.batch_stream import BatchCursor, ParquetRowGroupPosition
    from datasluice.ports import CheckpointableResourceReader
    from datasluice.sync.materialize import cleanup_checkpointed, materialize, materialize_checkpointed

    prior_version_box = [context.prior_version]
    use_checkpointed = context.checkpointed_kind and (
        context.checkpoint is not None or context.prior_artifact is None or not context.destination_was_healthy
    )
    if not use_checkpointed:
        return (
            materialize(resource, reader=reader, destination_uri=destination_uri, stored_checksum=context.watermark),
            "materialized",
            False,
            prior_version_box,
        )
    action = "materialized"
    if resume and context.checkpoint is not None:
        if not isinstance(reader, CheckpointableResourceReader):
            raise DataSluiceError(
                f"continuation reader for resource {resource.id!r} cannot resume row group "
                f"{context.checkpoint.row_group_index}; reader lacks open_from_cursor"
            )
        cursor = BatchCursor(
            context.checkpoint.next_batch_index, ParquetRowGroupPosition(context.checkpoint.row_group_index)
        )
        stream = reader.open_from_cursor(resource, cursor)
        start_batch_index = context.checkpoint.next_batch_index
        action = "resumed"
    else:
        stream = reader.open(resource)
        start_batch_index = 0
    record = materialize_checkpointed(
        resource,
        stream=stream,
        destination_uri=destination_uri,
        start_batch_index=start_batch_index,
        on_batch_persisted=partial(
            _persist_batch_state,
            state_store=state_store,
            state_key=key,
            prior_version_box=prior_version_box,
            source_version=context.stable_source_version,
            destination_uri=destination_uri,
            is_atomic=is_atomic,
        ),
    )
    post_source_version = _compute_source_version(resource)
    if (
        post_source_version is not None
        and context.stable_source_version is not None
        and post_source_version != context.stable_source_version
    ):
        cleanup_checkpointed(resource, destination_uri=destination_uri)
        raise DataSluiceError(
            f"Source for resource {resource.id!r} changed during sync; "
            "aborting to avoid publishing a mixed-version artifact. Retry the sync."
        )
    return record, action, True, prior_version_box


def _finish_resource_sync(
    resource: Any,
    record: Artifact,
    action: str,
    use_checkpointed: bool,
    fresh_watermark: str | None,
    state_store: Any,
    destination_uri: str,
    key: str,
    context: _SyncContext,
    prior_version_box: list[bytes | None],
    *,
    is_atomic: bool,
) -> SyncOutcome:
    from datasluice.domain import Artifact
    from datasluice.sync.materialize import cleanup_checkpointed

    checksum = record.content_digest.value
    if (
        fresh_watermark is None
        and context.watermark is not None
        and checksum == context.watermark
        and context.destination_was_healthy
        and not use_checkpointed
        and isinstance(context.prior_artifact, Artifact)
    ):
        return SyncOutcome(resource, action="skipped-unchanged", record=context.prior_artifact, state_key=key)
    completed_state = _completed_sync_state(key, fresh_watermark or checksum, record)
    if is_atomic:
        state_store.conditional_put(key, completed_state, prior_version_box[0])
    else:
        state_store.put(key, completed_state)
    if use_checkpointed:
        cleanup_checkpointed(resource, destination_uri=destination_uri)
    return SyncOutcome(resource, action=action, record=record, state_key=key)


def sync_resources(
    resources: Iterable[Any],
    *,
    state_store: Any,
    reader: Any,
    destination_uri: str,
    transport: Any | None = None,
    resume: bool = False,
) -> Iterator[SyncOutcome]:
    """Synchronize resources and emit each outcome after its state checkpoint.

    Note:
        For HTTP resources whose reader does not implement
        :class:`~datasluice.ports.ResponseAwareReader`, the conditional GET
        buffers the response body only to decide the fresh watermark and then
        discards it; materialization re-fetches the URL through the reader's
        own ``open`` path. Handing the buffered bytes to such readers would
        change receipt identity (compression and format sniffing run on a
        transport source, not on caller bytes), so this double fetch is kept
        deliberately; ``ResponseAwareReader`` implementations consume the
        buffered response directly and pay no second download.
    """
    resource_list = list(resources)
    validate_unique_identities(resource_list)

    # Probe once: stores implementing the additive AtomicStateStore capability
    # (FileStateStore) get CAS-protected transitions; others (InMemoryStateStore,
    # external implementors) fall back to unconditional put.
    from datasluice.ports import AtomicStateStore

    is_atomic = isinstance(state_store, AtomicStateStore)

    for resource in resource_list:
        kind = resource.access.kind if resource.access is not None else "http_download"
        if kind in ("query", "stream"):
            yield SyncOutcome(resource, action="skipped-unsupported")
            continue
        key = canonical_identity(resource)
        with _resource_transaction(state_store, key, is_atomic=is_atomic):
            context = _load_sync_context(
                resource, state_store, key, destination_uri, is_atomic=is_atomic, resume=resume
            )
            if context.outcome is not None:
                yield context.outcome
                continue
            materialize_reader, fresh_watermark, outcome = _conditional_reader(
                resource, reader, transport, destination_uri, key, context
            )
            if outcome is not None:
                yield outcome
                continue
            record, action, use_checkpointed, prior_version_box = _materialize_resource(
                resource,
                materialize_reader,
                state_store,
                destination_uri,
                key,
                context,
                resume=resume,
                is_atomic=is_atomic,
            )
            yield _finish_resource_sync(
                resource,
                record,
                action,
                use_checkpointed,
                fresh_watermark,
                state_store,
                destination_uri,
                key,
                context,
                prior_version_box,
                is_atomic=is_atomic,
            )


def _completed_sync_state(
    state_key: str,
    watermark: str,
    record: Artifact,
) -> SyncState:
    from datasluice.domain import SyncState

    return SyncState(
        cursor={state_key: watermark},
        last_synced_at=_utcnow_iso(),
        extra={"datasluice_completed_artifact": record.to_dict()},
    )


def _completed_artifact_record(
    state: SyncState | None,
    resource: Any,
    destination_uri: str,
) -> Artifact | LegacyArtifactRecord | None:
    if state is None:
        return None
    artifact = state.extra.get("datasluice_completed_artifact")
    if not isinstance(artifact, dict):
        return None
    from datasluice.domain import Artifact

    try:
        return Artifact.from_dict(artifact)
    except DataSluiceError:
        pass
    expected_uri = f"{destination_uri.rstrip('/')}/{canonical_identity(resource)}.parquet"
    if set(artifact) == {"destination_identity", "destination_size", "destination_checksum"}:
        if artifact["destination_identity"] != canonical_destination_identity(destination_uri):
            return None
        uri = expected_uri
    elif set(artifact) == {"destination_uri", "destination_size", "destination_checksum"}:
        uri = artifact["destination_uri"]
        if uri != expected_uri:
            return None
    else:
        return None
    size = artifact["destination_size"]
    checksum = artifact["destination_checksum"]
    if (
        not isinstance(uri, str)
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
        or not isinstance(checksum, str)
    ):
        return None
    return uri, "application/x-parquet", size, checksum


def _in_progress_state(cursor: Any, source_version: str | None, destination_uri: str) -> SyncState:
    from datasluice.data.batch_stream import BatchCursor, ParquetRowGroupPosition
    from datasluice.domain import SyncState

    if not isinstance(cursor, BatchCursor) or not isinstance(cursor.position, ParquetRowGroupPosition):
        raise DataSluiceError("Cannot persist an opaque or unsupported continuation cursor")
    checkpoint = {
        "version": 3,
        "status": "in_progress",
        "next_batch_index": cursor.next_batch_index,
        "position": {
            "kind": "parquet_row_group",
            "row_group_index": cursor.position.row_group_index,
        },
        "source_version": source_version,
        "destination_identity": canonical_destination_identity(destination_uri),
    }
    return SyncState(extra={"datasluice_checkpoint": checkpoint})


def _decode_checkpoint(state: SyncState) -> Any | None:
    if "datasluice_checkpoint" not in state.extra:
        return None
    checkpoint = state.extra["datasluice_checkpoint"]
    if not isinstance(checkpoint, dict):
        raise DataSluiceError("Corrupt datasluice checkpoint: expected dict")
    version = checkpoint.get("version")
    if version == 2:
        return _decode_checkpoint_v2(checkpoint)
    if version == 3:
        return _decode_checkpoint_v3(checkpoint)
    if version == 1:
        return _decode_checkpoint_v1(checkpoint)
    raise DataSluiceError(f"Corrupt datasluice checkpoint: unsupported version {version!r}")


def _decode_checkpoint_v2(checkpoint: dict[str, Any]) -> Any:
    expected_keys = {"version", "status", "next_batch_index", "position", "source_version"}
    if set(checkpoint) != expected_keys:
        raise DataSluiceError("Corrupt datasluice checkpoint v2: unexpected keys")
    position = checkpoint["position"]
    position_keys = {"kind", "row_group_index"}
    source_version = checkpoint["source_version"]
    if (
        checkpoint["status"] != "in_progress"
        or type(checkpoint["next_batch_index"]) is not int
        or checkpoint["next_batch_index"] < 0
        or not isinstance(position, dict)
        or set(position) != position_keys
        or position["kind"] != "parquet_row_group"
        or type(position["row_group_index"]) is not int
        or position["row_group_index"] < 0
        or not _is_source_version(source_version)
    ):
        raise DataSluiceError("Corrupt datasluice checkpoint v2: invalid field values")
    return _Checkpoint(
        checkpoint["next_batch_index"],
        position["row_group_index"],
        source_version,
        None,
    )


def _decode_checkpoint_v3(checkpoint: dict[str, Any]) -> Any:
    expected_keys = {"version", "status", "next_batch_index", "position", "source_version", "destination_identity"}
    if set(checkpoint) != expected_keys:
        raise DataSluiceError("Corrupt datasluice checkpoint v3: unexpected keys")
    position = checkpoint["position"]
    source_version = checkpoint["source_version"]
    destination_identity = checkpoint["destination_identity"]
    if (
        checkpoint["status"] != "in_progress"
        or type(checkpoint["next_batch_index"]) is not int
        or checkpoint["next_batch_index"] < 0
        or not isinstance(position, dict)
        or set(position) != {"kind", "row_group_index"}
        or position["kind"] != "parquet_row_group"
        or type(position["row_group_index"]) is not int
        or position["row_group_index"] < 0
        or not _is_source_version(source_version)
        or not _is_sha256(destination_identity)
    ):
        raise DataSluiceError("Corrupt datasluice checkpoint v3: invalid field values")
    return _Checkpoint(
        checkpoint["next_batch_index"],
        position["row_group_index"],
        source_version,
        destination_identity,
    )


def _decode_checkpoint_v1(checkpoint: dict[str, Any]) -> Any:
    expected_keys = {"version", "status", "next_batch_index", "position"}
    if set(checkpoint) != expected_keys:
        raise DataSluiceError("Corrupt datasluice checkpoint v1: unexpected keys")
    position = checkpoint["position"]
    position_keys = {"kind", "row_group_index"}
    if (
        checkpoint["status"] != "in_progress"
        or type(checkpoint["next_batch_index"]) is not int
        or checkpoint["next_batch_index"] < 0
        or not isinstance(position, dict)
        or set(position) != position_keys
        or position["kind"] != "parquet_row_group"
        or type(position["row_group_index"]) is not int
        or position["row_group_index"] < 0
    ):
        raise DataSluiceError("Corrupt datasluice checkpoint v1: invalid field values")
    return _Checkpoint(
        checkpoint["next_batch_index"],
        position["row_group_index"],
        None,
        None,
    )


@dataclass(frozen=True)
class _Checkpoint:
    """Decoded in-progress checkpoint carrying batch index, physical position, and source version."""

    next_batch_index: int
    row_group_index: int
    source_version: str | None
    destination_identity: str | None


def _is_source_version(value: Any) -> bool:
    return value is None or _is_sha256(value)


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdefABCDEF" for character in value)
    )


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


class _SingleStreamReader:
    def __init__(self, stream: Any) -> None:
        self._stream = stream

    def open(self, resource: Any) -> Any:
        stream = self._stream
        if stream is None:
            raise RuntimeError(f"Pre-opened stream for resource {resource.id!r} was already consumed")
        self._stream = None
        return stream

    def close(self) -> None:
        """Close the wrapped stream if it has not been handed off via :meth:`open`."""
        stream = self._stream
        self._stream = None
        if stream is not None and hasattr(stream, "close"):
            stream.close()

    def __enter__(self) -> _SingleStreamReader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class _BufferedResponseStream:
    def __init__(self, body: bytes) -> None:
        self._stream = io.BytesIO(body)
        self._closed = False
        self._entered = False
        self._exited = False
        self.enter_count = 0
        self.exit_count = 0

    def __enter__(self) -> io.BytesIO:
        if self._closed:
            raise RuntimeError("Buffered response stream was already closed")
        if not self._entered:
            self._entered = True
            self.enter_count += 1
        return self._stream

    def __exit__(self, *exc: object) -> None:
        if self._exited:
            return
        self._exited = True
        self.exit_count += 1
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stream.close()


def _conditional_validators(watermark: str | None) -> tuple[str | None, str | None]:
    if watermark is None:
        return None, None
    if watermark.startswith(('"', "W/")):
        return watermark, None
    return None, watermark


def _looks_like_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdefABCDEF" for character in value)


def _preferred_watermark(headers: Any) -> str | None:
    if headers is None:
        return None
    normalized = {str(name).lower(): value for name, value in headers.items()}
    etag = normalized.get("etag")
    if etag is not None:
        return str(etag)
    last_modified = normalized.get("last-modified")
    return str(last_modified) if last_modified is not None else None


def _compute_source_version(resource: Any) -> str | None:
    """Return a SHA-256 hex digest of the raw source bytes for checkpoint identity.

    The digest is computed once per resource per pass and carried through every
    batch checkpoint so resume can detect a source replacement between passes.
    Returns ``None`` for access kinds that are not checkpointed (HTTP).
    """
    import hashlib

    access = resource.access
    if access is None:
        return None
    if access.kind == "local_file":
        try:
            digest = hashlib.sha256()
            with open(access.path, "rb") as source:
                for chunk in iter(lambda: source.read(65536), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except OSError:
            return None
    if access.kind == "object_storage":
        from datasluice.io.filesystem import open_filesystem

        try:
            fs = open_filesystem(access.uri)
            digest = hashlib.sha256()
            path = _strip_uri_scheme(access.uri, fs)
            with fs.open(path, "rb") as source:
                for chunk in iter(lambda: source.read(65536), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except OSError:
            return None
    return None


def _strip_uri_scheme(uri: str, fs: Any) -> str:
    """Strip the fsspec storage scheme from *uri* to produce the path component."""
    protocol = getattr(fs, "protocol", None)
    if isinstance(protocol, str):
        for prefix in (f"{protocol}://", f"{protocol}:"):
            if uri.startswith(prefix):
                return uri[len(prefix) :].lstrip("/")
    if "://" in uri:
        return uri.split("://", 1)[1].lstrip("/")
    return uri
