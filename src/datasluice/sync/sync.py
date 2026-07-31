"""Checkpointed resource synchronization and per-resource outcomes."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from datasluice.exceptions import DataSluiceError
from datasluice.logging import get_logger
from datasluice.sync._identity import canonical_identity, validate_unique_identities

if TYPE_CHECKING:
    from datasluice.domain import SyncState

logger = get_logger("sync.sync")

_CONDITIONAL_SYNC_READY = True
_WITHIN_RESOURCE_RESUME_READY = True
_FAILURE_BOUNDARY_READY = True


@dataclass(frozen=True)
class SyncOutcome:
    """Describe the result of synchronizing one resource."""

    resource: Any
    action: str
    record: Any | None = None
    state_key: str | None = None


def sync_resources(
    resources: Iterable[Any],
    *,
    state_store: Any,
    reader: Any,
    destination_uri: str,
    transport: Any | None = None,
    cache: Any | None = None,
    resume: bool = False,
) -> Iterator[SyncOutcome]:
    """Synchronize resources and emit each outcome after its state checkpoint."""
    from datasluice.sync.materialize import materialize, materialize_checkpointed

    resource_list = list(resources)
    validate_unique_identities(resource_list)

    # Probe once: stores implementing the additive AtomicStateStore capability
    # (FileStateStore) get CAS-protected transitions; others (InMemoryStateStore,
    # external implementors) fall back to unconditional put (CR-02).
    from datasluice.ports import AtomicStateStore

    is_atomic = isinstance(state_store, AtomicStateStore)

    for resource in resource_list:
        kind = resource.access.kind if resource.access is not None else "http_download"
        if kind in ("query", "stream"):
            yield SyncOutcome(resource, action="skipped-unsupported")
            continue

        key = canonical_identity(resource)
        prior = state_store.get(key)
        checkpoint = _decode_checkpoint(prior) if prior is not None else None
        if resume and prior is not None and checkpoint is None:
            yield SyncOutcome(resource, action="resumed", state_key=key)
            continue

        if checkpoint is not None:
            current_version = _compute_source_version(resource)
            if (
                current_version is not None
                and checkpoint.source_version is not None
                and current_version != checkpoint.source_version
            ):
                logger.warning(
                    "Source for resource %r changed since checkpoint; discarding checkpoint and restarting",
                    resource.id,
                )
                checkpoint = None
                prior = None

        watermark = prior.cursor.get(key) if prior is not None else None
        materialize_reader = reader
        fresh_watermark: str | None = None
        access = resource.access
        url = getattr(access, "url", None) or resource.url

        if kind == "http_download" and url is not None:
            from datasluice.ports import ConditionalTransport

            should_fetch_conditionally = watermark is None or not _looks_like_sha256(watermark)
            if transport is not None and isinstance(transport, ConditionalTransport) and should_fetch_conditionally:
                etag, last_modified = _conditional_validators(watermark)
                result = transport.conditional_fetch(
                    url,
                    if_none_match=etag,
                    if_modified_since=last_modified,
                )
                if result.status_code == 304:
                    yield SyncOutcome(resource, action="skipped-unchanged", state_key=key)
                    continue
                fresh_watermark = _preferred_watermark(result.headers)
                if result.stream is not None and hasattr(reader, "open_response"):
                    materialize_reader = _SingleStreamReader(
                        reader.open_response(resource, result.stream, headers=result.headers)
                    )
                elif result.stream is not None:
                    with result.stream:
                        pass

        # Capture the prior raw version so every state transition passes it through the
        # conditional-write path (CR-02). The box is mutated by the checkpoint callback so
        # each batch checkpoint chains to the next and the completed write chains from the
        # last checkpoint.
        prior_version_box: list[bytes | None] = [state_store.read_version(key) if is_atomic else None]

        action = "materialized"
        use_checkpointed = (resource.format or "").upper() == "PARQUET" and kind in ("local_file", "object_storage")
        if use_checkpointed:
            source_version = _compute_source_version(resource)
            if resume and checkpoint is not None:
                from datasluice.data.batch_stream import BatchCursor, ParquetRowGroupPosition
                from datasluice.ports import CheckpointableResourceReader

                if not isinstance(reader, CheckpointableResourceReader):
                    raise DataSluiceError(
                        f"continuation reader for resource {resource.id!r} cannot resume row group "
                        f"{checkpoint.row_group_index}; reader lacks open_from_cursor"
                    )
                cursor = BatchCursor(
                    checkpoint.next_batch_index,
                    ParquetRowGroupPosition(checkpoint.row_group_index),
                )
                stream = reader.open_from_cursor(resource, cursor)
                start_batch_index = checkpoint.next_batch_index
                action = "resumed"
            else:
                stream = materialize_reader.open(resource)
                start_batch_index = 0

            def persist_batch(
                cursor: Any,
                state_key: str = key,
                _prior_version_box: list[bytes | None] = prior_version_box,
                _source_version: str | None = source_version,
            ) -> None:
                state = _in_progress_state(cursor, _source_version)
                if is_atomic:
                    state_store.conditional_put(state_key, state, _prior_version_box[0])
                    _prior_version_box[0] = state_store.read_version(state_key)
                else:
                    state_store.put(state_key, state)

            record = materialize_checkpointed(
                resource,
                stream=stream,
                destination_uri=destination_uri,
                start_batch_index=start_batch_index,
                on_batch_persisted=persist_batch,
            )
        else:
            record = materialize(
                resource,
                reader=materialize_reader,
                destination_uri=destination_uri,
                stored_checksum=watermark,
            )
        checksum = record[3]
        if fresh_watermark is None and watermark is not None and checksum == watermark:
            yield SyncOutcome(resource, action="skipped-unchanged", record=record, state_key=key)
            continue

        completed_state = _sync_state(key, fresh_watermark or checksum)
        if is_atomic:
            state_store.conditional_put(key, completed_state, prior_version_box[0])
        else:
            state_store.put(key, completed_state)
        yield SyncOutcome(resource, action=action, record=record, state_key=key)


def _sync_state(state_key: str, watermark: str) -> SyncState:
    from datasluice.domain import SyncState

    return SyncState(cursor={state_key: watermark}, last_synced_at=_utcnow_iso())


def _in_progress_state(cursor: Any, source_version: str | None) -> SyncState:
    from datasluice.data.batch_stream import BatchCursor, ParquetRowGroupPosition
    from datasluice.domain import SyncState

    if not isinstance(cursor, BatchCursor) or not isinstance(cursor.position, ParquetRowGroupPosition):
        raise DataSluiceError("Cannot persist an opaque or unsupported continuation cursor")
    checkpoint = {
        "version": 2,
        "status": "in_progress",
        "next_batch_index": cursor.next_batch_index,
        "position": {
            "kind": "parquet_row_group",
            "row_group_index": cursor.position.row_group_index,
        },
        "source_version": source_version,
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
        or not isinstance(position, dict)
        or set(position) != position_keys
        or position["kind"] != "parquet_row_group"
        or type(position["row_group_index"]) is not int
        or not _is_source_version(source_version)
    ):
        raise DataSluiceError("Corrupt datasluice checkpoint v2: invalid field values")
    return _Checkpoint(
        checkpoint["next_batch_index"],
        position["row_group_index"],
        source_version,
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
        or not isinstance(position, dict)
        or set(position) != position_keys
        or position["kind"] != "parquet_row_group"
        or type(position["row_group_index"]) is not int
    ):
        raise DataSluiceError("Corrupt datasluice checkpoint v1: invalid field values")
    return _Checkpoint(
        checkpoint["next_batch_index"],
        position["row_group_index"],
        None,
    )


@dataclass(frozen=True)
class _Checkpoint:
    """Decoded in-progress checkpoint carrying batch index, physical position, and source version."""

    next_batch_index: int
    row_group_index: int
    source_version: str | None


def _is_source_version(value: Any) -> bool:
    return value is None or (
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


def _conditional_validators(watermark: str | None) -> tuple[str | None, str | None]:
    if watermark is None:
        return None, None
    if watermark.startswith('"') or watermark.startswith("W/"):
        return watermark, None
    return None, watermark


def _looks_like_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdefABCDEF" for character in value)


def _preferred_watermark(headers: Any) -> str | None:
    if headers is None:
        return None
    etag = headers.get("ETag") or headers.get("etag")
    if etag is not None:
        return str(etag)
    last_modified = headers.get("Last-Modified") or headers.get("last-modified")
    return str(last_modified) if last_modified is not None else None


def _compute_source_version(resource: Any) -> str | None:
    """Return a SHA-256 hex digest of the raw source bytes for checkpoint identity (CR-07).

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
        except (FileNotFoundError, OSError):
            return None
    if access.kind == "object_storage":
        from datasluice.io.filesystem import open_filesystem

        fs = open_filesystem(access.uri)
        digest = hashlib.sha256()
        path = _strip_uri_scheme(access.uri, fs)
        with fs.open(path, "rb") as source:
            for chunk in iter(lambda: source.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()
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
