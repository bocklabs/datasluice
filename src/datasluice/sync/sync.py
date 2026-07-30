"""Checkpointed resource synchronization and per-resource outcomes."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from datasluice.logging import get_logger

if TYPE_CHECKING:
    from datasluice.domain import SyncState

logger = get_logger("sync.sync")

_CONDITIONAL_SYNC_READY = True


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
    from datasluice.sync.materialize import materialize

    for resource in resources:
        kind = resource.access.kind if resource.access is not None else "http_download"
        if kind in ("query", "stream"):
            yield SyncOutcome(resource, action="skipped-unsupported")
            continue

        key = f"{resource.id}"
        prior = state_store.get(key)
        if resume and prior is not None:
            yield SyncOutcome(resource, action="resumed", state_key=key)
            continue

        watermark = prior.cursor.get(resource.id) if prior is not None else None
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

        record = materialize(resource, reader=materialize_reader, destination_uri=destination_uri)
        checksum = record[3]
        if fresh_watermark is None and watermark is not None and checksum == watermark:
            yield SyncOutcome(resource, action="skipped-unchanged", record=record, state_key=key)
            continue

        state_store.put(key, _sync_state(resource.id, fresh_watermark or checksum))
        yield SyncOutcome(resource, action="materialized", record=record, state_key=key)


def _sync_state(resource_id: str, watermark: str) -> SyncState:
    from datasluice.domain import SyncState

    return SyncState(cursor={resource_id: watermark}, last_synced_at=_utcnow_iso())


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
