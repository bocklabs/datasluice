"""Immutable uData resource inputs and bounded upload sources."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import BinaryIO

from datasluice.domain.catalog.models import NativeRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.runtime.transport.base import UploadPart


def _freeze(value: object) -> object:
    if value is None or isinstance(value, (str, bool)) or type(value) is int:
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        raise ValueError("uData resource inputs require finite JSON numbers.")
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) and key for key in value):
            raise ValueError("uData resource input mappings require non-empty string keys.")
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    raise ValueError("uData resource inputs accept JSON-safe values only.")


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"uData resource {field_name} must be a non-empty string.")
    return value


class MidStreamUploadError(OSError):
    """A bounded upload source failed after the request already started sending bytes."""


class _BoundedSource:
    def __init__(self, source: BinaryIO, limit: int) -> None:
        self._source = source
        self._limit = limit
        self._read = 0
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("uData upload source is closed.")
        remaining = self._limit - self._read
        request_size = remaining + 1 if size < 0 else min(size, remaining + 1)
        chunk = self._source.read(request_size)
        if not isinstance(chunk, bytes):
            raise MidStreamUploadError("uData upload sources must return bytes.")
        self._read += len(chunk)
        if self._read > self._limit:
            raise MidStreamUploadError("uData upload source exceeds its byte limit.")
        return chunk

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._source.close()


@dataclass(frozen=True, slots=True)
class ResourceCreateInput:
    """Typed remote-resource create body."""

    title: str
    url: str = field(repr=False)
    filetype: str = "remote"
    type: str = "other"
    fields: Mapping[str, object] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _text(self.title, "title")
        _text(self.url, "url")
        if self.filetype != "remote":
            raise ValueError("Remote resource creation requires filetype='remote'.")
        _text(self.type, "type")
        if self.fields is not None:
            if not isinstance(self.fields, Mapping) or {"title", "url", "filetype", "type"} & set(self.fields):
                raise ValueError("Resource fields cannot replace typed resource values.")
            object.__setattr__(self, "fields", _freeze(dict(self.fields)))

    def payload(self) -> dict[str, object]:
        return {
            "title": self.title,
            "url": self.url,
            "filetype": self.filetype,
            "type": self.type,
            **{key: _thaw(value) for key, value in (self.fields or {}).items()},
        }


@dataclass(frozen=True, slots=True)
class ResourceUpdateInput:
    """Presence-aware resource update body."""

    fields: Mapping[str, object] = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.fields, Mapping) or not self.fields:
            raise ValueError("Resource updates require at least one JSON field.")
        object.__setattr__(self, "fields", _freeze(dict(self.fields)))

    def payload(self) -> dict[str, object]:
        return {key: _thaw(value) for key, value in self.fields.items()}


@dataclass(frozen=True, slots=True)
class ResourceUploadInput:
    """One bounded, single-use upload source."""

    source: BinaryIO = field(repr=False)
    file_name: str = field(repr=False)
    max_upload_bytes: int
    content_type: str | None = None
    _stream: _BoundedSource = field(init=False, repr=False, compare=False)
    _part_used: bool = field(init=False, default=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not callable(getattr(self.source, "read", None)) or not callable(getattr(self.source, "close", None)):
            raise ValueError("uData upload sources must be readable and closeable binary streams.")
        _text(self.file_name, "file name")
        if type(self.max_upload_bytes) is not int or self.max_upload_bytes < 1:
            raise ValueError("uData upload byte limits must be positive integers.")
        if self.content_type is not None:
            _text(self.content_type, "content type")
        object.__setattr__(self, "_stream", _BoundedSource(self.source, self.max_upload_bytes))

    def part(self) -> UploadPart:
        if self._part_used or self._stream._closed:
            raise ValueError("uData upload sources cannot be reused after use or closure.")
        object.__setattr__(self, "_part_used", True)
        return UploadPart("file", self._stream, self.file_name, self.content_type)

    def close(self) -> None:
        self._stream.close()


@dataclass(frozen=True, slots=True)
class ResourceMutationResult:
    """Resource mutation output retaining only a record and redacted receipt."""

    receipt: MutationReceipt
    record: NativeRecord | None = field(default=None, repr=False)
    records: tuple[NativeRecord, ...] = field(default=(), repr=False)
    extras: Mapping[str, object] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.receipt, MutationReceipt)
            or (self.record is not None and not isinstance(self.record, NativeRecord))
            or not isinstance(self.records, tuple)
            or not all(isinstance(item, NativeRecord) for item in self.records)
        ):
            raise ValueError("uData resource mutation results require a receipt and native records.")
        if self.extras is not None:
            if not isinstance(self.extras, Mapping):
                raise ValueError("uData resource mutation extras must be a mapping.")
            object.__setattr__(self, "extras", _freeze(self.extras))

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
            "records": [item.to_dict() for item in self.records],
            "extras": _thaw(self.extras) if self.extras is not None else None,
        }
