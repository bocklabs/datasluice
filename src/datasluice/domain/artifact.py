"""Strict schema-v1 Artifact wire contracts."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from datasluice._uri import sanitize_uri
from datasluice.exceptions import DataSluiceError

if TYPE_CHECKING:
    from datasluice.application import ResourceLocator

_SHA256_RE = re.compile(r"[0-9a-f]{64}$")
_EXTENSION_NAMESPACE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9.-]*[.][A-Za-z0-9][A-Za-z0-9.-]*$")
_DIGEST_KEYS = frozenset({"algorithm", "value"})
_PROVENANCE_KEYS = frozenset(
    {"source_locator", "resource_identity", "created_at", "materialization_mode", "transforms"}
)
_ARTIFACT_KEYS = frozenset(
    {
        "schema_version",
        "kind",
        "uri",
        "media_type",
        "size",
        "content_digest",
        "blob_digest",
        "provenance",
        "metadata",
        "extensions",
    }
)


def _contract_error(path: str) -> DataSluiceError:
    return DataSluiceError(f"Invalid schema-v1 contract at {path}")


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _object_dict(value: object, path: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _contract_error(path)
    result: dict[str, object] = {}
    for key, nested in value.items():
        if not isinstance(key, str):
            raise _contract_error(path)
        result[key] = nested
    return result


def _public_uri(value: object, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise _contract_error(path)
    try:
        parts = urlsplit(value)
    except ValueError as exc:
        raise _contract_error(path) from exc
    if parts.username is not None or parts.password is not None or sanitize_uri(value) != value:
        raise _contract_error(path)
    return value


def _freeze_json(value: object, path: str = "value") -> object:
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if math.isfinite(value):
            return value
        raise _contract_error(path)
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, nested in value.items():
            if not isinstance(key, str):
                raise _contract_error(path)
            frozen[key] = _freeze_json(nested, f"{path}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(nested, path) for nested in value)
    raise _contract_error(path)


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(nested) for nested in value]
    return value


def _freeze_extensions(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _contract_error("extensions")
    frozen: dict[str, object] = {}
    for namespace, extension in value.items():
        if not isinstance(namespace, str):
            raise _contract_error("extensions")
        if _EXTENSION_NAMESPACE_RE.fullmatch(namespace) is None:
            raise _contract_error("extensions")
        frozen[namespace] = _freeze_json(extension, f"extensions.{namespace}")
    return MappingProxyType(frozen)


@dataclass(frozen=True)
class Digest:
    """A structured SHA-256 digest."""

    algorithm: str
    value: str

    def __post_init__(self) -> None:
        if self.algorithm != "sha256" or not _is_sha256(self.value):
            raise _contract_error("digest")

    def to_dict(self) -> dict[str, str]:
        """Return a fresh JSON-safe digest envelope."""
        return {"algorithm": self.algorithm, "value": self.value}

    @classmethod
    def from_dict(cls, value: object) -> Digest:
        """Decode one strict digest envelope."""
        data = _object_dict(value, "digest")
        if set(data) != _DIGEST_KEYS:
            raise _contract_error("digest")
        algorithm = data["algorithm"]
        digest = data["value"]
        if not isinstance(algorithm, str) or not isinstance(digest, str):
            raise _contract_error("digest")
        return cls(algorithm=algorithm, value=digest)


@dataclass(frozen=True)
class ArtifactProvenance:
    """Typed provenance for one materialized Artifact."""

    source_locator: ResourceLocator
    resource_identity: str
    created_at: datetime
    materialization_mode: str
    transforms: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not hasattr(self.source_locator, "to_dict") or not _is_sha256(self.resource_identity):
            raise _contract_error("provenance")
        if not isinstance(self.created_at, datetime) or self.created_at.tzinfo is None:
            raise _contract_error("provenance.created_at")
        if self.materialization_mode not in {"parquet", "raw"}:
            raise _contract_error("provenance.materialization_mode")
        if not isinstance(self.transforms, tuple) or not all(isinstance(value, str) for value in self.transforms):
            raise _contract_error("provenance.transforms")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe provenance envelope."""
        created_at = self.created_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return {
            "source_locator": self.source_locator.to_dict(),
            "resource_identity": self.resource_identity,
            "created_at": created_at,
            "materialization_mode": self.materialization_mode,
            "transforms": list(self.transforms),
        }

    @classmethod
    def from_dict(cls, value: object) -> ArtifactProvenance:
        """Decode one strict provenance envelope."""
        data = _object_dict(value, "provenance")
        if set(data) != _PROVENANCE_KEYS:
            raise _contract_error("provenance")
        source_locator = data["source_locator"]
        resource_identity = data["resource_identity"]
        created_at = data["created_at"]
        materialization_mode = data["materialization_mode"]
        transforms = data["transforms"]
        if (
            not isinstance(resource_identity, str)
            or not isinstance(created_at, str)
            or not isinstance(materialization_mode, str)
            or not isinstance(transforms, list)
            or not all(isinstance(transform, str) for transform in transforms)
        ):
            raise _contract_error("provenance")
        try:
            parsed_created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise _contract_error("provenance.created_at") from exc
        if parsed_created_at.tzinfo is None:
            raise _contract_error("provenance.created_at")
        from datasluice.application import resource_locator_from_dict

        parsed_transforms = tuple(transform for transform in transforms if isinstance(transform, str))
        if len(parsed_transforms) != len(transforms):
            raise _contract_error("provenance.transforms")
        return cls(
            source_locator=resource_locator_from_dict(_object_dict(source_locator, "provenance.source_locator")),
            resource_identity=resource_identity,
            created_at=parsed_created_at,
            materialization_mode=materialization_mode,
            transforms=parsed_transforms,
        )


@dataclass(frozen=True)
class Artifact:
    """A strict, immutable schema-v1 materialization envelope."""

    uri: str
    media_type: str
    size: int
    content_digest: Digest
    blob_digest: Digest
    provenance: ArtifactProvenance
    metadata: Mapping[str, object] = field(default_factory=dict)
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _public_uri(self.uri, "uri")
        if not isinstance(self.media_type, str) or not self.media_type:
            raise _contract_error("media_type")
        if type(self.size) is not int or self.size < 0:
            raise _contract_error("size")
        if not isinstance(self.content_digest, Digest) or not isinstance(self.blob_digest, Digest):
            raise _contract_error("digest")
        if not isinstance(self.provenance, ArtifactProvenance):
            raise _contract_error("provenance")
        metadata = _freeze_json(self.metadata, "metadata")
        if not isinstance(metadata, Mapping):
            raise _contract_error("metadata")
        object.__setattr__(self, "uri", self.uri)
        object.__setattr__(self, "metadata", metadata)
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe Artifact envelope."""
        return {
            "schema_version": 1,
            "kind": "artifact",
            "uri": self.uri,
            "media_type": self.media_type,
            "size": self.size,
            "content_digest": self.content_digest.to_dict(),
            "blob_digest": self.blob_digest.to_dict(),
            "provenance": self.provenance.to_dict(),
            "metadata": _thaw_json(self.metadata),
            "extensions": _thaw_json(self.extensions),
        }

    def __getitem__(self, index: int) -> Any:
        return (self.uri, self.media_type, self.size, self.content_digest.value)[index]

    @classmethod
    def from_dict(cls, value: object) -> Artifact:
        """Decode one strict schema-v1 Artifact envelope."""
        data = _object_dict(value, "artifact")
        if set(data) != _ARTIFACT_KEYS:
            raise _contract_error("artifact")
        if data["schema_version"] != 1 or type(data["schema_version"]) is not int or data["kind"] != "artifact":
            raise _contract_error("artifact")
        uri = data["uri"]
        media_type = data["media_type"]
        size = data["size"]
        metadata = data["metadata"]
        extensions = data["extensions"]
        if not isinstance(uri, str) or not isinstance(media_type, str) or type(size) is not int:
            raise _contract_error("artifact")
        return cls(
            uri=uri,
            media_type=media_type,
            size=size,
            content_digest=Digest.from_dict(data["content_digest"]),
            blob_digest=Digest.from_dict(data["blob_digest"]),
            provenance=ArtifactProvenance.from_dict(data["provenance"]),
            metadata=_object_dict(metadata, "metadata"),
            extensions=_object_dict(extensions, "extensions"),
        )
