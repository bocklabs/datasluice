"""Canonical resource identity proofs (CR-01 blocker fix, SYNC-05/07).

The portal-controlled ``resource.id`` is no longer interpolated verbatim into
fsspec paths or used as the entire StateStore key. Every artifact path and
state key now derives from a SHA-256 canonical identity scoped by URL origin
plus ``resource.id`` so traversal-shaped ids cannot escape the destination
directory and equal ids across portals or datasets cannot alias artifacts or
state.
"""

from __future__ import annotations

import importlib
import os
from typing import Any

import pytest

from datasluice.domain import HttpDownload, Resource
from datasluice.exceptions import DataSluiceError
from datasluice.io.filesystem import open_filesystem
from datasluice.sync.materialize import materialize

try:
    identity_module: Any = importlib.import_module("datasluice.sync._identity")
except ImportError:
    identity_module = None
if identity_module is None or not hasattr(identity_module, "_CANONICAL_IDENTITY_READY"):
    if os.environ.get("DATASLUICE_TDD_RED") != "1":
        pytest.skip("canonical identity implementation pending GREEN phase", allow_module_level=True)

canonical_identity: Any = getattr(identity_module, "canonical_identity", lambda *_: "")
validate_unique_identities: Any = getattr(
    identity_module,
    "validate_unique_identities",
    lambda *_: None,
)


def _http_resource(resource_id: str, url: str) -> Resource:
    return Resource(id=resource_id, name=resource_id, url=url, format="CSV", access=HttpDownload(url=url))


class _RawReader:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read_bytes(self, resource: Any) -> bytes:
        return self.data


def test_traversal_resource_id_cannot_escape_destination(tmp_path) -> None:
    resource = Resource(id="/../../escaped", name="escaped", media_type="application/octet-stream")

    identity = canonical_identity(resource)

    assert isinstance(identity, str)
    assert len(identity) == 64
    assert all(character in "0123456789abcdef" for character in identity)
    assert "/" not in identity
    assert "\\" not in identity
    assert ".." not in identity

    destination = f"file://{tmp_path}/dest"
    record = materialize(
        resource,
        reader=_RawReader(b"payload-bytes"),
        destination_uri=destination,
        mode="raw",
    )
    final_uri = record[0]
    expected = f"{destination}/{identity}.bin"
    assert final_uri == expected

    fs = open_filesystem(destination)
    assert fs.exists(final_uri)
    listing = fs.find(destination)
    for path in listing:
        assert ".." not in path
        assert not path.rstrip("/").endswith("/escaped.bin")
    assert tmp_path.parent is not None


def test_cross_portal_collision_produces_distinct_identity() -> None:
    portal_a = _http_resource("same-id", "https://portal-a.test/data.csv")
    portal_b = _http_resource("same-id", "https://portal-b.test/data.csv")

    assert canonical_identity(portal_a) != canonical_identity(portal_b)


def test_cross_dataset_collision_within_portal() -> None:
    dataset_a = _http_resource("same-id", "https://portal.test/datasets/a/data.csv")
    dataset_b = _http_resource("same-id", "https://portal.test/datasets/b/data.csv")

    assert canonical_identity(dataset_a) == canonical_identity(dataset_b)

    with pytest.raises(DataSluiceError, match="Duplicate resource identity"):
        validate_unique_identities([dataset_a, dataset_b])


def test_duplicate_identity_rejected_before_write() -> None:
    first = _http_resource("dup", "https://portal.test/first.csv")
    second = _http_resource("dup", "https://portal.test/second.csv")

    with pytest.raises(DataSluiceError) as exc_info:
        validate_unique_identities([first, second])

    message = str(exc_info.value)
    assert "dup" in message
    assert "Duplicate resource identity" in message


def test_canonical_identity_is_deterministic() -> None:
    resource = _http_resource("stable-id", "https://portal.test/data.csv")

    first = canonical_identity(resource)
    second = canonical_identity(resource)
    third = canonical_identity(resource)

    assert first == second == third
    assert len(first) == 64


def test_local_resource_without_url_still_gets_stable_identity() -> None:
    from datasluice.domain import LocalFile

    resource = Resource(
        id="local-only",
        name="local-only",
        format="CSV",
        access=LocalFile(path="/tmp/data.csv"),
    )

    identity = canonical_identity(resource)
    assert len(identity) == 64

    other = Resource(
        id="local-only",
        name="local-only",
        format="CSV",
        access=LocalFile(path="/tmp/data.csv"),
    )
    assert canonical_identity(other) == identity
