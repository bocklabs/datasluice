"""Canonical resource identity (CR-01 blocker fix, SYNC-05/07).

Portal-controlled ``resource.id`` was previously interpolated verbatim into
fsspec paths and used as the entire :class:`StateStore` key. A traversal-shaped
id could escape the destination directory and equal ids across portals or
datasets could alias artifacts and state.

This module derives one canonical namespaced identity per resource, hashed with
SHA-256 over ``f"{url_origin}/{resource.id}"``. The hash is a 64-character
lowercase hex string with no path separators, so it can never form a path
escape, and equal ids at different URL origins, access locators (local paths,
object URIs), or URL paths produce distinct identities.

The module is dependency-free (``hashlib`` plus ``urllib.parse``) so it imports
cleanly on bare installs (D-P7-29) and is stable across repeated calls on the
same resource.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from datasluice.exceptions import DataSluiceError

if TYPE_CHECKING:
    from datasluice.domain import Resource

_CANONICAL_IDENTITY_READY = True

_LOCAL_ORIGIN = "local"


def canonical_identity(resource: Resource) -> str:
    """Return the SHA-256 canonical identity for *resource*.

    The identity is ``sha256(f"{url_origin}/{resource.id}")`` where
    ``url_origin`` is the resource access URL's ``scheme://netloc``, the
    access locator (local path or object URI) when the access carries one, or
    a stable sentinel when neither is available, so local-only resources still
    receive a scoped identity. The result is a 64-character lowercase hex
    string with no path separators.

    Args:
        resource: A resource-like object carrying ``id`` plus an access URL or
            ``resource.url``.

    Returns:
        The 64-character lowercase hex identity hash.
    """
    origin = _url_origin(resource)
    raw = f"{origin}/{resource.id}"
    return hashlib.sha256(raw.encode()).hexdigest()


def canonical_destination_identity(destination_uri: str) -> str:
    """Return a secret-free SHA-256 identity for a destination URI."""
    parts = urlsplit(destination_uri)
    scheme = parts.scheme.lower()
    host = parts.hostname
    if host is None:
        authority = parts.netloc.rsplit("@", 1)[-1].lower()
    else:
        authority = host.lower()
        try:
            port = parts.port
        except ValueError:
            port = None
        if port is not None:
            authority = f"{authority}:{port}"
    path = parts.path.rstrip("/")
    if scheme:
        scope = f"{scheme}://{authority}{path}"
    else:
        scope = path or destination_uri.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    return hashlib.sha256(scope.encode()).hexdigest()


def validate_unique_identities(resources: Iterable[Resource]) -> None:
    """Reject duplicate canonical identities before any artifact or state write.

    Iterates *resources* and raises :class:`DataSluiceError` naming both
    colliding resource ids when the same canonical identity is observed twice.
    Duplicate detection runs at sync-loop entry so a portal returning the same
    resource.id in two datasets cannot silently alias artifacts or state.

    Args:
        resources: Iterable of resources about to be synchronized.

    Raises:
        DataSluiceError: when two resources resolve to the same canonical
            identity. The error names both colliding ``resource.id`` values and
            the structural field path; it never echoes the resource URL.
    """
    seen: dict[str, str] = {}
    for resource in resources:
        identity = canonical_identity(resource)
        if identity in seen:
            prior_id = seen[identity]
            raise DataSluiceError(
                "Duplicate resource identity at state.cursor: "
                f"resource.id={prior_id!r} and resource.id={resource.id!r} "
                f"resolve to the same canonical identity"
            )
        seen[identity] = resource.id


def _url_origin(resource: Resource) -> str:
    """Extract the origin scope for canonical identity hashing.

    HTTP(S) resources scope by ``scheme://netloc``. Resources whose access
    carries a locator — an object-storage URI or a local path — scope by that
    locator, so equal ids at different paths cannot alias artifacts or state.
    Scheme-bearing URLs without a netloc (``file://``) scope by their path
    component. Falls back to a stable sentinel when nothing else is available.
    """
    access = resource.access
    url = getattr(access, "url", None) or resource.url
    loc = getattr(access, "uri", None) or getattr(access, "path", None)
    if url:
        parts = urlsplit(url)
        if parts.scheme and parts.netloc:
            authority = parts.netloc.rsplit("@", 1)[-1].lower()
            return f"{parts.scheme.lower()}://{authority}"
        if loc:
            return f"local:{loc}"
        if parts.scheme and parts.path:
            return f"{parts.scheme}:{parts.path}"
        if parts.path:
            return f"path:{parts.path}"
    if loc:
        return f"local:{loc}"
    return _LOCAL_ORIGIN
