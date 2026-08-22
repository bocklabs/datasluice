"""Resource downloader with caching and checksum verification."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from datasluice._uri import sanitize_uri
from datasluice.exceptions import DownloadError
from datasluice.io.cache import FileCache
from datasluice.io.local import ensure_dir, safe_filename, save_bytes
from datasluice.io.storage import Storage
from datasluice.logging import get_logger
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest, TransportFailure
from datasluice.runtime.transport.user_agent import build_user_agent

if TYPE_CHECKING:
    from datasluice.domain.resource import Resource

logger = get_logger("io.downloader")


class Downloader:
    """Downloads resources to local or pluggable storage.

    Args:
        transport: HTTP client for making requests.
        storage: Storage backend (defaults to :class:`LocalStorage`).
        cache: Optional cache to avoid re-downloading unchanged files.
    """

    def __init__(
        self,
        transport: CatalogTransport,
        *,
        storage: Storage | None = None,
        cache: FileCache | None = None,
    ) -> None:
        self.transport = transport
        self.storage = storage
        self.cache = cache

    def download(
        self,
        resource: Resource,
        dest: str | Path | None = None,
        *,
        filename: str | None = None,
        verify_hash: str | None = None,
        hash_algorithm: str = "sha256",
    ) -> Path:
        """Download a single *resource* and return the local file path.

        Args:
            resource: The resource to download.
            dest: Destination directory or file path.
            filename: Override filename (required when *dest* is a directory).
            verify_hash: Expected hex digest for verification.
            hash_algorithm: Hash algorithm for verification.

        Raises:
            DownloadError: If the resource has no URL or the download fails.
            ChecksumMismatchError: If verification fails.
        """
        if not resource.url:
            raise DownloadError(f"Resource {resource.id!r} has no URL")

        logger.info("Downloading %s -> %s", sanitize_uri(resource.url), dest or "(memory)")

        cache_key = resource.url
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                data = cached
            else:
                data = self._fetch(resource.url)
                self.cache.put(cache_key, data)
        else:
            data = self._fetch(resource.url)

        if verify_hash:
            import hashlib

            actual = hashlib.new(hash_algorithm, data).hexdigest()
            if actual.lower() != verify_hash.lower():
                from datasluice.exceptions import ChecksumMismatchError

                raise ChecksumMismatchError(
                    f"Checksum mismatch for {sanitize_uri(resource.url)}",
                    expected=verify_hash,
                    actual=actual,
                )

        fname = filename or safe_filename(resource.name or resource.id) or "resource"
        if self.storage and dest is None:
            uri = self.storage.write(data, fname)
            return Path(uri)

        return save_bytes(data, dest or ".", fname)

    def _fetch(self, url: str) -> bytes:
        """Fetch bytes through the runtime transport and validate the HTTP outcome."""
        try:
            response = self.transport.send(
                RuntimeRequest(method="GET", url=url, headers={"User-Agent": build_user_agent()})
            )
        except TransportFailure as exc:
            raise DownloadError(f"Download for {sanitize_uri(url)!r} failed: {exc}") from exc
        if not 200 <= response.status_code < 300:
            raise DownloadError(f"Download for {sanitize_uri(url)!r} returned HTTP {response.status_code}")
        return response.body

    def download_many(
        self,
        resources: list[Resource],
        dest: str | Path,
    ) -> list[Path]:
        """Download multiple *resources* into *dest*."""
        ensure_dir(dest)
        results: list[Path] = []
        for resource in resources:
            try:
                path = self.download(resource, dest)
                results.append(path)
            except DownloadError as exc:
                logger.error("Failed to download %s: %s", resource.id, exc)
        return results
