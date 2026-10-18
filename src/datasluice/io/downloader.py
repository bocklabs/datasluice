"""Resource downloader with caching and checksum verification."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from datasluice._uri import sanitize_uri
from datasluice.exceptions import ChecksumMismatchError, DownloadError
from datasluice.io._redirects import follow_redirects
from datasluice.io.local import ensure_dir, safe_filename, save_bytes
from datasluice.logging import get_logger
from datasluice.runtime.transport.base import CatalogTransport, RedirectPolicy, RuntimeRequest, TransportError
from datasluice.runtime.transport.user_agent import build_user_agent

if TYPE_CHECKING:
    from datasluice.domain.resource import Resource
    from datasluice.ports.cache import CachePort
    from datasluice.ports.storage import StoragePort
    from datasluice.runtime.transport.base import RuntimeResponse

logger = get_logger("io.downloader")


class Downloader:
    """Downloads resources to local or pluggable storage.

    Args:
        transport: HTTP client for making requests.
        storage: Optional :class:`~datasluice.ports.storage.StoragePort`. If
            absent, downloads are written to the local filesystem.
        cache: Optional :class:`~datasluice.ports.cache.CachePort` to avoid re-downloading unchanged files.
    """

    def __init__(
        self,
        transport: CatalogTransport,
        *,
        storage: StoragePort | None = None,
        cache: CachePort | None = None,
        allow_private_http: bool = False,
    ) -> None:
        self.transport = transport
        self.storage = storage
        self.cache = cache
        self.allow_private_http = allow_private_http

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
        cached = self.cache.get(cache_key) if self.cache else None
        data = cached if cached is not None else self._fetch(resource.url)

        if verify_hash:
            self._verify_checksum(resource.url, data, verify_hash, hash_algorithm, cache_key, cached)

        if self.cache and cached is None:
            self.cache.put(cache_key, data)

        fname = filename or safe_filename(resource.name or resource.id) or "resource"
        if self.storage and dest is None:
            uri = self.storage.write(data, fname)
            return Path(uri)

        return save_bytes(data, dest or ".", fname)

    def _verify_checksum(
        self,
        url: str,
        data: bytes,
        verify_hash: str,
        hash_algorithm: str,
        cache_key: str,
        cached: bytes | None,
    ) -> None:
        """Verify *data* against the expected digest, evicting a cached copy that no longer matches."""
        import hashlib

        actual = hashlib.new(hash_algorithm, data).hexdigest()
        if actual.lower() != verify_hash.lower():
            if self.cache is not None and cached is not None:
                self.cache.delete(cache_key)
            raise ChecksumMismatchError(
                f"Checksum mismatch for {sanitize_uri(url)}",
                expected=verify_hash,
                actual=actual,
            )

    def _fetch(self, url: str) -> bytes:
        """Fetch bytes through the runtime transport and validate the HTTP outcome."""

        def send(hop_url: str) -> RuntimeResponse:
            try:
                return self.transport.send(
                    RuntimeRequest(
                        method="GET",
                        url=hop_url,
                        headers={"User-Agent": build_user_agent()},
                        redirect_policy=RedirectPolicy.NO_FOLLOW,
                    )
                )
            except TransportError as exc:
                raise DownloadError(f"Download for {sanitize_uri(hop_url)!r} failed: {exc}") from exc

        response = follow_redirects(
            url, allow_private=self.allow_private_http, label="Download", error=DownloadError, send=send
        )
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
            except (DownloadError, ChecksumMismatchError, OSError):
                logger.exception("Failed to download %s", resource.id)
        return results
