"""High-level DataSluice client.

This is the primary entry point. It wires together adapter resolution,
transport, IO, and format reading behind a single, consistent interface.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from datasluice.adapters.factory import create_adapter
from datasluice.auth import BaseAuth
from datasluice.config.settings import Settings, load_settings
from datasluice.domain import Dataset, Organization, Query, Resource, SearchResult
from datasluice.io.downloader import Downloader
from datasluice.logging import configure_logging, get_logger
from datasluice.transport import HttpClient, RateLimiter, RetryPolicy

if TYPE_CHECKING:
    pass

logger = get_logger("client")


class DataSluice:
    """Unified client for open-data portals.

    Args:
        portal_url: Base URL of the open-data portal.
        portal_type: Optional explicit portal type (e.g. ``"ckan"``).
            Auto-detected when omitted.
        auth: Optional authentication strategy.
        settings: Optional pre-loaded settings. Loaded from the environment
            when omitted.
        transport: Optional pre-configured HTTP client.

    Example:
        >>> from datasluice import DataSluice
        >>> ds = DataSluice("https://catalog.data.gov")
        >>> results = ds.search("climate change")
        >>> for dataset in results:
        ...     print(dataset.title)
    """

    def __init__(
        self,
        portal_url: str,
        *,
        portal_type: str | None = None,
        auth: BaseAuth | None = None,
        settings: Settings | None = None,
        transport: HttpClient | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        configure_logging(self.settings.log_level)

        self.auth = auth
        self._transport = transport or self._build_transport()
        self.adapter = create_adapter(portal_url, portal_type=portal_type, auth=auth)
        self.adapter._transport = self._transport

        self._downloader: Downloader | None = None
        logger.debug("Initialised DataSluice for %s (%s)", portal_url, self.adapter.portal_type)

    def _build_transport(self) -> HttpClient:
        """Construct the HTTP client from settings."""
        rate_limiter = RateLimiter(requests_per_second=self.settings.rate_limit) if self.settings.rate_limit else None
        retry_policy = RetryPolicy(max_attempts=self.settings.http_retries)
        return HttpClient(
            auth=self.auth,
            timeout=self.settings.http_timeout,
            retry_policy=retry_policy,
            rate_limiter=rate_limiter,
            user_agent=self.settings.user_agent,
        )

    @property
    def downloader(self) -> Downloader:
        """Lazily-initialised downloader."""
        if self._downloader is None:
            self._downloader = Downloader(self._transport)
        return self._downloader

    def search(self, query: str | Query | None = None, **kwargs: Any) -> SearchResult:
        """Search for datasets.

        Args:
            query: Search text or a :class:`Query` object.
            **kwargs: Additional :class:`Query` fields (limit, tags, etc.).

        Returns:
            A :class:`SearchResult` page.
        """
        if isinstance(query, Query):
            q = query
        else:
            q = Query(text=query, **kwargs)
        return self.adapter.search(q)

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Fetch a single dataset by ID."""
        return self.adapter.get_dataset(dataset_id)

    def list_resources(self, dataset_id: str) -> list[Resource]:
        """List resources for a dataset."""
        return self.adapter.list_resources(dataset_id)

    def get_organization(self, organization_id: str) -> Organization:
        """Fetch organization/publisher metadata."""
        return self.adapter.get_organization(organization_id)

    def read(self, resource: Resource) -> list[dict[str, Any]]:
        """Download and parse a resource into a list of record dicts."""
        from datasluice.formats import get_reader

        if not resource.url:
            raise ValueError(f"Resource {resource.id!r} has no URL")
        data = self._transport.download(resource.url)
        fmt = (resource.format or "CSV").upper()
        reader = get_reader(fmt)
        return reader.read(data)

    def download(self, resource: Resource, dest: str | Path | None = None, **kwargs: Any) -> Path:
        """Download a single resource."""
        return self.downloader.download(resource, dest, **kwargs)

    def download_all(self, dataset: Dataset, dest: str | Path) -> list[Path]:
        """Download all resources in a dataset."""
        return self.downloader.download_many(dataset.resources, dest)

    def __repr__(self) -> str:
        return f"<DataSluice({self.adapter.portal_type!r}, {self.adapter.base_url!r})>"
