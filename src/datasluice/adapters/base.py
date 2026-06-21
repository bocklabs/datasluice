"""Abstract base class for all portal adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar

from datasluice.domain import Dataset, Organization, Query, Resource, SearchResult

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth
    from datasluice.transport import HttpClient


class BaseAdapter(ABC):
    """Protocol that every portal adapter must implement.

    Subclasses translate portal-native API responses into DataSluice's
    portal-agnostic :mod:`datasluice.domain` models.

    Attributes:
        portal_type: Canonical name for the portal platform (e.g. ``"ckan"``).
        base_url: Root URL of the portal instance.
    """

    portal_type: ClassVar[str] = "base"

    def __init__(
        self,
        base_url: str,
        *,
        auth: BaseAuth | None = None,
        transport: HttpClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self._transport = transport

    @property
    def transport(self) -> HttpClient:
        """Lazily initialised HTTP client."""
        if self._transport is None:
            from datasluice.transport import HttpClient

            self._transport = HttpClient(auth=self.auth)
        return self._transport

    @abstractmethod
    def search(self, query: Query | None = None) -> SearchResult:
        """Search for datasets matching *query*."""

    @abstractmethod
    def get_dataset(self, dataset_id: str) -> Dataset:
        """Fetch a single dataset by its portal-native *dataset_id*."""

    @abstractmethod
    def list_resources(self, dataset_id: str) -> list[Resource]:
        """Return all downloadable resources for *dataset_id*."""

    @abstractmethod
    def get_organization(self, organization_id: str) -> Organization:
        """Fetch publisher metadata for *organization_id*."""

    def __repr__(self) -> str:
        return f"<{type(self).__name__}({self.portal_type!r}, {self.base_url!r})>"
