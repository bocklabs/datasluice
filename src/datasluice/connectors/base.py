"""Abstract base class for all portal adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar

from datasluice.domain import Dataset, Query, Resource, SearchResult

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth
    from datasluice.ports import Transport


class BaseAdapter(ABC):
    """Protocol that every portal adapter must implement.

    Subclasses translate portal-native API responses into DataSluice's
    portal-agnostic :mod:`datasluice.domain` models.

    ``get_organization`` is intentionally NOT declared here ( feed,
    ): it lives only on the :class:`OrganizationCatalog` Protocol.
    Declaring it on ``BaseAdapter`` (as ``@abstractmethod`` or a default)
    would let PEP 544 ``runtime_checkable`` ``isinstance`` short-circuit on
    the base class, so every adapter would incorrectly satisfy
    ``OrganizationCatalog`` regardless of whether it actually implements the
    method (python/typing#800).

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
        transport: Transport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self._transport: Transport | None = transport

    @property
    def transport(self) -> Transport:
        """Lazily initialised HTTP transport."""
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

    def __repr__(self) -> str:
        return f"<{type(self).__name__}({self.portal_type!r}, {self.base_url!r})>"
