"""Abstract base for authentication strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseAuth(ABC):
    """Protocol for pluggable authentication.

    Each strategy knows how to decorate a request's headers with the
    appropriate credentials.
    """

    @abstractmethod
    def apply(
        self, headers: dict[str, str], params: dict[str, Any] | None = None
    ) -> tuple[dict[str, str], dict[str, Any]]:
        """Return a copy of *headers* (and optionally *params*) with credentials applied.

        Args:
            headers: Existing request headers.
            params: Existing query parameters (optional).

        Returns:
            A 2-tuple of ``(headers, params)`` with authentication added.
        """
