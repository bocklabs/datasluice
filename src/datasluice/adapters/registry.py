"""Registry for portal adapters.

Maintains a mapping from canonical portal type names to adapter classes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.exceptions import AdapterNotFoundError

if TYPE_CHECKING:
    from datasluice.adapters.base import BaseAdapter


class AdapterRegistry:
    """Mutable registry of portal adapter classes.

    Use :data:`registry` (the module-level singleton) to register and look up
    adapters.
    """

    def __init__(self) -> None:
        self._adapters: dict[str, type[BaseAdapter]] = {}

    def register(self, portal_type: str, adapter_cls: type[BaseAdapter]) -> None:
        """Register *adapter_cls* under the canonical *portal_type* name."""
        self._adapters[portal_type.lower()] = adapter_cls

    def unregister(self, portal_type: str) -> None:
        """Remove the adapter registered under *portal_type*."""
        self._adapters.pop(portal_type.lower(), None)

    def get(self, portal_type: str) -> type[BaseAdapter]:
        """Return the adapter class for *portal_type*.

        Raises:
            AdapterNotFoundError: If no adapter is registered.
        """
        try:
            return self._adapters[portal_type.lower()]
        except KeyError:
            raise AdapterNotFoundError(
                f"No adapter registered for portal type {portal_type!r}. "
                f"Known types: {', '.join(sorted(self._adapters)) or '(none)'}"
            ) from None

    def has(self, portal_type: str) -> bool:
        """Return ``True`` if an adapter is registered for *portal_type*."""
        return portal_type.lower() in self._adapters

    @property
    def known_types(self) -> list[str]:
        """Return a sorted list of all registered portal type names."""
        return sorted(self._adapters)

    def __contains__(self, portal_type: str) -> bool:
        return self.has(portal_type)


# Module-level adapter registry shared across the library.
registry = AdapterRegistry()
