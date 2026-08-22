"""Plugin manager for entry-point-based connector discovery.

The :class:`PluginManager` replaces the former module-level registry
singleton. Discovery is eager (performed in ``__init__``) and per-entry
isolated: a broken third-party plugin is recorded as a :class:`PluginFailure`
and never crashes session creation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any

from datasluice.exceptions import ConnectorNotFoundError
from datasluice.logging import get_logger

logger = get_logger("runtime.plugin_manager")

_BUILTIN_CONNECTOR_IDS = frozenset({"datasluice/ckan", "datasluice/udata", "datasluice/socrata"})


@dataclass(frozen=True)
class PluginFailure:
    """Record of a failed plugin discovery or load.

    Attributes:
        name: Entry-point name of the failed plugin.
        error: Human-readable description of the failure.
    """

    name: str
    error: str


class PluginManager:
    """Registry-free connector manager backed by ``importlib.metadata``.

    Built-in connectors are declared in the ``datasluice.connectors``
    entry-points group in ``pyproject.toml``; third-party connectors register
    their own entry points in the same group. The manager is an injected
    instance — never a module-level singleton.
    """

    def __init__(self, group: str = "datasluice.connectors") -> None:
        self._factories: dict[str, Callable[..., Any]] = {}
        self._failures: list[PluginFailure] = []
        try:
            discovered = entry_points(group=group)
        except Exception as exc:
            logger.warning("Entry-point discovery failed for group %r; no connectors loaded: %s", group, exc)
            return
        for ep in discovered:
            try:
                factory = ep.load()
            except Exception as exc:
                message = str(exc)
                if len(message) > 500:
                    message = message[:500]
                self._failures.append(PluginFailure(ep.name, message))
                logger.warning("Failed to load connector entry point %r: %s", ep.name, message)
                continue
            if ep.name in self._factories:
                self._failures.append(PluginFailure(ep.name, "duplicate entry point"))
                logger.warning("Duplicate connector entry point %r ignored", ep.name)
                continue
            self._factories[ep.name] = factory

    def register(self, name: str, factory: Callable[..., Any]) -> None:
        """Register *factory* programmatically."""
        if name in self._factories and self._factories[name] is not factory:
            logger.warning("Overwriting existing connector factory %r", name)
        self._factories[name] = factory

    def get(self, name: str) -> object:
        """Return the factory callable for *name*.

        Raises:
            ConnectorNotFoundError: If no connector is registered for *name*.
        """
        try:
            return self._factories[name]
        except KeyError:
            available = ", ".join(sorted(self._factories)) or "(none)"
            raise ConnectorNotFoundError(f"No connector registered for {name!r}. Available: {available}") from None

    def list_connectors(self) -> list[str]:
        """Return a sorted list of all registered connector names."""
        return sorted(self._factories)

    def list_failures(self) -> list[PluginFailure]:
        """Return a copy of the recorded plugin load failures."""
        return list(self._failures)
