"""Machine-readable CKAN action registry loaded from the checked-in manifest."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import resources
from types import MappingProxyType

from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.errors.catalog import CatalogValidationError
from datasluice.exceptions import DataSluiceError

MANIFEST_RESOURCE = "action_manifest.json"
_MANIFEST_PACKAGE = "datasluice.connectors.catalog.ckan"
_MANIFEST_KEYS = frozenset({"schema_version", "platform", "profile_version", "actions"})
_ENTRY_KEYS = frozenset({"name", "group", "owning_operation_id", "mutation_class", "result_kind"})
_MUTATION_CLASSES = frozenset({"read", "standard", "destructive"})
_RESULT_KINDS = frozenset({"record", "record-list", "value", "value-list", "mapping", "token-secret"})


class ActionManifestError(DataSluiceError):
    """The checked-in CKAN action manifest is missing or malformed."""


@dataclass(frozen=True, slots=True)
class ActionEntry:
    """One manifest-registered CKAN Action API endpoint."""

    name: str
    group: str
    owning_operation_id: str
    mutation_class: str
    result_kind: str


class ActionInventory:
    """A frozen lookup table of registered actions used by every wire dispatch."""

    __slots__ = ("_entries", "_by_name")

    def __init__(self, entries: tuple[ActionEntry, ...]) -> None:
        by_name: dict[str, ActionEntry] = {}
        for entry in entries:
            if not isinstance(entry, ActionEntry):
                raise ActionManifestError(f"{MANIFEST_RESOURCE} entries must be ActionEntry records.")
            if entry.name in by_name:
                raise ActionManifestError(f"Duplicate action name {entry.name!r} in {MANIFEST_RESOURCE}.")
            by_name[entry.name] = entry
        self._entries = tuple(entries)
        self._by_name: Mapping[str, ActionEntry] = MappingProxyType(dict(by_name))

    @classmethod
    def from_manifest(cls) -> ActionInventory:
        """Load and validate the checked-in manifest shipped with the package."""
        return cls(_load_entries())

    @property
    def entries(self) -> tuple[ActionEntry, ...]:
        """Return the frozen manifest-loaded action records."""
        return self._entries

    def lookup(self, action: str) -> ActionEntry:
        """Return the registered entry for one action name."""
        entry = self._by_name.get(action)
        if entry is None:
            raise CatalogValidationError(
                f"The action {action!r} is not registered in the checked-in CKAN action manifest.",
                operation=str(action),
                platform=CatalogPlatform.CKAN,
                safe_action="Dispatch only manifest-registered actions through their typed client methods.",
            )
        return entry


def _load_entries() -> tuple[ActionEntry, ...]:
    try:
        raw = resources.files(_MANIFEST_PACKAGE).joinpath(MANIFEST_RESOURCE).read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError) as exc:
        raise ActionManifestError(f"The CKAN action manifest {MANIFEST_RESOURCE} is missing.") from exc
    try:
        document = json.loads(raw)
    except ValueError as exc:
        raise ActionManifestError(f"The CKAN action manifest {MANIFEST_RESOURCE} is not valid JSON.") from exc
    if not isinstance(document, dict) or set(document) != _MANIFEST_KEYS:
        raise ActionManifestError(f"The CKAN action manifest {MANIFEST_RESOURCE} carries unexpected top-level keys.")
    if document["platform"] != "ckan" or not isinstance(document["profile_version"], str):
        raise ActionManifestError(f"The CKAN action manifest {MANIFEST_RESOURCE} names the wrong platform identity.")
    actions = document["actions"]
    if not isinstance(actions, list) or not actions:
        raise ActionManifestError(f"The CKAN action manifest {MANIFEST_RESOURCE} must list at least one action.")
    entries: list[ActionEntry] = []
    for item in actions:
        entries.append(_entry_from(item))
    return tuple(entries)


def _entry_from(item: object) -> ActionEntry:
    if not isinstance(item, dict) or set(item) != _ENTRY_KEYS:
        raise ActionManifestError(f"The CKAN action manifest {MANIFEST_RESOURCE} carries a malformed action entry.")
    for key in _ENTRY_KEYS:
        if not isinstance(item[key], str) or not item[key]:
            raise ActionManifestError(
                f"The CKAN action manifest {MANIFEST_RESOURCE} entry field {key!r} must be a non-empty string."
            )
    if item["mutation_class"] not in _MUTATION_CLASSES:
        raise ActionManifestError(
            f"The CKAN action manifest {MANIFEST_RESOURCE} entry declares an unknown mutation class."
        )
    if item["result_kind"] not in _RESULT_KINDS:
        raise ActionManifestError(
            f"The CKAN action manifest {MANIFEST_RESOURCE} entry declares an unknown result kind."
        )
    return ActionEntry(
        name=item["name"],
        group=item["group"],
        owning_operation_id=item["owning_operation_id"],
        mutation_class=item["mutation_class"],
        result_kind=item["result_kind"],
    )


CKAN_ACTIONS = ActionInventory.from_manifest()


def lookup(action: str) -> ActionEntry:
    """Return the default manifest-backed entry for one action name."""
    return CKAN_ACTIONS.lookup(action)
