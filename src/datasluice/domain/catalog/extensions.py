"""Validated connector extension and certification metadata."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re


_IDENTIFIER = re.compile(r"^[a-z][a-z0-9-]*$")
_ENTRY_POINT = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")
_RUNTIME_INSTALL_INSTRUCTIONS = ("pip install", "uv add", "uv sync", "poetry add", "conda install")
_BUILTIN_PLATFORMS = frozenset({"ckan", "socrata", "udata"})


class ActivationPolicy(StrEnum):
    """How a third-party connector may become active."""

    INACTIVE = "inactive"
    EXPLICIT = "explicit"


class InstallResponsibility(StrEnum):
    """Distribution layer responsible for an optional dependency."""

    BASE = "base"
    CONNECTOR = "connector"
    OPTIONAL = "optional"


@dataclass(frozen=True, slots=True)
class ConnectorId:
    """Namespaced stable identifier for one connector distribution."""

    vendor: str
    platform: str

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.vendor) or not _IDENTIFIER.fullmatch(self.platform):
            raise ValueError("Connector IDs must use lowercase vendor/platform identifiers.")
        if self.vendor == "datasluice" and self.platform not in _BUILTIN_PLATFORMS:
            raise ValueError("The datasluice vendor namespace is reserved for built-in connector IDs.")

    @classmethod
    def parse(cls, value: str) -> ConnectorId:
        """Parse a ``vendor/platform`` connector identifier."""
        parts = value.split("/")
        if len(parts) != 2 or not all(parts):
            raise ValueError("Connector IDs must use vendor/platform form.")
        return cls(vendor=parts[0], platform=parts[1])

    @property
    def is_builtin(self) -> bool:
        """Return whether this identifier names a maintained built-in connector."""
        return self.vendor == "datasluice"

    def __str__(self) -> str:
        """Return the canonical namespaced connector ID."""
        return f"{self.vendor}/{self.platform}"


@dataclass(frozen=True, slots=True)
class CertificationRecord:
    """Versioned identity of a connector's public contract report."""

    connector_id: ConnectorId
    contract_schema_version: str
    profile_version: str
    report_version: str
    report_id: str

    def __post_init__(self) -> None:
        for name, value in (
            ("contract schema version", self.contract_schema_version),
            ("profile version", self.profile_version),
            ("report version", self.report_version),
            ("report identity", self.report_id),
        ):
            if not value.strip():
                raise ValueError(f"Certification {name} is required.")


@dataclass(frozen=True, slots=True)
class OptionalInstallRequirement:
    """Descriptive guidance for a lazy optional dependency extra."""

    extra: str
    install_hint: str
    responsibility: InstallResponsibility = InstallResponsibility.OPTIONAL

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.extra):
            raise ValueError("Optional dependency extras must be lowercase identifiers.")
        if not self.install_hint.strip():
            raise ValueError("Optional dependency install hints are required.")
        if any(instruction in self.install_hint.lower() for instruction in _RUNTIME_INSTALL_INSTRUCTIONS):
            raise ValueError("Optional dependency metadata cannot request runtime installation.")
        if "datasluice[" not in self.install_hint.lower():
            raise ValueError("Optional dependency install hints must name a DataSluice extra.")


@dataclass(frozen=True, slots=True)
class ConnectorManifest:
    """Inspectable third-party connector metadata without runtime activation."""

    connector_id: ConnectorId
    entry_point: str
    profile_version: str
    optional_requirements: tuple[OptionalInstallRequirement, ...]
    certification: CertificationRecord | None
    activation_policy: ActivationPolicy = ActivationPolicy.INACTIVE
    overrides: ConnectorId | None = None

    def __post_init__(self) -> None:
        if not _ENTRY_POINT.fullmatch(self.entry_point):
            raise ValueError("Connector entry point must be a module:factory reference.")
        if not self.profile_version.strip():
            raise ValueError("Connector profile version is required.")
        object.__setattr__(self, "optional_requirements", tuple(self.optional_requirements))
        if self.connector_id.is_builtin:
            if self.overrides is not None:
                raise ValueError("Built-in connectors cannot declare overrides.")
            return
        if not self.optional_requirements:
            raise ValueError("Third-party manifests require optional dependency extra hints.")
        if self.certification is None:
            raise ValueError("Third-party manifests require certification metadata.")
        if self.certification.connector_id != self.connector_id:
            raise ValueError("Certification connector identity must match the manifest.")
        if self.certification.profile_version != self.profile_version:
            raise ValueError("Certification profile version must match the manifest.")
        if self.overrides is not None and not self.overrides.is_builtin:
            raise ValueError("Connector overrides may only name a built-in connector.")

    def is_activated(self, selected_connector_id: ConnectorId | None) -> bool:
        """Return whether an explicit caller selection activates this manifest."""
        return selected_connector_id == self.connector_id

    def require_activation(self, selected_connector_id: ConnectorId | None) -> None:
        """Reject implicit activation and silent built-in overrides."""
        if not self.is_activated(selected_connector_id):
            raise ValueError("Connector activation requires explicit caller selection of the connector ID.")
