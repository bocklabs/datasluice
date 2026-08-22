"""Unit tests for port Protocol definitions — runtime-checkability and surface."""

from __future__ import annotations

from typing import Protocol

from datasluice.ports import (
    AtomicStateStore,
    CachePort,
    CatalogPort,
    CheckpointableResourceReader,
    OrganizationCatalog,
    PortalDetector,
    ResourceReader,
    ResponseAwareReader,
    SearchableCatalog,
    StateStore,
    StoragePort,
)

ALL_PROTOCOLS = [
    AtomicStateStore,
    CachePort,
    CatalogPort,
    CheckpointableResourceReader,
    OrganizationCatalog,
    PortalDetector,
    ResourceReader,
    ResponseAwareReader,
    SearchableCatalog,
    StateStore,
    StoragePort,
]

ALL_PORT_NAMES = frozenset(
    {
        "AtomicStateStore",
        "CachePort",
        "CatalogPort",
        "CheckpointableResourceReader",
        "OrganizationCatalog",
        "PortalDetector",
        "ResourceReader",
        "ResponseAwareReader",
        "SearchableCatalog",
        "StateStore",
        "StoragePort",
    }
)


def test_all_protocols_are_runtime_checkable() -> None:
    for protocol in ALL_PROTOCOLS:
        assert getattr(protocol, "_is_runtime_protocol", False) is True, f"{protocol.__name__} is not runtime_checkable"


def test_protocol_name_index_and_class_roster_cannot_drift() -> None:
    """ALL_PORT_NAMES and ALL_PROTOCOLS describe exactly the same protocol set."""
    assert set(ALL_PORT_NAMES) == {protocol.__name__ for protocol in ALL_PROTOCOLS}
    assert len(ALL_PORT_NAMES) == len(ALL_PROTOCOLS)


def test_all_protocols_subclass_typing_protocol() -> None:
    for protocol in ALL_PROTOCOLS:
        assert issubclass(protocol, Protocol), f"{protocol.__name__} is not a Protocol subclass"


def test_catalog_port_has_portal_type_member() -> None:
    # Protocol data members are annotation-only (not class attributes); verify the
    # declaration via annotations, which is how runtime_checkable isinstance
    # discovers the expected attribute on conforming instances.
    assert "portal_type" in CatalogPort.__annotations__
    from typing import get_type_hints

    assert get_type_hints(CatalogPort)["portal_type"] is str


def test_searchable_catalog_extends_catalog_port() -> None:
    # issubclass() raises for protocols with non-method members (portal_type);
    # verify inheritance via the MRO instead.
    assert CatalogPort in SearchableCatalog.__mro__
    assert CatalogPort in SearchableCatalog.__bases__


def test_organization_catalog_extends_catalog_port() -> None:
    assert CatalogPort in OrganizationCatalog.__mro__
    assert CatalogPort in OrganizationCatalog.__bases__


def test_all_protocols_importable_individually() -> None:
    for name in [
        "AtomicStateStore",
        "CachePort",
        "CatalogPort",
        "CheckpointableResourceReader",
        "OrganizationCatalog",
        "PortalDetector",
        "ResourceReader",
        "ResponseAwareReader",
        "SearchableCatalog",
        "StateStore",
        "StoragePort",
    ]:
        import datasluice.ports as ports

        assert hasattr(ports, name), f"{name} missing from datasluice.ports"


def test_ports_all_exposes_exactly_the_documented_protocols() -> None:
    import datasluice.ports as ports

    assert set(ports.__all__) == ALL_PORT_NAMES
    assert list(ports.__all__) == sorted(ports.__all__)


def test_checkpointable_resource_reader_declares_open_from_cursor() -> None:
    assert hasattr(CheckpointableResourceReader, "open_from_cursor")


def test_portal_detector_declares_detect() -> None:
    assert hasattr(PortalDetector, "detect")


def test_storage_port_declares_write_read_exists() -> None:
    for method in ("write", "read", "exists"):
        assert hasattr(StoragePort, method), f"StoragePort missing {method}"


def test_cache_port_declares_get_put_delete() -> None:
    for method in ("get", "put", "delete"):
        assert hasattr(CachePort, method), f"CachePort missing {method}"


def test_state_store_declares_get_put_delete() -> None:
    for method in ("get", "put", "delete"):
        assert hasattr(StateStore, method), f"StateStore missing {method}"


def test_resource_reader_declares_open() -> None:
    assert hasattr(ResourceReader, "open")


def test_response_aware_reader_declares_open_response() -> None:
    assert hasattr(ResponseAwareReader, "open_response")
