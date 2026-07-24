"""Unit tests for port Protocol definitions — runtime-checkability and surface."""

from __future__ import annotations

from typing import Protocol

from datasluice.ports import (
    CachePort,
    CatalogPort,
    CredentialProvider,
    OrganizationCatalog,
    PortalDetector,
    ResourceReader,
    SearchableCatalog,
    StateStore,
    StoragePort,
    Transport,
)

ALL_PROTOCOLS = [
    CachePort,
    CatalogPort,
    CredentialProvider,
    OrganizationCatalog,
    PortalDetector,
    ResourceReader,
    SearchableCatalog,
    StateStore,
    StoragePort,
    Transport,
]


def test_all_protocols_are_runtime_checkable() -> None:
    for protocol in ALL_PROTOCOLS:
        assert getattr(protocol, "_is_runtime_protocol", False) is True, f"{protocol.__name__} is not runtime_checkable"


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
        "CachePort",
        "CatalogPort",
        "CredentialProvider",
        "OrganizationCatalog",
        "PortalDetector",
        "ResourceReader",
        "SearchableCatalog",
        "StateStore",
        "StoragePort",
        "Transport",
    ]:
        import datasluice.ports as ports

        assert hasattr(ports, name), f"{name} missing from datasluice.ports"


def test_ports_all_contains_ten_names() -> None:
    import datasluice.ports as ports

    assert len(ports.__all__) == 10


def test_transport_declares_request_get_json_download() -> None:
    for method in ("request", "get_json", "download"):
        assert hasattr(Transport, method), f"Transport missing {method}"


def test_credential_provider_declares_resolve() -> None:
    assert hasattr(CredentialProvider, "resolve")


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
