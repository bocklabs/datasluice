"""Parity checks for catalog-native Protocol families."""

from __future__ import annotations

import inspect

import pytest

from datasluice.contracts.catalog.native.ckan import AsyncCKANServices, SyncCKANServices
from datasluice.contracts.catalog.native.socrata import AsyncSocrataServices, SyncSocrataServices
from datasluice.contracts.catalog.native.udata import AsyncUDataServices, SyncUDataServices


@pytest.mark.parametrize(
    ("sync_services", "async_services", "expected"),
    [
        (
            SyncCKANServices,
            AsyncCKANServices,
            {
                "action_discovery",
                "datasets",
                "resources",
                "organizations",
                "groups",
                "users",
                "vocabularies_licenses",
                "relationships_activity",
                "views",
                "datastore",
                "filestore",
                "extensions",
            },
        ),
        (
            SyncUDataServices,
            AsyncUDataServices,
            {
                "root_profile",
                "datasets",
                "resources",
                "organizations_memberships",
                "users_tokens",
                "auth_oauth",
                "taxonomies",
                "social",
                "geography",
                "harvest_moderation_admin",
                "extensions",
            },
        ),
        (
            SyncSocrataServices,
            AsyncSocrataServices,
            {
                "soda",
                "catalog",
                "assets_permissions",
                "identity_permissions",
                "auth",
                "async_status",
            },
        ),
    ],
)
def test_integrated_native_service_groups_exist_in_both_modes(
    sync_services: type[object], async_services: type[object], expected: set[str]
) -> None:
    """Every integrated platform area has one explicit native sync and async service."""
    assert expected <= set(sync_services.__dict__)
    assert expected <= set(async_services.__dict__)


@pytest.mark.parametrize(
    ("sync_services", "async_services"),
    [
        (SyncCKANServices, AsyncCKANServices),
        (SyncUDataServices, AsyncUDataServices),
        (SyncSocrataServices, AsyncSocrataServices),
    ],
)
def test_native_service_projections_are_signature_equivalent(
    sync_services: type[object], async_services: type[object]
) -> None:
    """Service properties retain a one-for-one sync and async native projection."""
    members = set(sync_services.__dict__) & set(async_services.__dict__)
    services = {member for member in members if not member.startswith("_")}

    assert services
    for member in services:
        sync_property = sync_services.__dict__[member]
        async_property = async_services.__dict__[member]
        assert isinstance(sync_property, property)
        assert isinstance(async_property, property)
        assert inspect.signature(sync_property.fget).parameters == inspect.signature(async_property.fget).parameters


def test_opted_out_legacy_surfaces_are_absent_from_native_contracts() -> None:
    """Locked legacy CKAN and SODA 2 APIs cannot return as Protocol members."""
    retired_members = {"legacy", "soda_v2", "ckan_api_v2"}

    for protocol in (SyncCKANServices, AsyncCKANServices, SyncSocrataServices, AsyncSocrataServices):
        public_members = {member.lower() for member in protocol.__dict__ if not member.startswith("_")}

        assert retired_members.isdisjoint(public_members)
