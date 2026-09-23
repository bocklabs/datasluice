"""Pinned user and token service contract."""

from __future__ import annotations

import importlib

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

ASSIGNED_METHODS = frozenset(
    {
        "get_me",
        "update_me",
        "delete_me",
        "my_avatar",
        "my_reuses",
        "my_datasets",
        "my_metrics",
        "my_org_datasets",
        "my_org_community_resources",
        "my_org_reuses",
        "my_org_discussions",
        "list_api_tokens",
        "create_api_token",
        "revoke_api_token",
        "list_org_invitations",
        "accept_org_invitation",
        "refuse_org_invitation",
        "list_users",
        "create_user",
        "user_avatar",
        "get_user",
        "update_user",
        "delete_user",
        "rotate_user_password",
        "get_user_contact_point",
        "follow_user",
        "suggest_users",
        "user_roles",
        "my_org_topics",
        "list_user_followers",
        "unfollow_user",
    }
)


def test_users_tokens_contract_exposes_every_assigned_method_in_both_modes() -> None:
    assert hasattr(SyncUDataClient, "users_tokens")
    assert hasattr(AsyncUDataClient, "users_tokens")
    module = importlib.import_module("datasluice.connectors.catalog.udata.services.users_tokens")
    assert ASSIGNED_METHODS <= set(dir(module.SyncUsersTokensService))
    assert ASSIGNED_METHODS <= set(dir(module.AsyncUsersTokensService))
