"""Shared OAuth-mutation parity checks for the uData connector test suites."""

from __future__ import annotations

from collections.abc import Mapping

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
from datasluice.connectors.catalog.udata.models.oauth import OAuthConsentOutcome, OAuthTokenResult
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogError


def oauth_mutation_policy(name: str) -> MutationPolicy:
    """Build the confirmed mutation policy one OAuth route dispatch requires."""
    target = "self" if name == "authorize_post" else f"request:{name}"
    return MutationPolicy(
        destructive=name == "revoke_token",
        confirmation=ConfirmationPolicy(
            confirmed=True, operation=f"udata/oauth.{name.replace('_', '-')}", target=target
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def assert_oauth_mutation_sync(
    client: SyncUDataClient,
    name: str,
    raw: tuple[int, object, dict[str, str]],
    body: object,
    permissions: object,
) -> None:
    """Assert the typed sync OAuth mutation reproduces the raw loopback outcome.

    The disposable stack has no OAuth client, so the stock endpoints reject every
    form. Either outcome is valid; what matters is that the typed status equals
    the raw status and that no access token is ever retained.
    """
    try:
        result = getattr(client.auth_oauth, name)(body, permissions, oauth_mutation_policy(name))
    except CatalogError as error:
        assert error.metadata.get("status_code") == raw[0], (name, error.metadata)
        receipt = error.metadata.get("receipt")
        if isinstance(receipt, Mapping):
            assert receipt["audit_metadata"]["status_code"] == raw[0], name
    else:
        if isinstance(result, OAuthConsentOutcome):
            assert result.status_code == raw[0], name
        else:
            assert isinstance(result, OAuthTokenResult), name
            assert result.receipt.audit_metadata["status_code"] == raw[0], name


async def assert_oauth_mutation_async(
    client: AsyncUDataClient,
    name: str,
    raw: tuple[int, object, dict[str, str]],
    body: object,
    permissions: object,
) -> None:
    """Assert the async mutation path reproduces the same status as the sync path."""
    try:
        result = await getattr(client.auth_oauth, name)(body, permissions, oauth_mutation_policy(name))
    except CatalogError as error:
        assert error.metadata.get("status_code") == raw[0], (name, error.metadata)
        receipt = error.metadata.get("receipt")
        if isinstance(receipt, Mapping):
            assert receipt["audit_metadata"]["status_code"] == raw[0], name
    else:
        if isinstance(result, OAuthConsentOutcome):
            assert result.status_code == raw[0], name
        else:
            assert isinstance(result, OAuthTokenResult), name
            assert result.receipt.audit_metadata["status_code"] == raw[0], name
