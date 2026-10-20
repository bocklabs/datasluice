from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.oauth import OAuthConsentOutcome, OAuthTokenResult
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient


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


def _assert_oauth_error_status(error: CatalogError, name: str, status_code: int) -> None:
    assert error.metadata.get("status_code") == status_code, (name, error.metadata)
    receipt = error.metadata.get("receipt")
    assert isinstance(receipt, Mapping), name
    assert receipt["audit_metadata"]["status_code"] == status_code, name


def _assert_oauth_result_status(result: object, name: str, status_code: int) -> None:
    if isinstance(result, OAuthConsentOutcome):
        assert result.status_code == status_code, name
    else:
        assert isinstance(result, OAuthTokenResult), name
        assert result.receipt.audit_metadata["status_code"] == status_code, name


def assert_oauth_mutation_sync(
    client: SyncUDataClient,
    name: str,
    raw: tuple[int, object, dict[str, str]],
    body: object,
    permissions: object,
) -> None:
    """Assert the typed sync OAuth mutation reproduces the raw status whatever the caller's stack returns.

    The result is a success outcome or a CatalogError carrying the raw status. Token non-retention is owned by the
    dedicated secret-safety tests, not by this parity check.
    """
    try:
        result = getattr(client.auth_oauth, name)(body, permissions, oauth_mutation_policy(name))
    except CatalogError as error:
        _assert_oauth_error_status(error, name, raw[0])
    else:
        _assert_oauth_result_status(result, name, raw[0])


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
        _assert_oauth_error_status(error, name, raw[0])
    else:
        _assert_oauth_result_status(result, name, raw[0])
