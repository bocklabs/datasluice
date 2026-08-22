"""Airflow hook composition for the DataSluice catalog runtime."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date

from airflow.sdk import BaseHook

from datasluice.application import DataSluice
from datasluice.domain.catalog.auth import CredentialResolver
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile
from datasluice.runtime.clients import SyncCatalogClient
from datasluice.runtime.credentials import credential_from_fields

_PLATFORMS = frozenset({"ckan", "udata", "socrata"})

_TOKEN_FIELDS = {
    CatalogPlatform.CKAN: "api_token",
    CatalogPlatform.UDATA: "api_key",
    CatalogPlatform.SOCRATA: "app_token",
}


class DatasluiceHook(BaseHook):
    """Build a sync catalog client from connection-defined explicit credentials.

    Live platform actions await the canonical executors delivered in Phases 3-5.
    """

    conn_name_attr = "airflow_conn_id"

    def __init__(self, *, airflow_conn_id: str) -> None:
        super().__init__()
        if not isinstance(airflow_conn_id, str) or not airflow_conn_id:
            raise ValueError("airflow_conn_id must be a non-empty string.")
        self.airflow_conn_id = airflow_conn_id

    def get_conn(self) -> SyncCatalogClient:
        """Construct one runtime client with the connection's explicit credential."""
        connection = self.get_connection(self.airflow_conn_id)
        extras = _connection_extras(connection)
        platform = _platform(extras)
        credential = credential_from_fields(platform, _credential_fields(connection, extras, platform))
        facade = DataSluice(credentials=CredentialResolver(explicit=credential))
        return facade.sync_client(_deferred_profile(platform))


def _connection_extras(connection: object) -> Mapping[str, object]:
    extras = getattr(connection, "extra_dejson", {})
    if not isinstance(extras, Mapping):
        raise ValueError("DataSluice connections require mapping-valued extras.")
    return extras


def _platform(extras: Mapping[str, object]) -> CatalogPlatform:
    value = extras.get("platform")
    if not isinstance(value, str) or value not in _PLATFORMS:
        raise ValueError("DataSluice connection extras require platform: ckan, udata, or socrata.")
    return CatalogPlatform(value)


def _credential_fields(
    connection: object,
    extras: Mapping[str, object],
    platform: CatalogPlatform,
) -> Mapping[str, object]:
    """Return connection credential fields with the password fallback applied."""
    fields = dict(extras)
    token_field = _TOKEN_FIELDS[platform]
    token = fields.get(token_field)
    if not isinstance(token, str) or not token:
        password = getattr(connection, "password", None)
        if isinstance(password, str) and password:
            fields[token_field] = password
    return fields


def _deferred_profile(platform: CatalogPlatform) -> DeclaredCapabilityProfile:
    operation = OperationId(platform.value, "provider", "deferred")
    return DeclaredCapabilityProfile(
        profile_version="provider-deferred",
        schema_version="1",
        platform_api_version="executor-pending",
        official_source_uri="https://bocklabs.github.io/datasluice/",
        source_accessed_at=date(2026, 8, 20),
        fixture_fingerprint="provider-deferred",
        operations={
            operation: OperationSpec(
                id=operation,
                tier=OperationTier.NORMALIZED,
                request_type="CatalogOperationRequest",
                response_type="ResultEnvelope",
                auth_class=AuthClass.AUTHENTICATED,
                mutation_class=MutationClass.READ,
                idempotency=Idempotency.SAFE,
                concurrency=ConcurrencyRequirement.NONE,
                atomicity=Atomicity.NONE,
                capability_class=CapabilityClass.CORE,
            )
        },
    )


__all__ = ["DatasluiceHook"]
