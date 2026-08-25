"""Airflow hook composition for the DataSluice catalog runtime."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date

from airflow.sdk import BaseHook

from datasluice.application import DataSluice
from datasluice.connectors.catalog.ckan import CKANClientSettings, create_sync_client
from datasluice.contracts.catalog.protocols import SyncCatalogClient
from datasluice.domain.catalog.auth import CKANCredential, CredentialResolver
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
from datasluice.runtime.credentials import credential_from_fields
from datasluice.runtime.transport.base import CatalogTransport

_TOKEN_FIELDS = {
    CatalogPlatform.CKAN: "api_token",
    CatalogPlatform.UDATA: "api_key",
    CatalogPlatform.SOCRATA: "app_token",
}
_PLATFORMS = frozenset(platform.value for platform in _TOKEN_FIELDS)


class DatasluiceHook(BaseHook):
    """Build a sync catalog client from connection-defined explicit credentials.

    Platform ``ckan`` connections compose real dual-surface CKAN live clients
    from the connection origin; other platforms build the deferred runtime
    client pending their canonical executors.
    """

    conn_name_attr = "airflow_conn_id"

    def __init__(self, *, airflow_conn_id: str) -> None:
        super().__init__()
        if not isinstance(airflow_conn_id, str) or not airflow_conn_id:
            raise ValueError("airflow_conn_id must be a non-empty string.")
        self.airflow_conn_id = airflow_conn_id
        self._facade: DataSluice | None = None
        self._client: SyncCatalogClient | None = None

    def get_conn(self) -> SyncCatalogClient:
        """Construct one runtime client with the connection's explicit credential."""
        if self._client is not None:
            return self._client
        connection = self.get_connection(self.airflow_conn_id)
        extras = _connection_extras(connection)
        platform = _platform(extras)
        if platform == CatalogPlatform.CKAN:
            self._client = _ckan_sync_client(connection, extras)
        else:
            credential = credential_from_fields(platform, _credential_fields(connection, extras, platform))
            self._facade = DataSluice(credentials=CredentialResolver(explicit=credential))
            self._client = self._facade.sync_client(_deferred_profile(platform))
        return self._client

    def close(self) -> None:
        """Close the cached client and its owning facade exactly once."""
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._facade is not None:
            self._facade.close()
            self._facade = None


def _ckan_sync_client(
    connection: object,
    extras: Mapping[str, object],
    *,
    sync_transport: CatalogTransport | Callable[[], CatalogTransport] | None = None,
) -> SyncCatalogClient:
    """Build one real CKAN live client from connection extras.

    Args:
        connection: The Airflow connection carrying the optional password fallback.
        extras: The connection extras mapping with platform, base_url, and api_token.
        sync_transport: Optional borrowed transport instance injected at construction
            for tests; production clients own their default transport.

    Returns:
        A synchronous dual-surface CKAN live client.

    Raises:
        ValueError: If extras carry no usable ``base_url`` key.
    """
    base_url = extras.get("base_url")
    if not isinstance(base_url, str) or not base_url:
        raise ValueError(
            "DataSluice ckan connections require a 'base_url' key in extras naming the deployment origin, "
            'e.g. {"platform": "ckan", "base_url": "https://catalog.example.gov", "api_token": "<token>"}.'
        )
    credential = credential_from_fields(
        CatalogPlatform.CKAN, _credential_fields(connection, extras, CatalogPlatform.CKAN)
    )
    if not isinstance(credential, CKANCredential):
        raise TypeError("CKAN Airflow connections require a CKAN credential.")
    probe_policy = "declared-baseline" if base_url.startswith("http://") else "auto"
    settings = CKANClientSettings(
        base_url=base_url,
        credential=credential,
        probe_policy=probe_policy,
        sync_transport=sync_transport,
    )
    return create_sync_client(settings)


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
