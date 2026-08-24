"""Opt-in HashiCorp Vault KV v2 credential discovery."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from threading import RLock
from typing import Protocol, cast

from datasluice.domain.catalog.auth import CatalogCredential, CredentialSource
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import _resolution_error, credential_from_fields


class VaultKvV2Client(Protocol):
    """Synchronous subset of a Vault KV v2 client."""

    def read_secret_version(self, *, path: str, mount_point: str) -> Mapping[str, object]:
        """Return one KV v2 secret envelope."""


class VaultKvClient(Protocol):
    """Vault KV client grouping."""

    v2: VaultKvV2Client


class VaultSecretsClient(Protocol):
    """Vault secret-engine client grouping."""

    kv: VaultKvClient


class VaultClient(Protocol):
    """Synchronous subset of an hvac Vault client."""

    secrets: VaultSecretsClient


type VaultClientFactory = Callable[[str, str], VaultClient]


class VaultCredentialProvider:
    """Discover one platform credential from caller-selected Vault KV v2 data."""

    def __init__(
        self,
        *,
        url: str,
        token: str,
        mount_point: str,
        path: str,
        client_factory: VaultClientFactory | None = None,
    ) -> None:
        self._url = url
        self._token = token
        self._mount_point = mount_point
        self._path = path
        self._client_factory = client_factory
        self._lock = RLock()
        self._client: VaultClient | None = None

    def _resolved_client(self) -> VaultClient:
        with self._lock:
            if self._client is None:
                self._client = (self._client_factory or _vault_client_factory())(self._url, self._token)
            return self._client

    def discover(
        self,
        platform: CatalogPlatform,
        context: Mapping[str, object],
    ) -> Mapping[CredentialSource, CatalogCredential]:
        """Read one KV v2 secret through an injectable synchronous client."""
        del context
        try:
            response = self._resolved_client().secrets.kv.v2.read_secret_version(
                path=self._path, mount_point=self._mount_point
            )
            credential = credential_from_fields(platform, _vault_fields(response))
        except ImportError:
            raise
        except Exception as exc:
            raise _resolution_error("HashiCorp Vault", platform, exc) from None
        return {CredentialSource.SECRET_MANAGER: credential}


def _vault_client_factory() -> VaultClientFactory:
    try:
        import hvac
    except ImportError as exc:
        message = "Vault secret discovery requires `uv sync --extra secrets-vault` (datasluice[secrets-vault])."
        raise ImportError(message) from exc

    def create_client(url: str, token: str) -> VaultClient:
        return cast(VaultClient, hvac.Client(url=url, token=token))

    return create_client


def _vault_fields(response: object) -> Mapping[str, object]:
    if not isinstance(response, Mapping):
        raise ValueError("Vault KV v2 must return a mapping response.")
    envelope = response.get("data")
    if not isinstance(envelope, Mapping) or not isinstance(fields := envelope.get("data"), Mapping):
        raise ValueError("Vault KV v2 must return fields under data.data.")
    return cast(Mapping[str, object], fields)


__all__ = ("VaultCredentialProvider",)
