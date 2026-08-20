"""Opt-in OS keychain credential discovery."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import cast

from datasluice.domain.catalog.auth import CatalogCredential, CredentialSource
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import _resolution_error, credential_from_secret

type PasswordGetter = Callable[[str, str], str | None]

_USERNAMES = {
    CatalogPlatform.CKAN: "ckan-api-token",
    CatalogPlatform.UDATA: "udata-api-key",
    CatalogPlatform.SOCRATA: "socrata-app-token",
}


class KeychainCredentialProvider:
    """Discover platform credentials from the caller-selected OS keychain."""

    def __init__(self, get_password: PasswordGetter | None = None) -> None:
        self._get_password = get_password

    def discover(
        self,
        platform: CatalogPlatform,
        context: Mapping[str, object],
    ) -> Mapping[CredentialSource, CatalogCredential]:
        """Read one platform secret from the keychain without transport construction."""
        del context
        username = _USERNAMES.get(platform)
        if username is None:
            return {}
        try:
            password = (self._get_password or _keyring_password_getter())("datasluice", username)
        except ImportError:
            raise
        except Exception as exc:
            raise _resolution_error("the OS keychain", platform, exc) from exc
        if password is None:
            return {}
        return {CredentialSource.KEYCHAIN: credential_from_secret(platform, password)}


def _keyring_password_getter() -> PasswordGetter:
    try:
        import keyring
    except ImportError as exc:
        message = "OS keychain credential discovery requires `uv sync --extra keychain` (datasluice[keychain])."
        raise ImportError(message) from exc
    return cast(PasswordGetter, keyring.get_password)


__all__ = ("KeychainCredentialProvider",)
