"""Opt-in environment credential discovery with a fixed narrow allowlist."""

from __future__ import annotations

import os
from collections.abc import Mapping

from datasluice.domain.catalog.auth import CatalogCredential, CredentialSource
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import credential_from_secret

_ENVIRONMENT_NAMES = {
    CatalogPlatform.CKAN: "DATASLUICE_CKAN_API_TOKEN",
    CatalogPlatform.UDATA: "DATASLUICE_UDATA_API_KEY",
    CatalogPlatform.SOCRATA: "DATASLUICE_SOCRATA_APP_TOKEN",
}


class EnvironmentCredentialProvider:
    """Discover platform credentials from the documented environment names."""

    def discover(
        self,
        platform: CatalogPlatform,
        context: Mapping[str, object],
    ) -> Mapping[CredentialSource, CatalogCredential]:
        """Read the one documented variable for platform, if it is present."""
        del context
        name = _ENVIRONMENT_NAMES.get(platform)
        if name is None or (secret := os.environ.get(name)) is None:
            return {}
        return {CredentialSource.ENVIRONMENT: credential_from_secret(platform, secret)}
