"""Explicit credential injection helpers."""

from __future__ import annotations

from datasluice.domain.catalog.auth import CatalogCredential, CredentialResolver


def explicit_resolver(credential: CatalogCredential) -> CredentialResolver:
    """Create a resolver whose caller-supplied credential always wins."""
    return CredentialResolver(explicit=credential)
