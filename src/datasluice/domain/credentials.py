"""Credential scoping model for host-bound credential policy."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CredentialScope:
    """Host-scoped policy controlling where credentials may be sent.

    Attributes:
        allowed_hosts: Hostnames the credential may be sent to.
        allowed_schemes: URL schemes the credential may travel over.
        send_on_redirect: Whether credentials are retained on redirects to allowed hosts.
    """

    allowed_hosts: tuple[str, ...] = field(default_factory=tuple)
    allowed_schemes: tuple[str, ...] = ("https",)
    send_on_redirect: bool = False
