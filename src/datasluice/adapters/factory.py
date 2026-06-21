"""Factory for creating adapters from portal URLs or type names."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .registry import registry

if TYPE_CHECKING:
    from datasluice.adapters.base import BaseAdapter
    from datasluice.auth import BaseAuth


def create_adapter(
    base_url: str,
    *,
    portal_type: str | None = None,
    auth: BaseAuth | None = None,
) -> BaseAdapter:
    """Instantiate an adapter for the given *base_url*.

    If *portal_type* is ``None`` the portal type is auto-detected using
    :mod:`datasluice.discovery`.

    Args:
        base_url: Root URL of the portal.
        portal_type: Explicit portal type (e.g. ``"ckan"``). When omitted,
            the type is detected automatically.
        auth: Optional authentication strategy.

    Returns:
        An adapter instance ready for use.

    Raises:
        PortalDetectionError: If auto-detection fails.
    """
    if portal_type is None:
        from datasluice.discovery import detect_portal_type

        portal_type = detect_portal_type(base_url)

    adapter_cls = registry.get(portal_type)
    return adapter_cls(base_url, auth=auth)
