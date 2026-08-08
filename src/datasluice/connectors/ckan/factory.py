"""Factory for the CKAN connector (entry-point target).

``create_ckan_connector`` is the callable registered under the
``datasluice.connectors`` entry-points group in ``pyproject.toml``.  The
:class:`PluginManager` resolves it from the installed distribution metadata and
calls it with a :class:`ConnectorContext` carrying the injected infra ports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.connectors.ckan.adapter import CKANAdapter

if TYPE_CHECKING:
    from datasluice.runtime.context import ConnectorContext


def create_ckan_connector(ctx: ConnectorContext) -> CKANAdapter:
    """Construct a :class:`CKANAdapter` wired to the context's transport/auth."""
    return CKANAdapter(ctx.base_url, auth=ctx.auth, transport=ctx.transport)
