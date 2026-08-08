"""Factory for the data.gouv.fr connector (entry-point target).

``create_datagouv_connector`` is the callable registered under the
``datasluice.connectors`` entry-points group in ``pyproject.toml``.  The
:class:`PluginManager` resolves it from the installed distribution metadata and
calls it with a :class:`ConnectorContext` carrying the injected infra ports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.connectors.datagouv.adapter import DataGouvAdapter

if TYPE_CHECKING:
    from datasluice.runtime.context import ConnectorContext


def create_datagouv_connector(ctx: ConnectorContext) -> DataGouvAdapter:
    """Construct a :class:`DataGouvAdapter` wired to the context's transport/auth."""
    return DataGouvAdapter(ctx.base_url, auth=ctx.auth, transport=ctx.transport)
