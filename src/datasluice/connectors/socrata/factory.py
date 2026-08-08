"""Factory for the Socrata connector (entry-point target).

``create_socrata_connector`` is the callable registered under the
``datasluice.connectors`` entry-points group in ``pyproject.toml``.  The
:class:`PluginManager` resolves it from the installed distribution metadata and
calls it with a :class:`ConnectorContext` carrying the injected infra ports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.connectors.socrata.adapter import SocrataAdapter

if TYPE_CHECKING:
    from datasluice.runtime.context import ConnectorContext


def create_socrata_connector(ctx: ConnectorContext) -> SocrataAdapter:
    """Construct a :class:`SocrataAdapter` wired to the context's transport/auth."""
    return SocrataAdapter(ctx.base_url, auth=ctx.auth, transport=ctx.transport)
