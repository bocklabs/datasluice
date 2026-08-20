"""Catalog runtime user-agent seam."""

from datasluice._version import __version__


def build_user_agent() -> str:
    """Return the stable DataSluice runtime user-agent."""
    return f"datasluice/{__version__}"
