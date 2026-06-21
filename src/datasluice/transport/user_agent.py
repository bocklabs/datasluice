"""User-agent string management."""

from __future__ import annotations

import platform

from datasluice._version import __version__

_DEFAULT_TEMPLATE = "datasluice/{version} (Python {python}; {os})"


def build_user_agent(extra: str | None = None) -> str:
    """Build a descriptive User-Agent string.

    Args:
        extra: Optional extra metadata appended in parentheses.
    """
    base = _DEFAULT_TEMPLATE.format(
        version=__version__,
        python=platform.python_version(),
        os=platform.platform(),
    )
    if extra:
        return f"{base} ({extra})"
    return base
