"""Version information for DataSluice, read from installed package metadata.

Kept in a separate module to break a circular import with
``transport/user_agent.py``. The value is derived from ``pyproject.toml``
(the single source of truth, kept in sync with release tags by
**release-please**) through ``importlib.metadata`` — there is no second
copy of the version to keep in sync. Do NOT move it into ``__init__.py``.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("datasluice")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"
