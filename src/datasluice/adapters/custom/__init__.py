"""Custom adapter subpackage.

Provides a template for implementing adapters for portals not yet
supported by DataSluice.  Subclass :class:`BaseAdapter` and register your
adapter with :data:`datasluice.adapters.registry`.
"""

from datasluice.adapters.custom.adapter import CustomAdapter

__all__ = ["CustomAdapter"]
