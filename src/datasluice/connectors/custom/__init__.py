"""Custom connector subpackage.

Provides a template for implementing connectors for portals not yet
supported by DataSluice.  Subclass :class:`BaseAdapter`, declare a factory
function, and register it under the ``datasluice.connectors`` entry-points
group in your distribution metadata.
"""

from datasluice.connectors.custom.adapter import CustomAdapter

__all__ = ["CustomAdapter"]
