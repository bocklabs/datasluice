"""Portal adapters for DataSluice.

Each adapter translates a specific open-data portal's API into DataSluice's
domain models.  Importing this package registers all built-in adapters with
the module-level :data:`registry`.
"""

from datasluice.adapters.base import BaseAdapter

# Register built-in adapters (imports have side effects via __init__).
from datasluice.adapters.ckan import CKANAdapter
from datasluice.adapters.custom import CustomAdapter
from datasluice.adapters.datagouv import DataGouvAdapter
from datasluice.adapters.factory import create_adapter
from datasluice.adapters.registry import AdapterRegistry, registry
from datasluice.adapters.socrata import SocrataAdapter

registry.register(CKANAdapter.portal_type, CKANAdapter)
registry.register(DataGouvAdapter.portal_type, DataGouvAdapter)
registry.register(SocrataAdapter.portal_type, SocrataAdapter)
registry.register(CustomAdapter.portal_type, CustomAdapter)

__all__ = [
    "BaseAdapter",
    "AdapterRegistry",
    "registry",
    "create_adapter",
    "CKANAdapter",
    "DataGouvAdapter",
    "SocrataAdapter",
    "CustomAdapter",
]
