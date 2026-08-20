"""CKAN live-client construction boundary."""

from __future__ import annotations

from typing import NoReturn

from datasluice.runtime.extras import require_extra


def create_live_client() -> NoReturn:
    """Begin construction of the future CKAN live client.

    Raises:
        ImportError: If the CKAN connector extra is unavailable.
        NotImplementedError: Until the CKAN runtime client ships.
    """
    require_extra("ckan")
    raise NotImplementedError("The CKAN live client is not implemented yet.")
