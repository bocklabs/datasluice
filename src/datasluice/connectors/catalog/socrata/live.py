"""Socrata live-client construction boundary."""

from __future__ import annotations

from typing import NoReturn

from datasluice.runtime.extras import require_extra


def create_live_client() -> NoReturn:
    """Begin construction of the future Socrata live client.

    Raises:
        ImportError: If the Socrata connector extra is unavailable.
        NotImplementedError: Until the Socrata runtime client ships.
    """
    require_extra("socrata")
    raise NotImplementedError("The Socrata live client is not implemented yet.")
