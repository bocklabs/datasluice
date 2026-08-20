"""uData live-client construction boundary."""

from __future__ import annotations

from typing import NoReturn

from datasluice.runtime.extras import require_extra


def create_live_client() -> NoReturn:
    """Begin construction of the future uData live client.

    Raises:
        ImportError: If the uData connector extra is unavailable.
        NotImplementedError: Until the uData runtime client ships.
    """
    require_extra("udata")
    raise NotImplementedError("The uData live client is not implemented yet.")
