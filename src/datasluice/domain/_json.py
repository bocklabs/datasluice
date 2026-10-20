from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from datasluice.exceptions import DataSluiceError


def object_dict(value: object, path: str, error: Callable[[str], DataSluiceError]) -> dict[str, object]:
    if not isinstance(value, dict):
        raise error(path)
    result: dict[str, object] = {}
    for key, nested in value.items():
        if not isinstance(key, str):
            raise error(path)
        result[key] = nested
    return result
