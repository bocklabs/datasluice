from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

type WireParams = dict[str, object]


def drop_unset(params: Mapping[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def wire_params(required: Mapping[str, object], optional: Mapping[str, object | None]) -> WireParams:
    return {**required, **drop_unset(optional)}


def detail_params(
    id: str,
    include_datasets: bool | None,
    include_dataset_count: bool | None,
    include_users: bool | None,
) -> WireParams:
    return wire_params(
        {"id": id},
        {
            "include_datasets": include_datasets,
            "include_dataset_count": include_dataset_count,
            "include_users": include_users,
        },
    )
