from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.errors.catalog import CatalogValidationError

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


def reject_deprecated(action: str, payload: Mapping[str, object]) -> None:
    """Reject parameters removed from the canonical CKAN Action API surface."""
    banned = {
        "current_package_list_with_resources": {"page"},
        "tag_search": {"fields"},
        "tag_autocomplete": {"fields"},
    }.get(action, set())
    clash = sorted(banned.intersection(payload))
    if clash:
        raise CatalogValidationError(
            f"The parameter(s) {clash} are officially deprecated for {action} and are not accepted.",
            operation=f"ckan/{action}",
            platform="ckan",
            safe_action="Use offset instead of page; use q and vocabulary_id instead of fields.",
        )
