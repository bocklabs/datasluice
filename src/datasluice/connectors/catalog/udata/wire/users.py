"""Exact stock uData user, invitation, and API-token wire contract."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import quote, urlencode

from datasluice.connectors.catalog.udata.models.users import (
    ApiTokenCreateInput,
    ApiTokenMetadata,
    UserCreateInput,
    UserDeleteOptions,
    UserListQuery,
    UserSuggestQuery,
    UserUpdateInput,
)
from datasluice.connectors.catalog.udata.wire.organizations import parse_page, parse_records
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = CatalogPlatform.UDATA
_USER = ResourceKind("user")
_TOKEN = ResourceKind("api-token")
_TOKEN_SCHEMA_ACTION = "Verify the response against the pinned token schema."

OPERATIONS = {
    name: f"udata/api-{version}.{name.replace('_', '-')}"
    for version, names in (
        (
            "v1",
            (
                "get_me",
                "update_me",
                "delete_me",
                "my_avatar",
                "my_reuses",
                "my_datasets",
                "my_metrics",
                "my_org_datasets",
                "my_org_community_resources",
                "my_org_reuses",
                "my_org_discussions",
                "list_api_tokens",
                "create_api_token",
                "revoke_api_token",
                "list_org_invitations",
                "accept_org_invitation",
                "refuse_org_invitation",
                "list_users",
                "create_user",
                "user_avatar",
                "get_user",
                "update_user",
                "delete_user",
                "rotate_user_password",
                "get_user_contact_point",
                "follow_user",
                "suggest_users",
                "user_roles",
                "list_user_followers",
                "unfollow_user",
            ),
        ),
        ("v2", ("my_org_topics",)),
    )
    for name in names
}

type Request = tuple[str, str, dict[str, str], object]
_PATHS = {
    **dict.fromkeys(("get_me", "update_me", "delete_me"), "/api/1/me/"),
    **{
        name: f"/api/1/me/{suffix}/"
        for name, suffix in (
            ("my_avatar", "avatar"),
            ("my_reuses", "reuses"),
            ("my_datasets", "datasets"),
            ("my_metrics", "metrics"),
            ("my_org_datasets", "org_datasets"),
            ("my_org_community_resources", "org_community_resources"),
            ("my_org_reuses", "org_reuses"),
            ("my_org_discussions", "org_discussions"),
            ("list_api_tokens", "api_tokens"),
            ("create_api_token", "api_tokens"),
            ("list_org_invitations", "org_invitations"),
        )
    },
    "list_users": "/api/1/users/",
    "create_user": "/api/1/users/",
    "suggest_users": "/api/1/users/suggest/",
    "user_roles": "/api/1/users/roles/",
    "my_org_topics": "/api/2/me/org_topics/",
}
_POST = frozenset(
    {
        "my_avatar",
        "create_api_token",
        "accept_org_invitation",
        "refuse_org_invitation",
        "create_user",
        "user_avatar",
        "rotate_user_password",
        "follow_user",
    }
)
_DELETE = frozenset({"delete_me", "delete_user", "revoke_api_token", "unfollow_user"})


def _segment(value: object, operation: str) -> str:
    if not isinstance(value, str) or not value or value in {".", ".."} or any(c in value for c in "/?#"):
        raise CatalogValidationError(
            "A uData user or token identifier must be one non-dot path segment.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Use the identifier from a prior typed read.",
        )
    return quote(value, safe="")


def _query(params: list[tuple[str, str]]) -> str:
    return "?" + urlencode(params) if params else ""


def _target_path(name: str, identifier: str | None) -> str:
    segment = _segment(identifier, OPERATIONS[name])
    if name == "revoke_api_token":
        return f"/api/1/me/api_tokens/{segment}/"
    if name in {"accept_org_invitation", "refuse_org_invitation"}:
        return f"/api/1/me/org_invitations/{segment}/{'accept' if name.startswith('accept') else 'refuse'}/"
    if name in {"list_user_followers", "follow_user", "unfollow_user"}:
        return f"/api/1/users/{segment}/followers/"
    suffix = {
        "user_avatar": "avatar/",
        "rotate_user_password": "rotate_password/",
        "get_user_contact_point": "contacts/",
    }.get(name, "")
    return f"/api/1/users/{segment}/{suffix}"


def build_request(name: str, *, identifier: str | None = None, query: object = None, body: object = None) -> Request:
    """Build one documented user-family request from its named route."""
    if name not in OPERATIONS:
        raise ValueError("The uData user route is not assigned to this family.")
    path = _PATHS[name] if name in _PATHS else _target_path(name, identifier)
    method = "GET"
    if name in _POST:
        method = "POST"
    elif name in _DELETE:
        method = "DELETE"
    elif name in {"update_me", "update_user"}:
        method = "PUT"
    if isinstance(query, (UserListQuery, UserSuggestQuery)):
        path += _query(query.query_params())
    elif isinstance(query, str):
        path += _query([("q", query)])
    elif isinstance(query, UserDeleteOptions):
        path += _query(
            [
                ("send_legal_notice", str(query.send_legal_notice).lower()),
                ("no_mail", str(query.no_mail).lower()),
                ("delete_comments", str(query.delete_comments).lower()),
            ]
        )
    elif query is not None:
        raise ValueError("Unsupported uData user query input.")
    if isinstance(body, (UserCreateInput, UserUpdateInput, ApiTokenCreateInput)):
        body = body.payload()
    return method, path, {}, body


def parse_user(payload: object, *, operation: str) -> NativeRecord:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("id"), str) or not payload["id"]:
        raise CatalogValidationError(
            "The uData user response omitted its id.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned user schema.",
        )
    if not isinstance(payload.get("first_name"), str) or not isinstance(payload.get("last_name"), str):
        raise CatalogValidationError(
            "The uData user response omitted its name.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned user schema.",
        )
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=_USER,
        id=CatalogId(platform=PLATFORM, resource_kind=_USER, value=payload["id"]),
        payload={key: value for key, value in payload.items() if key not in {"token", "token_hash", "password"}},
    )


def parse_token(payload: object, *, operation: str) -> ApiTokenMetadata:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData token response must be a JSON object.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action=_TOKEN_SCHEMA_ACTION,
        )
    token_id = payload.get("id")
    prefix = payload.get("token_prefix")
    if not isinstance(token_id, str) or not isinstance(prefix, str):
        raise CatalogValidationError(
            "The uData token response omitted safe token metadata.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action=_TOKEN_SCHEMA_ACTION,
        )
    string_fields = ("name", "created_at", "expires_at", "revoked_at", "kind", "last_used_at")
    optional = {key: payload.get(key) for key in string_fields}
    if any(value is not None and not isinstance(value, str) for value in optional.values()):
        raise CatalogValidationError(
            "The uData token metadata has an invalid field.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action=_TOKEN_SCHEMA_ACTION,
        )
    list_fields: dict[str, tuple[str, ...] | None] = {}
    for key in ("scopes", "user_agents"):
        value = payload.get(key)
        if value is not None and (
            not isinstance(value, list) or not all(isinstance(item, str) and item for item in value)
        ):
            raise CatalogValidationError(
                "The uData token metadata has an invalid field.",
                operation=operation,
                platform=PLATFORM.value,
                safe_action=_TOKEN_SCHEMA_ACTION,
            )
        list_fields[key] = tuple(value) if isinstance(value, list) else None
    return ApiTokenMetadata(id=token_id, token_prefix=prefix, **optional, **list_fields)


def parse_token_list(payload: object, *, operation: str) -> tuple[ApiTokenMetadata, ...]:
    if not isinstance(payload, list):
        raise CatalogValidationError(
            "The uData token list must be a JSON array.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action=_TOKEN_SCHEMA_ACTION,
        )
    return tuple(parse_token(item, operation=operation) for item in payload)


def parse_user_page(payload: object, *, operation: str):
    return parse_page(payload, operation=operation, kind=_USER)


def parse_mapping_list(payload: object, *, operation: str, kind: str) -> tuple[MappingRecord, ...]:
    return parse_records(payload, operation=operation, kind=ResourceKind(kind))
