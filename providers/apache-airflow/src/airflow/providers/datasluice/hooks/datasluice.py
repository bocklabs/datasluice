"""Connection-aware DataSluice Hook for Airflow 3."""

from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from airflow.sdk import BaseHook

from datasluice import (
    Artifact,
    CatalogResourceLocator,
    DataSluice,
    DataSluiceError,
    DirectResourceLocator,
    ResourceLocator,
    resource_locator_from_dict,
)

_SESSION_OPTIONS = frozenset({"timeout", "retries", "rate_limit", "page_size", "cache_dir", "cache_ttl"})
_OVERRIDE_OPTIONS = _SESSION_OPTIONS | frozenset({"portal_url", "portal_type"})
_SENSITIVE_QUERY_KEYS = frozenset({"access_token", "api_key", "auth", "password", "secret", "signature", "token"})
_DEFAULT_API_KEY_HEADER = "X-Api-Key"


class _ConnectionAuth:
    """Apply one connection-owned credential strategy without exposing its values."""

    def __init__(
        self,
        auth_type: str,
        *,
        secret: str | None = None,
        username: str | None = None,
        password: str | None = None,
        header_name: str = _DEFAULT_API_KEY_HEADER,
        param_name: str = "api_key",
        in_header: bool = True,
        in_query: bool = False,
        scheme: str = "Bearer",
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._auth_type = auth_type
        self._secret = secret
        self._username = username
        self._password = password
        self._header_name = header_name
        self._param_name = param_name
        self._in_header = in_header
        self._in_query = in_query
        self._scheme = scheme
        self._headers = dict(headers or {})

    def apply(
        self, headers: dict[str, str], params: dict[str, Any] | None = None
    ) -> tuple[dict[str, str], dict[str, Any]]:
        applied_headers = dict(headers)
        applied_params = dict(params or {})
        if self._auth_type == "api_key":
            if self._in_header and self._secret is not None:
                applied_headers[self._header_name] = self._secret
            if self._in_query and self._secret is not None:
                applied_params[self._param_name] = self._secret
        elif self._auth_type == "bearer" and self._secret is not None:
            applied_headers["Authorization"] = f"{self._scheme} {self._secret}"
        elif self._auth_type == "basic" and self._username is not None and self._password is not None:
            token = base64.b64encode(f"{self._username}:{self._password}".encode()).decode("ascii")
            applied_headers["Authorization"] = f"Basic {token}"
        elif self._auth_type == "headers":
            applied_headers.update(self._headers)
        return applied_headers, applied_params

    def __repr__(self) -> str:
        return f"<_ConnectionAuth type={self._auth_type!r}>"


@dataclass(frozen=True)
class _ResolvedConnection:
    """Execution-time, sanitized view of an Airflow Connection."""

    portal_url: str | None
    portal_type: str | None
    session_kwargs: Mapping[str, object]
    secrets: tuple[str, ...]


class DataSluiceHook(BaseHook):
    """Connect Airflow tasks to the public DataSluice facade."""

    conn_name_attr = "airflow_conn_id"
    default_conn_name = "datasluice_default"
    conn_type = "datasluice"
    hook_name = "DataSluice"

    def __init__(self, airflow_conn_id: str = default_conn_name, **overrides: object) -> None:
        super().__init__()
        if not isinstance(airflow_conn_id, str) or not airflow_conn_id:
            raise ValueError("airflow_conn_id must be a non-empty string")
        unsupported = set(overrides) - _OVERRIDE_OPTIONS
        if unsupported:
            raise ValueError("Only non-secret DataSluice connection overrides are supported")
        for name, value in overrides.items():
            _validate_option(name, value)
        self.airflow_conn_id = airflow_conn_id
        self._overrides = dict(overrides)

    def get_conn(self) -> DataSluice:
        """Build and return a DataSluice facade from the configured Connection."""
        config = self._resolve_connection()
        try:
            return DataSluice(**config.session_kwargs)
        except Exception as exc:
            _raise_redacted(exc, config.secrets)

    def search(
        self,
        portal_url: str | None = None,
        query: object = None,
        *,
        max_results: int = 50,
        portal_type: str | None = None,
        **query_kwargs: object,
    ) -> list[dict[str, object]]:
        """Search a portal and return bounded, JSON-safe catalog locators."""
        config = self._resolve_connection()
        try:
            limit = _validate_limit(max_results)
            selected_url = _select_portal_url(portal_url, config.portal_url)
            selected_portal_type = portal_type if portal_type is not None else config.portal_type
            search_kwargs = dict(query_kwargs)
            search_kwargs["limit"] = limit
            if selected_portal_type is not None:
                search_kwargs["portal_type"] = selected_portal_type
            with DataSluice(**config.session_kwargs) as facade:
                result = facade.search(selected_url, query, **search_kwargs)
                return _catalog_locators(result, selected_url, limit)
        except Exception as exc:
            _raise_redacted(exc, config.secrets)

    def materialize(
        self,
        locator: ResourceLocator | dict[str, object],
        destination_uri: str,
        *,
        mode: str = "parquet",
    ) -> Artifact:
        """Materialize exactly one locator and return its canonical Artifact."""
        selected_locator = resource_locator_from_dict(locator) if isinstance(locator, dict) else locator
        if not isinstance(selected_locator, (DirectResourceLocator, CatalogResourceLocator)):
            raise DataSluiceError("materialize requires one ResourceLocator")
        config = self._resolve_connection()
        try:
            with DataSluice(**config.session_kwargs) as facade:
                artifact = facade.materialize(selected_locator, destination_uri, mode=mode)
                if not isinstance(artifact, Artifact):
                    raise DataSluiceError("DataSluice materialize did not return an Artifact")
                return artifact
        except Exception as exc:
            _raise_redacted(exc, config.secrets)

    def _resolve_connection(self) -> _ResolvedConnection:
        connection = self.get_connection(self.airflow_conn_id)
        extras = _connection_extras(connection)
        portal_url = _resolve_text_option("portal_url", self._overrides, extras)
        if portal_url is None:
            portal_url = _first_text(extras, "portal_url", "base_url", "url") or _text(
                getattr(connection, "host", None)
            )
        portal_type = _resolve_text_option("portal_type", self._overrides, extras)
        if portal_type is None:
            portal_type = _first_text(extras, "portal_type", "connector")

        session_kwargs: dict[str, object] = {}
        for name in _SESSION_OPTIONS:
            value = _resolve_value(name, self._overrides, extras)
            if value is not None:
                session_kwargs[name] = _coerce_option(name, value)
        auth, secrets = _connection_auth(connection, extras, portal_url)
        if auth is not None:
            session_kwargs["auth"] = auth
        return _ResolvedConnection(
            portal_url=portal_url,
            portal_type=portal_type,
            session_kwargs=session_kwargs,
            secrets=secrets,
        )


def _connection_extras(connection: object) -> dict[str, object]:
    value = getattr(connection, "extra_dejson", {})
    if not isinstance(value, Mapping):
        return {}
    return {key: item for key, item in value.items() if isinstance(key, str)}


def _validate_option(name: str, value: object) -> None:
    if value is None:
        return
    if name in {"portal_url", "portal_type", "cache_dir"} and not isinstance(value, str):
        raise ValueError(f"Invalid non-secret DataSluice override: {name}")
    if name in {"retries", "page_size", "cache_ttl"} and (type(value) is not int or value < 0):
        raise ValueError(f"Invalid non-secret DataSluice override: {name}")
    if name in {"timeout", "rate_limit"} and (
        isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0
    ):
        raise ValueError(f"Invalid non-secret DataSluice override: {name}")


def _coerce_option(name: str, value: object) -> object:
    _validate_option(name, value)
    return value


def _resolve_value(name: str, overrides: Mapping[str, object], extras: Mapping[str, object]) -> object:
    if name in overrides and overrides[name] is not None:
        return overrides[name]
    return extras.get(name)


def _resolve_text_option(name: str, overrides: Mapping[str, object], extras: Mapping[str, object]) -> str | None:
    value = _resolve_value(name, overrides, extras)
    if value is None:
        return None
    if not isinstance(value, str):
        raise DataSluiceError(f"Invalid DataSluice connection option: {name}")
    return value


def _first_text(values: Mapping[str, object], *names: str) -> str | None:
    for name in names:
        value = _text(values.get(name))
        if value is not None:
            return value
    return None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _connection_auth(
    connection: object,
    extras: Mapping[str, object],
    portal_url: str | None,
) -> tuple[_ConnectionAuth | None, tuple[str, ...]]:
    login = _text(getattr(connection, "login", None)) or _text(extras.get("username"))
    password = _text(getattr(connection, "password", None)) or _text(extras.get("password"))
    api_key = _text(extras.get("api_key")) or password
    token = _text(extras.get("token")) or _text(extras.get("bearer_token")) or password
    raw_headers = extras.get("headers", extras.get("custom_headers"))
    headers = _headers(raw_headers)
    auth_type = _text(extras.get("auth_type"))
    if auth_type is None:
        if headers:
            auth_type = "headers"
        elif login and password:
            auth_type = "basic"
        elif api_key:
            auth_type = "api_key"
    normalized_type = auth_type.lower().replace("-", "_") if auth_type else "none"
    secrets = _secret_values(login, password, api_key, token, *(headers.values() if headers else ()))
    if normalized_type in {"none", "no_auth"}:
        return None, _add_url_secrets(secrets, portal_url)
    if normalized_type == "api_key":
        if api_key is None:
            raise DataSluiceError("DataSluice api_key authentication requires a Connection secret")
        header_name = _text(extras.get("api_key_header")) or _DEFAULT_API_KEY_HEADER
        param_name = _text(extras.get("api_key_param")) or "api_key"
        in_header = _boolean_option(extras.get("api_key_in_header"), True, "api_key_in_header")
        in_query = _boolean_option(extras.get("api_key_in_query"), False, "api_key_in_query")
        return (
            _ConnectionAuth(
                "api_key",
                secret=api_key,
                header_name=header_name,
                param_name=param_name,
                in_header=in_header,
                in_query=in_query,
            ),
            _add_url_secrets(secrets, portal_url),
        )
    if normalized_type == "bearer":
        if token is None:
            raise DataSluiceError("DataSluice bearer authentication requires a Connection secret")
        scheme = _text(extras.get("bearer_scheme")) or "Bearer"
        return _ConnectionAuth("bearer", secret=token, scheme=scheme), _add_url_secrets(secrets, portal_url)
    if normalized_type == "basic":
        if login is None or password is None:
            raise DataSluiceError("DataSluice basic authentication requires Connection login and password")
        return _ConnectionAuth("basic", username=login, password=password), _add_url_secrets(secrets, portal_url)
    if normalized_type == "headers":
        if not headers:
            raise DataSluiceError("DataSluice headers authentication requires Connection headers")
        return _ConnectionAuth("headers", headers=headers), _add_url_secrets(secrets, portal_url)
    raise DataSluiceError("Unsupported DataSluice Connection auth_type")


def _headers(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise DataSluiceError("DataSluice Connection headers must be a string mapping")
    return dict(value)


def _boolean_option(value: object, default: bool, name: str) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise DataSluiceError(f"Invalid DataSluice Connection option: {name}")
    return value


def _secret_values(*values: str | None) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def _add_url_secrets(secrets: tuple[str, ...], portal_url: str | None) -> tuple[str, ...]:
    if portal_url is None:
        return secrets
    try:
        query_secrets = tuple(
            value
            for name, value in parse_qsl(urlsplit(portal_url).query, keep_blank_values=True)
            if name.lower() in _SENSITIVE_QUERY_KEYS and value
        )
    except ValueError:
        query_secrets = ()
    return _secret_values(*secrets, *query_secrets)


def _select_portal_url(explicit: str | None, configured: str | None) -> str:
    selected = explicit or configured
    if not selected:
        raise DataSluiceError("DataSluice Connection has no portal_url")
    return selected


def _validate_limit(value: int) -> int:
    if type(value) is not int or not 1 <= value <= 1000:
        raise DataSluiceError("DataSluice search max_results must be between 1 and 1000")
    return value


def _catalog_locators(result: object, portal_url: str, limit: int) -> list[dict[str, object]]:
    locators: list[dict[str, object]] = []
    for dataset in getattr(result, "datasets", ()):
        dataset_id = getattr(dataset, "id", None)
        for resource in getattr(dataset, "resources", ()):
            locator = CatalogResourceLocator(
                portal_url=portal_url,
                dataset_id=dataset_id,
                resource_id=getattr(resource, "id", None),
            )
            locators.append(locator.to_dict())
            if len(locators) == limit:
                return locators
    return locators


def _raise_redacted(error: Exception, secrets: tuple[str, ...]) -> None:
    message = str(error)
    redacted = message
    for secret in secrets:
        redacted = redacted.replace(secret, "***")
    if redacted == message:
        raise error
    try:
        replacement = type(error)(redacted)
    except Exception:
        replacement = RuntimeError(redacted)
    raise replacement from None
