from __future__ import annotations

import asyncio
import json
import re
import threading
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from time import monotonic
from typing import TYPE_CHECKING, Never, cast
from urllib.parse import urlsplit

from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogUnavailableError,
    CatalogValidationError,
    UnsupportedCapabilityError,
)
from datasluice.runtime.transport.base import CatalogTransport, RedirectPolicy, RuntimeRequest, RuntimeResponse

if TYPE_CHECKING:
    from collections.abc import Callable

    from datasluice.runtime.clients import AsyncCatalogTransport
    from datasluice.runtime.resilience import DeadlineMonitor

_VERSION = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\Z", re.ASCII)
_PARAMETER = re.compile(r"\{[A-Za-z_]\w*\}", re.ASCII)
_METHODS = frozenset({"get", "post", "put", "patch", "delete"})
_MAX_BYTES = 4 * 1024 * 1024
_MAX_ROUTES = 4096
UDATA_PROFILE_RESOURCE = "catalog/profiles/udata-17.6.json"


def _version_parts(version: str) -> tuple[int, ...]:
    if not isinstance(version, str) or len(version) > 64 or not _VERSION.fullmatch(version):
        raise ValueError("uData versions require three unsigned semantic version components.")
    return tuple(int(part) for part in version.split("."))


@dataclass(frozen=True, slots=True)
class UDataVersionPolicy:
    minimum: str
    maximum_exclusive: str

    def __post_init__(self) -> None:
        if _version_parts(self.minimum) >= _version_parts(self.maximum_exclusive):
            raise ValueError("The uData compatibility range must increase.")

    @property
    def supported_range(self) -> str:
        return f">={self.minimum},<{self.maximum_exclusive}"

    def accepts(self, version: str) -> bool:
        try:
            return _version_parts(self.minimum) <= _version_parts(version) < _version_parts(self.maximum_exclusive)
        except ValueError:
            return False


@lru_cache(maxsize=1)
def udata_version_policy() -> UDataVersionPolicy:
    document = json.loads(
        resources.files("datasluice.contracts").joinpath(UDATA_PROFILE_RESOURCE).read_text(encoding="utf-8")
    )
    policy = document["compatibility"]
    return UDataVersionPolicy(policy["minimum"], policy["maximum_exclusive"])


def strict_json(body: bytes) -> object:
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("The deployment document contains duplicate keys.")
            result[key] = value
        return result

    def invalid_constant(value: str) -> Never:
        raise ValueError("The deployment document contains a non-finite number.")

    return json.loads(body, object_pairs_hook=unique, parse_constant=invalid_constant)


@dataclass(frozen=True, slots=True)
class UDataRouteSurface:
    routes: tuple[tuple[re.Pattern[str], frozenset[str]], ...]

    @classmethod
    def parse(cls, payload: object, api_version: str) -> UDataRouteSurface:
        if not isinstance(payload, dict) or payload.get("swagger") != "2.0":
            raise ValueError("The deployment must expose a Swagger 2.0 object.")
        if payload.get("basePath") != f"/api/{api_version}":
            raise ValueError("The Swagger base path does not match the requested API.")
        paths = payload.get("paths")
        if not isinstance(paths, dict) or not paths or len(paths) > _MAX_ROUTES:
            raise ValueError("The Swagger paths must be a bounded non-empty object.")
        for path in paths:
            _validate_path(path)
        ordered = sorted(paths, key=lambda path: tuple("{" not in part for part in path.split("/")), reverse=True)
        return cls(tuple(_parse_route(path, paths[path]) for path in ordered))

    def allows(self, method: str, path: str) -> bool:
        relative = "/" + urlsplit(path).path.split("/", 3)[-1]
        for template, methods in self.routes:
            if template.fullmatch(relative):
                return method.upper() in methods
        return False


def _validate_path(path: object) -> None:
    if not isinstance(path, str) or not path.startswith("/") or len(path) > 2048:
        raise ValueError("The Swagger path is invalid.")
    if any(char in path for char in "?#\\") or any(char.isspace() for char in path):
        raise ValueError("The Swagger path must not contain query or fragment data.")
    if any(len(_PARAMETER.findall(segment)) > 1 for segment in path.split("/")):
        raise ValueError("A Swagger segment must contain at most one parameter.")
    if any(char in _PARAMETER.sub("parameter", path) for char in "{}"):
        raise ValueError("The Swagger path contains a malformed parameter.")


def _parse_route(path: str, operations: object) -> tuple[re.Pattern[str], frozenset[str]]:
    if not isinstance(operations, dict):
        raise ValueError("The Swagger path methods must be an object.")
    if any(not isinstance(operations[method], dict) for method in operations if method in _METHODS):
        raise ValueError("The Swagger operation must be an object.")
    parts = re.split(r"(\{[A-Za-z_]\w*\})", path, flags=re.ASCII)
    pattern = "".join("[^/]+" if _PARAMETER.fullmatch(part) else re.escape(part) for part in parts)
    return re.compile(pattern), frozenset(method.upper() for method in operations if method in _METHODS)


class UDataRouteGate:
    def __init__(
        self,
        origin: str,
        transport: CatalogTransport | AsyncCatalogTransport,
        ttl_seconds: float,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._origin = origin
        self._transport = transport
        self._ttl = ttl_seconds
        self._clock = clock
        self._cache: dict[str, tuple[UDataRouteSurface, float]] = {}
        self._lock = threading.RLock()
        self._generation = 0
        self._async_lock: asyncio.Lock | None = None
        self._async_lock_loop: asyncio.AbstractEventLoop | None = None

    def _async_lock_for(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        lock = self._async_lock
        if lock is None or self._async_lock_loop is not loop:
            lock = asyncio.Lock()
            self._async_lock = lock
            self._async_lock_loop = loop
        return lock

    def invalidate(self) -> None:
        with self._lock:
            self._generation += 1
            self._cache.clear()

    @staticmethod
    def api_version(method: str, path: str) -> str | None:
        path = urlsplit(path).path
        if path.startswith("/oauth/"):
            return None
        match = re.match(r"/api/([12])/", path)
        if match is None:
            raise CatalogValidationError(
                "The typed uData path is outside the supported API surface.",
                operation="udata/route-admission",
                platform="udata",
                metadata={"route_state": "unsupported"},
                safe_action="Use a typed operation in the supported uData API surface.",
            )
        return match[1]

    def _request(self, api: str, deadline: DeadlineMonitor | None) -> RuntimeRequest:
        if deadline is not None:
            deadline.assert_dispatchable()
        return RuntimeRequest(
            method="GET",
            url=f"{self._origin}/api/{api}/swagger.json",
            headers={},
            max_response_bytes=_MAX_BYTES,
            redirect_policy=RedirectPolicy.NO_FOLLOW,
            deadline=deadline,
        )

    def _cached(self, api: str) -> UDataRouteSurface | None:
        cached = self._cache.get(api)
        return cached[0] if cached is not None and self._clock() - cached[1] < self._ttl else None

    def _decode(
        self, response: RuntimeResponse, api: str, deadline: DeadlineMonitor | None, generation: int
    ) -> UDataRouteSurface:
        if deadline is not None:
            deadline.assert_dispatchable()
        if not 200 <= response.status_code < 300 or len(response.body) > _MAX_BYTES:
            raise ValueError("The Swagger response is unavailable or oversized.")
        surface = UDataRouteSurface.parse(strict_json(response.body), api)
        if deadline is not None:
            deadline.assert_dispatchable()
        with self._lock:
            if generation == self._generation:
                self._cache[api] = (surface, self._clock())
        return surface

    @staticmethod
    def _unavailable(operation: str, api: str) -> CatalogUnavailableError:
        return CatalogUnavailableError(
            "The uData route surface cannot be verified.",
            operation=operation,
            platform="udata",
            capability_state="unavailable",
            metadata={"route_state": "unavailable", "api_version": api},
            safe_action="Restore anonymous access to the stock Swagger document and refresh the client evidence.",
        )

    def require(
        self,
        method: str,
        path: str,
        operation: str,
        observed_version: str,
        deadline: DeadlineMonitor | None = None,
    ) -> None:
        api = self.api_version(method, path)
        if api is None:
            return
        with self._lock:
            surface = self._cached(api)
            generation = self._generation
        if surface is None:
            try:
                surface = self._decode(
                    cast("CatalogTransport", self._transport).send(self._request(api, deadline)),
                    api,
                    deadline,
                    generation,
                )
            except BudgetExhaustedError:
                raise
            except Exception:
                raise self._unavailable(operation, api) from None
        self._check(surface, method, path, operation, observed_version, api)

    async def require_async(
        self,
        method: str,
        path: str,
        operation: str,
        observed_version: str,
        deadline: DeadlineMonitor | None = None,
    ) -> None:
        api = self.api_version(method, path)
        if api is None:
            return
        async with self._async_lock_for():
            surface = self._cached(api)
            if surface is None:
                generation = self._generation
                try:
                    response = await cast("AsyncCatalogTransport", self._transport).send(self._request(api, deadline))
                    surface = self._decode(response, api, deadline, generation)
                except BudgetExhaustedError:
                    raise
                except Exception:
                    raise self._unavailable(operation, api) from None
        self._check(surface, method, path, operation, observed_version, api)

    @staticmethod
    def _check(surface: UDataRouteSurface, method: str, path: str, operation: str, version: str, api: str) -> None:
        if not surface.allows(method, path):
            raise UnsupportedCapabilityError(
                "The uData deployment does not expose this operation.",
                operation=operation,
                platform="udata",
                capability_state="unsupported",
                metadata={"route_state": "absent", "api_version": api, "method": method, "observed_version": version},
                safe_action="Use an operation present in the deployment Swagger surface.",
            )
