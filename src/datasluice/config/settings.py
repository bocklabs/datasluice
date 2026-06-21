"""Runtime settings loaded from environment variables and defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from datasluice.config.defaults import (
    DEFAULT_CACHE_DIR,
    DEFAULT_CACHE_TTL,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RATE_LIMIT,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
)
from datasluice.exceptions import ConfigError


def _get_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ConfigError(f"Invalid float for {key!r}: {raw!r}") from None


def _get_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"Invalid integer for {key!r}: {raw!r}") from None


@dataclass
class Settings:
    """Resolved configuration for a DataSluice client session.

    Values are populated from environment variables with sensible defaults
    from :mod:`datasluice.config.defaults`.
    """

    http_timeout: float = field(default_factory=lambda: _get_float("DATASLUICE_HTTP_TIMEOUT", DEFAULT_TIMEOUT))
    http_retries: int = field(default_factory=lambda: _get_int("DATASLUICE_HTTP_RETRIES", DEFAULT_RETRIES))
    rate_limit: float = field(default_factory=lambda: _get_float("DATASLUICE_HTTP_RATE_LIMIT", DEFAULT_RATE_LIMIT))
    page_size: int = field(default_factory=lambda: _get_int("DATASLUICE_PAGE_SIZE", DEFAULT_PAGE_SIZE))
    cache_dir: str = field(default_factory=lambda: os.environ.get("DATASLUICE_CACHE_DIR", DEFAULT_CACHE_DIR))
    cache_ttl: int = field(default_factory=lambda: _get_int("DATASLUICE_CACHE_TTL", DEFAULT_CACHE_TTL))
    log_level: str = field(default_factory=lambda: os.environ.get("DATASLUICE_LOG_LEVEL", DEFAULT_LOG_LEVEL))
    api_key: str | None = field(default_factory=lambda: os.environ.get("DATASLUICE_API_KEY"))
    bearer_token: str | None = field(default_factory=lambda: os.environ.get("DATASLUICE_BEARER_TOKEN"))
    user_agent: str | None = field(default_factory=lambda: os.environ.get("DATASLUICE_USER_AGENT"))


def load_settings() -> Settings:
    """Load settings from the environment."""
    return Settings()
