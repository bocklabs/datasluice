"""Configuration for DataSluice."""

from datasluice.config.defaults import (
    DEFAULT_CACHE_DIR,
    DEFAULT_CACHE_TTL,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RATE_LIMIT,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
)
from datasluice.config.settings import Settings, load_settings

__all__ = [
    "Settings",
    "load_settings",
    "DEFAULT_TIMEOUT",
    "DEFAULT_RETRIES",
    "DEFAULT_RATE_LIMIT",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_CACHE_DIR",
    "DEFAULT_CACHE_TTL",
    "DEFAULT_LOG_LEVEL",
]
