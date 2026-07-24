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

__all__ = [
    "DEFAULT_TIMEOUT",
    "DEFAULT_RETRIES",
    "DEFAULT_RATE_LIMIT",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_CACHE_DIR",
    "DEFAULT_CACHE_TTL",
    "DEFAULT_LOG_LEVEL",
]
