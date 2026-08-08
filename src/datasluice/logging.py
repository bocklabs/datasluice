"""Structured logging utilities for DataSluice.

Owns the canonical ``SENSITIVE_HEADERS`` frozenset (lifted here from
``transport/redirect`` so the :class:`RedactingFilter` and the redirect handler
share one source of truth, D-P3-18, without a circular import —
``redirect.py`` imports it from here).
"""

from __future__ import annotations

import logging
import os
from typing import Any

_logger_name = "datasluice"

SENSITIVE_HEADERS = frozenset({"authorization", "cookie", "x-api-key", "x-auth-token"})

_REDACTED = "***"

_SENSITIVE_KEYS = SENSITIVE_HEADERS | {
    "x_api_key",
    "x_auth_token",
    "api_key",
    "token",
    "secret",
    "password",
}


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger for *name*, defaulting to the package logger.

    Args:
        name: Optional sub-logger name appended to the package logger.

    Returns:
        A configured :class:`logging.Logger` instance.
    """
    if name:
        return logging.getLogger(f"{_logger_name}.{name}")
    return logging.getLogger(_logger_name)


class RedactingFilter(logging.Filter):
    """Redact known sensitive keys from log records (INFRA-06, D-P3-18).

    Walks ``record.__dict__`` and ``record.args`` dicts replacing string values
    whose (lower-cased) key is in ``_SENSITIVE_KEYS`` with ``"***"``. Targeted:
    only known sensitive keys are touched — never value-pattern heuristics — so
    legitimate base64 / open-data payloads pass through unchanged (RESEARCH
    Pitfall 6). Set ``DATASLUICE_NO_REDACT=1`` to disable redaction entirely
    (escape hatch for test fixtures and debugging).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if os.environ.get("DATASLUICE_NO_REDACT") == "1":
            return True
        for key, value in list(record.__dict__.items()):
            if key.lower() in _SENSITIVE_KEYS and isinstance(value, str):
                record.__dict__[key] = _REDACTED
        if record.args:
            record.args = tuple(
                self._redact_mapping(a) if isinstance(a, dict) else a
                for a in (record.args if isinstance(record.args, tuple) else (record.args,))
            )
        return True

    @staticmethod
    def _redact_mapping(mapping: dict) -> dict:
        return {k: (_REDACTED if k.lower() in _SENSITIVE_KEYS else v) for k, v in mapping.items()}


def configure_logging(
    level: int | str = logging.INFO,
    format_string: str | None = None,
    **kwargs: Any,
) -> None:
    """Configure the package-level logger.

    Args:
        level: Logging level (e.g. ``logging.DEBUG`` or ``"DEBUG"``).
        format_string: Optional custom format string.
        **kwargs: Additional keyword arguments passed to
            :class:`logging.Handler`.
    """
    logger = logging.getLogger(_logger_name)
    if logger.handlers:
        return
    logger.setLevel(level)
    handler = logging.StreamHandler(**kwargs)
    handler.setFormatter(logging.Formatter(format_string or "%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
    handler.addFilter(RedactingFilter())
    logger.addHandler(handler)
