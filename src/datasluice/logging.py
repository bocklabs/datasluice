"""Structured logging utilities for DataSluice.

Owns the canonical ``SENSITIVE_HEADERS`` frozenset shared by the
:class:`RedactingFilter` and the redirect handler without a circular
import — the redirect handler imports it from here.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import Any

from datasluice.domain.catalog.redaction import REDACTED, redact_mapping

_logger_name = "datasluice"

SENSITIVE_HEADERS = frozenset({"authorization", "cookie", "x-api-key", "x-auth-token"})

_STANDARD_RECORD_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


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
    """Redact sensitive extras and args without touching LogRecord internals.

    Only caller-supplied ``extra`` keys and ``record.args`` entries pass
    through the central runtime gate. Standard :class:`~logging.LogRecord`
    internals (``exc_info``, ``exc_text``, ``msg``, ``pathname``,
    ``stack_info``, …) stay untouched so traceback formatting and message
    rendering keep working. Targeted: only known sensitive keys are touched —
    never value-pattern heuristics — so legitimate base64 / open-data payloads
    pass through unchanged. The central runtime gate owns the escape hatch for
    test fixtures and debugging.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if os.environ.get("DATASLUICE_NO_REDACT") == "1":
            return True

        extras = {key: value for key, value in record.__dict__.items() if key not in _STANDARD_RECORD_ATTRS}
        if extras:
            try:
                safe_extras = redact_mapping(extras)
            except Exception:
                safe_extras = {}
            for key in extras:
                record.__dict__[key] = safe_extras.get(key, REDACTED)
        if record.args:
            args = record.args if isinstance(record.args, tuple) else (record.args,)
            try:
                record.args = tuple(redact_mapping(arg) if isinstance(arg, Mapping) else arg for arg in args)
            except Exception:
                record.args = tuple(REDACTED if isinstance(arg, Mapping) else arg for arg in args)
        return True


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
