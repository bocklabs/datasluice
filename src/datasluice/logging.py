"""Structured logging utilities for DataSluice.

Owns the canonical ``SENSITIVE_HEADERS`` frozenset shared by the
:class:`RedactingFilter` and the redirect handler without a circular
import — the redirect handler imports it from here.
"""

from __future__ import annotations

import logging
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
        from datasluice.runtime.redaction import redact_event_metadata, redact_for_output

        extras = {key: value for key, value in record.__dict__.items() if key not in _STANDARD_RECORD_ATTRS}
        if extras:
            try:
                record.__dict__.update(redact_event_metadata(extras))
            except Exception:
                pass
        if record.args:
            args = record.args if isinstance(record.args, tuple) else (record.args,)
            try:
                record.args = tuple(redact_for_output(arg) for arg in args)
            except Exception:
                pass
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
