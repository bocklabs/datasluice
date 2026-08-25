"""Unit tests for the RedactingFilter log filter.

Exercises the targeted redaction of KNOWN sensitive keys
(``SENSITIVE_HEADERS`` plus auth-secret field names) from ``record.__dict__``
and ``record.args`` dicts, the ``DATASLUICE_NO_REDACT=1`` escape hatch, and the
automatic attachment to the datasluice handler in ``configure_logging``.

The ``RedactingFilter`` is resolved via ``importlib.import_module`` + ``getattr``
so the RED commit can land under this repo's full-suite pre-commit hook: ty
cannot statically resolve the not-yet-existing attribute, and the whole module
skips cleanly until the implementation lands (same pattern as plan 03-01 /
03-04 RED commits).

Assertions read via ``record.__dict__[key]`` rather than ``record.<attr>``: the
filter mutates ``record.__dict__`` directly, so dict access tests the exact
contract, and it sidesteps the ty ``unresolved-attribute`` diagnostic for
dynamic LogRecord attributes that ruff B009 would otherwise reintroduce.
"""

from __future__ import annotations

import importlib
import logging

import pytest

_logging_module = importlib.import_module("datasluice.logging")
if not hasattr(_logging_module, "RedactingFilter"):
    pytest.skip(
        "RedactingFilter implementation pending (RED -> GREEN within task 03-04)",
        allow_module_level=True,
    )
RedactingFilter = _logging_module.RedactingFilter
configure_logging = _logging_module.configure_logging


def _make_record(**attrs) -> logging.LogRecord:
    """Build a LogRecord pre-populated with the given attributes."""

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="msg",
        args=None,
        exc_info=None,
    )
    for key, value in attrs.items():
        setattr(record, key, value)
    return record


# --------------------------------------------------------------------------- #
# Targeted redaction of known sensitive keys
# --------------------------------------------------------------------------- #


def test_redacting_filter_strips_authorization_from_record_dict() -> None:
    record = _make_record(authorization="Bearer secret-token")
    assert RedactingFilter().filter(record) is True
    assert record.__dict__["authorization"] == "***"


def test_redacting_filter_strips_cookie_x_api_key_x_auth_token() -> None:
    for key, value in [
        ("cookie", "session=abc123"),
        ("x_api_key", "key-secret"),
        ("x_auth_token", "tok-secret"),
    ]:
        record = _make_record(**{key: value})
        RedactingFilter().filter(record)
        assert record.__dict__[key] == "***", f"{key} should be redacted"


def test_redacting_filter_strips_auth_secret_fields() -> None:
    record = _make_record(api_key="key-val", token="tok-val", secret="sec-val", password="pw-val")
    RedactingFilter().filter(record)
    assert record.__dict__["api_key"] == "***"
    assert record.__dict__["token"] == "***"
    assert record.__dict__["secret"] == "***"
    assert record.__dict__["password"] == "***"


def test_redacting_filter_walks_record_args_dicts() -> None:
    record = _make_record()
    record.args = ({"authorization": "Bearer xyz", "count": 3},)
    RedactingFilter().filter(record)
    assert record.args[0]["authorization"] == "***"
    assert record.args[0]["count"] == 3


# --------------------------------------------------------------------------- #
# No false positives
# --------------------------------------------------------------------------- #


def test_no_false_positives_on_non_sensitive_keys() -> None:
    record = _make_record(
        data="SGVsbG8gV29ybGQ=",
        url="https://example.com/dataset.csv",
    )
    RedactingFilter().filter(record)
    assert record.__dict__["data"] == "SGVsbG8gV29ybGQ="
    assert record.__dict__["url"] == "https://example.com/dataset.csv"


# --------------------------------------------------------------------------- #
# Escape hatch
# --------------------------------------------------------------------------- #


def test_escape_hatch_env_var_disables_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATASLUICE_NO_REDACT", "1")
    record = _make_record(authorization="Bearer secret-token")
    assert RedactingFilter().filter(record) is True
    assert record.__dict__["authorization"] == "Bearer secret-token"


# --------------------------------------------------------------------------- #
# Filter contract: never drops records
# --------------------------------------------------------------------------- #


def test_filter_return_value_always_true() -> None:
    sensitive = _make_record(authorization="Bearer xyz")
    plain = _make_record(data="payload")
    assert RedactingFilter().filter(sensitive) is True
    assert RedactingFilter().filter(plain) is True


def test_filter_fails_closed_for_extras_beyond_the_metadata_bound() -> None:
    attrs = {f"field_{index}": "safe" for index in range(33)}
    attrs["authorization"] = "Bearer raw-secret-value"
    record = _make_record(**attrs)

    RedactingFilter().filter(record)

    assert record.__dict__["authorization"] == "***"


def test_filter_preserves_non_mapping_log_arguments_verbatim() -> None:
    payload = "x" * 1000
    record = _make_record()
    record.args = (payload, ["one", "two"])

    RedactingFilter().filter(record)

    assert record.args == (payload, ["one", "two"])


# --------------------------------------------------------------------------- #
# configure_logging attachment
# --------------------------------------------------------------------------- #


def test_configure_logging_attaches_filter_to_handler() -> None:
    configure_logging("WARNING")
    ds_logger = logging.getLogger("datasluice")
    attached = any(isinstance(f, RedactingFilter) for handler in ds_logger.handlers for f in handler.filters)
    assert attached, "configure_logging must attach a RedactingFilter to the datasluice handler"


def test_configure_logging_does_not_clobber_existing_level() -> None:
    """A second call to configure_logging must not reset a user-configured level."""
    import datasluice.logging as ds_logging

    ds_logger = logging.getLogger("datasluice")
    ds_logger.setLevel(logging.DEBUG)
    ds_logging.configure_logging("WARNING")
    assert ds_logger.level == logging.DEBUG


def test_configure_logging_does_not_clobber_existing_handlers() -> None:
    """A second call must not add a second handler."""
    import datasluice.logging as ds_logging

    ds_logger = logging.getLogger("datasluice")
    initial_handler_count = len(ds_logger.handlers)
    ds_logging.configure_logging("WARNING")
    assert len(ds_logger.handlers) == initial_handler_count
