"""Central redaction gate for runtime output surfaces."""

from __future__ import annotations

import os
from collections.abc import Mapping

from datasluice.domain.catalog.redaction import redact_mapping, redact_value

_NO_REDACT_ENV_VAR = "DATASLUICE_NO_REDACT"


def redact_for_output(value: object) -> object:
    """Return a safe bounded value for an output surface."""
    if os.environ.get(_NO_REDACT_ENV_VAR) == "1":
        return value
    return redact_value(value)


def redact_event_metadata(value: Mapping[str, object]) -> Mapping[str, object]:
    """Return safe bounded event metadata for an output surface."""
    if os.environ.get(_NO_REDACT_ENV_VAR) == "1":
        return value
    return redact_mapping(value)
