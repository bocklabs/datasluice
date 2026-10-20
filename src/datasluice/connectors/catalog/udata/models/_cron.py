"""Shared crontab field policy for the harvest and worker-job schedule surfaces."""

from __future__ import annotations

import re

CRON_FIELD = re.compile(r"^[0-9*,/\-]+$")
CRON_WEEKDAY_FIELD = re.compile(
    r"^(?:\*|\d+|sun|mon|tue|wed|thu|fri|sat)(?:-(?:\*|\d+|sun|mon|tue|wed|thu|fri|sat))?(?:/\d+)?"
    r"(?:,(?:\*|\d+|sun|mon|tue|wed|thu|fri|sat)(?:-(?:\*|\d+|sun|mon|tue|wed|thu|fri|sat))?(?:/\d+)?)*$",
    re.IGNORECASE,
)
MAX_CRON_FIELD = 32
