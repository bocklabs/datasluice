"""Shared settlement exception policy for the uData connector layers."""

from __future__ import annotations

import asyncio

SETTLEMENT_ERRORS = (Exception, BaseExceptionGroup, GeneratorExit, KeyboardInterrupt, SystemExit)
ASYNC_SETTLEMENT_ERRORS = (*SETTLEMENT_ERRORS, asyncio.CancelledError)
