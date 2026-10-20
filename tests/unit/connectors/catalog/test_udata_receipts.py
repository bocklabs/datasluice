from __future__ import annotations

import asyncio

import pytest

from datasluice.connectors.catalog.udata.services._receipts import _mutation_outcome


@pytest.mark.parametrize("interrupt", [asyncio.CancelledError, KeyboardInterrupt, GeneratorExit])
def test_every_abandonment_signal_settles_as_cancelled(interrupt: type[BaseException]) -> None:
    assert _mutation_outcome(interrupt()) == "cancelled"
    assert _mutation_outcome(interrupt(), None, dispatched=False) == "cancelled"


def test_system_exit_is_not_an_abandonment_signal() -> None:
    assert _mutation_outcome(SystemExit()) == "failed"
