"""Incremental sync primitives: state stores, sync loop, idempotent materialize.

Eagerly re-exports the dep-free StateStore implementations (stdlib + fsspec
only) so :mod:`datasluice.sync` imports cleanly on bare installs (D-P7-29).
The pyarrow-adjacent :func:`materialize` primitive (Phase 07 Plan 02) is
resolved lazily via PEP 562 ``__getattr__`` so the data plane never pulls an
optional dependency at package import time.
"""

__all__ = [
    "FileStateStore",
    "InMemoryStateStore",
]

from datasluice.sync.state_store import FileStateStore, InMemoryStateStore


def __getattr__(name: str):  # PEP 562
    """Lazily export pyarrow-adjacent sync primitives.

    ``materialize`` is the lazy branch — it lands in Plan 07-02 and raises an
    actionable AttributeError until it exists (D-P7-29 stub discipline).
    """
    if name == "materialize":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r} (materialize lands in Phase 07 Plan 02)")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
