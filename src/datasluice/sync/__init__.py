"""Incremental sync primitives: state stores, sync loop, idempotent materialize.

Eagerly re-exports the dep-free StateStore implementations (stdlib + fsspec
only) so :mod:`datasluice.sync` imports cleanly on bare installs.
The pyarrow-adjacent :func:`materialize` primitive is
resolved lazily via PEP 562 ``__getattr__`` so the data plane never pulls an
optional dependency at package import time.
"""

__all__ = [
    "FileStateStore",
    "InMemoryStateStore",
    "SyncOutcome",
    "materialize",
    "sync_resources",
]

from datasluice.sync.state_store import FileStateStore, InMemoryStateStore
from datasluice.sync.sync import SyncOutcome, sync_resources


def __getattr__(name: str):
    """Lazily export pyarrow-adjacent sync primitives.

    ``materialize`` remains lazy so importing the package does not load pyarrow.
    """
    if name == "materialize":
        from datasluice.sync.materialize import materialize

        globals()[name] = materialize
        return materialize
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
