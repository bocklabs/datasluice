"""API-shape tests — ``detect_portal_type`` / ``detect_portal`` removed.

 is a one-way, USER-LOCKED decision: the legacy first-match functions
are gone and importing them raises ``ImportError``. Per PATTERNS Finding 2
(import-cycle lockstep), ``discovery/__init__.py`` no longer re-exports them.
"""

from __future__ import annotations

import pytest


def test_detect_portal_type_removed_from_detector() -> None:
    """``detect_portal_type`` is gone from ``datasluice.discovery.detector``."""

    with pytest.raises(ImportError):
        from datasluice.discovery.detector import detect_portal_type  # ty: ignore[unresolved-import]  # noqa: F401


def test_detect_portal_removed_from_detector() -> None:
    """``detect_portal`` alias is gone from ``datasluice.discovery.detector``."""

    with pytest.raises(ImportError):
        from datasluice.discovery.detector import detect_portal  # ty: ignore[unresolved-import]  # noqa: F401


def test_detect_portal_type_not_reexported_from_discovery() -> None:
    """``datasluice.discovery`` no longer re-exports ``detect_portal_type`` (lockstep)."""

    with pytest.raises(ImportError):
        from datasluice.discovery import detect_portal_type  # ty: ignore[unresolved-import]  # noqa: F401


def test_detect_portal_not_reexported_from_discovery() -> None:
    """``datasluice.discovery`` no longer re-exports ``detect_portal`` (lockstep)."""

    with pytest.raises(ImportError):
        from datasluice.discovery import detect_portal  # ty: ignore[unresolved-import]  # noqa: F401


def test_detect_is_reexported_from_discovery() -> None:
    """``datasluice.discovery`` re-exports the new ``detect`` sole public API."""

    from datasluice.discovery import detect
    from datasluice.discovery.detector import detect as detector_detect

    assert detect is detector_detect
