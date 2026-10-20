from __future__ import annotations

import importlib
from operator import attrgetter

import pytest


def test_detect_portal_type_removed_from_detector() -> None:
    """``detect_portal_type`` is gone from ``datasluice.discovery.detector``."""
    get_attribute = attrgetter("detect_portal_type")
    module = importlib.import_module("datasluice.discovery.detector")
    with pytest.raises(AttributeError):
        _ = get_attribute(module)


def test_detect_portal_removed_from_detector() -> None:
    """``detect_portal`` alias is gone from ``datasluice.discovery.detector``."""
    get_attribute = attrgetter("detect_portal")
    module = importlib.import_module("datasluice.discovery.detector")
    with pytest.raises(AttributeError):
        _ = get_attribute(module)


def test_detect_portal_type_not_reexported_from_discovery() -> None:
    """``datasluice.discovery`` no longer re-exports ``detect_portal_type`` (lockstep)."""
    get_attribute = attrgetter("detect_portal_type")
    module = importlib.import_module("datasluice.discovery")
    with pytest.raises(AttributeError):
        _ = get_attribute(module)


def test_detect_portal_not_reexported_from_discovery() -> None:
    """``datasluice.discovery`` no longer re-exports ``detect_portal`` (lockstep)."""
    get_attribute = attrgetter("detect_portal")
    module = importlib.import_module("datasluice.discovery")
    with pytest.raises(AttributeError):
        _ = get_attribute(module)


def test_detect_is_reexported_from_discovery() -> None:
    """``datasluice.discovery`` re-exports the new ``detect`` sole public API."""
    from datasluice.discovery import detect
    from datasluice.discovery.detector import detect as detector_detect

    assert detect is detector_detect
