"""Architectural guard: ``CustomAdapter`` is permanently removed.

Two complementary scans:

* **source-scan** walks every ``.py`` under ``src/datasluice/connectors/`` and
  asserts no file contains the substring ``CustomAdapter`` (catches dead files
  and re-exports alike);
* **import-scan** uses :func:`pkgutil.walk_packages` to enumerate every
  ``datasluice.connectors.*`` submodule and asserts none exposes
  ``CustomAdapter`` via ``hasattr`` (catches future reintroductions that the
  source-scan would miss if the symbol were dynamically injected).

Either scan alone is insufficient — both must pass. Mirrors the meta-test
pattern in ``test_no_global_state.py`` / ``test_no_dead_settings.py``.
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

import datasluice.connectors

_CONNECTORS_ROOT = Path(datasluice.connectors.__file__).resolve().parent


def test_no_customadapter_in_connectors_source() -> None:
    """No source file under ``src/datasluice/connectors/`` mentions ``CustomAdapter``."""
    offenders: list[str] = []
    for py_file in sorted(_CONNECTORS_ROOT.rglob("*.py")):
        if "__pycache__" in py_file.parts:
            continue
        if "CustomAdapter" in py_file.read_text(encoding="utf-8"):
            offenders.append(str(py_file))
    assert not offenders, f"CustomAdapter reintroduced in: {offenders}"


def test_no_customadapter_importable_from_connectors() -> None:
    """No ``datasluice.connectors.*`` submodule exposes ``CustomAdapter``."""
    exposed: list[str] = []
    for module_info in pkgutil.walk_packages(datasluice.connectors.__path__, "datasluice.connectors."):
        module_name = module_info.name
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if hasattr(module, "CustomAdapter"):
            exposed.append(module_name)
    assert not exposed, f"CustomAdapter importable from: {exposed}"


def test_custom_subpackage_directory_does_not_exist() -> None:
    """The ``connectors/custom/`` directory is gone."""
    assert not (_CONNECTORS_ROOT / "custom").exists(), "src/datasluice/connectors/custom/ was reintroduced"
