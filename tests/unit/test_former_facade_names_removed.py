"""Architectural guard: former façade class names are permanently removed.

Two complementary scans over the retired per-platform façade identifiers:

* **source-scan** walks every ``.py`` under ``src/datasluice/connectors/`` and
  asserts no file contains any former name substring (catches dead files
  and re-exports alike);
* **import-scan** uses :func:`pkgutil.walk_packages` to enumerate every
  ``datasluice.connectors.*`` submodule and asserts none exposes a former
  name via ``hasattr`` (catches future reintroductions that the source-scan
  would miss if the symbol were dynamically injected).

Either scan alone is insufficient — both must pass. Mirrors the meta-test
pattern in ``test_no_global_state.py`` / ``test_no_dead_settings.py``.
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

import datasluice.connectors

_CONNECTORS_ROOT = Path(datasluice.connectors.__file__).resolve().parent

_FORMER_FACADE_NAMES = ("CKANAdapter", "UDataAdapter", "SocrataAdapter", "CustomAdapter")


def test_no_former_facade_name_in_connectors_source() -> None:
    """No source file under ``src/datasluice/connectors/`` mentions a former name."""
    offenders: list[str] = []
    for py_file in sorted(_CONNECTORS_ROOT.rglob("*.py")):
        if "__pycache__" in py_file.parts:
            continue
        source = py_file.read_text(encoding="utf-8")
        for former in _FORMER_FACADE_NAMES:
            if former in source:
                offenders.append(f"{py_file}: {former}")
    assert not offenders, f"former facade names reintroduced in: {offenders}"


def test_no_former_facade_name_importable_from_connectors() -> None:
    """No ``datasluice.connectors.*`` submodule exposes a former name."""
    exposed: list[str] = []
    for module_info in pkgutil.walk_packages(datasluice.connectors.__path__, "datasluice.connectors."):
        module_name = module_info.name
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        for former in _FORMER_FACADE_NAMES:
            if hasattr(module, former):
                exposed.append(f"{module_name}.{former}")
    assert not exposed, f"former facade names importable from: {exposed}"


def test_custom_subpackage_directory_does_not_exist() -> None:
    """The ``connectors/custom/`` directory is gone."""
    assert not (_CONNECTORS_ROOT / "custom").exists(), "src/datasluice/connectors/custom/ was reintroduced"
