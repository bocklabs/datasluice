from __future__ import annotations

import pathlib

_CONNECTORS_ROOT = pathlib.Path(__file__).resolve().parents[2].parent / "src" / "datasluice" / "connectors"


def test_connectors_init_has_no_side_effect_registration() -> None:
    init_path = _CONNECTORS_ROOT / "__init__.py"
    source = init_path.read_text()
    assert "registry" not in source
    assert "AdapterRegistry" not in source
    assert "register(" not in source
    assert "import" not in source


def test_no_module_level_singleton_in_connectors_package() -> None:
    for py_file in sorted(_CONNECTORS_ROOT.rglob("*.py")):
        source = py_file.read_text()
        assert "registry = AdapterRegistry" not in source, f"{py_file} reintroduces AdapterRegistry singleton"
        assert "registry.register(" not in source, f"{py_file} reintroduces side-effect registration"
        assert "AdapterRegistry()" not in source, f"{py_file} instantiates an AdapterRegistry"
