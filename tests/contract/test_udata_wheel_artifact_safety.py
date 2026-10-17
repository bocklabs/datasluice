from __future__ import annotations

import zipfile
from typing import TYPE_CHECKING

import pytest

from scripts import check_udata_phase_gate as gate

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("member", "source", "allowed"),
    [
        ("datasluice/connectors/catalog/udata/clients.py", '_CONTROLLED_ORIGIN = "http://127.0.0.1:5640"', True),
        ("datasluice/connectors/catalog/udata/clients.py", '_CONTROLLED_ORIGIN = "http://10.0.0.1:5640"', False),
        ("datasluice/connectors/catalog/udata/clients.py", 'DEPLOYMENT = "http://127.0.0.1:5640"', False),
        ("other.py", '_CONTROLLED_ORIGIN = "http://127.0.0.1:5640"', False),
        (
            "datasluice/connectors/catalog/udata/wire/oauth.py",
            'OPS = {"access_token": "udata/oauth.access-token"}',
            True,
        ),
        ("datasluice/connectors/catalog/udata/wire/oauth.py", 'PATHS = {"access_token": "/oauth/token"}', True),
        ("datasluice/connectors/catalog/udata/wire/oauth.py", 'DATA = {"access_token": "actual-secret-value"}', False),
        ("datasluice/contracts/catalog/fakes.py", 'DATA = {"access_token": "never-report-this"}', True),
        ("datasluice/contracts/catalog/fakes.py", 'DATA = {"access_token": "actual-secret-value"}', False),
    ],
)
def test_wheel_scan_distinguishes_the_fixed_safety_boundary_from_retained_origins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, member: str, source: str, allowed: bool
) -> None:
    gate._review_gate()
    phase = tmp_path / "phase"
    reviews = tmp_path / "reviews"
    for directory in (phase / "oracle", reviews, tmp_path / "src/datasluice/contracts/catalog/fixtures/udata"):
        directory.mkdir(parents=True)
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    monkeypatch.setattr(gate, "PHASE_DIR", phase)
    monkeypatch.setattr(gate, "REVIEWS_ROOT", reviews)
    wheel = tmp_path / "candidate.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(member, source + "\n")
    if allowed:
        gate.require_artifact_safety(wheel)
    else:
        with pytest.raises(gate.GateError, match="private origin|sensitive content"):
            gate.require_artifact_safety(wheel)
