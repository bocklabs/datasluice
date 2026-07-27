"""Tests for the ``datasluice detect`` CLI command (D-P5-21).

Covers four behaviours required by Plan 05-03 Task 2:

* a successful detection renders a rich ``Detection Evidence`` table with one
  row per evidence entry and exits 0;
* an undetected portal exits 1 and prints ``No portal detected``;
* ``--type`` override skips detection entirely;
* the command uses the ``Annotated`` Typer form (B008-compliance guard).
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest
from typer.testing import CliRunner

import datasluice.cli.detect as detect_cmd
from datasluice.cli.app import app
from datasluice.domain.detection import DetectionEvidence, DetectionResult

runner = CliRunner()


def _evidence(n: int = 6) -> list[DetectionEvidence]:
    return [DetectionEvidence(check=f"/probe/{i}", matched=(i == 0), detail=f"detail-{i}") for i in range(n)]


def _patch_detect(monkeypatch: pytest.MonkeyPatch, result: DetectionResult) -> None:
    """Patch ``datasluice.discovery.detect`` so the CLI's lazy local import picks it up."""

    def _fake_detect(url: str, *, transport: Any, plugin_manager: Any) -> DetectionResult:
        return result

    monkeypatch.setattr("datasluice.discovery.detect", _fake_detect)


def test_render_rich_table_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful detection prints the portal type, confidence, and the evidence table."""
    result = DetectionResult(portal_type="ckan", confidence=1.0, evidence=_evidence(6))
    _patch_detect(monkeypatch, result)
    outcome = runner.invoke(app, ["detect", "https://example.gov"])
    assert outcome.exit_code == 0
    assert "ckan" in outcome.stdout
    assert "1.00" in outcome.stdout
    assert "Detection Evidence" in outcome.stdout
    assert "/probe/0" in outcome.stdout


def test_undetected_exit_code_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """An undetected portal exits 1 and prints ``No portal detected``."""
    result = DetectionResult(portal_type=None, confidence=0.0, evidence=_evidence(6))
    _patch_detect(monkeypatch, result)
    outcome = runner.invoke(app, ["detect", "https://example.gov"])
    assert outcome.exit_code == 1
    assert "No portal detected" in outcome.stdout


def test_type_override_skips_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """``--type ckan`` must NOT invoke ``detect()`` (D-P5-20 bypass)."""
    called = {"n": 0}

    def _explode(*args: Any, **kwargs: Any) -> DetectionResult:
        called["n"] += 1
        raise AssertionError("do_detect was called despite --type override")

    monkeypatch.setattr("datasluice.discovery.detect", _explode)
    outcome = runner.invoke(app, ["detect", "https://example.gov", "--type", "ckan"])
    assert outcome.exit_code == 0
    assert called["n"] == 0
    assert "Using explicit portal_type" in outcome.stdout


def test_cli_uses_annotated_typer_form() -> None:
    """B008 guard: ``detect()`` signature uses ``Annotated[...]`` not ``= typer.*`` defaults."""
    source = inspect.getsource(detect_cmd.detect)
    assert source.count("Annotated[") >= 2
    assert "= typer.Argument" not in source
    assert "= typer.Option" not in source
