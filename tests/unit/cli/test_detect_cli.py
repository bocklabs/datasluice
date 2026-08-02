"""Tests for the ``datasluice detect`` CLI command (D-P5-21, facade-only D-07/APP-08).

Covers:

* a successful detection renders a rich ``Detection Evidence`` table with one
  row per evidence entry and exits 0;
* an undetected portal exits 1 and prints ``No portal detected``;
* ``--type`` override skips detection entirely;
* the command delegates through ``DataSluice.detect`` (facade-only);
* the command uses the ``Annotated`` Typer form (B008-compliance guard);
* source inspection rejects private infrastructure imports.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import datasluice.cli.detect as detect_cmd
from datasluice.cli.app import app
from datasluice.domain.detection import DetectionEvidence, DetectionResult

runner = CliRunner()


def _evidence(n: int = 6) -> list[DetectionEvidence]:
    return [DetectionEvidence(check=f"/probe/{i}", matched=(i == 0), detail=f"detail-{i}") for i in range(n)]


class _FakeFacade:
    def __init__(self, result: DetectionResult) -> None:
        self._result = result
        self.detect_calls: list[str] = []
        self.closed = False

    def __enter__(self) -> _FakeFacade:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.closed = True

    def detect(self, url: str) -> DetectionResult:
        self.detect_calls.append(url)
        return self._result


def _patch_facade(monkeypatch: pytest.MonkeyPatch, facade: _FakeFacade) -> None:
    monkeypatch.setattr(detect_cmd, "DataSluice", lambda: facade)


def test_render_rich_table_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful detection prints the portal type, confidence, and the evidence table."""
    result = DetectionResult(portal_type="ckan", confidence=1.0, evidence=_evidence(6))
    facade = _FakeFacade(result)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["detect", "https://example.gov"])

    assert outcome.exit_code == 0
    assert "ckan" in outcome.stdout
    assert "1.00" in outcome.stdout
    assert "Detection Evidence" in outcome.stdout
    assert "/probe/0" in outcome.stdout
    assert facade.detect_calls == ["https://example.gov"]
    assert facade.closed


def test_undetected_exit_code_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """An undetected portal exits 1 and prints ``No portal detected``."""
    result = DetectionResult(portal_type=None, confidence=0.0, evidence=_evidence(6))
    facade = _FakeFacade(result)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["detect", "https://example.gov"])

    assert outcome.exit_code == 1
    assert "No portal detected" in outcome.stderr


def test_type_override_skips_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """``--type ckan`` must NOT invoke detect() (D-P5-20 bypass)."""
    facade = _FakeFacade(DetectionResult(portal_type="ckan", confidence=1.0, evidence=[]))
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["detect", "https://example.gov", "--type", "ckan"])

    assert outcome.exit_code == 0
    assert facade.detect_calls == []
    assert "Using explicit portal_type" in outcome.stdout


def test_detect_json_output_includes_full_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    """--output json preserves the complete public DetectionResult evidence."""
    import json

    result = DetectionResult(portal_type="ckan", confidence=1.0, evidence=_evidence(3))
    facade = _FakeFacade(result)
    _patch_facade(monkeypatch, facade)

    outcome = runner.invoke(app, ["detect", "https://example.gov", "--output", "json"])

    assert outcome.exit_code == 0, outcome.output
    parsed = json.loads(outcome.stdout)
    assert parsed["portal_type"] == "ckan"
    assert parsed["confidence"] == 1.0
    assert len(parsed["evidence"]) == 3
    assert parsed["evidence"][0]["check"] == "/probe/0"


def test_cli_uses_annotated_typer_form() -> None:
    """B008 guard: ``detect()`` signature uses ``Annotated[...]`` not ``= typer.*`` defaults."""
    source = inspect.getsource(detect_cmd.detect)
    assert source.count("Annotated[") >= 2
    assert "= typer.Argument" not in source
    assert "= typer.Option" not in source


def test_architecture_rejects_private_imports() -> None:
    """P-08-CLI-PRIVATE-BYPASS: detect must not import discovery/transport/session internals."""
    source = Path(detect_cmd.__file__).read_text()
    forbidden = [
        "from datasluice.discovery",
        "from datasluice.transport",
        "from datasluice.runtime",
        "from datasluice.io.downloader",
        "HttpClient",
        "PluginManager",
    ]
    for token in forbidden:
        assert token not in source, f"forbidden import found in detect.py: {token!r}"
