from __future__ import annotations

import subprocess
import sys

from tests.helpers import import_probe, import_purity


def test_probe_subprocess_runs_under_a_bounded_timeout(monkeypatch) -> None:
    """A blocking import must fail the guard instead of wedging the pytest worker."""
    captured: dict[str, object] = {}
    real_run = subprocess.run

    def recording_run(command, **kwargs):
        captured.update(kwargs)
        return real_run(command, **kwargs)

    monkeypatch.setattr(import_purity.subprocess, "run", recording_run)

    import_purity.assert_import_pulls_no_distributions("json", ("pyarrow",))

    assert captured["timeout"] == import_purity._PROBE_TIMEOUT_SECONDS


def test_probe_rejects_missing_arguments_with_a_usage_error(capsys) -> None:
    """Too few arguments report usage instead of raising a bare IndexError."""
    assert import_probe.main(["import_probe.py"]) == 2
    assert "usage:" in capsys.readouterr().out


def test_probe_rejects_an_unknown_mode(capsys) -> None:
    """An unrecognized mode is a usage error, not a silent no-op pass."""
    assert import_probe.main(["import_probe.py", "json", "pyarrow", "banana"]) == 2
    assert "usage:" in capsys.readouterr().out


def test_probe_rejects_empty_targets(capsys) -> None:
    """An empty target list is a usage error rather than ``import_module("")``."""
    assert import_probe.main(["import_probe.py", ",", "pyarrow", "module"]) == 2
    assert "usage:" in capsys.readouterr().out


def test_probe_rejects_empty_forbidden(capsys) -> None:
    """An empty forbidden list is a usage error rather than a vacuous pass."""
    assert import_probe.main(["import_probe.py", "json", ",", "module"]) == 2
    assert "usage:" in capsys.readouterr().out


def test_probe_accepts_a_trailing_comma_in_targets() -> None:
    """Targets are filtered symmetrically with forbidden entries."""
    result = subprocess.run(
        [sys.executable, str(import_purity._PROBE), "json,", "pyarrow", "module"],
        capture_output=True,
        text=True,
        timeout=import_purity._PROBE_TIMEOUT_SECONDS,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_probe_reports_a_forbidden_distribution() -> None:
    """A genuinely forbidden distribution still fails the probe."""
    result = import_purity._run_probe(("typer",), ("typer",), "distribution")
    assert result.returncode == 1
    assert "pulled forbidden modules" in result.stdout
