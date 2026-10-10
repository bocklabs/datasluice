"""Contract guards for the installed-wheel probe CLI entry point."""

from __future__ import annotations

from tests.e2e import installed_probe


def test_probe_reports_usage_without_a_mode(capsys) -> None:
    """A bare invocation reports usage instead of raising IndexError at argv[1]."""
    assert installed_probe.main(["installed_probe.py"]) == 2
    assert "usage:" in capsys.readouterr().out


def test_live_gate_error_requires_a_platform(capsys) -> None:
    """``live-gate-error`` without a platform reports usage instead of IndexError."""
    assert installed_probe.main(["installed_probe.py", "live-gate-error"]) == 2
    assert "usage:" in capsys.readouterr().out


def test_probe_reports_an_unknown_mode(capsys) -> None:
    """An unregistered probe name still reports the handled usage error."""
    assert installed_probe.main(["installed_probe.py", "not-a-probe"]) == 2
    assert "unknown probe" in capsys.readouterr().out
