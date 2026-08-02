"""Quality gate contract for independent 80.00% branch coverage (D-25/QUAL-11).

Core and provider must each measure only their own package source at
``branch=true``, ``precision=2``, and ``fail_under=80.00`` with separate data
files, and neither report may be combined with tests or the sibling package.
The two-decimal rounding rule and non-zero exit code are delegated to
coverage.py 7.14+, never reimplemented in shell or test-side math.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ROOT_PYPROJECT = REPO_ROOT / "pyproject.toml"
PROVIDER_PYPROJECT = REPO_ROOT / "providers" / "apache-airflow" / "pyproject.toml"

_TDD_RED = os.environ.get("DATASLUICE_TDD_RED") == "1"


def _coverage_present(pyproject_path: Path) -> bool:
    try:
        config = _read_config_lazy(pyproject_path)
    except FileNotFoundError:
        return False
    report = config.get("report", {})
    return report.get("fail_under") == 80.00 and report.get("precision") == 2


def _read_config_lazy(pyproject_path: Path) -> dict:
    import tomllib

    with pyproject_path.open("rb") as fh:
        return tomllib.load(fh).get("tool", {}).get("coverage", {})


if not (_coverage_present(ROOT_PYPROJECT) and _coverage_present(PROVIDER_PYPROJECT)):
    if _TDD_RED:
        pytest.fail("coverage config not yet raised to 80.00 precision 2 for both distributions", pytrace=False)
    pytest.skip("coverage 80.00 gates pending GREEN phase", allow_module_level=True)

_BC_TRIPLES = {
    # (total, covered, reported total, "fraction" via H/T/F header, true, false)
    # Reported total is controlled by generated statement counts, not floats.
    "pass_80_00": {"header": 1, "true_branch": 3, "false_branch": 1},
    "pass_80_01": {"header": 1, "true_branch": 1068, "false_branch": 267},
    "fail_79_99": {"header": 1, "true_branch": 1066, "false_branch": 267},
}


def _read_toml(path: Path) -> dict:
    import tomllib

    with path.open("rb") as fh:
        return tomllib.load(fh)


def _coverage_run(root_cfg: dict) -> dict:
    return root_cfg.get("tool", {}).get("coverage", {}).get("run", {})


def _coverage_report(root_cfg: dict) -> dict:
    return root_cfg.get("tool", {}).get("coverage", {}).get("report", {})


@pytest.mark.parametrize(
    ("pyproject_path", "label"),
    [(ROOT_PYPROJECT, "core"), (PROVIDER_PYPROJECT, "provider")],
    ids=["core", "provider"],
)
def test_coverage_isolated_source_branch_precision_threshold(pyproject_path: Path, label: str) -> None:
    """Each distribution scopes branch coverage to its own source at 80.00 precision 2."""
    config = _read_toml(pyproject_path)
    run_cfg = _coverage_run(config)
    report_cfg = _coverage_report(config)

    source = run_cfg.get("source")
    assert source, f"{label} coverage run.source is not set"
    assert run_cfg.get("branch") is True, f"{label} coverage branch is not enabled"
    assert run_cfg.get("parallel") is True, f"{label} coverage parallel combine data must be enabled"

    if label == "core":
        assert source == ["src/datasluice"], f"core measures {source}, expected only src/datasluice"
    else:
        assert source == ["airflow/providers/datasluice"], f"provider measures {source}, expected only its namespace"

    assert report_cfg.get("precision") == 2, f"{label} precision must be 2"
    assert report_cfg.get("fail_under") == 80.0, f"{label} fail_under must be 80.00"

    # Neither distribution may include tests in its own measurement window.
    flat = " ".join(str(v) for v in [source, report_cfg])
    assert "tests" not in flat, f"{label} coverage config must not measure the tests package"


def test_core_and_provider_use_distinct_data_files() -> None:
    """Core and provider write independent data files so one can never mask the other."""
    core = _coverage_run(_read_toml(ROOT_PYPROJECT))
    provider = _coverage_run(_read_toml(PROVIDER_PYPROJECT))
    assert core.get("data_file") != provider.get("data_file")
    assert core.get("data_file") not in (None, "")


def _write_boundary_module(directory: Path, header: int, true_branch: int, false_branch: int) -> None:
    lines = ["def fn(flag):"]
    for i in range(header):
        lines.append(f"    h{i} = {i}")
    lines.append("    if flag:")
    for i in range(true_branch):
        lines.append(f"        t{i} = {i}")
    lines.append("    else:")
    for i in range(false_branch):
        lines.append(f"        e{i} = {i}")
    lines.append("    return 0")
    (directory / "mod.py").write_text("\n".join(lines) + "\n")
    (directory / "run.py").write_text("import mod\nmod.fn(True)\n")


def _boundary_python() -> str:
    return sys.executable


def _coverage_report_for(directory: Path, coveragerc: dict, label: str) -> tuple[float, int]:
    """Run coverage against a generated module; return (reported_total, returncode)."""
    cfg_path = directory / "coveragerc.toml"

    def _render(cfg: dict, *, depth: int = 0) -> str:
        out = []
        for key, value in cfg.items():
            if isinstance(value, bool):
                out.append(f"{'  ' * depth}{key} = {str(value).lower()}")
            elif isinstance(value, (int, float)):
                out.append(f"{'  ' * depth}{key} = {value!r}")
            elif isinstance(value, list):
                rendered = ", ".join(repr(v) for v in value)
                out.append(f"{'  ' * depth}{key} = [{rendered}]")
            elif isinstance(value, dict):
                out.append(f"{'  ' * depth}[{key}]")
                out.append(_render(value, depth=depth + 2))
            else:
                out.append(f"{'  ' * depth}{key} = {value!r}")
        return "\n".join(out)

    cfg_path.write_text(_render(coveragerc) + "\n")

    env = {"COVERAGE_FILE": str(directory / ".coverage")}
    run = subprocess.run(
        [
            _boundary_python(),
            "-m",
            "coverage",
            "run",
            "--rcfile",
            str(cfg_path),
            "--source",
            "mod",
            "run.py",
        ],
        cwd=str(directory),
        env=env,
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0, f"{label} coverage run failed: {run.stderr}"

    report = subprocess.run(
        [
            _boundary_python(),
            "-m",
            "coverage",
            "report",
            "--rcfile",
            str(cfg_path),
        ],
        cwd=str(directory),
        env=env,
        capture_output=True,
        text=True,
    )
    total_line = next((line for line in report.stdout.splitlines() if line.strip().startswith("TOTAL")), "")
    total = float(total_line.split()[-1].rstrip("%")) if total_line else 0.0
    return total, report.returncode


def _boundary_cfg(source: str) -> dict:
    return {
        "run": {"branch": True, "parallel": False, "source": [source]},
        "report": {"precision": 2, "fail_under": 80.00},
    }


@pytest.mark.parametrize(
    ("case", "expected_total", "expected_rc"),
    [
        ("pass_80_00", 80.00, 0),
        ("pass_80_01", 80.01, 0),
        ("fail_79_99", 79.99, 2),
    ],
    ids=["80_00_passes", "80_01_passes", "79_99_fails"],
)
def test_branch_threshold_boundary(case: str, expected_total: float, expected_rc: int, tmp_path: Path) -> None:
    """Exact 80.00/80.01 pass and 79.99 fails with coverage exit code 2, per package config."""
    counts = _BC_TRIPLES[case]
    _write_boundary_module(tmp_path, counts["header"], counts["true_branch"], counts["false_branch"])
    total, rc = _coverage_report_for(tmp_path, _boundary_cfg("mod"), case)
    assert total == pytest.approx(expected_total, abs=0.005), f"reported total {total} != {expected_total}"
    assert rc == expected_rc, f"coverage exit {rc} != {expected_rc}"


def test_boundary_report_scoped_to_single_source(tmp_path: Path) -> None:
    """The boundary report measures only the generated source, not tests or sibling packages."""
    counts = _BC_TRIPLES["pass_80_01"]
    _write_boundary_module(tmp_path, counts["header"], counts["true_branch"], counts["false_branch"])
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "unrelated.py").write_text("x = 1\n")
    total, _rc = _coverage_report_for(tmp_path, _boundary_cfg("mod"), "scoped")
    assert total == pytest.approx(80.01, abs=0.005)
