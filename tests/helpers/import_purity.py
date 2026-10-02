from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROBE = Path(__file__).resolve().parent / "import_probe.py"
_PROBE_TIMEOUT_SECONDS = 60


def _run_probe(targets: tuple[str, ...], forbidden: tuple[str, ...], mode: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(_REPO_ROOT / "src")
    return subprocess.run(
        [sys.executable, str(_PROBE), ",".join(targets), ",".join(forbidden), mode],
        capture_output=True,
        text=True,
        env=env,
        timeout=_PROBE_TIMEOUT_SECONDS,
    )


def assert_import_pulls_no_distributions(targets: str | tuple[str, ...], forbidden: tuple[str, ...]) -> None:
    """Assert importing ``targets`` loads no importable distribution named in ``forbidden``.

    Matches on the distribution root, so ``pyarrow`` also covers ``pyarrow.parquet``.
    """
    resolved = (targets,) if isinstance(targets, str) else targets
    result = _run_probe(resolved, forbidden, "distribution")
    assert result.returncode == 0, f"{result.stdout}{result.stderr}"


def assert_import_pulls_no_modules(targets: str | tuple[str, ...], forbidden: tuple[str, ...]) -> None:
    """Assert importing ``targets`` loads no module equal to or under ``forbidden``.

    Matches on the full module path, so naming a package also covers every one
    of its submodules.
    """
    resolved = (targets,) if isinstance(targets, str) else targets
    result = _run_probe(resolved, forbidden, "module")
    assert result.returncode == 0, f"{result.stdout}{result.stderr}"
