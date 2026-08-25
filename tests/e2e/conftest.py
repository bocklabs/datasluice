"""Shared installed-wheel fixtures for end-to-end tests."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture(scope="session")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the project wheel once for every isolated end-to-end environment."""
    wheel_dir = tmp_path_factory.mktemp("wheels")
    build = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(wheel_dir)],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=180,
    )
    if build.returncode != 0:
        pytest.fail(f"uv build failed: {build.stderr[:300]}", pytrace=False)
    wheels = list(wheel_dir.glob("datasluice-*.whl"))
    assert len(wheels) == 1, f"expected one wheel, found {wheels}"
    return wheels[0]
