"""Installed-artifact proof for the strict uData tracer slice."""

from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path


def _unpacked_wheel_source(built_wheel: Path, tmp_path: Path) -> Path:
    unpacked = tmp_path / "wheel"
    with zipfile.ZipFile(built_wheel) as archive:
        archive.extractall(unpacked)
    return unpacked


def test_wheel_ships_udata_176_contract_files_and_no_legacy_profile(built_wheel: Path, tmp_path: Path) -> None:
    unpacked = _unpacked_wheel_source(built_wheel, tmp_path)
    package = unpacked / "datasluice" / "connectors" / "catalog" / "udata"
    profiles = unpacked / "datasluice" / "contracts" / "catalog" / "profiles"
    fixtures = unpacked / "datasluice" / "contracts" / "catalog" / "fixtures" / "udata"

    for module in ("settings.py", "clients.py", "probes.py", "mapping.py", "live.py", "factory.py", "connector.py"):
        assert (package / module).is_file(), module
    for module in (
        "models/taxonomies.py",
        "wire/taxonomies.py",
        "services/taxonomies.py",
        "models/root_profile.py",
        "wire/root_profile.py",
        "services/root_profile.py",
        "models/resources.py",
        "wire/resources.py",
        "services/resources.py",
        "models/organizations.py",
        "wire/organizations.py",
        "services/organizations_memberships.py",
        "models/users.py",
        "secrets.py",
        "wire/users.py",
        "services/users_tokens.py",
        "models/oauth.py",
        "wire/oauth.py",
        "services/auth_oauth.py",
        "models/activity_discussions.py",
        "wire/activity_discussions.py",
        "services/activity_discussions.py",
        "models/reuses.py",
        "wire/reuses.py",
        "services/reuses.py",
        "models/posts_reports.py",
        "wire/posts_reports.py",
        "services/posts_reports.py",
        "models/spatial.py",
        "wire/spatial.py",
        "services/spatial.py",
    ):
        assert (package / module).is_file(), module
    assert (profiles / "udata-17.6.json").is_file()
    assert (fixtures / "root_profile.json").is_file()
    assert (fixtures / "cases.json").is_file()
    assert not (profiles / "udata-17.3.json").exists()

    profile = json.loads((profiles / "udata-17.6.json").read_text(encoding="utf-8"))
    assert profile["profile_version"] == "17.6.0"
    assert profile["platform"] == "udata"


def test_wheel_import_proves_the_tracer_path_from_installed_content(built_wheel: Path, tmp_path: Path) -> None:
    """A fresh interpreter importing only the unpacked wheel runs the full tracer."""
    unpacked = _unpacked_wheel_source(built_wheel, tmp_path)
    tracer = Path(__file__).resolve().parent / "udata_wheel_tracer.py"
    completed = subprocess.run(
        [sys.executable, str(tracer), str(unpacked)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=tmp_path,
    )
    assert completed.returncode == 0, completed.stderr
    assert "TRACER_OK" in completed.stdout
