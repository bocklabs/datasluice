import hashlib
import re
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[4] / "dev" / "udata-evidence"


def _requirements(name: str) -> dict[str, str]:
    records = (_ROOT / name).read_text(encoding="utf-8").replace("\\\n", "").splitlines()
    result = {}
    for record in records:
        assert re.fullmatch(r"[a-z0-9-]+==[^\s*]+(?:\s+--hash=sha256:[0-9a-f]{64})+", record)
        package = record.split("==", 1)[0]
        assert package not in result
        result[package] = record
    return result


def test_build_manifest_binds_every_copied_input_to_the_dockerfile() -> None:
    manifest = (_ROOT / "build-inputs.sha256").read_bytes()
    dockerfile = (_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert f"{hashlib.sha256(manifest).hexdigest()}  /opt/udata-build/build-inputs.sha256" in dockerfile
    expected = {
        "build-requirements.in",
        "build-requirements.txt",
        "debian.sources",
        "requirements-auth.txt",
        "requirements.in",
        "requirements.txt",
        "upstream/pyproject.toml",
    }
    entries = dict(line.split("  ", 1)[::-1] for line in manifest.decode().splitlines())
    assert set(entries) == expected
    for name, digest in entries.items():
        assert hashlib.sha256((_ROOT / name).read_bytes()).hexdigest() == digest
        assert name in dockerfile
    assert "sha256sum --check build-inputs.sha256" in dockerfile
    assert "cmp /opt/udata/pyproject.toml /opt/udata-build/upstream/pyproject.toml" in dockerfile


def test_runtime_and_build_locks_cover_upstream_and_compatibility_requirements() -> None:
    runtime = _requirements("requirements.txt")
    build = _requirements("build-requirements.txt")
    upstream = tomllib.loads((_ROOT / "upstream/pyproject.toml").read_text(encoding="utf-8"))
    for requirement in upstream["project"]["dependencies"]:
        name = re.split(r"[\[<>=!]", requirement, maxsplit=1)[0]
        assert name in runtime
    for requirement in upstream["build-system"]["requires"]:
        name = re.split(r"[<>=!]", requirement, maxsplit=1)[0]
        assert name in build
    for requirement in (_ROOT / "requirements.in").read_text(encoding="utf-8").splitlines():
        name = requirement.split("==", 1)[0]
        assert runtime[name].startswith(requirement + " ")
    assert len(runtime) == 109
    assert set(build) == {"packaging", "setuptools", "setuptools-scm", "vcs-versioning", "wheel"}
    assert _requirements("requirements-auth.txt") == {"flask-security-too": runtime["flask-security-too"]}


def test_each_python_install_enforces_the_corresponding_hash_boundary() -> None:
    dockerfile = (_ROOT / "Dockerfile").read_text(encoding="utf-8").replace("\\\n", "")
    installs = re.findall(r"uv pip install\s+([^&]+)", dockerfile)
    assert len(installs) == 3
    assert "--require-hashes" in installs[0]
    assert "--requirements /opt/udata-build/requirements.txt" in installs[0]
    for install in installs[:2]:
        assert "--require-build-hashes" in install
        assert "--build-constraints /opt/udata-build/build-requirements.txt" in install
    assert "--no-deps" in installs[1]
    assert installs[1].strip().endswith("/opt/udata")
    assert "--require-hashes" in installs[2]
    assert "--no-deps" in installs[2]
    assert "--only-binary :all:" in installs[2]
    assert "--requirements /opt/udata-build/requirements-auth.txt" in installs[2]


def test_debian_installs_use_exact_versions_from_signed_immutable_snapshots() -> None:
    dockerfile = (_ROOT / "Dockerfile").read_text(encoding="utf-8").replace("\\\n", "")
    install = re.search(r"apt-get install --yes --no-install-recommends\s+([^&]+)", dockerfile)
    assert install is not None
    pins = dict(package.split("=", 1) for package in install.group(1).split())
    assert pins == {
        "build-essential": "12.9",
        "git": "1:2.39.5-0+deb12u3",
        "libffi-dev": "3.4.4-1",
        "libjpeg62-turbo-dev": "1:2.1.5-2",
        "libxml2-dev": "2.9.14+dfsg-1.3~deb12u6",
        "libxslt1-dev": "1.1.35-1+deb12u4",
    }
    sources = (_ROOT / "debian.sources").read_text(encoding="utf-8")
    uris = re.findall(r"^URIs: (.+)$", sources, flags=re.MULTILINE)
    assert uris == [
        "https://snapshot.debian.org/archive/debian/20260824T000000Z/",
        "https://snapshot.debian.org/archive/debian-security/20260824T000000Z/",
    ]
    assert sources.count("Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg") == 2
    assert "trusted=yes" not in sources
    assert "--allow-unauthenticated" not in dockerfile
    assert dockerfile.index("cp debian.sources /etc/apt/sources.list.d/debian.sources") < dockerfile.index(
        "apt-get update"
    )


@pytest.fixture
def evidence_wheel(tmp_path: Path) -> Path:
    wheel = tmp_path / "evidence_probe-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "evidence_probe-1.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: evidence-probe\nVersion: 1.0\n"
        )
        archive.writestr("evidence_probe-1.0.dist-info/WHEEL", "Wheel-Version: 1.0\nTag: py3-none-any\n")
        archive.writestr("evidence_probe-1.0.dist-info/RECORD", "")
    return wheel


@pytest.mark.parametrize("modified", [False, True])
def test_uv_checks_distribution_bytes_before_installation(tmp_path: Path, evidence_wheel: Path, modified: bool) -> None:
    wheel = evidence_wheel
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(f"evidence-probe @ {wheel.as_uri()} --hash=sha256:{digest}\n", encoding="utf-8")
    if modified:
        with zipfile.ZipFile(wheel, "a") as archive:
            archive.writestr("evidence_probe.py", "changed = True\n")
    result = subprocess.run(
        [
            "uv",
            "--no-config",
            "--offline",
            "pip",
            "install",
            "--python",
            sys.executable,
            "--target",
            str(tmp_path / "target"),
            "--no-deps",
            "--no-cache",
            "--require-hashes",
            "--requirements",
            str(requirements),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if modified:
        assert result.returncode != 0
        assert "Hash mismatch" in result.stderr
        assert not (tmp_path / "target/evidence_probe-1.0.dist-info").exists()
    else:
        assert result.returncode == 0, result.stderr
        assert (tmp_path / "target/evidence_probe-1.0.dist-info").is_dir()


def test_uv_rejects_build_dependencies_without_a_trusted_hash(tmp_path: Path, evidence_wheel: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "pyproject.toml").write_text(
        '[build-system]\nrequires = ["evidence-probe==1.0"]\nbuild-backend = "missing_backend"\n',
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "uv",
            "--no-config",
            "--offline",
            "pip",
            "install",
            "--python",
            sys.executable,
            "--target",
            str(tmp_path / "target"),
            "--no-deps",
            "--no-cache",
            "--no-index",
            "--find-links",
            str(evidence_wheel.parent),
            "--require-build-hashes",
            "--build-constraints",
            str(_ROOT / "build-requirements.txt"),
            str(source),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "no trusted hash was provided for: `evidence-probe`" in result.stderr
    assert not (tmp_path / "target/evidence_probe-1.0.dist-info").exists()
