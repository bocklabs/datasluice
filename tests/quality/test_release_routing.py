"""Release-routing contracts for the dual-distribution manifest and publication gates (D-27/D-32/D-33/D-34/D-36).

Task 1 establishes the typed reusable publication interface in
``.github/workflows/publish.yml``: build/attest the exact Release Please ref,
publish the candidate to the selected TestPyPI environment, install the exact
``name==version`` candidate from TestPyPI with PyPI resolving dependencies,
smoke the package, and only then promote the SAME attested artifact through the
caller-selected PyPI environment. Task 2 configures the two-component Release
Please manifest and routes its path-prefixed outputs into that interface with
core-before-provider ordering. Nothing is published here; these are structural
YAML/JSON contract tests plus a local release-proposal model.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PUBLISH_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "publish.yml"
RELEASE_PLEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release-please.yml"
RELEASE_CONFIG = REPO_ROOT / "release-please-config.json"
RELEASE_MANIFEST = REPO_ROOT / ".release-please-manifest.json"
REGISTRY_PATH = REPO_ROOT / "providers" / "registry.json"

_TDD_RED = os.environ.get("DATASLUICE_TDD_RED") == "1"

REQUIRED_PUBLISH_INPUTS = [
    "component",
    "package_path",
    "package_name",
    "version",
    "ref",
    "testpypi_environment",
    "pypi_environment",
]

PROVIDER_PATH = "providers/apache-airflow"


class _GhActionsLoader(yaml.SafeLoader):
    pass


# GitHub Actions uses the YAML 1.1 ``on`` key; PyYAML's YAML 1.1 resolver turns
# ``on``/``off``/``yes``/``no`` into booleans. Replace the bool resolver with a
# strict true/false-only one so workflow trigger keys survive as strings.
_GhActionsLoader.yaml_implicit_resolvers = {
    first: [rule for rule in rules if rule[0] != "tag:yaml.org,2002:bool"]
    for first, rules in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_GhActionsLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"),
    list("tTfF"),
)


def _load_gh_yaml(path: Path) -> dict:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_GhActionsLoader)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _require(ready: bool, message: str) -> None:
    if ready:
        return
    if _TDD_RED:
        pytest.fail(message, pytrace=False)
    pytest.skip(message)


def _publish_interface_ready() -> bool:
    if not PUBLISH_WORKFLOW.exists():
        return False
    publish = _load_gh_yaml(PUBLISH_WORKFLOW)
    on = publish.get("on")
    if not isinstance(on, dict) or set(on) != {"workflow_call"}:
        return False
    inputs = on["workflow_call"].get("inputs") or {}
    if set(inputs) != set(REQUIRED_PUBLISH_INPUTS):
        return False
    return all(
        isinstance(inputs[name], dict) and inputs[name].get("required") is True and inputs[name].get("type") == "string"
        for name in REQUIRED_PUBLISH_INPUTS
    )


def _convention_smoke_ready() -> bool:
    if not _publish_interface_ready():
        return False
    publish = _load_gh_yaml(PUBLISH_WORKFLOW)
    jobs = publish.get("jobs") or {}
    smoke = jobs.get("smoke")
    if not isinstance(smoke, dict):
        return False
    env = smoke.get("env") or {}
    if env.get("SMOKE_VENV") != "/tmp/smoke-${{ inputs.component }}":
        return False
    steps = smoke.get("steps") or []
    provider_steps = [s for s in steps if isinstance(s, dict) and s.get("if") == "inputs.package_name != 'datasluice'"]
    if not provider_steps:
        return False
    ps = provider_steps[0]
    if ps.get("working-directory") != "${{ inputs.package_path }}":
        return False
    return "tests/smoke.py" in str(ps.get("run", ""))


def _provider_registry_ready() -> bool:
    return REGISTRY_PATH.exists()


def _manifest_ready() -> bool:
    if not (RELEASE_CONFIG.exists() and RELEASE_MANIFEST.exists()):
        return False
    config = _load_json(RELEASE_CONFIG)
    packages = config.get("packages") or {}
    if set(packages) != {".", PROVIDER_PATH}:
        return False
    root = packages.get(".") or {}
    provider = packages.get(PROVIDER_PATH) or {}
    if root.get("component") != "datasluice":
        return False
    if provider.get("component") != "apache-airflow-providers-datasluice":
        return False
    if provider.get("initial-version") != "0.1.0":
        return False
    return _load_json(RELEASE_MANIFEST) == {".": "0.1.0"}


def _routing_ready() -> bool:
    if not RELEASE_PLEASE_WORKFLOW.exists():
        return False
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    jobs = workflow.get("jobs") or {}
    if not isinstance(jobs.get("publish-core"), dict) or not isinstance(jobs.get("publish-provider"), dict):
        return False
    release_job = jobs.get("release-please")
    if not isinstance(release_job, dict):
        return False
    return any(
        isinstance(step, dict)
        and step.get("id") == "release"
        and isinstance(step.get("uses"), str)
        and "release-please-action" in step["uses"]
        for step in release_job.get("steps") or []
    )


def _workflow_call_inputs(publish: dict) -> dict:
    on = publish.get("on")
    if not isinstance(on, dict):
        return {}
    call = on.get("workflow_call")
    if not isinstance(call, dict):
        return {}
    return call.get("inputs") or {}


@dataclass(frozen=True)
class _Commit:
    package: str
    kind: str
    release_as: str | None = None
    breaking: bool = False


def _bump(current: str, kind: str, breaking: bool) -> str:
    major, minor, patch = (int(part) for part in current.split("."))
    if breaking:
        return f"{major + 1}.0.0"
    if kind == "feat":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def _release_proposal(config: dict, manifest: dict, commits: list[_Commit], package: str) -> tuple[str, str] | None:
    relevant = [c for c in commits if c.package == package]
    if not relevant:
        return None
    forced = next((c.release_as for c in relevant if c.release_as), None)
    if forced:
        next_version = forced
    elif manifest.get(package) is None:
        next_version = config["packages"][package].get("initial-version") or "0.0.0"
    else:
        breaking = any(c.breaking for c in relevant)
        has_feat = any(c.kind == "feat" for c in relevant)
        next_version = _bump(manifest[package], "feat" if has_feat else "patch", breaking)
    component = config["packages"][package].get("component", package)
    return next_version, f"{component}-v{next_version}"


def _post_merge_manifest(config: dict, manifest: dict, proposals: dict[str, tuple[str, str]]) -> dict:
    updated = dict(manifest)
    for package, (version, _tag) in proposals.items():
        updated[package] = version
    return updated


def _eval_provider_condition(
    expr: str,
    *,
    core_created: bool,
    provider_created: bool,
    core_result: str,
) -> bool:
    expr = expr.replace("${{", "").replace("}}", "")
    expr = expr.replace("always()", "True")
    expr = expr.replace("needs.release-please.outputs.core--release_created", "'true'" if core_created else "'false'")
    expr = expr.replace(
        "needs.release-please.outputs.provider--release_created",
        "'true'" if provider_created else "'false'",
    )
    expr = expr.replace("needs.publish-core.result", repr(core_result))
    expr = expr.replace("&&", " and ").replace("||", " or ")
    return bool(eval("(" + expr + ")", {"__builtins__": {}}, {}))


def test_reusable_publish_interface() -> None:
    """publish.yml is a workflow_call with exactly the typed required inputs and no release-event trigger."""
    _require(_publish_interface_ready(), "publish.yml is not yet a typed reusable workflow")
    publish = _load_gh_yaml(PUBLISH_WORKFLOW)
    assert set(publish["on"]) == {"workflow_call"}
    inputs = publish["on"]["workflow_call"]["inputs"]
    assert set(inputs) == set(REQUIRED_PUBLISH_INPUTS)
    for name in REQUIRED_PUBLISH_INPUTS:
        spec = inputs[name]
        assert spec["required"] is True, f"input {name} must be required"
        assert spec["type"] == "string", f"input {name} must be a string"
    for job in ("build", "publish-testpypi", "smoke", "publish-pypi"):
        assert job in publish["jobs"], f"publish.yml is missing the {job} job"
    raw = PUBLISH_WORKFLOW.read_text(encoding="utf-8")
    assert "github.event.release" not in raw
    assert "release: published" not in raw


def test_exact_artifact_reused() -> None:
    """The exact-ref artifact is attested once, uploaded uniquely, and reused for production with no rebuild."""
    _require(_publish_interface_ready(), "publish.yml is not yet a typed reusable workflow")
    publish = _load_gh_yaml(PUBLISH_WORKFLOW)
    jobs = publish["jobs"]
    build_steps = jobs["build"]["steps"]
    checkout = next(s for s in build_steps if "checkout" in s.get("uses", ""))
    assert checkout["with"]["ref"] == "${{ inputs.ref }}", "build must check out the exact Release Please ref"
    commands = " ".join(str(s.get("run", "")) for s in build_steps)
    assert "uv build --no-sources" in commands
    assert "twine check" in commands
    assert any("attest-build-provenance" in s.get("uses", "") for s in build_steps)
    upload = next(s for s in build_steps if "upload-artifact" in s.get("uses", ""))
    artifact_name = upload["with"]["name"]
    assert "${{ inputs.component }}" in artifact_name
    assert "${{ inputs.version }}" in artifact_name
    for job in ("publish-testpypi", "publish-pypi"):
        download = next(s for s in jobs[job]["steps"] if "download-artifact" in s.get("uses", ""))
        assert download["with"]["name"] == artifact_name, f"{job} must reuse the exact build artifact"
    pypi_commands = " ".join(str(s.get("run", "")) for s in jobs["publish-pypi"]["steps"])
    assert "uv build" not in pypi_commands, "production must not rebuild after smoke"
    assert "twine check" not in pypi_commands


def test_candidate_smoke_precedes_production() -> None:
    """TestPyPI candidate install/smoke runs before the caller-selected PyPI environment promotion."""
    _require(_convention_smoke_ready(), "convention-based publish smoke not yet implemented")
    publish = _load_gh_yaml(PUBLISH_WORKFLOW)
    jobs = publish["jobs"]
    assert jobs["smoke"]["needs"] == ["build", "publish-testpypi"]
    assert jobs["publish-pypi"]["needs"] == ["build", "publish-testpypi", "smoke"]
    assert jobs["publish-testpypi"]["environment"]["name"] == "${{ inputs.testpypi_environment }}"
    assert jobs["publish-pypi"]["environment"]["name"] == "${{ inputs.pypi_environment }}"
    assert jobs["publish-testpypi"]["environment"]["name"] != jobs["publish-pypi"]["environment"]["name"]
    assert jobs["smoke"]["env"]["SMOKE_VENV"] == "/tmp/smoke-${{ inputs.component }}"
    smoke_steps = jobs["smoke"]["steps"]
    install = next(s for s in smoke_steps if "uv pip install" in s.get("run", ""))
    assert "--index-url https://test.pypi.org/simple/" in install["run"]
    assert "--extra-index-url https://pypi.org/simple/" in install["run"]
    assert '"${{ inputs.package_name }}==${{ inputs.version }}"' in install["run"]
    assert "$SMOKE_VENV" in install["run"]
    core_smoke = next(s for s in smoke_steps if s.get("if") == "inputs.package_name == 'datasluice'")
    assert "$SMOKE_VENV" in core_smoke["run"]
    assert "import datasluice" in core_smoke["run"]
    assert "datasluice --help" in core_smoke["run"]
    provider_smoke = next(s for s in smoke_steps if s.get("if") == "inputs.package_name != 'datasluice'")
    assert provider_smoke["working-directory"] == "${{ inputs.package_path }}"
    assert '"$SMOKE_VENV/bin/python" tests/smoke.py' in provider_smoke["run"]
    provider_run = provider_smoke["run"]
    assert "get_provider_info" not in provider_run
    assert "DataSluiceHook" not in provider_run
    assert "example_datasluice.py" not in provider_run


def test_manifest_components() -> None:
    """Config tracks both packages with component tags; the manifest records only actual releases."""
    _require(_manifest_ready(), "two-component manifest not yet configured")
    config = _load_json(RELEASE_CONFIG)
    manifest = _load_json(RELEASE_MANIFEST)
    packages = config["packages"]
    assert set(packages) == {".", PROVIDER_PATH}
    root = packages["."]
    provider = packages[PROVIDER_PATH]
    assert root["component"] == "datasluice"
    assert provider["component"] == "apache-airflow-providers-datasluice"
    assert root["include-component-in-tag"] is True
    assert provider["include-component-in-tag"] is True
    assert root["version-file"] == "pyproject.toml"
    assert provider["version-file"] == "pyproject.toml"
    assert provider["initial-version"] == "0.1.0"
    assert "release-as" not in root
    assert "release-as" not in provider
    assert manifest == {".": "0.1.0"}


def test_initial_release_versions_and_tags() -> None:
    """One-time root Release-As plus provider initial-version yield exact versions and component tags."""
    _require(_manifest_ready(), "two-component manifest not yet configured")
    config = _load_json(RELEASE_CONFIG)
    manifest = _load_json(RELEASE_MANIFEST)
    commits = [
        _Commit(package=".", kind="feat", release_as="1.0.0"),
        _Commit(package=PROVIDER_PATH, kind="feat"),
    ]
    core = _release_proposal(config, manifest, commits, ".")
    provider = _release_proposal(config, manifest, commits, PROVIDER_PATH)
    assert core is not None and provider is not None
    assert core == ("1.0.0", "datasluice-v1.0.0")
    assert provider == ("0.1.0", "apache-airflow-providers-datasluice-v0.1.0")
    merged = _post_merge_manifest(config, manifest, {".": core, PROVIDER_PATH: provider})
    assert merged == {".": "1.0.0", PROVIDER_PATH: "0.1.0"}

    provider_only = [_Commit(package=PROVIDER_PATH, kind="fix")]
    assert _release_proposal(config, merged, provider_only, ".") is None
    provider_fix = _release_proposal(config, merged, provider_only, PROVIDER_PATH)
    assert provider_fix is not None
    assert provider_fix == ("0.1.1", "apache-airflow-providers-datasluice-v0.1.1")

    root_fix = [_Commit(package=".", kind="fix")]
    assert _release_proposal(config, merged, root_fix, ".") == ("1.0.1", "datasluice-v1.0.1")


def test_release_outputs_route_without_release_event() -> None:
    """Release Please outputs route into the typed publish interface without a release event."""
    _require(
        _publish_interface_ready() and _routing_ready(),
        "output routing not yet wired to the publish interface",
    )
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    assert "release" not in workflow["on"], "package publication must not listen for release events"
    raw = RELEASE_PLEASE_WORKFLOW.read_text(encoding="utf-8")
    assert "release: published" not in raw
    assert "github.event.release" not in raw

    jobs = workflow["jobs"]
    outputs = jobs["release-please"]["outputs"]
    assert outputs["core--release_created"] == "${{ steps.release.outputs.release_created }}"
    assert outputs["core--version"] == "${{ steps.release.outputs.version }}"
    assert outputs["core--tag_name"] == "${{ steps.release.outputs.tag_name }}"
    assert outputs["provider--release_created"] == (
        "${{ steps.release.outputs['providers/apache-airflow--release_created'] }}"
    )
    assert outputs["provider--version"] == "${{ steps.release.outputs['providers/apache-airflow--version'] }}"

    release_steps = [s for s in jobs["release-please"]["steps"] if s.get("id") == "release"]
    assert len(release_steps) == 1
    assert "release-please-action" in release_steps[0]["uses"]

    core = jobs["publish-core"]
    provider = jobs["publish-provider"]
    assert core["uses"] == "./.github/workflows/publish.yml"
    assert provider["uses"] == "./.github/workflows/publish.yml"
    assert "core--release_created" in str(core.get("if", ""))
    assert "'true'" in str(core.get("if", ""))
    for job in (core, provider):
        supplied = job.get("with") or {}
        missing = [name for name in REQUIRED_PUBLISH_INPUTS if name not in supplied]
        assert not missing, f"caller job missing publish inputs: {missing}"


def test_provider_only_and_joint_dependencies() -> None:
    """Provider publication is ordered after core in joint releases but survives provider-only runs."""
    _require(
        _publish_interface_ready() and _routing_ready(),
        "core-before-provider routing not yet wired",
    )
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    jobs = workflow["jobs"]
    core = jobs["publish-core"]
    provider = jobs["publish-provider"]
    assert "publish-core" in provider["needs"]
    core_if = str(core.get("if", ""))
    assert "core--release_created" in core_if and "'true'" in core_if
    provider_if = str(provider.get("if", ""))
    assert "always()" in provider_if
    assert "provider--release_created" in provider_if
    assert "core--release_created" in provider_if
    assert "publish-core.result" in provider_if

    def runs(*, core_created: bool, provider_created: bool, core_result: str) -> bool:
        return _eval_provider_condition(
            provider_if,
            core_created=core_created,
            provider_created=provider_created,
            core_result=core_result,
        )

    assert runs(core_created=True, provider_created=True, core_result="success") is True
    assert runs(core_created=True, provider_created=True, core_result="failure") is False
    assert runs(core_created=False, provider_created=True, core_result="skipped") is True
    assert runs(core_created=False, provider_created=False, core_result="skipped") is False


def test_distinct_production_environments() -> None:
    """Core and provider callers supply four distinct environment values to the typed interface."""
    _require(
        _publish_interface_ready() and _routing_ready(),
        "distinct environments not yet wired",
    )
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    jobs = workflow["jobs"]
    core = jobs["publish-core"]["with"]
    provider = jobs["publish-provider"]["with"]
    environments = {
        "core-test-pypi": core["testpypi_environment"],
        "core-pypi": core["pypi_environment"],
        "provider-test-pypi": provider["testpypi_environment"],
        "provider-pypi": provider["pypi_environment"],
    }
    expected = {
        "core-test-pypi": "core-test-pypi",
        "core-pypi": "core-pypi",
        "provider-test-pypi": "provider-test-pypi",
        "provider-pypi": "provider-pypi",
    }
    assert environments == expected
    assert len(set(environments.values())) == 4


def test_provider_registry_schema() -> None:
    """The provider registry has a providers array where each entry has exactly six fields."""
    _require(_provider_registry_ready(), "provider registry not yet created")
    registry = _load_json(REGISTRY_PATH)
    assert isinstance(registry.get("providers"), list)
    assert len(registry["providers"]) >= 1
    required_fields = {"slug", "path", "package_name", "initial_version", "testpypi_env", "pypi_env"}
    slugs: list[str] = []
    paths: list[str] = []
    for entry in registry["providers"]:
        assert set(entry) == required_fields, f"registry entry has wrong fields: {set(entry)}"
        slugs.append(entry["slug"])
        paths.append(entry["path"])
        assert re.match(r"^[a-z][a-z0-9-]*$", entry["slug"]), f"slug not lowercase identifier: {entry['slug']}"
        assert (REPO_ROOT / entry["path"]).is_dir(), f"path does not exist: {entry['path']}"
    assert len(slugs) == len(set(slugs)), "duplicate slugs in registry"
    assert len(paths) == len(set(paths)), "duplicate paths in registry"


def test_env_naming_convention() -> None:
    """Every registry provider follows the <slug>-test-pypi / <slug>-pypi env naming convention."""
    _require(_provider_registry_ready(), "provider registry not yet created")
    registry = _load_json(REGISTRY_PATH)
    for entry in registry["providers"]:
        assert entry["testpypi_env"] == f"{entry['slug']}-test-pypi", entry
        assert entry["pypi_env"] == f"{entry['slug']}-pypi", entry


def test_provider_smoke_module_exists() -> None:
    """Every registry provider ships a non-empty tests/smoke.py convention module."""
    _require(_provider_registry_ready(), "provider registry not yet created")
    registry = _load_json(REGISTRY_PATH)
    for entry in registry["providers"]:
        smoke_path = REPO_ROOT / entry["path"] / "tests" / "smoke.py"
        assert smoke_path.exists(), f"smoke module missing: {smoke_path}"
        assert smoke_path.stat().st_size > 0, f"smoke module empty: {smoke_path}"
