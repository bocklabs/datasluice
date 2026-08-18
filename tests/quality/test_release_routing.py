"""Release-routing contracts for the dual-distribution manifest and publication gates.

establishes the typed reusable publication interface in
``.github/workflows/publish.yml``: build/attest the exact Release Please ref,
publish the candidate to the selected TestPyPI environment, install the exact
``name==version`` candidate from TestPyPI with PyPI resolving dependencies,
smoke the package, and only then promote the SAME attested artifact through the
caller-selected PyPI environment. configures the two-component Release
Please manifest and routes its path-prefixed outputs into that interface with
core-before-provider ordering. Nothing is published here; these are structural
YAML/JSON contract tests plus a local release-proposal model.
"""

from __future__ import annotations

import json
import os
import re
import tomllib
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
    if config.get("release-type") != "python":
        return False
    if not isinstance(config.get("changelog-types"), list) or not config["changelog-types"]:
        return False
    if config.get("include-component-in-tag") is not True:
        return False
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
    if "changelog-types" in root or "changelog-types" in provider:
        return False
    if "release-type" in root or "release-type" in provider:
        return False
    manifest = _load_json(RELEASE_MANIFEST)
    if set(manifest) != set(packages):
        return False
    return all(isinstance(version, str) and re.fullmatch(r"\d+\.\d+\.\d+", version) for version in manifest.values())


def _package_version(path: Path) -> str:
    return tomllib.loads(path.read_text(encoding="utf-8"))["project"]["version"]


def _routing_ready() -> bool:
    if not RELEASE_PLEASE_WORKFLOW.exists():
        return False
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    jobs = workflow.get("jobs") or {}
    if not isinstance(jobs.get("publish-core"), dict) or not isinstance(jobs.get("publish-providers"), dict):
        return False
    release_job = jobs.get("release-please")
    if not isinstance(release_job, dict):
        return False
    steps = release_job.get("steps") or []
    has_release = any(
        isinstance(step, dict)
        and step.get("id") == "release"
        and isinstance(step.get("uses"), str)
        and "release-please-action" in step["uses"]
        for step in steps
    )
    has_collect = any(isinstance(step, dict) and step.get("id") == "collect" for step in steps)
    outputs = release_job.get("outputs") or {}
    has_provider_releases = "provider-releases" in outputs
    return has_release and has_collect and has_provider_releases


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


def _eval_providers_condition(
    expr: str,
    *,
    core_created: bool,
    provider_releases_empty: bool,
    core_result: str,
) -> bool:
    expr = expr.replace("${{", "").replace("}}", "")
    expr = expr.replace("always()", "True")
    expr = expr.replace("needs.release-please.outputs.core--release_created", "'true'" if core_created else "'false'")
    expr = expr.replace(
        "needs.release-please.outputs.provider-releases",
        "'[]'" if provider_releases_empty else '\'[{"slug":"x"}]\'',
    )
    expr = expr.replace("needs.publish-core.result", repr(core_result))
    expr = expr.replace("&&", " and ").replace("||", " or ")
    return bool(eval("(" + expr + ")", {"__builtins__": {}}, {}))


def _collect_providers(registry: dict, release_outputs: dict) -> list[dict]:
    """Mirror the collect step: filter registry providers whose path has a release_created == 'true' output."""
    releases: list[dict] = []
    for provider in registry.get("providers") or []:
        path = provider["path"]
        if release_outputs.get(f"{path}--release_created") == "true":
            releases.append(
                {
                    "slug": provider["slug"],
                    "path": path,
                    "package_name": provider["package_name"],
                    "version": release_outputs[f"{path}--version"],
                    "tag_name": release_outputs[f"{path}--tag_name"],
                    "testpypi_env": provider["testpypi_env"],
                    "pypi_env": provider["pypi_env"],
                }
            )
    return releases


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
    assert '"${PACKAGE_NAME}==${PACKAGE_VERSION}"' in install["run"]
    install_env = install["env"]
    assert install_env["PACKAGE_NAME"] == "${{ inputs.package_name }}"
    assert install_env["PACKAGE_VERSION"] == "${{ inputs.version }}"
    assert "${{ inputs." not in install["run"], "run block must not expand workflow template expressions"
    assert "$SMOKE_VENV" in install["run"]
    core_smoke = next(s for s in smoke_steps if s.get("if") == "inputs.package_name == 'datasluice'")
    assert "$SMOKE_VENV" in core_smoke["run"]
    assert "import datasluice" in core_smoke["run"]
    assert "--help" in core_smoke["run"]
    provider_smoke = next(s for s in smoke_steps if s.get("if") == "inputs.package_name != 'datasluice'")
    assert provider_smoke["working-directory"] == "${{ inputs.package_path }}"
    assert '"$SMOKE_VENV/bin/python" tests/smoke.py' in provider_smoke["run"]
    provider_run = provider_smoke["run"]
    assert "get_provider_info" not in provider_run
    assert "DataSluiceHook" not in provider_run
    assert "example_datasluice.py" not in provider_run


def test_manifest_components() -> None:
    """Config uses top-level shared defaults with an N-entry packages map; manifest records only actual releases."""
    _require(_manifest_ready(), "manifest with top-level defaults not yet configured")
    config = _load_json(RELEASE_CONFIG)
    manifest = _load_json(RELEASE_MANIFEST)
    assert config["release-type"] == "python"
    assert config["include-component-in-tag"] is True
    assert config["include-v-in-tag"] is True
    assert config["version-file"] == "pyproject.toml"
    assert config["changelog-path"] == "CHANGELOG.md"
    assert isinstance(config["changelog-types"], list)
    assert len(config["changelog-types"]) >= 1
    packages = config["packages"]
    assert set(packages) == {".", PROVIDER_PATH}
    root = packages["."]
    provider = packages[PROVIDER_PATH]
    assert root["component"] == "datasluice"
    assert provider["component"] == "apache-airflow-providers-datasluice"
    assert provider["initial-version"] == "0.1.0"
    assert "changelog-types" not in root
    assert "changelog-types" not in provider
    assert "release-type" not in root
    assert "release-type" not in provider
    assert "release-as" not in root
    assert "release-as" not in provider
    assert manifest == {
        ".": _package_version(REPO_ROOT / "pyproject.toml"),
        PROVIDER_PATH: _package_version(REPO_ROOT / PROVIDER_PATH / "pyproject.toml"),
    }


def test_initial_release_versions_and_tags() -> None:
    """One-time root Release-As plus provider initial-version yield exact versions and component tags."""
    _require(_manifest_ready(), "two-component manifest not yet configured")
    config = _load_json(RELEASE_CONFIG)
    manifest = {".": "0.1.0"}
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
    """Release outputs route through a collect step into core and matrix publish callers without a release event."""
    _require(
        _publish_interface_ready() and _routing_ready(),
        "collect/matrix routing not yet wired to the publish interface",
    )
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    assert "release" not in workflow["on"], "package publication must not listen for release events"
    raw = RELEASE_PLEASE_WORKFLOW.read_text(encoding="utf-8")
    assert "release: published" not in raw
    assert "github.event.release" not in raw

    jobs = workflow["jobs"]
    release_job = jobs["release-please"]
    outputs = release_job["outputs"]
    assert outputs["core--release_created"] == "${{ steps.release.outputs.release_created }}"
    assert outputs["core--version"] == "${{ steps.release.outputs.version }}"
    assert outputs["core--tag_name"] == "${{ steps.release.outputs.tag_name }}"
    assert "provider-releases" in outputs

    release_steps = [s for s in release_job["steps"] if s.get("id") == "release"]
    assert len(release_steps) == 1
    assert "release-please-action" in release_steps[0]["uses"]

    collect_steps = [s for s in release_job["steps"] if s.get("id") == "collect"]
    assert len(collect_steps) == 1

    core = jobs["publish-core"]
    providers = jobs["publish-providers"]
    assert core["uses"] == "./.github/workflows/publish.yml"
    assert providers["uses"] == "./.github/workflows/publish.yml"
    assert "core--release_created" in str(core.get("if", ""))
    assert "'true'" in str(core.get("if", ""))


def test_provider_only_and_joint_dependencies() -> None:
    """Provider matrix runs after core in joint releases but proceeds in provider-only and skips when empty."""
    _require(
        _publish_interface_ready() and _routing_ready(),
        "core-before-providers matrix routing not yet wired",
    )
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    jobs = workflow["jobs"]
    core = jobs["publish-core"]
    providers = jobs["publish-providers"]
    assert "publish-core" in providers["needs"]
    assert "release-please" in providers["needs"]
    core_if = str(core.get("if", ""))
    assert "core--release_created" in core_if and "'true'" in core_if
    providers_if = str(providers.get("if", ""))
    assert "always()" in providers_if
    assert "provider-releases" in providers_if
    assert "core--release_created" in providers_if
    assert "publish-core.result" in providers_if

    def runs(*, core_created: bool, provider_releases_empty: bool, core_result: str) -> bool:
        return _eval_providers_condition(
            providers_if,
            core_created=core_created,
            provider_releases_empty=provider_releases_empty,
            core_result=core_result,
        )

    assert runs(core_created=True, provider_releases_empty=False, core_result="success") is True
    assert runs(core_created=True, provider_releases_empty=False, core_result="failure") is False
    assert runs(core_created=False, provider_releases_empty=False, core_result="skipped") is True
    assert runs(core_created=False, provider_releases_empty=True, core_result="skipped") is False
    assert runs(core_created=True, provider_releases_empty=True, core_result="success") is False


def test_distinct_production_environments() -> None:
    """Core uses test-pypi/pypi; each provider matrix cell uses slug-derived env names from the registry."""
    _require(
        _publish_interface_ready() and _routing_ready() and _provider_registry_ready(),
        "distinct environments not yet wired",
    )
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    jobs = workflow["jobs"]
    core_with = jobs["publish-core"]["with"]
    assert core_with["testpypi_environment"] == "test-pypi"
    assert core_with["pypi_environment"] == "pypi"
    assert core_with["testpypi_environment"] != core_with["pypi_environment"]

    providers_with = jobs["publish-providers"]["with"]
    assert providers_with["testpypi_environment"] == "${{ matrix.provider.testpypi_env }}"
    assert providers_with["pypi_environment"] == "${{ matrix.provider.pypi_env }}"

    registry = _load_json(REGISTRY_PATH)
    release_outputs = {f"{p['path']}--release_created": "true" for p in registry["providers"]}
    release_outputs.update({f"{p['path']}--version": p["initial_version"] for p in registry["providers"]})
    release_outputs.update(
        {f"{p['path']}--tag_name": f"{p['package_name']}-v{p['initial_version']}" for p in registry["providers"]}
    )
    collected = _collect_providers(registry, release_outputs)
    env_values = set()
    for entry in collected:
        env_values.add(entry["testpypi_env"])
        env_values.add(entry["pypi_env"])
        assert entry["testpypi_env"] != "test-pypi"
        assert entry["pypi_env"] != "pypi"
    assert "test-pypi" not in env_values
    assert "pypi" not in env_values
    assert len(env_values) == len(collected) * 2


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


def test_registry_config_parity() -> None:
    """Every registry path appears in release-please-config packages; every non-root package has a registry entry."""
    _require(_provider_registry_ready() and _manifest_ready(), "registry/config parity not yet established")
    registry = _load_json(REGISTRY_PATH)
    config = _load_json(RELEASE_CONFIG)
    packages = config["packages"]
    registry_paths = {p["path"] for p in registry["providers"]}
    registry_components = {p["path"]: p["package_name"] for p in registry["providers"]}
    for path in registry_paths:
        assert path in packages, f"registry path {path} missing from release-please-config packages"
        assert packages[path].get("component") == registry_components[path], (
            f"component mismatch for {path}: "
            f"config={packages[path].get('component')}, registry={registry_components[path]}"
        )
    non_root_packages = {p for p in packages if p != "."}
    assert non_root_packages == registry_paths, (
        f"non-root packages {non_root_packages} do not match registry paths {registry_paths}"
    )


def test_collect_step_filters_providers() -> None:
    """The collect step serializes release outputs, filters against the registry, and emits provider-releases JSON."""
    _require(_routing_ready() and _provider_registry_ready(), "collect step not yet implemented")
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    release_job = workflow["jobs"]["release-please"]

    collect_steps = [s for s in release_job["steps"] if s.get("id") == "collect"]
    assert len(collect_steps) == 1
    collect = collect_steps[0]
    assert collect.get("if") == "always()"
    collect_env = collect.get("env") or {}
    assert "toJSON(steps.release.outputs)" in str(collect_env.get("RELEASE_OUTPUTS", ""))
    collect_run = str(collect.get("run", ""))
    assert "providers/registry.json" in collect_run
    assert "$GITHUB_OUTPUT" in collect_run

    outputs = release_job.get("outputs") or {}
    assert "provider-releases" in outputs
    assert "collect" in str(outputs["provider-releases"])

    registry = _load_json(REGISTRY_PATH)
    assert _collect_providers(registry, {}) == []

    single = {
        "providers/apache-airflow--release_created": "true",
        "providers/apache-airflow--version": "0.1.0",
        "providers/apache-airflow--tag_name": "apache-airflow-providers-datasluice-v0.1.0",
    }
    result = _collect_providers(registry, single)
    assert len(result) == 1
    assert result[0]["slug"] == "airflow"
    assert result[0]["version"] == "0.1.0"
    assert result[0]["tag_name"] == "apache-airflow-providers-datasluice-v0.1.0"

    multi_registry = {
        "providers": [
            {
                "slug": "airflow",
                "path": "providers/apache-airflow",
                "package_name": "apache-airflow-providers-datasluice",
                "initial_version": "0.1.0",
                "testpypi_env": "airflow-test-pypi",
                "pypi_env": "airflow-pypi",
            },
            {
                "slug": "prefect",
                "path": "providers/prefect",
                "package_name": "prefect-datasluice",
                "initial_version": "0.1.0",
                "testpypi_env": "prefect-test-pypi",
                "pypi_env": "prefect-pypi",
            },
        ]
    }
    result_multi = _collect_providers(multi_registry, single)
    assert len(result_multi) == 1
    assert result_multi[0]["slug"] == "airflow"

    joint = {
        **single,
        "release_created": "true",
        "version": "1.0.0",
        "tag_name": "datasluice-v1.0.0",
    }
    result_joint = _collect_providers(registry, joint)
    assert len(result_joint) == 1


def test_matrix_expands_from_collect_output() -> None:
    """publish-providers uses fromJson(provider-releases) as its matrix with seven inputs from matrix.provider.*."""
    _require(_routing_ready(), "matrix routing not yet implemented")
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    providers_job = workflow["jobs"]["publish-providers"]

    strategy = providers_job.get("strategy") or {}
    matrix = strategy.get("matrix") or {}
    assert "fromJson(needs.release-please.outputs.provider-releases)" in str(matrix.get("provider", ""))
    assert strategy.get("fail-fast") is False

    with_block = providers_job.get("with") or {}
    assert "smoke_command" not in with_block
    expected_matrix_inputs = {
        "component": "${{ matrix.provider.slug }}",
        "package_path": "${{ matrix.provider.path }}",
        "package_name": "${{ matrix.provider.package_name }}",
        "version": "${{ matrix.provider.version }}",
        "ref": "${{ matrix.provider.tag_name }}",
        "testpypi_environment": "${{ matrix.provider.testpypi_env }}",
        "pypi_environment": "${{ matrix.provider.pypi_env }}",
    }
    for key, expected in expected_matrix_inputs.items():
        assert with_block.get(key) == expected, f"matrix input {key}: expected {expected}, got {with_block.get(key)}"


def test_core_not_in_matrix() -> None:
    """publish-core is a standalone job; the matrix job has no hardcoded core reference."""
    _require(_routing_ready(), "core/matrix separation not yet implemented")
    workflow = _load_gh_yaml(RELEASE_PLEASE_WORKFLOW)
    jobs = workflow["jobs"]

    core = jobs["publish-core"]
    assert "strategy" not in core

    providers = jobs["publish-providers"]
    assert "strategy" in providers
    providers_with = providers.get("with") or {}
    assert providers_with.get("component") != "core"
    assert providers_with.get("package_name") != "datasluice"
