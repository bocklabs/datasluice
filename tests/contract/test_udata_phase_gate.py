from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from scripts import check_udata_phase_gate as gate
from scripts import extract_udata_oracle as udata_oracle

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = Path(__file__).resolve().parents[2]


def test_profile_operations_rejects_duplicate_ids(tmp_path: Path) -> None:
    profile = {"operations": [{"id": "udata/api-v1.avatar"}, {"id": "udata/api-v1.avatar"}]}
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(profile), encoding="utf-8")
    with pytest.raises(SystemExit, match="duplicate operations"):
        gate.profile_operations(path)


def test_profile_operations_returns_the_declared_ids() -> None:
    operations = gate.profile_operations()
    assert "udata/api-v1.avatar" in operations
    assert len(operations) == len(set(operations))


def _local_receipts_present() -> bool:
    """True when every family's review receipts are on disk, as in a full local checkout."""
    return all(
        any((gate.REVIEWS_ROOT / gate.REVIEWS_DIR_NAMES.get(family, family)).glob("*/review-receipt.json"))
        for family in gate.FAMILIES
    )


def _review_gate() -> Any:
    from scripts import check_udata_review_gate as review_gate

    return review_gate


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    )
    return completed.stdout.strip()


def _review_artifacts(scratch: Path, reviewed_sha: str) -> tuple[Path, Path]:
    scratch.mkdir(parents=True, exist_ok=True)
    source = scratch / "source-review.md"
    source.write_text("# Independent review\n\nEvery finding is classified and resolved.\n", encoding="utf-8")
    threads = scratch / "current-thread.json"
    threads.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "reviewed_sha": reviewed_sha,
                "threads": [{"classification": "fixed", "summary": "review currency regression"}],
            }
        ),
        encoding="utf-8",
    )
    return source, threads


def _capture_family_receipt(
    review_gate: Any, reviews_root: Path, family: str, reviewed_sha: str, source: Path, threads: Path
) -> None:
    review_gate.capture_review(
        reviews_root=reviews_root,
        family=gate.REVIEWS_DIR_NAMES.get(family, family),
        reviewed_sha=reviewed_sha,
        base_sha=reviewed_sha,
        reviewer_id="phase-gate-reviewer",
        author_ids=("phase-gate-author",),
        source_review=source,
        current_threads=threads,
        parent_receipt=None,
        post_fix=False,
    )


def test_review_current_fails_closed_without_receipts(tmp_path: Path) -> None:
    with pytest.raises(gate.GateError, match="tracer is not reviewed at the merge head"):
        gate.require_review_current(reviews_root=tmp_path)


def test_review_current_requires_every_family_at_the_current_head(tmp_path: Path) -> None:
    review_gate = _review_gate()
    head = _git_head()
    source, threads = _review_artifacts(tmp_path / "artifacts-head", head)
    for family in gate.FAMILIES:
        _capture_family_receipt(review_gate, tmp_path, family, head, source, threads)
    gate.require_review_current(reviews_root=tmp_path)


def test_review_current_rejects_a_family_pinned_to_another_head(tmp_path: Path) -> None:
    review_gate = _review_gate()
    head = _git_head()
    stale = "0" * 40
    source, threads = _review_artifacts(tmp_path / "artifacts-head", head)
    stale_source, stale_threads = _review_artifacts(tmp_path / "artifacts-stale", stale)
    for family in gate.FAMILIES:
        if family == "tracer":
            _capture_family_receipt(review_gate, tmp_path, family, stale, stale_source, stale_threads)
        else:
            _capture_family_receipt(review_gate, tmp_path, family, head, source, threads)
    with pytest.raises(gate.GateError, match="tracer is not reviewed at the merge head"):
        gate.require_review_current(reviews_root=tmp_path)
    _capture_family_receipt(review_gate, tmp_path, "tracer", head, source, threads)
    gate.require_review_current(reviews_root=tmp_path)


def test_coverage_inventory_is_the_locked_denominator() -> None:
    if not gate.COVERAGE_PATH.exists():
        pytest.skip("the coverage matrix requires the local-only .planning tree")
    inventory = gate.coverage_inventory()
    assert len(inventory) == 268
    assert all("." in name for name in inventory)
    assert sum(1 for name in inventory if name.startswith("udata.")) == 268


def test_validation_authority_fails_on_pending_or_draft(tmp_path: Path) -> None:
    pending = tmp_path / "validation-pending.md"
    pending.write_text("status: pending", encoding="utf-8")
    with pytest.raises(gate.GateError, match="pending or draft"):
        gate.require_validation_authoritative(pending)
    draft = tmp_path / "validation-draft.md"
    draft.write_text("Status: draft", encoding="utf-8")
    with pytest.raises(gate.GateError, match="pending or draft"):
        gate.require_validation_authoritative(draft)
    clean = tmp_path / "validation-clean.md"
    clean.write_text("status: compliant; every row green", encoding="utf-8")
    gate.require_validation_authoritative(clean)


def test_semantic_conformance_requires_current_head_reviews(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    review_gate = _review_gate()
    monkeypatch.setattr(gate, "REVIEWS_ROOT", tmp_path / "reviews")
    head = _git_head()
    stale = "0" * 40
    source, threads = _review_artifacts(tmp_path / "inputs", stale)
    for family in gate.FAMILIES:
        _capture_family_receipt(review_gate, gate.REVIEWS_ROOT, family, stale, source, threads)
    covered, total = gate.semantic_conformance()
    assert (covered, total) == (0, len(gate.FAMILIES))
    source, threads = _review_artifacts(tmp_path / "inputs-head", head)
    _capture_family_receipt(review_gate, gate.REVIEWS_ROOT, "tracer", head, source, threads)
    assert gate.semantic_conformance() == (1, total)


def test_each_reviewed_family_has_a_receipt_that_verifies_at_its_recorded_sha() -> None:
    """Known-good: each reviewed family carries at least one receipt valid at its recorded sha.

    Mirrors the phase gate's credit rule: the first internally verifying receipt carries the family's credit, so later
    receipts are not checked here.
    """
    if not _local_receipts_present():
        pytest.skip("the review receipts require the local-only dev/udata-evidence tree")
    review_gate = _review_gate()

    reviewed_families = gate.FAMILIES
    for family in reviewed_families:
        reviews_dir = gate.REVIEWS_ROOT / gate.REVIEWS_DIR_NAMES.get(family, family)
        receipts = sorted(reviews_dir.glob("*/review-receipt.json")) if reviews_dir.exists() else []
        assert receipts, family
        errors: list[str] = []
        for receipt_path in receipts:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            try:
                review_gate.verify_review(
                    gate.REVIEWS_ROOT, gate.REVIEWS_DIR_NAMES.get(family, family), receipt["reviewed_sha"]
                )
            except review_gate.ReviewGateError as err:
                errors.append(f"{receipt['reviewed_sha'][:10]}: {err}")
                continue
            break
        else:
            raise AssertionError(f"no valid receipt for {family}: {errors}")


@pytest.fixture
def independent_stock_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    phase = tmp_path / "phase"
    oracle_dir = phase / "oracle"
    oracle_dir.mkdir(parents=True)
    gate._review_gate()
    oracle = gate.importlib.import_module("extract_udata_oracle")
    routes = [{"api_version": "v1", "method": method, "path": "/api/1/things/<thing>/"} for method in ("GET", "POST")]
    document = {"schema_version": 1, "source_commit": oracle.PINNED_COMMIT, "routes": routes}
    captures = [oracle_dir / f"udata-{name}-routes.json" for name in ("source", "swagger", "urlmap")]
    for path in captures:
        path.write_text(json.dumps(document), encoding="utf-8")
    result = oracle.reconcile_route_documents(*captures)
    (oracle_dir / "udata-oracle-candidate.json").write_text(json.dumps(result), encoding="utf-8")
    (phase / "04-PREFLIGHT.md").write_text(
        "---\nstatus: approved\ndecision: approve-baseline-defer-targets\n"
        f"source_commit: {oracle.PINNED_COMMIT}\nroute_count: 2\n"
        f"source_digest: {oracle.digest_file(captures[0])}\n"
        f"swagger_digest: {oracle.digest_file(captures[1])}\n"
        f"url_map_digest: {oracle.digest_file(captures[2])}\n"
        "targets_approved: false\napproved_by: synthetic-test\napproved_at: 2026-10-16T00:00:00Z\n---\n",
        encoding="utf-8",
    )
    coverage = phase / "COVERAGE.md"
    coverage.write_text(
        "| udata.v1.get_thing | INTEGRATE | test |\n| udata.v1.create_thing | INTEGRATE | test |\n"
        "| 1 | `udata.v1.get_thing` | v1 | GET | `/api/1/things/<thing>/` | test | 04-11 | INTEGRATE | source |\n"
        "| 2 | `udata.v1.create_thing` | v1 | POST | `/api/1/things/<thing>/` | test | 04-11 | INTEGRATE | source |\n",
        encoding="utf-8",
    )
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps(
            {"profile_version": "17.6.0", "operations": [{"id": f"operation-{index}"} for index in range(len(routes))]}
        ),
        encoding="utf-8",
    )
    bindings = tmp_path / "bindings.json"
    bindings.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "profile_version": "17.6.0",
                "source_commit": oracle.PINNED_COMMIT,
                "non_route_operations": {},
                "bindings": [
                    {"capability_id": capability, "operation_id": f"operation-{index}", **route}
                    for index, (capability, route) in enumerate(
                        zip(("udata.v1.get_thing", "udata.v1.create_thing"), routes, strict=True)
                    )
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(gate, "PHASE_DIR", phase)
    monkeypatch.setattr(gate, "COVERAGE_PATH", coverage)
    monkeypatch.setattr(gate, "PROFILE_PATH", profile)
    monkeypatch.setattr(gate, "STOCK_BINDINGS_PATH", bindings)
    return oracle_dir


def test_stock_bijection_checks_independent_route_captures(independent_stock_inputs: Path) -> None:
    gate.require_stock_bijection()


@pytest.mark.parametrize("capture", ["source", "swagger", "urlmap", "oracle-candidate"])
def test_stock_bijection_rejects_capture_tampering(independent_stock_inputs: Path, capture: str) -> None:
    name = f"udata-{capture}.json" if capture == "oracle-candidate" else f"udata-{capture}-routes.json"
    path = independent_stock_inputs / name
    document = json.loads(path.read_text(encoding="utf-8"))
    document["routes"][0]["method"] = "DELETE"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(gate.GateError):
        gate.require_stock_bijection()


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("status: approved", "status: pending"),
        ("route_count: 2", "route_count: 1"),
        ("approved_by: synthetic-test", "approved_by:"),
        (f"source_commit: {udata_oracle.PINNED_COMMIT}", f"source_commit: {'0' * 40}"),
    ],
)
def test_stock_bijection_rejects_invalid_approval(independent_stock_inputs: Path, old: str, new: str) -> None:
    path = gate.PHASE_DIR / "04-PREFLIGHT.md"
    path.write_text(path.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")
    with pytest.raises(gate.GateError):
        gate.require_stock_bijection()


def test_stock_bijection_rejects_wrong_coverage_verb(independent_stock_inputs: Path) -> None:
    text = gate.COVERAGE_PATH.read_text(encoding="utf-8").replace("| GET |", "| DELETE |")
    gate.COVERAGE_PATH.write_text(text, encoding="utf-8")
    with pytest.raises(gate.GateError, match="coverage rows"):
        gate.require_stock_bijection()


@pytest.mark.parametrize("defect", ["missing", "duplicate", "extra", "invalid-id"])
def test_stock_bijection_rejects_profile_gaps(independent_stock_inputs: Path, defect: str) -> None:
    profile = json.loads(gate.PROFILE_PATH.read_text(encoding="utf-8"))
    operations = profile["operations"]
    if defect == "missing":
        operations.pop()
    elif defect == "duplicate":
        operations[1]["id"] = operations[0]["id"]
    elif defect == "extra":
        operations.append({"id": "unclassified-operation"})
    else:
        operations[0]["id"] = []
    gate.PROFILE_PATH.write_text(json.dumps(profile), encoding="utf-8")
    with pytest.raises(gate.GateError, match="profile"):
        gate.require_stock_bijection()


def test_stock_bijection_allows_explicit_grouped_operations(independent_stock_inputs: Path) -> None:
    document = json.loads(gate.STOCK_BINDINGS_PATH.read_text(encoding="utf-8"))
    document["bindings"][1]["operation_id"] = "operation-0"
    document["non_route_operations"] = {"operation-1": "Alias. Operation-0 owns both stock routes."}
    gate.STOCK_BINDINGS_PATH.write_text(json.dumps(document), encoding="utf-8")
    gate.require_stock_bijection()
    profile = json.loads(gate.PROFILE_PATH.read_text(encoding="utf-8"))
    profile["operations"].pop()
    gate.PROFILE_PATH.write_text(json.dumps(profile), encoding="utf-8")
    document["non_route_operations"] = {}
    gate.STOCK_BINDINGS_PATH.write_text(json.dumps(document), encoding="utf-8")
    gate.require_stock_bijection()
    document["bindings"].pop()
    gate.STOCK_BINDINGS_PATH.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(gate.GateError, match="missing=.*udata.v1.create_thing"):
        gate.require_stock_bijection()


def _binding_defect_mutators() -> dict[str, Callable[[dict[str, Any]], None]]:
    """Return one mutator per binding defect, each editing the parsed document in place."""

    def missing(document: dict[str, Any]) -> None:
        document["bindings"].pop()
        document["non_route_operations"] = {"operation-1": "Synthetic family contract."}

    def extra(document: dict[str, Any]) -> None:
        bindings = document["bindings"]
        bindings.append({**bindings[0], "capability_id": "udata.v1.plugin", "path": "/api/1/plugin/"})

    def duplicate_route(document: dict[str, Any]) -> None:
        bindings = document["bindings"]
        bindings[1].update({key: bindings[0][key] for key in ("api_version", "method", "path")})

    def canonical_duplicate_route(document: dict[str, Any]) -> None:
        duplicate_route(document)
        document["bindings"][1]["path"] = "/api/1/things/{thing}/"

    def duplicate_capability(document: dict[str, Any]) -> None:
        bindings = document["bindings"]
        bindings[1]["capability_id"] = bindings[0]["capability_id"]

    def swapped_capabilities(document: dict[str, Any]) -> None:
        bindings = document["bindings"]
        bindings[0]["capability_id"], bindings[1]["capability_id"] = (
            bindings[1]["capability_id"],
            bindings[0]["capability_id"],
        )

    def wrong_route(document: dict[str, Any]) -> None:
        document["bindings"][0]["path"] = "/api/1/plugin/"

    def wrong_slash(document: dict[str, Any]) -> None:
        document["bindings"][0]["path"] = "/api/1/things/<thing>"

    def wrong_verb(document: dict[str, Any]) -> None:
        document["bindings"][0]["method"] = "DELETE"

    def wrong_version(document: dict[str, Any]) -> None:
        document["bindings"][0]["api_version"] = "v2"

    def unknown_owner(document: dict[str, Any]) -> None:
        document["bindings"][0]["operation_id"] = "undeclared-operation"

    def missing_owner(document: dict[str, Any]) -> None:
        del document["bindings"][0]["operation_id"]

    def invalid_owner(document: dict[str, Any]) -> None:
        document["bindings"][0]["operation_id"] = []

    def invalid_capability(document: dict[str, Any]) -> None:
        document["bindings"][0]["capability_id"] = []

    def invalid_route(document: dict[str, Any]) -> None:
        del document["bindings"][0]["path"]

    def unclassified_owner(document: dict[str, Any]) -> None:
        document["non_route_operations"] = {"operation-0": "Cannot also own routes."}

    def undeclared_non_route(document: dict[str, Any]) -> None:
        document["non_route_operations"] = {"unknown": "Undeclared operation."}

    def empty_reason(document: dict[str, Any]) -> None:
        document["non_route_operations"] = {"operation-0": " "}

    def invalid_reason(document: dict[str, Any]) -> None:
        document["non_route_operations"] = {"operation-0": []}

    def schema(document: dict[str, Any]) -> None:
        document["schema_version"] = 2

    def source(document: dict[str, Any]) -> None:
        document["source_commit"] = "0" * 40

    def profile_version(document: dict[str, Any]) -> None:
        document["profile_version"] = "18.0.0"

    def bindings_not_list(document: dict[str, Any]) -> None:
        document["bindings"] = {}

    def non_route_not_object(document: dict[str, Any]) -> None:
        document["non_route_operations"] = []

    def binding_not_object(document: dict[str, Any]) -> None:
        document["bindings"][0] = None

    def no_bindings(document: dict[str, Any]) -> None:
        del document["bindings"]

    return {
        "missing": missing,
        "extra": extra,
        "duplicate-route": duplicate_route,
        "canonical-duplicate-route": canonical_duplicate_route,
        "duplicate-capability": duplicate_capability,
        "swapped-capabilities": swapped_capabilities,
        "wrong-route": wrong_route,
        "wrong-slash": wrong_slash,
        "wrong-verb": wrong_verb,
        "wrong-version": wrong_version,
        "unknown-owner": unknown_owner,
        "missing-owner": missing_owner,
        "invalid-owner": invalid_owner,
        "invalid-capability": invalid_capability,
        "invalid-route": invalid_route,
        "unclassified-owner": unclassified_owner,
        "undeclared-non-route": undeclared_non_route,
        "empty-reason": empty_reason,
        "invalid-reason": invalid_reason,
        "schema": schema,
        "source": source,
        "profile-version": profile_version,
        "bindings-not-list": bindings_not_list,
        "non-route-not-object": non_route_not_object,
        "binding-not-object": binding_not_object,
        "no-bindings": no_bindings,
    }


_BINDING_DEFECTS = _binding_defect_mutators()


@pytest.mark.parametrize("defect", sorted(_BINDING_DEFECTS))
def test_stock_bijection_rejects_binding_gaps(independent_stock_inputs: Path, defect: str) -> None:
    document = json.loads(gate.STOCK_BINDINGS_PATH.read_text(encoding="utf-8"))
    _BINDING_DEFECTS[defect](document)
    gate.STOCK_BINDINGS_PATH.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(gate.GateError):
        gate.require_stock_bijection()


@pytest.mark.parametrize("artifact", ["bindings", "profile"])
@pytest.mark.parametrize("content", ["{", "[]", "null"])
def test_stock_bijection_rejects_unreadable_documents(
    independent_stock_inputs: Path, artifact: str, content: str
) -> None:
    path = gate.STOCK_BINDINGS_PATH if artifact == "bindings" else gate.PROFILE_PATH
    path.write_text(content, encoding="utf-8")
    with pytest.raises(gate.GateError, match="cannot be verified"):
        gate.require_stock_bijection()


def test_stock_bijection_requires_the_binding_artifact(
    independent_stock_inputs: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gate, "STOCK_BINDINGS_PATH", independent_stock_inputs / "missing.json")
    with pytest.raises(gate.GateError, match="cannot be verified"):
        gate.require_stock_bijection()


@pytest.mark.parametrize("defect", ["missing", "duplicate-capability", "duplicate-route", "unlisted-capability"])
def test_stock_bijection_rejects_coverage_gaps(independent_stock_inputs: Path, defect: str) -> None:
    lines = gate.COVERAGE_PATH.read_text(encoding="utf-8").splitlines(keepends=True)
    if defect == "missing":
        lines.pop()
    elif defect == "duplicate-capability":
        lines[-1] = lines[-1].replace("udata.v1.create_thing", "udata.v1.get_thing")
    elif defect == "duplicate-route":
        lines[-1] = lines[-1].replace("POST", "GET")
    else:
        lines[-1] = lines[-1].replace("udata.v1.create_thing", "udata.v1.other")
    gate.COVERAGE_PATH.write_text("".join(lines), encoding="utf-8")
    with pytest.raises(gate.GateError, match="coverage rows"):
        gate.require_stock_bijection()


@pytest.mark.parametrize("check", ["require_review_current", "require_artifact_safety", "require_cleanup"])
def test_default_command_requires_security_checks(monkeypatch: pytest.MonkeyPatch, check: str) -> None:
    monkeypatch.setattr(sys, "argv", ["phase-gate"])
    monkeypatch.setattr(gate, "surface_completeness", lambda: (268, 268))
    monkeypatch.setattr(gate, "semantic_conformance", lambda: (19, 19))
    for name in ("require_review_current", "require_artifact_safety", "require_cleanup"):
        monkeypatch.setattr(gate, name, lambda *args: None)

    def reject(*args: object) -> None:
        raise gate.GateError("synthetic security refusal")

    monkeypatch.setattr(gate, check, reject)
    assert gate.main() == 1


@pytest.fixture
def clean_gate_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    monkeypatch.setattr(gate, "PHASE_DIR", tmp_path / "phase")
    monkeypatch.setattr(gate, "REVIEWS_ROOT", tmp_path / "reviews")
    for path in (
        gate.PHASE_DIR / "oracle",
        gate.REVIEWS_ROOT,
        tmp_path / "src/datasluice/contracts/catalog/fixtures/udata",
    ):
        path.mkdir(parents=True)
    monkeypatch.setattr(
        gate,
        "_run",
        lambda command: (0, f"worktree {tmp_path}\nHEAD {'a' * 40}\n") if "worktree" in command else (0, ""),
    )
    return tmp_path


def test_cleanup_accepts_one_clean_worktree(clean_gate_tree: Path) -> None:
    gate.require_cleanup()


def test_cleanup_rejects_incomplete_review_directories(clean_gate_tree: Path) -> None:
    (gate.REVIEWS_ROOT / "family" / ("a" * 40)).mkdir(parents=True)
    with pytest.raises(gate.GateError, match="incomplete review directory"):
        gate.require_cleanup()


def test_cleanup_rejects_review_staging_directories(clean_gate_tree: Path) -> None:
    (gate.REVIEWS_ROOT / "family" / ".review-recovery-interrupted").mkdir(parents=True)
    with pytest.raises(gate.GateError, match="recovery markers"):
        gate.require_cleanup()


@pytest.mark.parametrize("marker", ["nested/04-19-RECOVERY.md", "nested/.review-fix-recovery-pending.json"])
def test_cleanup_rejects_nested_recovery_markers(clean_gate_tree: Path, marker: str) -> None:
    path = gate.PHASE_DIR / marker
    path.parent.mkdir(parents=True)
    path.touch()
    with pytest.raises(gate.GateError, match="recovery markers"):
        gate.require_cleanup()


def test_cleanup_rejects_detached_worktrees(clean_gate_tree: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "_run", lambda command: (0, f"worktree {clean_gate_tree}\n\nworktree /other\ndetached\n"))
    with pytest.raises(gate.GateError, match="extra git worktrees"):
        gate.require_cleanup()


@pytest.mark.parametrize("failed_command", ["worktree", "status"])
def test_cleanup_rejects_unreadable_git_state(
    clean_gate_tree: Path, monkeypatch: pytest.MonkeyPatch, failed_command: str
) -> None:
    def run(command: list[str]) -> tuple[int, str]:
        if failed_command in command:
            return 1, ""
        return 0, f"worktree {clean_gate_tree}\n"

    monkeypatch.setattr(gate, "_run", run)
    with pytest.raises(gate.GateError, match="cannot be read"):
        gate.require_cleanup()


def _wheel(path: Path, content: str = "safe structural evidence") -> Path:
    wheel = path / "candidate.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("nested/evidence.json", content)
    return wheel


@pytest.mark.parametrize(
    "content",
    [
        '{"nested": {"endpoint": "http://127.0.0.1"}}',
        '{"nested": {"password": "synthetic-secret"}}',
        '{"nested": {"access_token": "synthetic-secret"}}',
        '{"nested": {"token": "short"}}',
        '{"nested": {"api_key": "short"}}',
        '{"nested": {"url": "http:\\/\\/10.0.0.1"}}',
        '{"nested": {"pass\\u0077ord": "synthetic-secret"}}',
    ],
)
@pytest.mark.parametrize("location", ["artifact", "wheel"])
def test_artifact_safety_rejects_nested_unsafe_content(clean_gate_tree: Path, content: str, location: str) -> None:
    wheel = _wheel(clean_gate_tree, content if location == "wheel" else "safe evidence")
    if location == "artifact":
        nested = gate.REVIEWS_ROOT / "nested" / "more" / "retained.json"
        nested.parent.mkdir(parents=True)
        nested.write_text(content, encoding="utf-8")
    with pytest.raises(gate.GateError, match="sensitive content|private origin"):
        gate.require_artifact_safety(wheel)


def test_artifact_safety_accepts_safe_artifacts_and_wheel(clean_gate_tree: Path) -> None:
    gate.require_artifact_safety(_wheel(clean_gate_tree))


def test_artifact_safety_accepts_token_semantics_prose(clean_gate_tree: Path) -> None:
    wheel = _wheel(
        clean_gate_tree, '{"observed_stock_semantics": {"token": "RFC 6749 form-encoded grant description"}}'
    )
    gate.require_artifact_safety(wheel)


@pytest.mark.parametrize(
    "source",
    [
        'PASSWORD = "synthetic-secret"',
        'password: str = "synthetic-secret"',
        'example = {"api_key": "synthetic-secret"}',
        'endpoint = "http://127.0.0.1"',
    ],
)
def test_artifact_safety_checks_python_literals_in_the_wheel(clean_gate_tree: Path, source: str) -> None:
    wheel = _wheel(clean_gate_tree)
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr("datasluice/example.py", source)
    with pytest.raises(gate.GateError, match="sensitive content|private origin"):
        gate.require_artifact_safety(wheel)


def test_artifact_safety_does_not_treat_python_annotations_as_secrets(clean_gate_tree: Path) -> None:
    wheel = _wheel(clean_gate_tree)
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr("datasluice/example.py", "api_key: SecretValue\npassword = credential.password\n")
    gate.require_artifact_safety(wheel)


def test_artifact_safety_rejects_unreadable_members(clean_gate_tree: Path) -> None:
    wheel = _wheel(clean_gate_tree)
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr("nested/unreadable.json", b"\xff")
    with pytest.raises(gate.GateError, match="artifact safety check failed"):
        gate.require_artifact_safety(wheel)


def test_artifact_safety_requires_a_candidate_wheel(clean_gate_tree: Path) -> None:
    with pytest.raises(gate.GateError, match="candidate wheel is required"):
        gate.require_artifact_safety()


def test_artifact_safety_rejects_symbolic_links(clean_gate_tree: Path) -> None:
    (gate.REVIEWS_ROOT / "external").symlink_to(clean_gate_tree / "outside")
    wheel = _wheel(clean_gate_tree)
    with pytest.raises(gate.GateError, match="symbolic link"):
        gate.require_artifact_safety(wheel)
