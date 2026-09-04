---
phase: 04-udata-connector
reviewed: 2026-08-28T17:09:21Z
depth: quick
files_reviewed: 39
files_reviewed_list:
  - .github/workflows/release-please.yaml
  - AGENTS.md
  - dev/udata-evidence/Dockerfile
  - dev/udata-evidence/README.md
  - dev/udata-evidence/compose.yaml
  - dev/udata-evidence/reviews/.gitkeep
  - dev/udata-evidence/seeds/seed.py
  - pyproject.toml
  - scripts/capture_udata_route_documents.py
  - scripts/capture_udata_stack_evidence.py
  - scripts/check_udata_review_gate.py
  - scripts/extract_udata_oracle.py
  - src/datasluice/connectors/catalog/udata/__init__.py
  - src/datasluice/connectors/catalog/udata/clients.py
  - src/datasluice/connectors/catalog/udata/factory.py
  - src/datasluice/connectors/catalog/udata/live.py
  - src/datasluice/connectors/catalog/udata/mapping.py
  - src/datasluice/connectors/catalog/udata/probes.py
  - src/datasluice/connectors/catalog/udata/settings.py
  - src/datasluice/contracts/catalog/fixtures/udata/cases.json
  - src/datasluice/contracts/catalog/fixtures/udata/evidence.json
  - src/datasluice/contracts/catalog/native/udata.py
  - src/datasluice/contracts/catalog/profiles/udata-17.6.json
  - tests/e2e/test_cli_existing.py
  - tests/e2e/test_udata_wheel.py
  - tests/integration/__init__.py
  - tests/integration/connectors/__init__.py
  - tests/integration/connectors/catalog/__init__.py
  - tests/integration/connectors/catalog/test_udata_controlled.py
  - tests/quality/test_catalog_surface.py
  - tests/quality/test_compatibility_matrix.py
  - tests/quality/test_release_routing.py
  - tests/unit/connectors/catalog/test_udata_preflight.py
  - tests/unit/connectors/catalog/test_udata_probes.py
  - tests/unit/connectors/catalog/test_udata_public.py
  - tests/unit/connectors/catalog/test_udata_review_gate.py
  - tests/unit/connectors/catalog/test_udata_tracer.py
  - tests/unit/contracts/catalog/test_udata_profile.py
  - tests/unit/runtime/test_extras_boundaries.py
findings:
  critical: 1
  warning: 0
  info: 0
  total: 1
status: issues_found
---

# Phase 04: Code Review Report

**Reviewed:** 2026-08-28T17:09:21Z
**Depth:** quick
**Files Reviewed:** 39
**Status:** issues_found

## Summary

All 39 explicitly scoped files were scanned with the prescribed quick-review patterns. One blocker was found: a quality test evaluates release-workflow-derived text as Python code. No hardcoded-secret, debug-artifact, or empty-catch matches were found.

## Critical Issues

### CR-01: Workflow-derived text is executed with `eval`

**Classification:** BLOCKER
**File:** `tests/quality/test_release_routing.py:249`
**Issue:** `_eval_providers_condition` passes text derived from the release workflow to Python `eval`. Supplying an empty `__builtins__` mapping is not a safe expression sandbox; a changed workflow can inject expressions outside the intended boolean grammar and execute them during CI. This makes the release-quality gate an avoidable code-execution surface.
**Fix:** Replace `eval` with a closed evaluator that parses an allowlisted AST consisting only of the expected boolean operators, comparisons, constants, and approved variable names. Reject every other node before evaluation, or encode the finite condition cases directly as ordinary Python boolean expressions over the three test inputs.

---

_Reviewed: 2026-08-28T17:09:21Z_
_Reviewer: the agent (gsd-code-reviewer)_
_Depth: quick_
