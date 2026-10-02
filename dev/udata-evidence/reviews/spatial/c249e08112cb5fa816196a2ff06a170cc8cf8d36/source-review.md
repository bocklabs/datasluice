# uData spatial family (plan 04-09) — final independent source review

**Reviewer id:** `cg:codex:reviewer-3f8b21d7-6c04-4a19-b5e2-9d17c8a4e603:04-09:final`
**Reviewed head:** `c249e08112cb5fa816196a2ff06a170cc8cf8d36`
**Parent reviewed head:** `68b7cae76377db469388ce9fe95b92d238feb6b1`
**Base sha (plan start):** `50a540b81a3e82a0f6493494145422603c6cf7d0`
**Branch:** `gsd/phase-04-udata-connector` (working tree clean at review time, `git status --porcelain` = empty)
**Reviewer independence:** distinct from `codex-implementation-pass-04-09`, `codex-fix-pass-04-09`,
`agent-7c3f19d4-…`, `agent-9d41b7c2-…`. No code was edited, no fix applied, no commit made.

**Scope:** round-3 fixes `787e4b2` (recapture note) and `c249e08` (provenance walk), plus regression
verification of every item previously cleared in rounds 1–2, plus the two carried-forward findings.

Round-3 diff is exactly two files: `src/datasluice/contracts/catalog/fixtures/udata/evidence.json`
(one line, the spatial `recapture_note`) and `tests/unit/contracts/catalog/test_udata_profile.py`
(+54/−4). No spatial implementation file, no Dockerfile, and no prior family's evidence value was touched.

---

## Verdict

**PASS_WITH_FINDINGS.** Both round-2 blockers are genuinely closed and verified from git, not from the
fix commit's own prose. Every recorded stack digest re-derives exactly. The two never-addressed findings
(SF-04, SF-05) are reclassified `not_actionable` with reasoning recorded below; neither is a defect in the
shipped code. One new observation (FR-01) is disclosed: the provenance gate is *silent-but-loud* — it
skips in CI rather than failing — which is the correct trade-off for the direction SR-01 flagged, but it
means the invariant is enforced locally, not in CI. No `open` finding remains.

---

## Resolution

### SF-01 — blanket-rebound `dockerfile_sha256` on prior families — **resolved**
`fixed` @ `68b7cae`. Independently re-derived: for all 9 `controlled_*` families, `dockerfile_sha256` and
`compose_sha256` equal the true SHA-256 of that file at the commit that first defined the family. I did not
trust the test — I re-derived from `git show <intro>:dev/udata-evidence/Dockerfile` myself (full table in
"Final checks"). 0 mismatches. The seven prior families all resolve to digest `4cb0f78fee1b` /
`8b9ac03113e9`; spatial legitimately differs (`5a4889bbfe52` / `ca1fc88f7bd2`) because plan 04-09 actually
changed the Dockerfile at `496f628`. The difference is now *explained* rather than blanket.

### SF-02 — spatial `implementation_sha` predated the tests it named — **resolved**
`fixed` @ `0989e4c`. `controlled_spatial_evidence.implementation_sha` is now `496f628b7b164fe7c168e8765d09a477566c985a`,
the commit that introduced both the spatial record and the controlled tests it names. Cross-checked:
`controlled_test_sha256` (`3f6addbf…`) equals the live `test_udata_controlled.py`, and
`wheel_test_sha256` (`193c5cdc…`) equals the live `tests/e2e/test_udata_wheel.py`. Both names in `test_ids`
resolve to real test functions in the controlled suite.

### SF-03 — `recapture_note` falsely claimed granularities answers `[]` — **resolved**
`fixed` @ `f07678f`, and re-verified in round 3. Against pinned upstream
`/private/tmp/udata-17.6-checkout/udata/core/spatial/models.py:120-125`:

```python
@cache.memoize()
def get_spatial_granularities(lang):
    with language(lang):
        return [(geolevel.id, _(geolevel.name)) for geolevel in GeoLevel.objects] + [
            (id, str(label)) for id, label in BASE_GRANULARITIES
        ]
```

`BASE_GRANULARITIES` (`constants.py:3-6`) is the literal `[("poi", L_("POI")), ("other", L_("Other"))]`
appended *after* the (empty) GeoLevel rows. So granularities is never empty on a fresh stack. The note's
current wording ("appends the hardcoded BASE_GRANULARITIES literal to the GeoLevel rows, so it answers the
two base entries (poi, other)") is exactly right.

### SF-04 — spatial raw paths hand-transcribed; spatial absent from structural execution gate — **reclassified `not_actionable`**
`not_actionable`, fix_commit `null`. Reasoning, stated plainly so it is not mistaken for a softened defect:

*Fact 1 — the specific artifact named in round 1 does not exist.* `src/datasluice/connectors/catalog/udata/oracle/udata-17.6.0.json`
is absent (`find src -type d -name oracle` → nothing; `find . -name "udata-17.6.0.json"` → nothing).

*Fact 2 — but an oracle artifact for the pinned commit does exist, elsewhere, and it corroborates the
transcription.* `.planning/phases/04-udata-connector/oracle/udata-oracle-candidate.json`
(`source_commit: 0546582058d84706812a1c37387576efc4e5ad1f`, `status: reconciled`, 268 routes, with recorded
input digests) contains exactly the seven spatial routes at exactly the paths the connector emits:
`/api/1/spatial/zones/suggest/`, `/zones/<list:ids>/`, `/zone/<id>/datasets/`, `/zone/<id>/`, `/levels/`,
`/granularities/`, `/coverage/<level>/`. The generating script `scripts/extract_udata_oracle.py` exists
(26 KB, tracked). `04-VALIDATION.md:136-139` cites both accurately.

*Fact 3 — I re-verified all seven paths by hand against pinned `udata/core/spatial/api.py` myself.* All seven
`@ns.route(...)` declarations match the connector's `_SPATIAL_PATH` + literal path construction in
`src/datasluice/connectors/catalog/udata/wire/spatial.py`. Nothing is mis-transcribed.

So the *defect* round 1 alleged (paths asserted without provenance) is not present: the paths are
corroborated by an independent oracle extraction and match upstream. What is genuinely absent is spatial's
membership in `_EVIDENCE_CLAIMING_TESTS` (`test_udata_profile.py:152-170`), which structurally gates only
`oauth`, `user`, `organization`, `taxonomy`. That means the "load-bearing structural derivation" the gate's
own docstring describes (`test_udata_profile.py:621-644`) does not cover spatial — the seven spatial reads
rest on the recorded `route_differential.read_modes/read_operations` being compared against itself.

Why `not_actionable` and not `open`: adding spatial to that AST gate requires teaching
`evidence_execution.py` a new `CLIENT_FAMILIES` entry (`spatial`) and reshaping `_family_arguments` /
`_getattr_methods` for a tuple-of-ids argument shape the gate does not model. That is a change to a
cross-family verification instrument used by four other approved families, and it is **not required by plan
04-09**. Under the repo's own YAGNI rule ("designs, abstractions, dependencies, and tests must prove their
necessibility before being added"; "do not create a second implementation to preserve obsolete logic"), doing
that here would be scope creep touching already-approved evidence. The honest classification is: a real,
recorded coverage gap against a *different* family's gate, out of this plan's scope. It is disclosed here and
in `04-VALIDATION.md` rather than silently dropped, and it is not a false statement in any shipped artifact.

I want to be explicit about the limit: a reviewer who believes cross-family structural parity is a release
gate should treat this as open. I do not, because nothing misstates what was executed, and the recorded
differential matches the controlled test that ran.

### SF-05 — 3/3 branch-discrimination claim not re-runnable — **reclassified `not_actionable`**
`not_actionable`, fix_commit `null`. `04-VALIDATION.md:145-148` narrates three mutations applied and reverted
(removing the FeatureCollection `features` guard, skipping single-pass segment quoting, accepting an empty
zone id list), and honestly discloses that the `features` mutation initially survived and forced the envelope
test to be strengthened. That is a *process* claim about a one-time falsification, not a claim about the code's
current state, and no shipped artifact asserts a mutation score. Nothing in `evidence.json` or the test suite
depends on it. Re-running mutation testing is a repo-wide initiative with no tooling in `.github/workflows/ci.yaml`
and no script committed; adding one for a single plan's three hand-applied edits would be exactly the
"build a test system the task does not need" failure mode AGENTS.md warns against. Not actionable within this
plan. Disclosed rather than hidden.

### SF-06 — concurrency cells argued N/A — **carried, `not_actionable`**
Unchanged from round 2. The seven spatial rows are stock public reads under `/api/1/spatial` with no
concurrency semantics upstream (`api.py` has no `ETag`/`If-Match`/version argument on any of the seven
routes). "N/A" is the factually correct cell, not a dodge. Re-confirmed: no concurrency token or conditional
header exists on any spatial route in pinned `api.py`.

### SR-01 — provenance walk under shallow CI checkout — **resolved**
`fixed` @ `c249e08`. This was the round-2 blocker and it is the item I examined hardest.

*Premise confirmed:* `.github/workflows/ci.yaml` has 10 `actions/checkout@v7.0.1` steps and **no `fetch-depth`
on any of them** (grep for `checkout|fetch-depth|fetch_depth` → the only `with:` input on each step is
`persist-credentials`, set to false). Default is
depth 1. The core test job runs `pytest tests` (line 110), which includes the provenance test.

*Is the invariant still real in a full clone?* **Yes — I falsified it empirically.** I wrote a historical
`evidence.json` with every family's `dockerfile_sha256` overwritten to the current Dockerfile's digest
(exactly reproducing SF-01), without committing, then ran the test:

```
AssertionError: controlled_activity_discussion_evidence.dockerfile_sha256 records 5a4889bbfe52
  but dev/udata-evidence/Dockerfile at 01a4dd80f1c7 hashes to 4cb0f78fee1b
  tests/unit/contracts/catalog/test_udata_profile.py:305: AssertionError
```

It failed for the right reason, naming the historical commit. I then restored the file with
`git show HEAD:<path> > <path>` and proved byte-identity: `sha256sum -c` → `OK`
(`4003198445de5e0c6f9138393706ca0adc190e07fa2f0e7c28ed914ea712d7c6`), `git status --porcelain` → empty
(0 changed files). No modification remains in the repo.

*Does the "every family resolves to HEAD" signal false-positive on a new family?* **No.** This is the
subtle part and it is correct. The guard requires **every** family to resolve to HEAD. Adding a new family at
HEAD makes *that one* family resolve to HEAD while the eight older ones still resolve to their own
introducing commits, so `all(...)` is False and the walk proceeds. I confirmed the real head: 9 families, 0
resolve to HEAD, `all(... == head)` is `False`. Only a walk that genuinely saw the tip alone makes *all* of
them collapse to HEAD. A hypothetical single-family repo is degenerate (vacuous `all()`), but this repo has
9 and the docs are honest about the reasoning.

*Can the check be defeated into a silent pass?* The dangerous direction would be a **false skip** on a full
clone, turning a real regression into a green suite. I tested the three ways that could happen:
(1) `git rev-parse --is-shallow-repository` returns `"true"` only for a genuinely shallow repo (verified:
`false` here, `true` in my `--depth 1` probe clone).
(2) `revisions` empty → skip; unreachable while `families` is non-empty (the file is tracked and present).
(3) no-git / not-a-repository → `subprocess.run(..., check=True)` raises `CalledProcessError`, so the test
**errors loudly**, it does not skip silently. I confirmed the rc=128 path explicitly.
So every failure mode is loud; none produces a false pass.

*Does the skip hide a real regression in CI, and is it disclosed?* Yes, it does skip in CI — and that is
correct and deliberate. Under SR-01's original code the same shallow clone produced a **false FAIL that
inverted the invariant** (it asserted historical records match HEAD, i.e. it would have blessed a rebind).
Trading "wrong failure" for "honest skip with a reason" is the right direction. And it *is* disclosed: the
skip message names the cause and tells the reader what to do —
`provenance walk unavailable: the clone is shallow, so commits before HEAD are absent; run this test
against a full clone` (verified verbatim in a real `--depth 1` clone of this very HEAD). FR-01 below records
the residual gap.

### SR-02 — "four zone/level targets answer 404" — **resolved**
`fixed` @ `787e4b2`. I audited **every clause** of the current `recapture_note` against pinned
`udata/core/spatial/api.py`, `models.py`, and `constants.py`:

| Clause | Verdict | Evidence |
|---|---|---|
| Every assigned row is a public read | true | all seven routes have no `@api.secure`; `api.py:45-176` |
| GeoLevel/GeoZone empty because seed creates no spatial rows | true | `dev/udata-evidence/seeds/seed.py` contains 0 occurrences of `GeoLevel`/`GeoZone`/`spatial`/`zone` |
| Exactly **three** routes use `get_or_404` → 404 | true | `ZoneAPI` (`api.py:110-118`), `ZoneDatasetsAPI` (`:96-108`), `SpatialCoverageAPI` (`:158-176`); `flask_mongoengine/engine.py:146-155` shows `get_or_404` → `abort(404)` |
| zones: `in_bulk` + unguarded comprehension → `KeyError` → 500 | true | `ZonesAPI` (`api.py:87-100`): `zones = GeoZone.objects.in_bulk(ids_list)` then `zones = [zones[id] for id in ids_list]` — unguarded, so absent id raises `KeyError` → 500 |
| granularities answers the two base entries | true | `models.py:120-125` + `constants.py:3-6` (see SF-03) |
| differential asserts typed mapping for the 500, not a body | true | `test_udata_controlled.py:4632-4636` asserts `status == 500`; `:4646` compares `status_code` metadata |
| percent-encoded **exactly once** by the connector's own wire tests emitting `fr%3A1100001` | true | `test_udata_spatial.py:105-110` asserts `wire.spatial_zones_request(("fr:1100001","commune:75056"))` → `/zones/fr%3A1100001,commune%3A75056/`; also `:126` (`/zone/fr%3A1100001/`) and `:129` (`/coverage/country%3Afr/`) |

The new single-encoding clause is **exact**: the named wire test exists and asserts that literal path.
The old unprovable clause ("the loopback container traceback shows the decoded colon form") is gone — I
grepped the full repo and no artifact asserts a container traceback for spatial. `suggest_zones` is
correctly *not* in the "four absent-target routes" group: it iterates existing `GeoZone.objects(...)`, so
with an empty collection the sort key never evaluates and it answers 200 `[]`.

### SR-03 — superseded commit 401e40f left in history — **carried, `fixed`**
`fixed` @ `68b7cae` (round 2 already judged the self-correcting pair acceptable; I concur and did not
re-open). `401e40f` "restores the falsified Dockerfile digests" and `68b7cae` corrects it — an auditable
two-step that is *more* verifiable than a history rewrite, since the corrected value is provable at each
step. I independently confirmed the end state is correct (see SF-01 table: all 9 families resolve to true
digests at their introducing commits).

---

## Final checks with no defect

Recorded because each was previously flagged or is a claim that could have regressed silently.

**Evidence provenance is factually true end-to-end (the item SR-01 was protecting).** I re-derived, without
using the repo's own test, for every `controlled_*` family:

| family | introducing commit | dockerfile | compose |
|---|---|---|---|
| controlled_activity_discussion_evidence | `01a4dd80f1c7` | `4cb0f78fee1b` ✓ | `8b9ac03113e9` ✓ |
| controlled_organization_evidence | `dbcb9be3a304` | `4cb0f78fee1b` ✓ | `8b9ac03113e9` ✓ |
| controlled_posts_report_evidence | `e95affa5d71d` | `4cb0f78fee1b` ✓ | `8b9ac03113e9` ✓ |
| controlled_reuse_evidence | `e635dda64e54` | `4cb0f78fee1b` ✓ | `8b9ac03113e9` ✓ |
| controlled_taxonomy_evidence | `a9cdabd3de07` | `4cb0f78fee1b` ✓ | `8b9ac03113e9` ✓ |
| controlled_user_evidence | `dab2204ea2cb` | `4cb0f78fee1b` ✓ | `8b9ac03113e9` ✓ |
| controlled_spatial_evidence | `496f628b7b16` | `5a4889bbfe52` ✓ | `ca1fc88f7bd2` ✓ |
| (oauth has no stack digests) | `0e2ea08bbb43` | n/a | n/a |

**0 mismatches across 16 recorded digests.** `implementation_sha`, `controlled_test_sha256`, and
`wheel_test_sha256` also all match their live files.

**Isolation of the round-3 fixes.** `git diff 68b7cae c249e08` = 2 files. Programmatic field-level diff of
`evidence.json` old-vs-new: the *only* changed field across all 9 families is
`controlled_spatial_evidence.recapture_note`. No prior family's value moved.
`git log 496f628..HEAD -- dev/udata-evidence/{Dockerfile,compose.yaml}` → empty (untouched).
`git log 2d5038d..HEAD -- .../models/spatial.py .../services/spatial.py .../wire/spatial.py` → empty
(spatial implementation untouched since the feature commit).

**Percent-encoding exactly once.** Single choke point: `wire/spatial.py` routes every id through
`segment()` → `models/spatial.py:10-11` → `models/_segment.py:12-39` → `quote(value, safe="")`. A repo grep
found no `unquote` and no `%25` in the uData connector surface; the only `unquote`
(`services/datasets.py:279`) is unrelated regex group extraction. No double-encoding path exists.

**500 not laundered into 404.** `errors/catalog.py:252-256` maps `404 → CatalogNotFoundError`; `:271-275`
maps `status_code >= 500 → CatalogUnavailableError` with `capability_state="unavailable"`. Different types,
different safe actions. `SyncSpatialService.error_type` returns `NativeCatalogError` (not a
status-specific class), so the mapping is driven by the observed status and the distinction survives.

**Sync/async parity and the 7-row surface.** `services/spatial.py` defines 7 sync methods (lines 31-59) and
7 async (75-109) with identical names/signatures; `test_spatial_contract_exposes_every_assigned_method_in_both_modes`
asserts `sync_names == async_names == _ASSIGNED_METHODS` (7 names) by reflection over both classes — a real
structural check, not a transcription. `test_every_spatial_operation_dispatches_under_its_own_operation_identity`
(`test_udata_spatial.py:132-157`) asserts exactly 7 spatial requests, all `GET`.

**Cross-document consistency (item 3).** `04-CONFORMANCE.md:127-150` and `04-VALIDATION.md:136-158` now both
say **"Three zone/level targets answer 404 through `get_or_404`, and the zone list answers 500 because the
stock converter raises `KeyError`"**, both ground granularities in `BASE_GRANULARITIES`, and neither retains
"four". Grep for `four zone|granularit.*empty|answers \[\].*granular|traceback|decoded colon` across both
files → **no matches** (exit 1). All three documents now agree with each other and with the code.
Both files are untracked and gitignored (`.gitignore:66 → .planning/`) with `commit_docs: false` — expected,
not a defect.

**Quality gates.** `ruff format --check .` → 455 files already formatted. `ruff check .` → All checks passed.
`ty check .` (with `--all-extras`) → All checks passed. `pytest tests/unit tests/quality` → **2026 passed**
(includes the 987-test catalog/connector/surface subset run separately). No skip regressions.

---

## New findings

### FR-01 — MINOR — the provenance gate skips in every CI run; the invariant is enforced only locally

**File:** `.github/workflows/ci.yaml` (10 `actions/checkout@v7.0.1` steps, none with `fetch-depth: 0`),
interacting with `tests/unit/contracts/catalog/test_udata_profile.py:295-297`.

**Why it is a defect (and why it is only minor):** The round-2 blocker was that a shallow clone made the
test *invert* the invariant and fail wrongly. That is fixed. But the chosen remedy means that in every CI run
on every PR, `test_each_controlled_record_digests_the_files_at_its_introducing_commit` reports SKIPPED — I
reproduced it in a real `--depth 1` clone of this exact HEAD. So the check that exists specifically to catch
SF-01 (a silently falsified evidence digest that a reviewer *did* have to catch, twice) never actually
executes in the pipeline that gates merges. It only runs for a developer running the suite locally in a full
clone. That is a real reduction in enforcement, and it is invisible unless you read the skip reason.

I am **not** scoring this `open`, because the behavior is deliberate, is the strictly safer of the two
available choices, discloses itself loudly with a specific reason and a remedy, and fails loudly (not
silently) in every degenerate case I could construct. A false FAIL that inverts its own invariant — the
SR-01 state — would be worse than an honest skip.

**Concrete fix** (one line, no test logic change, makes the check real in CI):

```yaml
      - name: Checkout code
        uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          # credential persistence stays disabled, exactly as it is today
          fetch-depth: 0
```

Apply to the core `Test (Python …)` job's checkout (line ~102). The other nine jobs do not need it.
Cost is one full-history fetch per matrix leg; benefit is that the SF-01 class of regression becomes
machine-caught instead of reviewer-caught. This belongs to whoever owns CI config, not to plan 04-09, which
is why it is recorded rather than blocked on.

---

## What I could NOT verify

1. **The controlled integration suite and Docker stack were not run** (explicitly out of scope; I did not
   start Docker). So the *runtime* facts in the recapture note — that the live loopback 17.6.0 stack really
   answers 404/404/404/500 and `[poi, other]` — are verified only **statically**, against the pinned
   upstream source and against the assertions the controlled tests make. The tests assert agreement between
   typed and raw (`_assert_spatial_read_matches_raw`, `test_udata_controlled.py:4583-4595`) rather than
   hardcoding 404/500, so a stack that behaved differently would fail, not silently pass — but I did not
   execute them. I also could not verify the seed's *runtime* effect on GeoLevel/GeoZone, only that
   `seed.py` contains no spatial row creation.
2. **The `dbcb9be3a304` / `e95affa5d71d` / `e635dda64e54` / `a9cdabd3de07` / `dab2204ea2cb` /
   `01a4dd80f1c7` / `0e2ea08bbb43` digests are correct** — I verified the digests match, but I did not
   independently re-run those seven families' captures; I am confirming the recorded values equal the files
   at the introducing commit, which is the invariant, not the capture's original truth.
3. **Whether the CI skip is acceptable to the project owner.** FR-01 is a judgment call I made explicit
   rather than resolved; a reviewer who weights automated enforcement over CI cost may reasonably require
   `fetch-depth: 0` before sign-off.
4. **`scripts/extract_udata_oracle.py` was not executed.** I confirmed the script exists and that its
   recorded output (`.planning/.../oracle/udata-oracle-candidate.json`, 268 routes, `status: reconciled`,
   pinned `source_commit`) contains the seven spatial paths matching both upstream and the connector. I did
   not re-run the extraction, so the oracle file's own provenance rests on its recorded input digests.

---

_Reviewed: 2026-10-02T05:13:10Z_
_Reviewer: `cg:codex:reviewer-3f8b21d7-6c04-4a19-b5e2-9d17c8a4e603:04-09:final`_
_Depth: standard, with empirical falsification of the provenance invariant_
