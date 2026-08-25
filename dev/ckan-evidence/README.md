# ckan-evidence: controlled mutation stack (D-10/D-24)

This compose project stands up the controlled CKAN evidence stack used to capture
the D-20/D-25 hard-gate evidence (real destructive receipts and a real bulk run)
into the versioned fixture corpus. It is **manual-run only** and must **never be
wired into PR CI in any form** (QUAL-02/D-10): every test that runs on a PR reads
captured fixtures only.

## Topology (D-24)

| Service | Image | Float policy |
|---------|-------|--------------|
| ckan | `ckan/ckan-base:2.11.5` | Bump the tag to the latest 2.11.x before each bring-up; record what resolved |
| solr | `ckan/ckan-solr:2.11-solr9` | Pinned per the upstream ckan-docker naming for the 2.11 line |
| db | `postgres:17` | Official image, current stable |
| redis | `redis:8` | Official image, current stable |

- CKAN **floats to the latest 2.11.x** at implementation time while the capability
  profile stays pinned at `2.11.5` — the stack doubles as an early drift detector
  for the 2.11 line. Any behavioral difference observed here is useful drift signal.
- Extensions: **datastore + resource views enabled** (`CKAN_PLUGINS=envvars
  image_view text_view datatables_view datastore activity`), **sqlsearch left at
  its server-side default OFF** (the `ckan.datastore.sqlsearch.enabled` gate is
  never set) so optional-tier probes observe a real enabled/disabled split.
- All published ports bind to `127.0.0.1` only. There is no host-network mode.
- No password or secret literals exist in this project; every secret is generated
  at bring-up time (`openssl rand`) and lives only in the container environment or
  the 0600 credentials file.

## Manual lifecycle (up → seeds → capture → down -v)

Run everything from this directory (`dev/ckan-evidence/`). Docker Engine and
Compose are prerequisites; nothing here is executed by CI.

### 1. Bring the stack up

```bash
export POSTGRES_PASSWORD="$(openssl rand -hex 16)"
export CKAN_BEAKER_SESSION_SECRET="$(openssl rand -hex 16)"
export CKAN_WTF_CSRF_SECRET_KEY="$(openssl rand -hex 16)"
docker compose up -d
```

The first pull resolves the image tags within the 2.11.x float. **Record the
resolved tags and digests immediately** — the capture driver embeds them in every
artifact, and the phase summary must observe them:

```bash
docker image inspect --format '{{.RepoTags}} {{index .RepoDigests 0}}' \
  ckan/ckan-base:2.11.5 ckan/ckan-solr:2.11-solr9 postgres:17 redis:8
```

Wait for health: `docker compose ps` shows all four services healthy (CKAN's
first boot migrates the database and can take a minute or two).

### 2. Seed identities and structure

```bash
CREDENTIALS=/tmp/ckan-evidence-credentials.json   # path OUTSIDE this repository
./seeds/bootstrap_identities.sh "$CREDENTIALS"    # prints only the path; never a secret
./seeds/bootstrap_org_structure.sh "$CREDENTIALS"
./seeds/enable_datastore_views.sh
```

- `bootstrap_identities.sh` creates exactly the three deterministic DataSluice
  identities (`datasluice-sysadmin` with the sysadmin role, `datasluice-org-admin`,
  `datasluice-user`), generates fresh passwords via `openssl rand` (never literals,
  never printed, passed into the container over stdin), issues API tokens, and
  writes `{origin, tokens by role}` to the credentials file with **mode 0600**.
  Re-runs are idempotent at the observable level: identities are asserted present,
  prior capture tokens are revoked before fresh ones are issued.
- `bootstrap_org_structure.sh` seeds one organization owned by the org-admin with
  two datasets carrying one resource each. It reads the sysadmin token from the
  credentials file and passes it to curl through a stdin config (`curl -K -`), so
  no secret ever appears on a command line.
- `enable_datastore_views.sh` creates the datastore database if missing, applies
  `ckan datastore set-permissions`, and configures the default resource views.
  Default resource views apply to resources created after the config change; the
  seeded datasets above may be re-created (delete + re-run the seed) if default
  views on them are ever needed.

### 3. Run the capture driver

From the **repository root**:

```bash
uv run python scripts/capture_stack_evidence.py \
  --origin http://127.0.0.1:5500 \
  --credentials-file "$CREDENTIALS" \
  --bulk-count 5 \
  --out-dir /tmp/ckan-evidence-artifacts
```

The driver reads credentials only from the 0600 file (there are no token flags),
builds clients with the explicit `probe_policy="declared-baseline"` loopback
posture (the HTTPS validator stays untouched — Pitfall 4 resolution), performs
real destructive mutations through typed methods with confirmed policies
(dataset/organization/group purge families plus per-tier forbidden captures), and
runs the bulk create+delete flow through the runtime `BulkExecutor`
single-threaded. Every artifact records provenance: resolved image digests (via
`docker image inspect` when available) and the CKAN-reported version string.

### 4. Tear everything down

```bash
docker compose down -v
rm -f "$CREDENTIALS"
```

`down -v` prunes the named volumes; deleting the credentials file completes the
documented teardown. Confirm with `docker compose ps` (no running services) and
that the credentials file is gone before walking away.

## Capturing the sqlsearch drift signal (optional, per Pitfall 5)

With the stack up, one anonymous read of the unregistered action shows the wire
truth for the D-02 classification:

```bash
curl -sS http://127.0.0.1:5500/api/3/action/datastore_search_sql -d '{"sql":"SELECT 1"}'
```

The observed unknown-action envelope is compared against
`classify_probe_response` when folding evidence into the corpus; if the shape
differs from the canned assumption, `classify_probe_response` is adjusted and the
observed shape lands as a fixture row (stack-as-drift-detector per D-24).

## Never in CI

No GitHub Actions workflow references this directory, these images, or this
stack. `grep -rn "ckan-evidence" .github/workflows/` must stay empty. Consuming
tests read the captured fixtures under
`src/datasluice/contracts/catalog/fixtures/ckan/` only (QUAL-02/D-10).
