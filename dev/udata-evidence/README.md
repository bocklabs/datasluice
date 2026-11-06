# Controlled uData evidence

This stack is exclusively for disposable, loopback-controlled evidence. It must never be pointed at a public deployment.

1. Create `dev/udata-evidence/.env` with four distinct random disposable local values for `UDATA_SECRET_KEY`, `UDATA_API_TOKEN_SECRET`, `MINIO_ROOT_PASSWORD`, and `UDATA_EVIDENCE_STACK_NONCE`, plus `MINIO_LICENSE_FILE` pointing to an AIStor license file on the host (the storage service refuses to start without it); do not commit the `.env` file.
2. Verify every image digest against approved release evidence, build the app from the exact upstream commit in `Dockerfile`, and start the stack with `docker compose --env-file .env -f dev/udata-evidence/compose.yaml up --build -d`. The stock server runs through `udata serve`; uData 17.6 has no `udata run` command.
3. Seed deterministic administrator, organization-administrator, and regular-user identities plus the evidence organization and its datasets only through `uv run python dev/udata-evidence/seeds/seed.py --origin http://127.0.0.1:5640`.
4. Capture sanitized metadata only with `uv run python scripts/capture_udata_stack_evidence.py --origin http://127.0.0.1:5640 --version 17.6.0 --output /tmp/udata-evidence.json`.
5. Stop and remove the disposable stack after evidence capture with `docker compose --env-file .env -f dev/udata-evidence/compose.yaml down`.

The compose file binds all exposed ports to loopback, uses digest-pinned images, has no persistent volumes, and the capture script stores neither credentials nor request/response bodies. The seed program generates a password inside the disposable container and never writes or prints it.

## File storage

uData 17.6 stores files through `flask-storage` 2.0.0, not PyFilesystem: `Storage.configure` reads `{NAME}_FS_ROOT`, `FS_ROOT` and `FS_URL`, and the default `local` backend resolves `root` to `os.path.join(FS_ROOT, NAME)`. `FS_ROOT` defaults to `<instance_path>/fs`, and because udata is installed with `--system` Flask's `auto_find_instance_path` resolves `instance_path` to `/usr/local/var/udata-instance`. That is why the `udata` service mounts a `/usr/local/var` tmpfs: without it every upload would fail on the read-only root filesystem. Uploads therefore land in `/usr/local/var/udata-instance/fs/resources`, and `read_only: true` plus the tmpfs keeps them off the host.

`FS_URL` is the public base URL each storage prefixes onto the names it was handed; it is never opened as a destination, so no bytes leave the container through it. It is pinned to the same loopback origin compose publishes (`127.0.0.1:5640`) so that every uploaded resource URL captured from this stack resolves to the disposable instance that produced it instead of naming a third-party host. Setting it to a filesystem URL is not meaningful: the value is not parsed as one, and a scheme-less value is rewritten into a malformed `http://` URL. A non-`http(s)` `FS_URL` also forces `Storage.base_url` to read `request.is_secure`, so it needs a request context; the pinned `http://` origin avoids that.

## Compatibility pins

`requirements.in` retains four compatibility pins for the pinned uData commit. A dependency refresh must preserve them:

- `setuptools<81` keeps `pkg_resources` importable at runtime. The compose file's `PYTHONWARNINGS` filter for `pkg_resources is deprecated as an API` exists because that API is still exercised, which is only true while it is importable.
- `bcrypt==4.0.1` avoids the `__about__` removal in bcrypt 4.1+, which passlib reads when it probes the installed bcrypt version.
- `flask-security-too==5.6.1` pins the auth stack the pinned udata commit expects. It is reinstalled with `--no-deps --force-reinstall` after uninstalling `Flask-Security` so the resolved auth stack cannot shift the shared dependencies (Flask, Werkzeug, Jinja2, blinker) out from under udata, and the build asserts `flask_security.utils.get_within_delta` imports.
- `flask-caching==2.3.1` is the udata-compatible cache backend for the same commit.

`requirements.txt` locks all 109 runtime packages, including the transitive dependencies of both auth distributions.
The resolver uses the exact upstream `upstream/pyproject.toml` and all four compatibility pins together.
The Dockerfile checks that the cloned source has the same metadata before installation.
It installs uData with `--no-deps` only after the hash-checked dependency installation succeeds.
The final auth reinstall uses the same hashes through `requirements-auth.txt` and cannot change dependencies.

## Verified build inputs

The locks target CPython 3.13.15 on Linux amd64 with glibc 2.36, matching the pinned Python image.
The initial resolution preserves installed runtime versions from local image
`sha256:f3d5c6619f3f4bcbef6db2ddaabd1893ad4b2c406dc3246d8027cda646638719`.
PyPI supplied the distribution hashes. The image inspection supplied versions, not trusted hashes or build provenance.
All hashes for the 112 unique runtime and build distributions matched their version-specific PyPI JSON records.
On 2026-10-16, a temporary-target installation checked all runtime distributions with `--require-hashes` and `--require-build-hashes`.
Eight source-only dependencies built with hash-checked build tools. A separate full-history source checkout built uData 17.6.0 with the same constraints.
These initial source-only checks did not build or start a Docker image.
On 2026-10-17, the owner approved a complete Linux image build and controlled verification.
The build checked the manifest, signed Debian snapshots, exact apt versions, runtime hashes, and build hashes successfully.
The running service reported version `17.6.0`, and deterministic seeding succeeded.

`build-requirements.txt` locks setuptools, wheel, setuptools-scm, and their transitive dependencies.
Every source build requires hashes through `--build-constraints` and `--require-build-hashes`.
A new undeclared build dependency fails instead of resolving without a hash.
The uv image uses version 0.13.1 because the previous 0.12.5 image lacks this enforcement.
The registry supplied and confirmed the new image digest. No Docker pull occurred.

Debian packages use the signed `20260824T000000Z` snapshots and exact direct version pins.
The snapshot timestamp matches the Debian base layer's source record. The snapshot also fixes transitive apt candidates.
`Check-Valid-Until: no` permits expired snapshot metadata. Debian signature and package-hash checks remain enabled.
The six versions came from the snapshot's amd64 package indexes, not from guessed release numbers.
The compressed index SHA-256 values were:

- `debian/dists/bookworm/main/binary-amd64/Packages.xz`: `9e0b5aabb2465b3d2e7a7fe27f9913846277833f7a2826e7767acccff5b588c5`
- `debian-security/dists/bookworm-security/main/binary-amd64/Packages.xz`: `476b7d393c173a29a40619e3a514f9bb3bb4d01d2d252e2fd117bb3bf941340e`

Regenerate the locks from the repository root with uv 0.13.1:

```bash
uv pip compile dev/udata-evidence/build-requirements.in --python-version 3.13.15 --python-platform x86_64-manylinux_2_36 --only-binary :all: --generate-hashes --no-header --no-annotate --output-file dev/udata-evidence/build-requirements.txt
uv pip compile dev/udata-evidence/upstream/pyproject.toml dev/udata-evidence/requirements.in --python-version 3.13.15 --python-platform x86_64-manylinux_2_36 --build-constraints dev/udata-evidence/build-requirements.txt --require-build-hashes --generate-hashes --no-header --no-annotate --output-file dev/udata-evidence/requirements.txt
```

Copy the `flask-security-too` record from `requirements.txt` to `requirements-auth.txt` after a refresh.
Regenerate `build-inputs.sha256` for its seven listed files with `sha256sum`.
Update the manifest's SHA-256 literal in the Dockerfile. Update the controlled transport's Dockerfile digest after review.
This binds external build inputs through the Dockerfile digest without permitting silent manifest changes.

The pre-remediation local image predates these inputs and has no build-provenance labels.
Its repository name, reported version, and input-file hashes do not prove that its binary used these inputs.
The approved 2026-10-17 build produced image ID `sha256:50a6c57d6452fcdce3da17d31769cb0c72d312fdde21b606320d3d309b5918a6`.
The controlled verifier now requires this exact image ID in addition to the source checks and reported version.
A later rebuild cannot receive approval merely because it uses the same repository name.
Verify a new build independently before changing the approved image ID.

## Seeding controlled credentials

Mint the three controlled evidence roles (admin, organization_admin, member) with one in-container exec so the plaintext crosses the shell boundary exactly once:

```bash
bash dev/udata-evidence/seeds/bootstrap_tokens.sh /tmp/udata-evidence-credentials.json
```

The credentials file is written atomically with mode 0600, and the script refuses any destination inside the repository so issued tokens can never be committed.

## Source and review provenance

Extract source-only route signatures from a detached checkout at `0546582058d84706812a1c37387576efc4e5ad1f` with `uv run python scripts/extract_udata_oracle.py --source-root /path/to/udata --source-output /tmp/udata-source.json`. Reconcile that output only with separately captured Swagger and controlled URL-map documents; a disagreement blocks the preflight.

Capture the two runtime route documents against the running loopback stack:

```bash
docker compose --env-file dev/udata-evidence/.env -f dev/udata-evidence/compose.yaml exec -T udata python -c "<url-map dump command>" > /tmp/udata-urlmap-raw.json
uv run python scripts/capture_udata_route_documents.py swagger --origin http://127.0.0.1:5640 --output /tmp/udata-swagger-routes.json
uv run python scripts/capture_udata_route_documents.py url-map --input /tmp/udata-urlmap-raw.json --output /tmp/udata-urlmap-routes.json
```

The URL-map dump command emits only `{path, method, endpoint}` records for `/api/1`, `/api/2`, and `/oauth` rules. Both captures remove the flask-restx documentation endpoints (`/`, `/swagger.json`), ignore implicit `HEAD`/`OPTIONS` methods, and canonicalize parameter spellings. Generated Swagger cannot express the OAuth blueprint, so reconciliation requires Swagger to match the v1/v2 subset exactly while the URL map must match the full source set. Namespace exclusions such as `/api/1/proconnect` are explicit `--exclude-namespace` scope decisions recorded in the preflight, never silent omissions.

Capture review artifacts under `reviews/<family>/<reviewed-sha>/` with `source-review.md`, `current-thread.json`, and `review-receipt.json`. The checker rejects mutable reuse, self-review, unsafe artifacts, missing classifications, invalid post-fix provenance, digest mismatches, and anything not bound to the current Git HEAD.

Both export forms use `schema_version: 1`, the exact `reviewed_sha`, and a `threads` list in `current-thread.json`:

- A classified export contains one or more findings. Each finding requires `classification: fixed`, `invalid`, or `not_actionable`. Existing classified exports remain valid without migration.
- A clean export contains `threads: []` and a `clean_review` object. Bare empty exports fail. A clean attestation cannot accompany any finding, including an unresolved finding.

The clean attestation requires these fields:

| Field | Required value |
|---|---|
| `verdict` | The exact string `no_findings` |
| `reviewer_id` | The nonblank actual native reviewer ID, equal to the capture and receipt identity |
| `source_review_sha256` | The lowercase SHA-256 digest of the actual `source-review.md` bytes |
| `scope` | A nonempty list of nonblank inspected repository file paths |
| `evidence` | A nonempty list of nonblank source or test references that the reviewer inspected |
| `method` | A nonblank description of the review that occurred |
| `limitations` | Nonblank evidence limits, including the absence of new live checks when applicable |

Capture and verification apply the same attestation checks. Both forms retain receipt schema version 1 and digests for the report and thread bytes.
Both retain author/reviewer separation, exact SHA binding, redaction, safe artifacts, same-family parent linkage, and immutable captures.
Capture checks both copied artifacts before atomic publication. Duplicate SHA directories remain prohibited.

The native run record establishes factual reviewer provenance. A Git author label does not establish reviewer identity or separation.
The parent must compare scope and evidence with the genuine report and native run record.
Machine checks validate structure and binding. They cannot prove that a review occurred.
An invented finding or identity cannot replace genuine evidence. Temporary test fixtures are never certification evidence.
