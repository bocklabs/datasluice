# Controlled uData evidence

This stack is exclusively for disposable, loopback-controlled evidence. It must never be pointed at a public deployment.

1. Create `dev/udata-evidence/.env` with four distinct random disposable local values for `UDATA_SECRET_KEY`, `UDATA_API_TOKEN_SECRET`, `MINIO_ROOT_PASSWORD`, and `UDATA_EVIDENCE_STACK_NONCE`, plus `MINIO_LICENSE_FILE` pointing to an AIStor license file on the host (the storage service refuses to start without it); do not commit the `.env` file.
2. Verify every image digest against approved release evidence, build the app from the exact upstream commit in `Dockerfile`, and start the stack with `docker compose --env-file .env -f dev/udata-evidence/compose.yaml up --build -d`. The stock server runs through `udata serve`; uData 17.6 has no `udata run` command.
3. Seed deterministic administrator, organization-administrator, and regular-user roles only through `uv run python dev/udata-evidence/seeds/seed.py --origin http://127.0.0.1:5640`.
4. Capture sanitized metadata only with `uv run python scripts/capture_udata_stack_evidence.py --origin http://127.0.0.1:5640 --version 17.6.0 --output /tmp/udata-evidence.json`.
5. Stop and remove the disposable stack after evidence capture with `docker compose --env-file .env -f dev/udata-evidence/compose.yaml down`.

The compose file binds all exposed ports to loopback, uses digest-pinned images, has no persistent volumes, and the capture script stores neither credentials nor request/response bodies. The seed program generates a password inside the disposable container and never writes or prints it.

## File storage

uData 17.6 stores files through `flask-storage` 2.0.0, not PyFilesystem: `Storage.configure` reads `{NAME}_FS_ROOT`, `FS_ROOT` and `FS_URL`, and the default `local` backend resolves `root` to `os.path.join(FS_ROOT, NAME)`. `FS_ROOT` defaults to `<instance_path>/fs`, and because udata is installed with `--system` Flask's `auto_find_instance_path` resolves `instance_path` to `/usr/local/var/udata-instance`. That is why the `udata` service mounts a `/usr/local/var` tmpfs: without it every upload would fail on the read-only root filesystem. Uploads therefore land in `/usr/local/var/udata-instance/fs/resources`, and `read_only: true` plus the tmpfs keeps them off the host.

`FS_URL` is the public base URL each storage prefixes onto the names it was handed; it is never opened as a destination, so no bytes leave the container through it. It is pinned to the same loopback origin compose publishes (`127.0.0.1:5640`) so that every uploaded resource URL captured from this stack resolves to the disposable instance that produced it instead of naming a third-party host. Setting it to a filesystem URL is not meaningful: the value is not parsed as one, and a scheme-less value is rewritten into a malformed `http://` URL. A non-`http(s)` `FS_URL` also forces `Storage.base_url` to read `request.is_secure`, so it needs a request context; the pinned `http://` origin avoids that.

## Compatibility pins

The pins in the `uv pip install` line are load-bearing for the pinned udata commit and must not be dropped silently by a dependency refresh:

- `setuptools<81` keeps `pkg_resources` importable at runtime. The compose file's `PYTHONWARNINGS` filter for `pkg_resources is deprecated as an API` exists because that API is still exercised, which is only true while it is importable.
- `bcrypt==4.0.1` avoids the `__about__` removal in bcrypt 4.1+, which passlib reads when it probes the installed bcrypt version.
- `flask-security-too==5.6.1` pins the auth stack the pinned udata commit expects. It is reinstalled with `--no-deps --force-reinstall` after uninstalling `Flask-Security` so the resolved auth stack cannot shift the shared dependencies (Flask, Werkzeug, Jinja2, blinker) out from under udata, and the build asserts `flask_security.utils.get_within_delta` imports.
- `flask-caching==2.3.1` is the udata-compatible cache backend for the same commit.

All pins resolve in one `uv pip install` invocation together with `/opt/udata`, so an unsatisfiable constraint between udata and these packages fails the build loudly instead of being silently resolved by a second transaction.

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
