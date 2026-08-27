# Controlled uData evidence

This stack is exclusively for disposable, loopback-controlled evidence. It must never be pointed at a public deployment.

1. Create `dev/udata-evidence/.env` with three distinct random disposable local values for `UDATA_SECRET_KEY`, `UDATA_API_TOKEN_SECRET`, and `MINIO_ROOT_PASSWORD`; do not commit it.
2. Verify every image digest against approved release evidence, build the app from the exact upstream commit in `Dockerfile`, and start the stack with `docker compose --env-file .env -f dev/udata-evidence/compose.yaml up --build -d`.
3. Seed deterministic administrator, organization-administrator, and regular-user roles only through `uv run python dev/udata-evidence/seeds/seed.py --origin http://127.0.0.1:5640`.
4. Capture sanitized metadata only with `uv run python scripts/capture_udata_stack_evidence.py --origin http://127.0.0.1:5640 --version 17.6.0 --output /tmp/udata-evidence.json`.
5. Stop and remove the disposable stack after evidence capture with `docker compose --env-file .env -f dev/udata-evidence/compose.yaml down`.

The compose file binds all exposed ports to loopback, uses digest-pinned images, has no persistent volumes, and the capture script stores neither credentials nor request/response bodies. The seed program generates a password inside the disposable container and never writes or prints it.

## Source and review provenance

Extract source-only route signatures from a detached checkout at `0546582058d84706812a1c37387576efc4e5ad1f` with `uv run python scripts/extract_udata_oracle.py --source-root /path/to/udata --source-output /tmp/udata-source.json`. Reconcile that output only with separately captured Swagger and controlled URL-map documents; a disagreement blocks the preflight.

Capture review artifacts under `reviews/<family>/<reviewed-sha>/` with `source-review.md`, `current-thread.json`, and `review-receipt.json`. The checker rejects mutable reuse, self-review, unsafe artifacts, missing classifications, invalid post-fix provenance, digest mismatches, and anything not bound to the current Git HEAD.
