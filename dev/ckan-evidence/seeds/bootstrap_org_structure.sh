#!/usr/bin/env bash
set -euo pipefail

CREDENTIALS_FILE="${1:?usage: bootstrap_org_structure.sh <credentials-file>}"
COMPOSE_FILE="$(cd "$(dirname "$0")/.." && pwd)/compose.yaml"
EVIDENCE_ORIGIN="${CKAN_EVIDENCE_ORIGIN:-http://127.0.0.1:5500}"
ORG_NAME="datasluice-evidence-org"
ORG_ADMIN="datasluice-org-admin"
DATASETS="datasluice-evidence-ds-one datasluice-evidence-ds-two"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }
api_call() {
  local action="$1" payload="$2" token
  token="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["tokens"]["sysadmin"])' "$CREDENTIALS_FILE")"
  printf 'header = "Authorization: %s"\n' "$token" | curl -sS --config - -H 'Content-Type: application/json' \
    -d "$payload" "${EVIDENCE_ORIGIN}/api/3/action/${action}" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)))'
}
succeeded() {
  echo "$1" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("success") else 1)'
}

for identity in datasluice-sysadmin "$ORG_ADMIN" datasluice-user; do
  compose exec -T ckan ckan user show "$identity" >/dev/null
  echo "identity present: ${identity}"
done

if succeeded "$(api_call organization_show "{\"id\": \"${ORG_NAME}\"}")"; then
  echo "organization present: ${ORG_NAME}"
else
  api_call organization_create "{\"name\": \"${ORG_NAME}\", \"title\": \"DataSluice Evidence Organization\"}" >/dev/null
  echo "organization created: ${ORG_NAME}"
fi

if succeeded "$(api_call organization_member_create "{\"id\": \"${ORG_NAME}\", \"username\": \"${ORG_ADMIN}\", \"role\": \"admin\"}")"; then
  echo "organization admin granted: ${ORG_ADMIN}"
else
  echo "failed to grant organization admin: ${ORG_ADMIN}" >&2
  exit 1
fi

for dataset in $DATASETS; do
  if succeeded "$(api_call package_show "{\"id\": \"${dataset}\"}")"; then
    echo "dataset present: ${dataset}"
    continue
  fi
  payload="$(printf '{"name": "%s", "title": "DataSluice evidence %s", "owner_org": "%s", "resources": [{"name": "evidence resource", "url": "%s/evidence-%s.csv", "format": "CSV"}]}' \
    "$dataset" "$dataset" "$ORG_NAME" "$EVIDENCE_ORIGIN" "$dataset")"
  if succeeded "$(api_call package_create "$payload")"; then
    echo "dataset created with one resource: ${dataset}"
  else
    echo "failed to create dataset: ${dataset}" >&2
    exit 1
  fi
done

echo "organization structure seeding complete (org: ${ORG_NAME}, datasets: ${DATASETS})"
