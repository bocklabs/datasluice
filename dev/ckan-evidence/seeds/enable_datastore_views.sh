#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "$0")/.." && pwd)/compose.yaml"
POSTGRES_USER="${CKAN_EVIDENCE_DB_USER:-ckan}"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

if compose exec -T db psql -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='datastore'" | grep -q 1; then
  echo "datastore database present"
else
  compose exec -T db psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE datastore OWNER ${POSTGRES_USER}"
  echo "datastore database created"
fi

compose exec -T ckan ckan datastore set-permissions | compose exec -T db psql -U "$POSTGRES_USER" -d datastore
echo "datastore read/write permissions applied"

compose exec -T ckan ckan config-tool /srv/app/ckan.ini "ckan.views.default_views = image_view text_view datatables_view"
echo "default resource views configured (image_view text_view datatables_view)"

echo "datastore and resource-view enablement complete (sqlsearch left at its server-side default: OFF)"
