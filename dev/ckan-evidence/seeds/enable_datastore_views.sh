#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "$0")/.." && pwd)/compose.yaml"
POSTGRES_USER="ckan"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

if compose exec -T db psql -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='datastore'" | grep -q 1; then
  echo "datastore database present"
else
  echo "datastore database is missing; recreate the evidence stack so postgres-init restores the database, role, and grants" >&2
  exit 1
fi

compose exec -T ckan ckan datastore set-permissions | compose exec -T db psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d datastore
echo "datastore read/write permissions applied"

compose exec -T ckan ckan config-tool /srv/app/ckan.ini "ckan.views.default_views = image_view text_view datatables_view"
compose restart ckan
echo "default resource views configured (image_view text_view datatables_view)"

echo "datastore and resource-view enablement complete (sqlsearch left at its server-side default: OFF)"
