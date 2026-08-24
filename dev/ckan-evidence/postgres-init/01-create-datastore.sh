#!/bin/bash
# Create the separate datastore database and its read-only role (upstream
# ckan-docker pattern). CKAN's datastore extension refuses same write/read URLs,
# so the read side must authenticate as datastore_ro against the datastore db.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v datastore_ro_password="$DATASTORE_RO_PASSWORD" <<-EOSQL
    CREATE DATABASE datastore OWNER ${POSTGRES_USER} ENCODING 'utf-8';
    CREATE ROLE datastore_ro WITH LOGIN PASSWORD :'datastore_ro_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    GRANT ALL PRIVILEGES ON DATABASE datastore TO ${POSTGRES_USER};
EOSQL
