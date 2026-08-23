#!/bin/bash
# Create the separate datastore database and its read-only role (upstream
# ckan-docker pattern). CKAN's datastore extension refuses same write/read URLs,
# so the read side must authenticate as datastore_ro against the datastore db.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE datastore OWNER ckan ENCODING 'utf-8';
    CREATE ROLE datastore_ro WITH LOGIN PASSWORD '${DATASTORE_RO_PASSWORD}' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    GRANT ALL PRIVILEGES ON DATABASE datastore TO ckan;
EOSQL
