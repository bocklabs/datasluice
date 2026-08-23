#!/usr/bin/env bash
set -euo pipefail

CREDENTIALS_FILE="${1:?usage: bootstrap_identities.sh <credentials-file> (path outside the repository, never committed)}"
COMPOSE_FILE="$(cd "$(dirname "$0")/.." && pwd)/compose.yaml"
EVIDENCE_ORIGIN="${CKAN_EVIDENCE_ORIGIN:-http://127.0.0.1:5500}"
IDENTITIES="datasluice-sysadmin datasluice-org-admin datasluice-user"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

# ckan user show exits 0 even for missing users, so presence is checked in the DB.
identity_exists() {
  local name="$1"
  [ "$(compose exec -T db psql -U ckan -d ckan -tAc "SELECT count(*) FROM \"user\" WHERE name = '${name}'")" = "1" ]
}

ensure_user() {
  local name="$1"
  if identity_exists "$name"; then
    echo "identity present: ${name}"
    return 0
  fi
  openssl rand -base64 24 | compose exec -T ckan sh -c 'ckan user add "$1" email="$1@datasluice.invalid" password="$(cat)"' sh "$name"
  echo "identity created: ${name}"
}

issue_token() {
  local name="$1" token_name="$2" token
  compose exec -T ckan ckan user token revoke "$name" "$token_name" >/dev/null 2>&1 || true
  # Token output goes to stdout; container log lines go to stderr under -T only
  # sometimes, so take the last line that looks like a JWT.
  token="$(compose exec -T ckan ckan user token add "$name" "$token_name" 2>/dev/null | grep -Eo 'eyJ[A-Za-z0-9_.-]+' | tail -n 1)"
  if [ -z "$token" ]; then
    echo "failed to issue token for ${name}" >&2
    exit 1
  fi
  printf '%s' "$token"
}

for identity in $IDENTITIES; do
  ensure_user "$identity"
done

compose exec -T db psql -U ckan -d ckan -c "UPDATE \"user\" SET sysadmin = true WHERE name = 'datasluice-sysadmin' AND sysadmin = false;" >/dev/null
echo "sysadmin role granted: datasluice-sysadmin"

SYSADMIN_TOKEN="$(issue_token datasluice-sysadmin datasluice-evidence-capture)"
ORG_ADMIN_TOKEN="$(issue_token datasluice-org-admin datasluice-evidence-capture)"
USER_TOKEN="$(issue_token datasluice-user datasluice-evidence-capture)"

umask 077
mkdir -p "$(dirname "$CREDENTIALS_FILE")"
{
  printf '{\n'
  printf '  "origin": "%s",\n' "$EVIDENCE_ORIGIN"
  printf '  "tokens": {\n'
  printf '    "sysadmin": "%s",\n' "$SYSADMIN_TOKEN"
  printf '    "org_admin": "%s",\n' "$ORG_ADMIN_TOKEN"
  printf '    "user": "%s"\n' "$USER_TOKEN"
  printf '  }\n'
  printf '}\n'
} > "$CREDENTIALS_FILE"
chmod 600 "$CREDENTIALS_FILE"

echo "credentials written with mode 0600: ${CREDENTIALS_FILE}"
echo "identity seeding complete (presence asserted for: ${IDENTITIES})"
