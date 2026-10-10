#!/usr/bin/env bash
set -euo pipefail

CREDENTIALS_FILE="${1:?usage: bootstrap_tokens.sh <credentials-file> (path outside the repository, never committed)}"
COMPOSE_FILE="$(cd "$(dirname "$0")/.." && pwd)/compose.yaml"
ENV_FILE="${2:-$(cd "$(dirname "$0")/.." && pwd)/.env}"
EVIDENCE_ORIGIN="${UDATA_EVIDENCE_ORIGIN:-http://127.0.0.1:5640}"
TOKEN_NAME="datasluice-evidence-capture"

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
CREDENTIALS_PATH="$(python3 -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).expanduser().resolve())' "$CREDENTIALS_FILE")"
if [ -n "$REPO_ROOT" ] && [[ "$CREDENTIALS_PATH" == "$REPO_ROOT"/* ]]; then
  echo "refusing to write credentials inside the repository: ${CREDENTIALS_FILE}" >&2
  exit 1
fi

compose() { docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"; }

# One in-container program issues every token so a single exec mints the set and
# the plaintext never crosses the shell boundary more than once.
issue_tokens() {
  compose exec -T udata python - "$TOKEN_NAME" <<'PYTHON'
import json
import sys

from udata.app import create_app, standalone
from udata.core.api_token.models import ApiToken
from udata.models import User

token_name = sys.argv[1]
roles = {
    "admin": "administrator@evidence.invalid",
    "organization_admin": "organization-admin@evidence.invalid",
    "member": "user@evidence.invalid",
}

app = standalone(create_app())
issued = {}
with app.app_context():
    for role, email in roles.items():
        user = User.objects(email=email).first()
        if user is None:
            raise SystemExit(f"seeded user missing for role {role}: {email}")
        for stale in ApiToken.objects(user=user, name=token_name):
            stale.delete()
        _, plaintext = ApiToken.generate(user, name=token_name)
        issued[role] = plaintext

print(json.dumps(issued))
PYTHON
}

issued_json="$(issue_tokens)"
readarray -t issued_lines <<<"$issued_json"
if [ "${#issued_lines[@]}" -ne 1 ]; then
  echo "token issuance did not return a single JSON record" >&2
  exit 1
fi

umask 077
mkdir -p "$(dirname "$CREDENTIALS_FILE")"
tmp_file="$(mktemp "$(dirname "$CREDENTIALS_FILE")/udata-evidence-credentials.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT

ISSUED_JSON="$issued_json" CREDENTIALS_FILE="$tmp_file" python3 - <<'PYTHON'
import json
import os
from pathlib import Path

issued = json.loads(os.environ["ISSUED_JSON"])
if set(issued) != {"admin", "organization_admin", "member"}:
    raise SystemExit("issued credential set is incomplete")
document = {
    "origin": "http://127.0.0.1:5640",
    "tokens": issued,
}
Path(os.environ["CREDENTIALS_FILE"]).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
PYTHON

chmod 600 "$tmp_file"
mv -f "$tmp_file" "$CREDENTIALS_FILE"
trap - EXIT

echo "credentials written with mode 0600: ${CREDENTIALS_FILE}"
echo "token seeding complete for roles: admin organization_admin member"
