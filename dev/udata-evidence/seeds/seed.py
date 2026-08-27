"""Validate the bounded local seed target without storing credentials."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from urllib.parse import urlparse

SEED_PROGRAM = """
from datetime import UTC, datetime
from secrets import token_urlsafe

from flask_security.utils import hash_password
from udata.app import create_app, standalone
from udata.core.organization.models import Member
from udata.models import Organization, User, datastore

app = standalone(create_app())
with app.app_context():
    def ensure_user(email, first_name, admin=False):
        user = User.objects(email=email).first()
        if user is None:
            user = datastore.create_user(
                email=email,
                first_name=first_name,
                last_name="Evidence",
                password=hash_password(token_urlsafe(48)),
                confirmed_at=datetime.now(UTC),
            )
        if admin and not user.has_role("admin"):
            datastore.add_role_to_user(user, datastore.find_or_create_role("admin"))
        return user

    ensure_user("administrator@evidence.invalid", "Administrator", admin=True)
    organization_admin = ensure_user("organization-admin@evidence.invalid", "OrganizationAdmin")
    ensure_user("user@evidence.invalid", "User")
    organization = Organization.objects(slug="evidence-organization").first()
    if organization is None:
        organization = Organization(name="Evidence Organization")
        organization.members = [Member(user=organization_admin, role="admin")]
        organization.save()
    elif not organization.is_admin(organization_admin):
        organization.members.append(Member(user=organization_admin, role="admin"))
        organization.save()
""".strip()


def validate_origin(origin: str) -> str:
    """Require the fixed loopback uData evidence origin."""
    parsed = urlparse(origin)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.port != 5640:
        raise ValueError("seeding is limited to http://127.0.0.1:5640")
    return origin


def seed_roles(origin: str, compose_file: Path) -> None:
    """Seed deterministic disposable roles through the local uData container only."""
    validate_origin(origin)
    if not compose_file.is_file():
        raise ValueError("controlled evidence compose file is missing")
    completed = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "exec", "-T", "udata", "python", "-c", SEED_PROGRAM],
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("controlled evidence role seeding failed")


def main() -> int:
    """Validate a local-only seed operation before its controlled implementation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--compose-file", type=Path, default=Path(__file__).parents[1] / "compose.yaml")
    args = parser.parse_args()
    seed_roles(args.origin, args.compose_file)
    print("seeded deterministic local roles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
