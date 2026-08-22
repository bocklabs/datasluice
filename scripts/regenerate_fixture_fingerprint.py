"""Regenerate a pinned capability profile's fixture fingerprint after corpus edits.

Usage:
    uv run python scripts/regenerate_fixture_fingerprint.py --platform ckan

Reads the platform's checked-in ``cases.json`` bytes, computes their SHA-256
hexdigest, writes it into that platform's profile JSON ``fixture_fingerprint``
field, and prints the value. The script refuses to change anything when the
strict fixture loader would still fail for any other reason.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datasluice.contracts.catalog.fixtures import load_reference_fixture_set  # noqa: E402

PLATFORMS = ("ckan", "udata", "socrata")
PROFILES_DIR = REPO_ROOT / "src" / "datasluice" / "contracts" / "catalog" / "profiles"
FIXTURES_DIR = REPO_ROOT / "src" / "datasluice" / "contracts" / "catalog" / "fixtures"


def _profile_path(platform: str) -> Path:
    matches = [
        path
        for path in PROFILES_DIR.glob("*.json")
        if json.loads(path.read_text(encoding="utf-8")).get("platform") == platform
    ]
    if len(matches) != 1:
        raise SystemExit(f"Platform {platform!r} must have exactly one pinned profile, found {len(matches)}.")
    return matches[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, choices=PLATFORMS)
    args = parser.parse_args()

    profile_path = _profile_path(args.platform)
    cases_path = FIXTURES_DIR / args.platform / "cases.json"
    original_bytes = profile_path.read_bytes()
    fingerprint = hashlib.sha256(cases_path.read_bytes()).hexdigest()

    profile = json.loads(original_bytes.decode("utf-8"))
    if not isinstance(profile.get("fixture_fingerprint"), str):
        raise SystemExit(f"Profile {profile_path.name} has no string fixture_fingerprint field.")
    profile["fixture_fingerprint"] = fingerprint
    updated_bytes = (json.dumps(profile, indent=2, ensure_ascii=False) + "\n").encode("utf-8")

    profile_path.write_bytes(updated_bytes)
    try:
        load_reference_fixture_set(args.platform)
    except Exception as error:
        profile_path.write_bytes(original_bytes)
        raise SystemExit(
            f"Refusing to regenerate: the {args.platform} fixture set still fails to load ({error})."
        ) from error

    print(fingerprint)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
