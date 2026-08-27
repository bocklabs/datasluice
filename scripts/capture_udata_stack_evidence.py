"""Record only sanitized metadata from the loopback uData evidence stack."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import urlparse


def sanitize_origin(origin: str) -> str:
    """Accept only loopback origins and remove paths, credentials, and queries."""
    parsed = urlparse(origin)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"127.0.0.1", "::1", "localhost"}:
        raise ValueError("evidence origin must be a loopback HTTP(S) origin")
    if parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("evidence origin cannot contain credentials, paths, queries, or fragments")
    return f"{parsed.scheme}://{parsed.netloc}"


def write_capture(origin: str, version: str, output: Path) -> None:
    """Write a minimal response-free evidence record."""
    safe_origin = sanitize_origin(origin)
    if version != "17.6.0":
        raise ValueError("controlled evidence requires exact uData version 17.6.0")
    record = {
        "schema_version": 1,
        "origin_digest": hashlib.sha256(safe_origin.encode()).hexdigest(),
        "version": version,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    """Run the metadata-only capture command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    write_capture(args.origin, args.version, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
