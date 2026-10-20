from __future__ import annotations

from urllib.parse import SplitResult, urlsplit


def parse_origin(value: str, platform: str) -> SplitResult:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{platform} client settings require a non-empty base URL string.")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{platform} base URLs must be absolute HTTP(S) origins.")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(f"{platform} base URLs cannot carry userinfo credentials.")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{platform} base URLs cannot carry query strings or fragments.")
    return parsed
