from __future__ import annotations

import pytest

from datasluice._uri import sanitize_uri, validate_http_target


def test_strips_userinfo_from_netloc() -> None:
    assert sanitize_uri("https://AKIA123:TOPSECRET@s3.example.com/bucket/key") == "https://s3.example.com/bucket/key"


def test_redacts_sensitive_query_values() -> None:
    sanitized = sanitize_uri("https://api.example.test/x?api_key=TOPSECRET&format=csv")
    assert "TOPSECRET" not in sanitized
    assert "api_key=***" in sanitized
    assert "format=csv" in sanitized


def test_redacts_known_secret_query_keys() -> None:
    for key in (
        "token",
        "access_token",
        "signature",
        "credential",
        "X-API-Key",
        "X-Amz-Signature",
        "X-Amz-Credential",
        "X-Amz-Security-Token",
        "sig",
        "X-Goog-Signature",
        "X-Goog-Credential",
    ):
        sanitized = sanitize_uri(f"https://api.example.test/x?{key}=secret-value")
        assert sanitized == f"https://api.example.test/x?{key}=***", key


def test_strips_userinfo_and_drops_invalid_port() -> None:
    assert sanitize_uri("https://user:pw@host:notaport/x?token=s") == "https://host/x?token=***"


def test_redacts_sensitive_query_values_on_file_uris() -> None:
    sanitized = sanitize_uri("file:///tmp/source.csv?api_key=secret-value")
    assert sanitized == "file:///tmp/source.csv?api_key=***"


def test_preserves_port() -> None:
    sanitized = sanitize_uri("https://api.example.test:8443/x?api_key=verylongsecret")
    assert sanitized.startswith("https://api.example.test:8443/")
    assert "verylongsecret" not in sanitized


def test_returns_non_uri_strings_unchanged() -> None:
    assert sanitize_uri("/bare/path/state.json") == "/bare/path/state.json"
    assert sanitize_uri("not-a-uri-at-all") == "not-a-uri-at-all"


def test_handles_uri_without_query() -> None:
    assert sanitize_uri("https://api.example.test/x") == "https://api.example.test/x"


def test_preserves_ipv6_brackets_with_port() -> None:
    assert sanitize_uri("https://[::1]:8443/path") == "https://[::1]:8443/path"


def test_preserves_ipv6_brackets_without_port() -> None:
    assert sanitize_uri("https://[::1]/path") == "https://[::1]/path"


def test_preserves_fragment() -> None:
    assert sanitize_uri("https://api.example.test/x#section") == "https://api.example.test/x#section"


@pytest.mark.parametrize("value", ["http://example.com:0/", "http://example.com:65536/"])
def test_rejects_unusable_http_port(value: str) -> None:
    with pytest.raises(ValueError, match="port"):
        validate_http_target(value)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/data",
        "http://2130706433/data",
        "http://0x7f000001/data",
        "http://0x7f.0.0.1/data",
        "http://0177.0.0.1/data",
        "http://127.1/data",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/data",
        "http://metadata.internal/data",
        "http://LOCALHOST/data",
    ],
)
def test_validate_http_target_rejects_private_and_shorthand_hosts(url: str) -> None:
    with pytest.raises(ValueError, match="locality"):
        validate_http_target(url)
    assert validate_http_target(url, allow_private=True) == url


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("https://user:pass@example.com/x", "userinfo"),
        ("ftp://example.com/x", "scheme"),
        ("http:///x", "host"),
        ("http://example.com/x y", "format"),
        ("http://h:0/x", "port"),
    ],
)
def test_validate_http_target_rejects_malformed_targets(url: str, reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        validate_http_target(url)


def test_validate_http_target_allows_public_targets() -> None:
    assert validate_http_target("https://api.example.com/x?q=1") == "https://api.example.com/x?q=1"
    assert validate_http_target("http://127.0.0.1:8080/x", allow_private=True) == "http://127.0.0.1:8080/x"
