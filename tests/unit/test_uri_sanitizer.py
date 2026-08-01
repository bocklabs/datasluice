"""Tests for the URI display sanitizer (CR-07)."""

from __future__ import annotations

from datasluice._uri import sanitize_uri


def test_strips_userinfo_from_netloc() -> None:
    assert sanitize_uri("https://AKIA123:TOPSECRET@s3.example.com/bucket/key") == "https://s3.example.com/bucket/key"


def test_redacts_sensitive_query_values() -> None:
    sanitized = sanitize_uri("https://api.example.test/x?api_key=TOPSECRET&format=csv")
    assert "TOPSECRET" not in sanitized
    assert "api_key=***" in sanitized
    assert "format=csv" in sanitized


def test_redacts_known_secret_query_keys() -> None:
    for key in ("token", "access_token", "signature", "credential", "X-API-Key"):
        sanitized = sanitize_uri(f"https://api.example.test/x?{key}=secret-value")
        assert "secret-value" not in sanitized, key


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
