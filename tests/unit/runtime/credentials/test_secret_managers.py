"""Tests for opt-in secret-manager credential discovery."""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import cast

import pytest

from datasluice.domain.catalog.auth import CKANCredential, CredentialSource, SecretValue, SocrataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import CredentialResolutionError
from datasluice.runtime.credentials.aws import AwsSecretsManagerProvider
from datasluice.runtime.credentials.vault import VaultClientFactory, VaultCredentialProvider


def test_missing_boto3_names_the_aws_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "boto3", None)

    with pytest.raises(ImportError, match=r"datasluice\[secrets-aws\]"):
        AwsSecretsManagerProvider("datasluice/ckan").discover(CatalogPlatform.CKAN, {})


def test_aws_json_secret_discovers_secret_value() -> None:
    calls: list[tuple[str, str | None]] = []

    def client_factory(region: str | None) -> _AwsClient:
        calls.append(("factory", region))
        return _AwsClient('{"api_token": "aws-secret"}', calls)

    discovered = AwsSecretsManagerProvider(
        "datasluice/ckan", region="eu-central-1", client_factory=client_factory
    ).discover(CatalogPlatform.CKAN, {})

    credential = discovered[CredentialSource.SECRET_MANAGER]
    assert calls == [("factory", "eu-central-1"), ("get_secret_value", "datasluice/ckan")]
    assert isinstance(credential, CKANCredential)
    assert isinstance(credential.api_token, SecretValue)
    assert credential.api_token.reveal() == "aws-secret"


def test_missing_hvac_names_the_vault_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "hvac", None)

    with pytest.raises(ImportError, match=r"datasluice\[secrets-vault\]"):
        _vault_provider().discover(CatalogPlatform.CKAN, {})


def test_aws_numeric_scalar_secret_falls_back_to_plain_text() -> None:
    discovered = AwsSecretsManagerProvider(
        "datasluice/ckan", client_factory=lambda region: _AwsClient("123456789", [])
    ).discover(CatalogPlatform.CKAN, {})

    credential = discovered[CredentialSource.SECRET_MANAGER]
    assert isinstance(credential, CKANCredential)
    assert isinstance(credential.api_token, SecretValue)
    assert credential.api_token.reveal() == "123456789"


def test_aws_boolean_scalar_secret_falls_back_to_plain_text() -> None:
    discovered = AwsSecretsManagerProvider(
        "datasluice/ckan", client_factory=lambda region: _AwsClient("true", [])
    ).discover(CatalogPlatform.CKAN, {})

    credential = discovered[CredentialSource.SECRET_MANAGER]
    assert isinstance(credential, CKANCredential)
    assert isinstance(credential.api_token, SecretValue)
    assert credential.api_token.reveal() == "true"


def test_aws_secret_binary_only_responses_are_rejected() -> None:
    class _BinaryOnlyClient:
        def get_secret_value(self, *, SecretId: str) -> dict[str, bytes]:
            return {"SecretBinary": b"aws-secret"}

    with pytest.raises(CredentialResolutionError, match=r"details redacted: \*\*\*"):
        AwsSecretsManagerProvider("datasluice/ckan", client_factory=lambda region: _BinaryOnlyClient()).discover(
            CatalogPlatform.CKAN, {}
        )


def test_aws_json_secrets_missing_required_fields_are_rejected() -> None:
    with pytest.raises(CredentialResolutionError, match=r"details redacted: \*\*\*"):
        AwsSecretsManagerProvider(
            "datasluice/ckan", client_factory=lambda region: _AwsClient('{"username": "someone"}', [])
        ).discover(CatalogPlatform.CKAN, {})


def test_vault_kv_v1_envelopes_are_rejected() -> None:
    client_factory = cast(VaultClientFactory, lambda url, token: _VaultClient({"data": {"app_token": "vault-token"}}))

    with pytest.raises(CredentialResolutionError, match=r"details redacted: \*\*\*"):
        _vault_provider(client_factory=client_factory).discover(CatalogPlatform.CKAN, {})


def test_vault_double_nested_secret_discovers_secret_values() -> None:
    def client_factory(url: str, token: str) -> object:
        return _VaultClient(
            {"data": {"data": {"app_token": "vault-app-token", "username": "reader", "password": "vault-password"}}}
        )

    discovered = _vault_provider(client_factory=cast(VaultClientFactory, client_factory)).discover(
        CatalogPlatform.SOCRATA, {}
    )

    credential = discovered[CredentialSource.SECRET_MANAGER]
    assert isinstance(credential, SocrataCredential)
    assert isinstance(credential.app_token, SecretValue)
    assert credential.app_token.reveal() == "vault-app-token"
    assert credential.username == "reader"
    assert credential.password is not None
    assert isinstance(credential.password, SecretValue)
    assert credential.password.reveal() == "vault-password"


def test_vault_provider_passes_configured_url_token_and_requested_path() -> None:
    """The provider forwards its configured url/token and requested path/mount to the client."""
    calls: list[tuple[str, object]] = []

    def client_factory(url: str, token: str) -> _VaultClient:
        calls.append(("client", (url, token)))
        return _VaultClient({"data": {"data": {"api_token": "vault-app-token"}}}, calls)

    discovered = VaultCredentialProvider(
        url="https://vault.example",
        token="vault-token",
        mount_point="secret",
        path="datasluice/ckan",
        client_factory=cast(VaultClientFactory, client_factory),
    ).discover(CatalogPlatform.CKAN, {})

    credential = discovered[CredentialSource.SECRET_MANAGER]
    assert isinstance(credential, CKANCredential)
    assert isinstance(credential.api_token, SecretValue)
    assert credential.api_token.reveal() == "vault-app-token"
    assert calls == [
        ("client", ("https://vault.example", "vault-token")),
        ("read_secret_version", {"path": "datasluice/ckan", "mount_point": "secret"}),
    ]


@pytest.mark.parametrize(
    ("source", "secret"),
    [
        ("aws", "aws-secret"),
        ("vault", "vault-token"),
    ],
)
def test_secret_manager_failures_are_redacted(source: str, secret: str) -> None:
    provider = (
        AwsSecretsManagerProvider("datasluice/ckan", client_factory=lambda region: _FailingAwsClient())
        if source == "aws"
        else _vault_provider(client_factory=cast(VaultClientFactory, lambda url, token: _FailingVaultClient()))
    )

    with pytest.raises(CredentialResolutionError, match=r"details redacted: \*\*\*") as exc_info:
        provider.discover(CatalogPlatform.CKAN, {})

    message = str(exc_info.value)
    assert secret not in message
    assert "https://vault.example" not in message


class _AwsClient:
    def __init__(self, secret: str, calls: list[tuple[str, str | None]]) -> None:
        self._secret = secret
        self._calls = calls

    def get_secret_value(self, *, SecretId: str) -> dict[str, str]:
        self._calls.append(("get_secret_value", SecretId))
        return {"SecretString": self._secret}


class _VaultClient:
    def __init__(self, response: dict[str, object], calls: list[tuple[str, object]] | None = None) -> None:
        self._response = response
        self._calls = calls
        self.secrets = SimpleNamespace(kv=SimpleNamespace(v2=SimpleNamespace(read_secret_version=self._read)))

    def _read(self, **kwargs: object) -> dict[str, object]:
        if self._calls is not None:
            self._calls.append(("read_secret_version", kwargs))
        return self._response


class _FailingAwsClient:
    def get_secret_value(self, *, SecretId: str) -> dict[str, str]:
        raise RuntimeError("aws-secret")


class _FailingVaultClient:
    secrets = SimpleNamespace(
        kv=SimpleNamespace(
            v2=SimpleNamespace(read_secret_version=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("vault-token")))
        )
    )


def _vault_provider(client_factory: VaultClientFactory | None = None) -> VaultCredentialProvider:
    return VaultCredentialProvider(
        url="https://vault.example",
        token="vault-token",
        mount_point="kv",
        path="datasluice/ckan",
        client_factory=client_factory,
    )
