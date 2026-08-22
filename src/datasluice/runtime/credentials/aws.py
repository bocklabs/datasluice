"""Opt-in AWS Secrets Manager credential discovery."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Protocol, cast

from datasluice.domain.catalog.auth import CatalogCredential, CredentialSource
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.runtime.credentials import _resolution_error, credential_from_fields, credential_from_secret


class AwsSecretsManagerClient(Protocol):
    """Synchronous subset of an AWS Secrets Manager client."""

    def get_secret_value(self, *, SecretId: str) -> Mapping[str, object]:
        """Return one secret response."""


type AwsClientFactory = Callable[[str | None], AwsSecretsManagerClient]


class AwsSecretsManagerProvider:
    """Discover one platform credential from caller-selected AWS Secrets Manager data."""

    def __init__(
        self, secret_id: str, *, region: str | None = None, client_factory: AwsClientFactory | None = None
    ) -> None:
        self._secret_id = secret_id
        self._region = region
        self._client_factory = client_factory

    def discover(
        self,
        platform: CatalogPlatform,
        context: Mapping[str, object],
    ) -> Mapping[CredentialSource, CatalogCredential]:
        """Read the configured string secret through an injectable synchronous client."""
        del context
        try:
            response = (self._client_factory or _aws_client_factory())(self._region).get_secret_value(
                SecretId=self._secret_id
            )
            secret = _secret_string(response)
            credential = _credential_from_aws_secret(platform, secret)
        except ImportError:
            raise
        except Exception as exc:
            raise _resolution_error("AWS Secrets Manager", platform, exc) from exc
        return {CredentialSource.SECRET_MANAGER: credential}


def _aws_client_factory() -> AwsClientFactory:
    try:
        import boto3
    except ImportError as exc:
        message = "AWS secret discovery requires `uv sync --extra secrets-aws` (datasluice[secrets-aws])."
        raise ImportError(message) from exc

    def create_client(region: str | None) -> AwsSecretsManagerClient:
        return cast(AwsSecretsManagerClient, boto3.client("secretsmanager", region_name=region))

    return create_client


def _secret_string(response: object) -> str:
    if not isinstance(response, Mapping) or not isinstance(secret := response.get("SecretString"), str):
        raise ValueError("AWS Secrets Manager must return a string SecretString.")
    return secret


def _credential_from_aws_secret(platform: CatalogPlatform, secret: str) -> CatalogCredential:
    try:
        parsed = json.loads(secret)
    except json.JSONDecodeError:
        return credential_from_secret(platform, secret)
    if isinstance(parsed, Mapping):
        return credential_from_fields(platform, cast(Mapping[str, object], parsed))
    if isinstance(parsed, str):
        return credential_from_secret(platform, parsed)
    if parsed is None or isinstance(parsed, bool | int | float):
        return credential_from_secret(platform, secret)
    raise ValueError("AWS Secrets Manager JSON secrets must be an object, string, or scalar.")


__all__ = ("AwsSecretsManagerProvider",)
