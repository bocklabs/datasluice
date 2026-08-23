"""Typed CKAN mutation outcomes and secret-safe token results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from datasluice.domain.catalog.auth import SecretValue
from datasluice.domain.catalog.models import _freeze_json, _thaw_json
from datasluice.domain.catalog.redaction import REDACTED


@dataclass(frozen=True, slots=True)
class CKANTokenResult:
    """A created CKAN API token whose value is disclosed only through an explicit accessor."""

    result_metadata: Mapping[str, object]
    _secret: SecretValue = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.result_metadata, Mapping):
            raise ValueError("CKAN token result metadata must be a JSON object.")
        frozen = _freeze_json(dict(self.result_metadata), "ckan_token_result.metadata")
        if not isinstance(frozen, Mapping):
            raise ValueError("CKAN token result metadata must be a JSON object.")
        object.__setattr__(self, "result_metadata", MappingProxyType(dict(frozen)))
        if not isinstance(self._secret, SecretValue):
            object.__setattr__(self, "_secret", SecretValue(self._secret))

    @classmethod
    def from_token_result(cls, payload: object) -> CKANTokenResult:
        """Wrap one ``api_token_create`` result mapping into the secret-safe carrier.

        Args:
            payload: The decoded result value of an ``api_token_create`` action.

        Returns:
            A token result carrying every non-token key losslessly.

        Raises:
            ValueError: If the payload is not an object or carries no token.
        """
        if not isinstance(payload, Mapping):
            raise ValueError("CKAN token results must be JSON objects.")
        secret = payload.get("token")
        if not isinstance(secret, str) or not secret:
            raise ValueError("CKAN token results must carry a non-empty token value.")
        metadata = {key: value for key, value in payload.items() if key != "token"}
        return cls(result_metadata=metadata, _secret=SecretValue(secret))

    @property
    def token(self) -> SecretValue:
        """Return the created token as a reveal-only secret."""
        return self._secret

    def reveal(self) -> str:
        """Return the raw token value for authenticated transport boundaries only."""
        return self._secret.reveal()

    def to_dict(self) -> dict[str, object]:
        """Return the redacted serialization form with the token replaced by the marker."""
        payload = _thaw_json(self.result_metadata)
        if not isinstance(payload, dict):
            raise ValueError("CKAN token result metadata must thaw into a JSON object.")
        return {**payload, "token": REDACTED}
