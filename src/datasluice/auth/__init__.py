"""Authentication strategies for DataSluice."""

from datasluice.auth.api_key import APIKeyAuth
from datasluice.auth.base import BaseAuth
from datasluice.auth.basic import BasicAuth
from datasluice.auth.bearer import BearerAuth
from datasluice.auth.headers import HeadersAuth
from datasluice.auth.none import NoAuth

__all__ = [
    "BaseAuth",
    "NoAuth",
    "APIKeyAuth",
    "BearerAuth",
    "BasicAuth",
    "HeadersAuth",
]
