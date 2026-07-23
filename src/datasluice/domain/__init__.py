"""Portal-agnostic domain models for DataSluice."""

from datasluice.domain.credentials import CredentialScope
from datasluice.domain.dataset import Dataset
from datasluice.domain.license import License
from datasluice.domain.organization import Organization
from datasluice.domain.query import Query
from datasluice.domain.resource import Resource
from datasluice.domain.result import SearchResult

__all__ = [
    "CredentialScope",
    "Dataset",
    "License",
    "Organization",
    "Query",
    "Resource",
    "SearchResult",
]
