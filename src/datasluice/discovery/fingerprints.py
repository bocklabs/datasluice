"""Portal type fingerprints for auto-detection.

Each entry maps a fingerprint (URL path or page signature) to a canonical
portal type.  The detector checks these against a portal URL.
"""

from __future__ import annotations

# Ordered list of (check_function, portal_type) pairs.
# Each check function receives the portal URL and fetched HTML/headers and
# returns a confidence score (0.0–1.0).

# Simple path-based fingerprints
PATH_FINGERPRINTS: dict[str, str] = {
    "/api/3/action/package_search": "ckan",
    "/api/3/action/group_list": "ckan",
    "/api/1/datasets/": "datagouv",
    "/api/1/organizations/": "datagouv",
    "/api/catalog/v1": "socrata",
    "/api/views.json": "socrata",
}

# HTML signature substrings (found in <head> or meta tags)
HTML_FINGERPRINTS: dict[str, str] = {
    "ckan": "ckan",
    "data.gouv.fr": "datagouv",
    "udata": "datagouv",
    "socrata": "socrata",
}
