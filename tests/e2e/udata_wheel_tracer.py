import asyncio
import json
import secrets
import sys
from io import BytesIO
from typing import cast

import datasluice
from datasluice.connectors.catalog.udata.clients import create_async_client, create_sync_client, declared_udata_profile
from datasluice.connectors.catalog.udata.models.activity_discussions import (
    ActivityQuery,
    CommentInput,
    DiscussionCreateInput,
    DiscussionSearchQuery,
    DiscussionUpdateInput,
)
from datasluice.connectors.catalog.udata.models.contact_visualization import (
    ContactPointCreateInput,
    ContactPointUpdateInput,
    VisualizationCreateInput,
    VisualizationImageInput,
    VisualizationUpdateInput,
)
from datasluice.connectors.catalog.udata.models.datasets import DatasetCreateInput
from datasluice.connectors.catalog.udata.models.jobs import CrontabSchedule, JobScheduleInput
from datasluice.connectors.catalog.udata.models.oauth import (
    OAuthClientRequest,
    OAuthRevokeRequest,
    OAuthTokenRequest,
)
from datasluice.connectors.catalog.udata.models.organizations import OrganizationCreateInput, OrganizationUpdateInput
from datasluice.connectors.catalog.udata.models.posts_reports import (
    NotificationQuery,
    PostListQuery,
    PostSearchQuery,
    ReportCreateInput,
    ReportUpdateInput,
)
from datasluice.connectors.catalog.udata.models.resources import ResourceCreateInput, ResourceUploadInput
from datasluice.connectors.catalog.udata.models.reuses import ReuseCreateInput, ReuseSuggestQuery
from datasluice.connectors.catalog.udata.models.spatial import SpatialDatasetQuery, SpatialSuggestQuery
from datasluice.connectors.catalog.udata.models.taxonomies import BadgeCreateInput, SuggestQuery
from datasluice.connectors.catalog.udata.models.topics import (
    TopicCreateInput,
    TopicElementInput,
    TopicElementLink,
    TopicElementsCreateInput,
    TopicUpdateInput,
)
from datasluice.connectors.catalog.udata.models.users import ApiTokenCreateInput
from datasluice.connectors.catalog.udata.probes import UDataVersionError
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError
from datasluice.runtime.transport.base import AsyncRuntimeStreamResponse, RuntimeResponse, RuntimeStreamResponse

assert datasluice.__file__ and datasluice.__file__.startswith(sys.argv[1])

op_id = next(
    op for op in declared_udata_profile().operations if op.method == "dataset-list-search-show-create-update-delete"
)

JSON_HEADERS = {"Content-Type": "application/json"}


def _json(payload):
    """Return a JSON body and its response headers."""
    return json.dumps(payload).encode(), dict(JSON_HEADERS)


def _bare_json(payload):
    """Return a JSON body with no response headers."""
    return json.dumps(payload).encode(), {}


def _html():
    """Return the empty HTML body the OAuth pages answer with."""
    return b"\n", {"Content-Type": "text/html; charset=utf-8"}


def _empty():
    """Return an empty body and no response headers."""
    return b"", {}


def _post_page():
    """Return the payload the stubbed post listing answers with."""
    return {
        "data": [{"id": "wheel-post", "name": "Wheel post"}],
        "next_page": None,
        "page": 1,
        "page_size": 20,
        "previous_page": None,
        "total": 1,
    }


def _assert_token_material_absent(result, minted, label):
    """Reveal the one-time token and prove the minted plaintext is retained nowhere.

    The serialized mapping never carried a ``token`` key, so the property worth
    proving is that the plaintext the transport minted survives in no surface a
    caller can inspect: the receipt, the audit metadata, or any repr.
    """
    revealed = result.secret.reveal_once()
    assert revealed == minted, label
    surfaces = {
        "to_dict": json.dumps(result.to_dict()),
        "receipt": json.dumps(result.receipt.to_dict()),
        "audit_metadata": repr(result.receipt.audit_metadata),
        "result repr": repr(result),
        "secret repr": repr(result.secret),
    }
    leaked = sorted(name for name, surface in surfaces.items() if minted in surface)
    assert not leaked, f"{label} retained the minted token in {leaked}"


def _site_response(transport, url, method):
    """Return the stub for a site, account, or API token route."""
    if url.endswith("/api/1/site/"):
        return _json(
            {
                "feed_size": 0,
                "id": "s",
                "keywords": [],
                "metrics": {},
                "title": "uData",
                "version": transport.version,
            }
        )
    if url.endswith("/api/1/me/"):
        return _json({"id": "wheel-user", "first_name": "Wheel", "last_name": "User"})
    if url.endswith("/api/1/me/api_tokens/"):
        if method == "GET":
            return _json([{"id": "wheel-token-id", "token_prefix": "wheel-prefix"}])
        if method == "POST":
            return _json({"id": "wheel-token-id", "token_prefix": "wheel-prefix", "token": transport.token_value})
    if url.endswith("/api/1/me/api_tokens/wheel-token-id/") and method == "DELETE":
        return _empty()
    return None


def _oauth_response(url, method):
    """Return the stub for an OAuth token, revocation, or browser-page route."""
    if "/oauth/token" in url and method == "POST":
        return _json({"access_token": "wheel-access", "token_type": "Bearer", "expires_in": 60})
    if "/oauth/revoke" in url and method == "POST":
        return _empty()
    if "/oauth/client_info" in url or "/oauth/authorize" in url:
        return _html()
    if "/oauth/error" in url:
        return _html()
    return None


def _taxonomy_response(url):
    """Return the stub for a badge, suggest, licence, frequency, extension, or schema route."""
    if url.endswith("/api/1/datasets/badges/"):
        return _json({"pivotal-data": "Pivotal data"})
    if "/datasets/suggest/formats/" in url or "/datasets/suggest/mime/" in url:
        return _json([{"text": "csv"}])
    if url.endswith("/api/1/datasets/licenses/"):
        return _json([{"id": "lov2", "title": "Licence Ouverte"}])
    if url.endswith("/api/1/datasets/frequencies/"):
        return _json([{"id": "punctual", "label": "Punctual"}])
    if url.endswith("/api/1/datasets/extensions/"):
        return _json(["csv"])
    if url.endswith("/api/1/datasets/schemas/") or "/api/2/datasets/abc/schemas/" in url:
        return _json([])
    return None


def _discussion_response(url, method):
    """Return the stub for a discussion, search, or activity route."""
    if "/api/1/discussions" in url:
        return _json(
            {"id": "wheel-discussion", "title": "Wheel discussion", "discussion": [], "extras": {}, "closed": None}
        )
    if "/api/1/activity/" in url:
        return _json(
            {
                "data": [{"id": "wheel-activity", "label": "dataset.created"}],
                "next_page": None,
                "page": 1,
                "page_size": 20,
                "previous_page": None,
                "total": 1,
            }
        )
    if "/api/2/discussions/search/" in url:
        return _json({"data": [], "facets": {}, "links": {}, "meta": {"total": 0}})
    return None


def _report_response(url, method):
    """Return the stub for a report or notification route."""
    if "/api/1/notifications/" in url:
        return _json(
            {"data": [{"id": "wheel-notification", "handled_at": None}], "page": 1, "page_size": 20, "total": 1}
        )
    if "/api/1/reports/reasons/" in url:
        return _json([{"value": "spam", "label": "Spam"}])
    if "/api/1/reports/" in url:
        return _json({"id": "wheel-report", "reason": "spam", "message": "wheel"})
    return None


def _reuse_page():
    """Return the payload the stubbed reuse collections answer with."""
    return {
        "data": [{"id": "wheel-reuse", "title": "Wheel reuse"}],
        "next_page": None,
        "page": 1,
        "page_size": 20,
        "previous_page": None,
        "total": 1,
    }


def _reuse_record():
    """Return the payload the stubbed single-reuse and reuse mutation routes answer with."""
    return _json({"featured": False, "id": "wheel-reuse", "title": "Wheel reuse"})


def _reuse_response(url, method):
    """Return the stub for a reuse collection, a single reuse, or a reuse taxonomy route."""
    path = url.partition("?")[0]
    if path.endswith("/api/1/reuses/recent.atom"):
        return b"<feed/>", {"Content-Type": "application/atom+xml"}
    if path.endswith("/api/1/reuses/suggest/"):
        return _json([{"text": "wheel"}])
    if path.endswith("/api/1/reuses/badges/"):
        return _json({"reuses": "Reuses"})
    if path.endswith("/api/1/reuses/types/"):
        return _json([{"id": "application", "label": "Application"}])
    if path.endswith("/api/1/reuses/topics/"):
        return _json([{"id": "health", "label": "Health"}])
    if path.endswith("/api/1/reuses/"):
        return _json(_reuse_page()) if method == "GET" else _reuse_record()
    if "/api/2/reuses/" in path:
        return _json(_reuse_page())
    if "/api/1/reuses/" in path:
        return _empty() if method == "DELETE" else _reuse_record()
    return None


def _topic_page():
    """Return the payload the stubbed topic collections answer with."""
    return {
        "data": [{"id": "wheel-topic", "name": "Wheel topic", "featured": False}],
        "next_page": None,
        "page": 1,
        "page_size": 20,
        "previous_page": None,
        "total": 1,
    }


def _topic_record():
    """Return the payload the stubbed single-topic and topic-element routes answer with."""
    return _json({"id": "wheel-topic", "name": "Wheel topic", "featured": False, "private": False})


def _topic_response(url, method):
    """Return the stub for a topic collection, single topic, element, or featured route."""
    path = url.partition("?")[0]
    if path.endswith("/api/2/topics/search/"):
        return _json({"data": [], "facets": {}, "links": {}, "meta": {"total": 0}})
    if path.endswith("/api/2/topics/wheel-topic/elements/"):
        if method == "GET":
            return _json(
                {
                    "data": [{"id": "wheel-element", "title": "Wheel element", "tags": []}],
                    "page": 1,
                    "page_size": 20,
                    "total": 1,
                }
            )
        return _json([{"id": "wheel-element", "title": "Wheel element", "tags": []}])
    if "/api/2/topics/wheel-topic/elements/wheel-element/" in path:
        return _empty() if method == "DELETE" else _json({"id": "wheel-element", "title": "Wheel element", "tags": []})
    if path.endswith("/api/2/topics/wheel-topic/featured/"):
        return _topic_record()
    if path.endswith("/api/2/topics/wheel-topic/"):
        return _empty() if method == "DELETE" else _topic_record()
    if path.endswith("/api/2/topics/"):
        return _json(_topic_page())
    return None


def _cv_record():
    """Return the payload the stubbed single contact-point and visualization routes answer with."""
    return {
        "id": "wheel-viz",
        "title": "Wheel visualization",
        "slug": "wheel-visualization",
        "description": "d",
        "private": False,
        "extras": {},
        "deleted_at": None,
        "x_axis": {"column_x": "col", "type": "discrete"},
        "y_axis": None,
        "series": [{"resource_id": "wheel-resource", "type": "line"}],
        "image": None,
        "metrics": {},
        "permissions": {},
        "owner": None,
        "organization": None,
        "created_at": None,
        "last_modified": None,
    }


def _cv_contact_record():
    """Return the payload the stubbed contact-point routes answer with."""
    return {"id": "wheel-contact", "name": "Wheel contact", "role": "creator", "email": None, "contact_form": None}


def _cv_response(url, method):
    """Return the stub for a contact-point or visualization route."""
    path = url.partition("?")[0]
    if path.endswith("/api/1/contacts/roles/"):
        return _json([{"id": "contact", "label": "Contact"}])
    if path.endswith("/api/1/contacts/wheel-contact/"):
        return _empty() if method == "DELETE" else _json(_cv_contact_record())
    if path.endswith("/api/1/contacts/"):
        return _json(_cv_contact_record())
    if path.endswith("/api/1/visualizations/wheel-viz/image/"):
        return _json({"success": True, "image": {"cardinal": "1", "original": "wheel-visualization.png"}})
    if path.endswith("/api/1/visualizations/wheel-viz/"):
        return _empty() if method == "DELETE" else _json(_cv_record())
    if path.endswith("/api/1/visualizations/"):
        page = {
            "data": [_cv_record()],
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "previous_page": None,
            "total": 1,
        }
        return _json(page) if method == "GET" else _json(_cv_record())
    return None


def _spatial_response(url):
    """Return the stub for a spatial level, granularity, zone, or coverage route."""
    path = url.partition("?")[0]
    if path.endswith("/api/1/spatial/granularities/"):
        return _json([{"id": "poi", "name": "POI"}])
    if path.endswith("/api/1/spatial/levels/"):
        return _json([{"id": "country:fr", "name": "Pays"}])
    if path.endswith("/api/1/spatial/zones/suggest/"):
        return _json([{"code": "FR-01", "id": "wheel-zone", "level": "country:fr", "name": "Zone", "uri": "z"}])
    if path.endswith("/api/1/spatial/zone/wheel-zone/datasets/"):
        return _json([{"id": "wheel-dataset", "title": "Wheel dataset"}])
    if path.endswith("/api/1/spatial/zone/wheel-zone/"):
        return _json({"id": "wheel-zone", "type": "Feature"})
    if path.endswith("/api/1/spatial/coverage/country%3Afr/"):
        return _json({"features": [], "type": "FeatureCollection"})
    return None


def _content_response(url, method):
    """Return the stub for a post, post-feed, or reuse route."""
    if url.endswith("/api/1/posts/wheel-post/"):
        return _json({"id": "wheel-post", "name": "Wheel post"})
    if "/api/1/posts/" in url or "/api/2/posts/" in url:
        if url.endswith("/api/1/posts/recent.atom"):
            return b"<feed/>", {"Content-Type": "application/atom+xml"}
        return _json(_post_page())
    return _reuse_response(url, method)


def _jobs_response(url, method):
    if url.endswith("/api/1/workers/jobs/schedulables/"):
        return _json(["wheel-task"])
    if url.endswith("/api/1/workers/tasks/wheel-task-run/"):
        return _json({"id": "wheel-task-run", "status": "SUCCESS", "result": None, "exc": None, "traceback": None})
    if url.endswith("/api/1/workers/jobs/wheel-job/"):
        if method == "DELETE":
            return _empty()
        return _json(
            {
                "id": "wheel-job",
                "name": "wheel-job",
                "description": None,
                "task": "wheel-task",
                "crontab": {"minute": "0", "hour": "*", "day_of_week": "*", "day_of_month": "*", "month_of_year": "*"},
                "interval": None,
                "args": [],
                "kwargs": {},
                "schedule": "0 * * * *",
                "last_run_at": None,
                "last_run_id": None,
                "enabled": False,
            }
        )
    if url.endswith("/api/1/workers/jobs/"):
        if method == "GET":
            return _json(
                [
                    {
                        "id": "wheel-job",
                        "name": "wheel-job",
                        "description": None,
                        "task": "wheel-task",
                        "crontab": {
                            "minute": "0",
                            "hour": "*",
                            "day_of_week": "*",
                            "day_of_month": "*",
                            "month_of_year": "*",
                        },
                        "interval": None,
                        "args": [],
                        "kwargs": {},
                        "schedule": "0 * * * *",
                        "last_run_at": None,
                        "last_run_id": None,
                        "enabled": False,
                    }
                ]
            )
        return _json(
            {
                "id": "wheel-job",
                "name": "wheel-job",
                "description": None,
                "task": "wheel-task",
                "crontab": {"minute": "0", "hour": "*", "day_of_week": "*", "day_of_month": "*", "month_of_year": "*"},
                "interval": None,
                "args": [],
                "kwargs": {},
                "schedule": "0 * * * *",
                "last_run_at": None,
                "last_run_id": None,
                "enabled": False,
            }
        )
    return None


class Transport:
    def __init__(self, version="17.6.0"):
        self.requests = []
        self.close_count = 0
        self.version = version
        self.token_value = secrets.token_urlsafe(36)

    def _route(self, request):
        """Return the stub body and headers the first matching route produces."""
        url = request.url
        method = request.method
        response = _site_response(self, url, method)
        if response is not None:
            return response
        response = _oauth_response(url, method)
        if response is not None:
            return response
        if url.endswith(".csv"):
            return b"id\nwheel\n", {"Content-Type": "text/csv"}
        if "/api/1/organizations/abc/" in url:
            return _json({"id": "abc", "name": "Wheel organization", "description": "d"})
        response = _taxonomy_response(url)
        if response is not None:
            return response
        response = _discussion_response(url, method)
        if response is not None:
            return response
        response = _report_response(url, method)
        if response is not None:
            return response
        response = _spatial_response(url)
        if response is not None:
            return response
        response = _topic_response(url, method)
        if response is not None:
            return response
        response = _cv_response(url, method)
        if response is not None:
            return response
        response = _jobs_response(url, method)
        if response is not None:
            return response
        response = _content_response(url, method)
        if response is not None:
            return response
        if url.endswith("/api/1/dataservices/wheel-dataservice/"):
            return _json({"id": "wheel-dataservice", "title": "Wheel dataservice"})
        if method == "POST":
            return _json(
                {
                    "id": "wheel-created",
                    "name": "Wheel organization",
                    "title": "Wheel dataset",
                    "slug": "wheel-dataset",
                    "description": "d",
                    "private": False,
                }
            )
        return _bare_json(
            {
                "data": [{"id": "abc", "title": "T"}],
                "next_page": None,
                "page": 1,
                "page_size": 20,
                "previous_page": None,
                "total": 1,
            }
        )

    def send(self, request):
        url = request.url
        self.requests.append(url)
        body, headers = self._route(request)
        status = 204 if request.method == "DELETE" else 201 if request.method == "POST" else 200
        if url.partition("?")[0].endswith("/api/2/topics/wheel-topic/featured/"):
            status = 200
        if url.partition("?")[0].endswith("/api/1/visualizations/wheel-viz/image/"):
            status = 200
        return RuntimeResponse(status_code=status, headers=headers, body=body)

    def send_stream(self, request):
        response = self.send(request)
        return RuntimeStreamResponse(response.status_code, response.headers, iter((response.body,)), lambda: None)

    def close(self):
        self.close_count += 1


class AsyncTransport(Transport):
    async def send(self, request):
        return Transport.send(self, request)

    async def aclose(self):
        self.close_count += 1

    async def send_stream(self, request):
        response = await self.send(request)

        async def chunks():
            yield response.body

        return AsyncRuntimeStreamResponse(response.status_code, response.headers, chunks(), lambda: None)


transport = Transport()
sync_credential = UDataCredential(api_key="sync-wheel-key")
client = create_sync_client(
    UDataClientSettings(base_url="http://127.0.0.1:5640", sync_transport=transport, credential=sync_credential)
)
assert client.site_version().version == "17.6.0"
assert client.dataservices.get_dataservice("wheel-dataservice").payload["id"] == "wheel-dataservice"
root_profile = client.root_profile.get()
assert root_profile.id == "s"
root_export = client.root_profile.datasets_csv()
assert root_export.size_bytes == len(b"id\nwheel\n")
envelope = client.datasets_list(
    CatalogOperationRequest(operation_id=op_id, payload={}),
    CatalogOperationGuard(operation_id=op_id),
)
service_page = client.datasets.list()
assert service_page.items[0].id.value == "abc"
organization = client.organizations_memberships.get_organization("abc")
try:
    client.organizations_memberships.get_organization("")
except CatalogValidationError:
    pass
else:
    raise AssertionError("invalid organization id was dispatched")
assert organization.id.value == "abc"
sync_permissions = EffectivePermissions.for_credential(sync_credential, platform=CatalogPlatform.UDATA)
sync_admin_permissions = EffectivePermissions.for_credential(
    sync_credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
)
sync_resource = client.resources.create(
    "abc",
    ResourceCreateInput(title="Sync wheel resource", url="https://example.test/sync.csv"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-create",
            target="abc",
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
sync_upload = client.resources.upload(
    "abc",
    ResourceUploadInput(BytesIO(b"abc"), "sync.csv", 3),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-upload-new",
            target="abc",
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert sync_resource.record is not None and sync_upload.record is not None
organization_created = client.organizations_memberships.create_organization(
    OrganizationCreateInput(name="Wheel organization", description="d"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.create-organization", target="Wheel organization"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert organization_created.record is not None
assert organization_created.receipt.operation == "udata/api-v1.create-organization"
assert organization_created.receipt.outcome == "succeeded"
assert organization_created.receipt.audit_metadata["status_code"] == 201
assert client.users_tokens.get_me(sync_permissions).id.value == "wheel-user"
assert client.users_tokens.list_api_tokens(sync_permissions)[0].token_prefix == "wheel-prefix"
created_token = client.users_tokens.create_api_token(
    ApiTokenCreateInput(name="wheel"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.create-api-token", target="new-api-token"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
oauth_token = client.auth_oauth.access_token(
    OAuthTokenRequest(grant_type="client_credentials", client_id="wheel-client", client_secret="wheel-secret"),
    sync_permissions,
)
assert oauth_token.receipt.outcome == "succeeded"
assert oauth_token.token_type == "Bearer"
assert "wheel-access" not in json.dumps(oauth_token.to_dict())
oauth_revoked = client.auth_oauth.revoke_token(
    OAuthRevokeRequest(token="wheel-access"),
    sync_permissions,
    MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/oauth.revoke-token", target="request:revoke_token"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert oauth_revoked.receipt.operation == "udata/oauth.revoke-token"
assert client.auth_oauth.oauth_error().session_gated is True
assert client.auth_oauth.authorize(OAuthClientRequest(client_id="wheel-client"), sync_permissions).session_gated is True
assert created_token.receipt.audit_metadata["status_code"] == 201
_assert_token_material_absent(created_token, transport.token_value, "sync api token")
revoked_token = client.users_tokens.revoke_api_token(
    created_token.metadata.id,
    sync_permissions,
    MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.revoke-api-token", target=created_token.metadata.id
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert revoked_token.receipt.audit_metadata["status_code"] == 204
assert client.spatial.spatial_granularities()[0].payload["name"] == "POI"
assert client.spatial.spatial_levels()[0].payload["id"] == "country:fr"
assert client.spatial.suggest_zones(SpatialSuggestQuery(q="fr", size=10))[0].payload["code"] == "FR-01"
assert client.spatial.spatial_zone("wheel-zone").payload["type"] == "Feature"
assert (
    client.spatial.spatial_zone_datasets("wheel-zone", SpatialDatasetQuery(size=25))[0].payload["id"] == "wheel-dataset"
)
assert client.spatial.spatial_coverage("country:fr").payload["type"] == "FeatureCollection"
assert client.topics.list_topics().payload["total"] == 1
assert client.topics.get_topic("wheel-topic").payload["name"] == "Wheel topic"
assert cast("dict[str, object]", client.topics.search_topics().payload["meta"])["total"] == 0
assert client.topics.topic_elements("wheel-topic").payload["total"] == 1
wheel_topic_created = client.topics.create_topic(
    TopicCreateInput(name="Wheel topic", description="d"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v2.create-topic", target="Wheel topic"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert wheel_topic_created.record is not None
assert wheel_topic_created.receipt.audit_metadata["status_code"] == 201
wheel_topic_updated = client.topics.update_topic(
    "wheel-topic",
    TopicUpdateInput(description="updated"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v2.update-topic", target="wheel-topic"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert wheel_topic_updated.receipt.operation == "udata/api-v2.update-topic"
wheel_elements = client.topics.topic_elements_create(
    "wheel-topic",
    TopicElementsCreateInput((TopicElementLink("Dataset", "abc"),)),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v2.topic-elements-create", target="wheel-topic"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert wheel_elements.receipt.audit_metadata["status_code"] == 201
assert (
    client.topics.feature_topic(
        "wheel-topic",
        sync_admin_permissions,
        MutationPolicy(
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v2.feature-topic", target="wheel-topic"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.audit_metadata["status_code"]
    == 200
)
assert (
    client.topics.unfeature_topic(
        "wheel-topic",
        sync_admin_permissions,
        MutationPolicy(
            destructive=True,
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v2.unfeature-topic", target="wheel-topic"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.audit_metadata["status_code"]
    == 200
)
assert (
    client.topics.delete_topic(
        "wheel-topic",
        sync_permissions,
        MutationPolicy(
            destructive=True,
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v2.delete-topic", target="wheel-topic"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).record
    is None
)
assert client.contact_visualization.list_visualizations().total == 1
assert client.contact_visualization.get_visualization("wheel-viz").payload["title"] == "Wheel visualization"
assert [dict(r.payload)["id"] for r in client.contact_visualization.contact_point_roles()] == ["contact"]
assert client.contact_visualization.get_contact_point("wheel-contact").payload["name"] == "Wheel contact"
viz_created = client.contact_visualization.create_visualization(
    VisualizationCreateInput(
        title="Wheel visualization",
        description="d",
        x_axis={"column_x": "col", "type": "discrete"},
        series=({"resource_id": "wheel-resource", "type": "line"},),
    ),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.create-visualization", target="Wheel visualization"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert viz_created.receipt.audit_metadata["status_code"] == 201
assert (
    client.contact_visualization.update_visualization(
        "wheel-viz",
        VisualizationUpdateInput(title="updated"),
        sync_permissions,
        MutationPolicy(
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.update-visualization", target="wheel-viz"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.operation
    == "udata/api-v1.update-visualization"
)
assert (
    client.contact_visualization.visualization_image(
        "wheel-viz",
        VisualizationImageInput(data=b"wheel-png", content_type="image/png"),
        sync_permissions,
        MutationPolicy(
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.visualization-image", target="wheel-viz"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.audit_metadata["status_code"]
    == 200
)
assert (
    client.contact_visualization.delete_visualization(
        "wheel-viz",
        sync_permissions,
        MutationPolicy(
            destructive=True,
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.delete-visualization", target="wheel-viz"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).record
    is None
)
contact_created = client.contact_visualization.create_contact_point(
    ContactPointCreateInput(name="Wheel contact", role="creator"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.create-contact-point", target="contacts"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert contact_created.receipt.audit_metadata["status_code"] == 201
assert (
    client.contact_visualization.update_contact_point(
        "wheel-contact",
        ContactPointUpdateInput(role="publisher"),
        sync_permissions,
        MutationPolicy(
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.update-contact-point", target="wheel-contact"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.operation
    == "udata/api-v1.update-contact-point"
)
assert (
    client.contact_visualization.delete_contact_point(
        "wheel-contact",
        sync_permissions,
        MutationPolicy(
            destructive=True,
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.delete-contact-point", target="wheel-contact"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.audit_metadata["status_code"]
    == 204
)
assert client.worker_jobs.list_schedulables(sync_admin_permissions) == ("wheel-task",)
assert dict(client.worker_jobs.get_task("wheel-task-run", sync_admin_permissions).payload)["status"] == "SUCCESS"
assert [dict(r.payload)["id"] for r in client.worker_jobs.list_jobs(sync_admin_permissions)] == ["wheel-job"]
assert dict(client.worker_jobs.get_job("wheel-job", sync_admin_permissions).payload)["name"] == "wheel-job"
wheel_job_created = client.worker_jobs.create_job(
    JobScheduleInput(name="wheel-job", task="wheel-task", crontab=CrontabSchedule(minute="0")),
    sync_admin_permissions,
    MutationPolicy(
        destructive=False,
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.create-job", target="wheel-job"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert wheel_job_created.record is not None
assert dict(wheel_job_created.record.payload)["id"] == "wheel-job"
assert client.taxonomies.available_badges().payload["pivotal-data"] == "Pivotal data"
assert client.taxonomies.extensions() == ("csv",)
assert client.taxonomies.licenses()[0].payload["id"] == "lov2"
assert client.taxonomies.frequencies()[0].payload["label"] == "Punctual"
assert client.taxonomies.suggest_formats(SuggestQuery("cs"))[0].payload["text"] == "csv"
assert client.taxonomies.schemas() == ()
assert client.taxonomies.dataset_schemas("abc") == ()
taxonomy_badge = client.taxonomies.add_badge(
    "abc",
    BadgeCreateInput("pivotal-data"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.add-dataset-badge", target="abc"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert taxonomy_badge.record is not None
taxonomy_deleted = client.taxonomies.delete_badge(
    "abc",
    "pivotal-data",
    sync_permissions,
    MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.delete-dataset-badge", target="abc:pivotal-data"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert taxonomy_deleted.receipt.audit_metadata["status_code"] == 204
assert client.activity_discussions.activity(ActivityQuery()).payload["total"] == 1
assert client.activity_discussions.list_discussions().payload["id"] == "wheel-discussion"
assert client.activity_discussions.get_discussion("wheel-discussion").payload["title"] == "Wheel discussion"
assert (
    cast("dict[str, object]", client.activity_discussions.search_discussions(DiscussionSearchQuery()).payload["meta"])[
        "total"
    ]
    == 0
)
assert (
    client.activity_discussions.create_discussion(
        DiscussionCreateInput(title="t", comment="c", subject={"id": "abc", "class": "Dataset"}),
        sync_permissions,
        MutationPolicy(
            confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.create-discussion", target="abc"),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.operation
    == "udata/api-v1.create-discussion"
)
assert (
    client.activity_discussions.comment_discussion(
        "wheel-discussion",
        CommentInput(comment="hello"),
        sync_permissions,
        MutationPolicy(
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.comment-discussion", target="wheel-discussion"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.operation
    == "udata/api-v1.comment-discussion"
)
assert (
    client.activity_discussions.update_discussion(
        "wheel-discussion",
        DiscussionUpdateInput(title="new"),
        sync_permissions,
        MutationPolicy(
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.update-discussion", target="wheel-discussion"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.operation
    == "udata/api-v1.update-discussion"
)
assert (
    client.activity_discussions.edit_discussion_comment(
        "wheel-discussion",
        "0",
        CommentInput(comment="edited"),
        sync_permissions,
        MutationPolicy(
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.edit-discussion-comment", target="wheel-discussion:0"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.operation
    == "udata/api-v1.edit-discussion-comment"
)
assert (
    client.activity_discussions.delete_discussion(
        "wheel-discussion",
        sync_permissions,
        MutationPolicy(
            destructive=True,
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.delete-discussion", target="wheel-discussion"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.audit_metadata["status_code"]
    == 204
)
assert (
    client.activity_discussions.delete_discussion_comment(
        "wheel-discussion",
        "1",
        sync_permissions,
        MutationPolicy(
            destructive=True,
            confirmation=ConfirmationPolicy(
                confirmed=True, operation="udata/api-v1.delete-discussion-comment", target="wheel-discussion:1"
            ),
            concurrency=ConcurrencyPolicy(overwrite=True),
        ),
    ).receipt.audit_metadata["status_code"]
    == 204
)
assert client.posts_reports.list_posts(PostListQuery(q="wheel")).payload["total"] == 1
assert client.posts_reports.get_post("wheel-post").payload["name"] == "Wheel post"
atom = client.posts_reports.recent_posts_atom_feed()
assert atom.payload["media_type"] == "application/atom+xml"
assert client.posts_reports.search_posts(PostSearchQuery(q="wheel")).payload["total"] == 1
created_report = client.posts_reports.create_report(
    ReportCreateInput(subject={"class": "Dataset", "id": "abc"}, reason="spam", message="wheel"),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.create-report", target="abc"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert created_report.record is not None
assert client.posts_reports.get_report("wheel-report", sync_admin_permissions).payload["reason"] == "spam"
assert client.posts_reports.list_reports_reasons()[0].payload["value"] == "spam"
updated_report = client.posts_reports.update_report(
    "wheel-report",
    ReportUpdateInput(message="updated"),
    sync_admin_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.update-report", target="wheel-report"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert updated_report.record is not None
read_notification = client.posts_reports.read_notification(
    "wheel-notification",
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.read-notification", target="wheel-notification"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert read_notification.record is not None
assert client.reuses.list_reuses().payload["total"] == 1
assert client.reuses.recent_reuses_atom_feed().payload["media_type"] == "application/atom+xml"
assert client.reuses.suggest_reuses(ReuseSuggestQuery("re"))[0].payload["text"] == "wheel"
assert client.reuses.available_reuse_badges().payload["reuses"] == "Reuses"
assert client.reuses.reuse_types()[0].payload["label"] == "Application"
assert client.reuses.reuse_topics()[0].payload["label"] == "Health"
assert client.reuses.get_reuse("wheel-reuse").payload["title"] == "Wheel reuse"
assert client.reuses.list_v2().payload["total"] == 1
assert client.reuses.search_v2().payload["total"] == 1
created_reuse = client.reuses.create_reuse(
    ReuseCreateInput(
        title="Wheel reuse",
        description="Wheel",
        type="application",
        url="https://example.test/reuse",
        topic="health",
    ),
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.create-reuse", target="Wheel reuse"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
linked_reuse = client.reuses.reuse_add_dataset(
    "wheel-reuse",
    "abc",
    sync_permissions,
    MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True, operation="udata/api-v1.reuse-add-dataset", target="wheel-reuse:abc"
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
deleted_reuse = client.reuses.delete_reuse(
    "wheel-reuse",
    sync_permissions,
    MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.delete-reuse", target="wheel-reuse"),
        concurrency=ConcurrencyPolicy(overwrite=True),
    ),
)
assert created_reuse.record is not None and linked_reuse.record is not None
assert created_reuse.receipt.audit_metadata["status_code"] == 201
assert deleted_reuse.record is None
assert deleted_reuse.receipt.audit_metadata["status_code"] == 204
client.close()

async_transport = AsyncTransport()
async_credential = UDataCredential(api_key="wheel-key")
async_client = create_async_client(
    UDataClientSettings(base_url="http://127.0.0.1:5640", async_transport=async_transport, credential=async_credential)
)


async def run_async():
    async with async_client as active:
        assert (await active.dataservices.get_dataservice("wheel-dataservice")).payload["id"] == "wheel-dataservice"
        page = await active.datasets.list()
        root_profile = await active.root_profile.get()
        root_export = await active.root_profile.datasets_csv()
        organization = await active.organizations_memberships.get_organization("abc")
        permissions = EffectivePermissions.for_credential(async_credential, platform=CatalogPlatform.UDATA)
        created = await active.datasets.create(
            DatasetCreateInput(title="Wheel dataset", description="d"),
            permissions=permissions,
            mutation_policy=MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.create-dataset", target="Wheel dataset"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        resource_created = await active.resources.create(
            "abc",
            ResourceCreateInput(title="Wheel resource", url="https://example.test/data.csv"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True,
                    operation="udata/api-v1.dataset-resource-create-update-reorder-upload-delete-create",
                    target="abc",
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        organization_created = await active.organizations_memberships.create_organization(
            OrganizationCreateInput(name="Async wheel organization", description="d"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.create-organization", target="Async wheel organization"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        user = await active.users_tokens.get_me(permissions)
        token_list = await active.users_tokens.list_api_tokens(permissions)
        token_created = await active.users_tokens.create_api_token(
            ApiTokenCreateInput(name="async wheel"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.create-api-token", target="new-api-token"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        token_revoked = await active.users_tokens.revoke_api_token(
            token_created.metadata.id,
            permissions,
            MutationPolicy(
                destructive=True,
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.revoke-api-token", target=token_created.metadata.id
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        assert user.id.value == "wheel-user"
        assert token_list[0].token_prefix == "wheel-prefix"
        assert token_created.receipt.audit_metadata["status_code"] == 201
        _assert_token_material_absent(token_created, async_transport.token_value, "async api token")
        assert token_revoked.receipt.audit_metadata["status_code"] == 204
        assert (await active.spatial.spatial_granularities())[0].payload["name"] == "POI"
        assert (await active.spatial.spatial_levels())[0].payload["id"] == "country:fr"
        assert (await active.spatial.suggest_zones(SpatialSuggestQuery(q="fr", size=10)))[0].payload["code"] == "FR-01"
        assert (await active.spatial.spatial_zone("wheel-zone")).payload["type"] == "Feature"
        assert (await active.spatial.spatial_zone_datasets("wheel-zone", SpatialDatasetQuery(size=25)))[0].payload[
            "id"
        ] == "wheel-dataset"
        assert (await active.spatial.spatial_coverage("country:fr")).payload["type"] == "FeatureCollection"
        assert (await active.topics.list_topics()).payload["total"] == 1
        assert (await active.topics.get_topic("wheel-topic")).payload["name"] == "Wheel topic"
        assert cast("dict[str, object]", (await active.topics.search_topics()).payload["meta"])["total"] == 0
        assert (await active.topics.topic_elements("wheel-topic")).payload["total"] == 1
        async_topic_created = await active.topics.create_topic(
            TopicCreateInput(name="Wheel topic", description="d"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v2.create-topic", target="Wheel topic"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        assert async_topic_created.record is not None
        assert async_topic_created.receipt.audit_metadata["status_code"] == 201
        async_element_updated = await active.topics.topic_element_update(
            "wheel-topic",
            "wheel-element",
            TopicElementInput(title="Wheel element"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v2.topic-element-update", target="wheel-topic:wheel-element"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        assert async_element_updated.receipt.operation == "udata/api-v2.topic-element-update"
        assert (
            await active.topics.topic_element_delete(
                "wheel-topic",
                "wheel-element",
                permissions,
                MutationPolicy(
                    destructive=True,
                    confirmation=ConfirmationPolicy(
                        confirmed=True,
                        operation="udata/api-v2.topic-element-delete",
                        target="wheel-topic:wheel-element",
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).record is None
        assert (
            await active.topics.delete_topic(
                "wheel-topic",
                permissions,
                MutationPolicy(
                    destructive=True,
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v2.delete-topic", target="wheel-topic"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).record is None
        assert (await active.contact_visualization.list_visualizations()).total == 1
        assert (await active.contact_visualization.get_visualization("wheel-viz")).payload["title"] == (
            "Wheel visualization"
        )
        assert [dict(r.payload)["id"] for r in await active.contact_visualization.contact_point_roles()] == ["contact"]
        assert (
            await active.contact_visualization.create_visualization(
                VisualizationCreateInput(
                    title="Wheel visualization",
                    description="d",
                    x_axis={"column_x": "col", "type": "discrete"},
                    series=({"resource_id": "wheel-resource", "type": "line"},),
                ),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.create-visualization", target="Wheel visualization"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).receipt.audit_metadata["status_code"] == 201
        assert (
            await active.contact_visualization.visualization_image(
                "wheel-viz",
                VisualizationImageInput(data=b"wheel-png", content_type="image/png"),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.visualization-image", target="wheel-viz"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).receipt.audit_metadata["status_code"] == 200
        assert (
            await active.contact_visualization.delete_visualization(
                "wheel-viz",
                permissions,
                MutationPolicy(
                    destructive=True,
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.delete-visualization", target="wheel-viz"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).record is None
        assert (
            await active.contact_visualization.create_contact_point(
                ContactPointCreateInput(name="Wheel contact", role="creator"),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.create-contact-point", target="contacts"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).receipt.audit_metadata["status_code"] == 201
        assert (
            await active.contact_visualization.delete_contact_point(
                "wheel-contact",
                permissions,
                MutationPolicy(
                    destructive=True,
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.delete-contact-point", target="wheel-contact"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).record is None
        async_permissions = EffectivePermissions.for_credential(
            async_credential, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
        )
        assert await active.worker_jobs.list_schedulables(async_permissions) == ("wheel-task",)
        assert (
            dict((await active.worker_jobs.get_task("wheel-task-run", async_permissions)).payload)["status"]
            == "SUCCESS"
        )
        assert (
            await active.worker_jobs.create_job(
                JobScheduleInput(name="wheel-job", task="wheel-task", crontab=CrontabSchedule(minute="0")),
                async_permissions,
                MutationPolicy(
                    destructive=False,
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.create-job", target="wheel-job"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).receipt.audit_metadata["status_code"] == 201
        assert (await active.taxonomies.available_badges()).payload["pivotal-data"] == "Pivotal data"
        assert await active.taxonomies.extensions() == ("csv",)
        assert (await active.taxonomies.suggest_mime(SuggestQuery("js")))[0].payload["text"] == "csv"
        taxonomy_badge = await active.taxonomies.add_badge(
            "abc",
            BadgeCreateInput("pivotal-data"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.add-dataset-badge", target="abc"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        taxonomy_deleted = await active.taxonomies.delete_badge(
            "abc",
            "pivotal-data",
            permissions,
            MutationPolicy(
                destructive=True,
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.delete-dataset-badge", target="abc:pivotal-data"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        assert taxonomy_badge.record is not None
        assert taxonomy_deleted.receipt.audit_metadata["status_code"] == 204
        assert (await active.activity_discussions.activity(ActivityQuery())).payload["total"] == 1
        wheel_discussion = await active.activity_discussions.get_discussion("wheel-discussion")
        assert wheel_discussion.payload["id"] == "wheel-discussion"
        wheel_search = await active.activity_discussions.search_discussions(DiscussionSearchQuery())
        assert cast("dict[str, object]", wheel_search.payload["meta"])["total"] == 0
        assert (
            await active.activity_discussions.create_discussion(
                DiscussionCreateInput(title="t", comment="c", subject={"id": "abc", "class": "Dataset"}),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.create-discussion", target="abc"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).receipt.operation == "udata/api-v1.create-discussion"
        assert (
            await active.activity_discussions.delete_discussion(
                "wheel-discussion",
                permissions,
                MutationPolicy(
                    destructive=True,
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.delete-discussion", target="wheel-discussion"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        ).receipt.audit_metadata["status_code"] == 204
        assert (await active.posts_reports.list_posts(PostListQuery())).payload["total"] == 1
        async_report = await active.posts_reports.create_report(
            ReportCreateInput(subject={"class": "Dataset", "id": "abc"}, reason="spam", message="wheel"),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(confirmed=True, operation="udata/api-v1.create-report", target="abc"),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        assert async_report.record is not None
        assert (await active.posts_reports.list_notifications(permissions, NotificationQuery())).payload["total"] == 1
        assert (await active.reuses.list_reuses()).payload["total"] == 1
        assert (await active.reuses.get_reuse("wheel-reuse")).payload["title"] == "Wheel reuse"
        async_reuse = await active.reuses.create_reuse(
            ReuseCreateInput(
                title="Async wheel reuse",
                description="Wheel",
                type="application",
                url="https://example.test/async-reuse",
                topic="health",
            ),
            permissions,
            MutationPolicy(
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.create-reuse", target="Async wheel reuse"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        async_reuse_deleted = await active.reuses.delete_reuse(
            "wheel-reuse",
            permissions,
            MutationPolicy(
                destructive=True,
                confirmation=ConfirmationPolicy(
                    confirmed=True, operation="udata/api-v1.delete-reuse", target="wheel-reuse"
                ),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
        )
        assert async_reuse.record is not None
        assert async_reuse_deleted.record is None
        try:
            await active.organizations_memberships.get_organization("")
        except CatalogValidationError:
            pass
        else:
            raise AssertionError("invalid organization id was dispatched")
        try:
            await active.organizations_memberships.update_organization(
                "invalid/id",
                OrganizationUpdateInput(description="invalid"),
                permissions,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation="udata/api-v1.update-organization", target="invalid/id"
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        except CatalogValidationError as error:
            receipt = error.__dict__["mutation_receipt"]
            assert receipt.operation == "udata/api-v1.update-organization"
            assert receipt.outcome == "rejected"
        else:
            raise AssertionError("invalid organization mutation was dispatched")
        return (
            page.items[0].id.value,
            root_profile.id,
            root_export.size_bytes,
            organization,
            created,
            resource_created,
            organization_created,
        )


(
    async_result,
    async_root_id,
    async_export_size,
    organization,
    created,
    resource_created,
    organization_created,
) = asyncio.run(run_async())
assert async_result == "abc"
assert async_root_id == "s"
assert async_export_size == len(b"id\nwheel\n")
assert organization.id.value == "abc"
assert created.record.id.value == "wheel-created"
assert created.receipt.outcome == "succeeded"
assert created.receipt.audit_metadata["status_code"] == 201
assert resource_created.record.id.value == "wheel-created"
assert organization_created.record.id.value == "wheel-created"
assert organization_created.receipt.operation == "udata/api-v1.create-organization"
assert organization_created.receipt.outcome == "succeeded"
assert organization_created.receipt.audit_metadata["status_code"] == 201
recorded = [getattr(r, "url", r) for r in transport.requests]
expected = [
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/dataservices/wheel-dataservice/",
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/site/datasets.csv",
    "http://127.0.0.1:5640/api/1/datasets/",
    "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/organizations/abc/",
    "http://127.0.0.1:5640/api/1/datasets/abc/resources/",
    "http://127.0.0.1:5640/api/1/datasets/abc/upload/",
    "http://127.0.0.1:5640/api/1/organizations/",
    "http://127.0.0.1:5640/api/1/me/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/",
    "http://127.0.0.1:5640/oauth/token",
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/oauth/revoke",
    "http://127.0.0.1:5640/oauth/error",
    "http://127.0.0.1:5640/oauth/authorize?client_id=wheel-client&response_type=code",
    "http://127.0.0.1:5640/api/1/me/api_tokens/wheel-token-id/",
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/spatial/granularities/",
    "http://127.0.0.1:5640/api/1/spatial/levels/",
    "http://127.0.0.1:5640/api/1/spatial/zones/suggest/?q=fr&size=10",
    "http://127.0.0.1:5640/api/1/spatial/zone/wheel-zone/",
    "http://127.0.0.1:5640/api/1/spatial/zone/wheel-zone/datasets/?size=25",
    "http://127.0.0.1:5640/api/1/spatial/coverage/country%3Afr/",
    "http://127.0.0.1:5640/api/2/topics/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/",
    "http://127.0.0.1:5640/api/2/topics/search/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/elements/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/2/topics/",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/elements/",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/featured/",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/featured/",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/",
    "http://127.0.0.1:5640/api/1/visualizations/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/visualizations/wheel-viz/",
    "http://127.0.0.1:5640/api/1/contacts/roles/",
    "http://127.0.0.1:5640/api/1/contacts/wheel-contact/",
    "http://127.0.0.1:5640/api/1/visualizations/",
    "http://127.0.0.1:5640/api/1/visualizations/wheel-viz/",
    "http://127.0.0.1:5640/api/1/visualizations/wheel-viz/image/",
    "http://127.0.0.1:5640/api/1/visualizations/wheel-viz/",
    "http://127.0.0.1:5640/api/1/contacts/",
    "http://127.0.0.1:5640/api/1/contacts/wheel-contact/",
    "http://127.0.0.1:5640/api/1/contacts/wheel-contact/",
    "http://127.0.0.1:5640/api/1/workers/jobs/schedulables/",
    "http://127.0.0.1:5640/api/1/workers/tasks/wheel-task-run/",
    "http://127.0.0.1:5640/api/1/workers/jobs/",
    "http://127.0.0.1:5640/api/1/workers/jobs/wheel-job/",
    "http://127.0.0.1:5640/api/1/workers/jobs/",
    "http://127.0.0.1:5640/api/1/datasets/badges/",
    "http://127.0.0.1:5640/api/1/datasets/extensions/",
    "http://127.0.0.1:5640/api/1/datasets/licenses/",
    "http://127.0.0.1:5640/api/1/datasets/frequencies/",
    "http://127.0.0.1:5640/api/1/datasets/suggest/formats/?q=cs&size=10",
    "http://127.0.0.1:5640/api/1/datasets/schemas/",
    "http://127.0.0.1:5640/api/2/datasets/abc/schemas/",
    "http://127.0.0.1:5640/api/1/datasets/abc/badges/",
    "http://127.0.0.1:5640/api/1/datasets/abc/badges/pivotal-data/",
    "http://127.0.0.1:5640/api/1/activity/",
    "http://127.0.0.1:5640/api/1/discussions/",
    "http://127.0.0.1:5640/api/1/discussions/wheel-discussion/",
    "http://127.0.0.1:5640/api/2/discussions/search/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/discussions/",
    "http://127.0.0.1:5640/api/1/discussions/wheel-discussion/",
    "http://127.0.0.1:5640/api/1/discussions/wheel-discussion/",
    "http://127.0.0.1:5640/api/1/discussions/wheel-discussion/comments/0/",
    "http://127.0.0.1:5640/api/1/discussions/wheel-discussion/",
    "http://127.0.0.1:5640/api/1/discussions/wheel-discussion/comments/1/",
    "http://127.0.0.1:5640/api/1/posts/?page=1&page_size=20&q=wheel",
    "http://127.0.0.1:5640/api/1/posts/wheel-post/",
    "http://127.0.0.1:5640/api/1/posts/recent.atom",
    "http://127.0.0.1:5640/api/2/posts/search/?page=1&page_size=20&q=wheel",
    "http://127.0.0.1:5640/api/1/reports/",
    "http://127.0.0.1:5640/api/1/reports/wheel-report/",
    "http://127.0.0.1:5640/api/1/reports/reasons/",
    "http://127.0.0.1:5640/api/1/reports/wheel-report/",
    "http://127.0.0.1:5640/api/1/notifications/wheel-notification/read/",
    "http://127.0.0.1:5640/api/1/reuses/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/reuses/recent.atom?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/reuses/suggest/?q=re&size=10",
    "http://127.0.0.1:5640/api/1/reuses/badges/",
    "http://127.0.0.1:5640/api/1/reuses/types/",
    "http://127.0.0.1:5640/api/1/reuses/topics/",
    "http://127.0.0.1:5640/api/1/reuses/wheel-reuse/",
    "http://127.0.0.1:5640/api/2/reuses/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/2/reuses/search/?page=1&page_size=50",
    "http://127.0.0.1:5640/api/1/reuses/",
    "http://127.0.0.1:5640/api/1/reuses/wheel-reuse/datasets/",
    "http://127.0.0.1:5640/api/1/reuses/wheel-reuse/",
]
assert recorded == expected
async_recorded = [getattr(r, "url", r) for r in async_transport.requests]
assert set(async_recorded) == {
    "http://127.0.0.1:5640/api/1/dataservices/wheel-dataservice/",
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/datasets/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/site/datasets.csv",
    "http://127.0.0.1:5640/api/1/datasets/",
    "http://127.0.0.1:5640/api/1/datasets/abc/resources/",
    "http://127.0.0.1:5640/api/1/organizations/abc/",
    "http://127.0.0.1:5640/api/1/organizations/",
    "http://127.0.0.1:5640/api/1/me/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/",
    "http://127.0.0.1:5640/api/1/me/api_tokens/wheel-token-id/",
    "http://127.0.0.1:5640/api/1/datasets/badges/",
    "http://127.0.0.1:5640/api/1/datasets/extensions/",
    "http://127.0.0.1:5640/api/1/datasets/suggest/mime/?q=js&size=10",
    "http://127.0.0.1:5640/api/1/datasets/abc/badges/",
    "http://127.0.0.1:5640/api/1/datasets/abc/badges/pivotal-data/",
    "http://127.0.0.1:5640/api/1/activity/",
    "http://127.0.0.1:5640/api/1/discussions/wheel-discussion/",
    "http://127.0.0.1:5640/api/1/discussions/",
    "http://127.0.0.1:5640/api/2/discussions/search/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/posts/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/reports/",
    "http://127.0.0.1:5640/api/1/notifications/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/reuses/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/reuses/",
    "http://127.0.0.1:5640/api/1/reuses/wheel-reuse/",
    "http://127.0.0.1:5640/api/1/spatial/granularities/",
    "http://127.0.0.1:5640/api/1/spatial/levels/",
    "http://127.0.0.1:5640/api/1/spatial/zones/suggest/?q=fr&size=10",
    "http://127.0.0.1:5640/api/1/spatial/zone/wheel-zone/",
    "http://127.0.0.1:5640/api/1/spatial/zone/wheel-zone/datasets/?size=25",
    "http://127.0.0.1:5640/api/1/spatial/coverage/country%3Afr/",
    "http://127.0.0.1:5640/api/2/topics/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/",
    "http://127.0.0.1:5640/api/2/topics/search/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/elements/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/2/topics/",
    "http://127.0.0.1:5640/api/2/topics/wheel-topic/elements/wheel-element/",
    "http://127.0.0.1:5640/api/1/visualizations/?page=1&page_size=20",
    "http://127.0.0.1:5640/api/1/visualizations/wheel-viz/",
    "http://127.0.0.1:5640/api/1/contacts/roles/",
    "http://127.0.0.1:5640/api/1/visualizations/",
    "http://127.0.0.1:5640/api/1/visualizations/wheel-viz/image/",
    "http://127.0.0.1:5640/api/1/contacts/",
    "http://127.0.0.1:5640/api/1/contacts/wheel-contact/",
    "http://127.0.0.1:5640/api/1/workers/jobs/schedulables/",
    "http://127.0.0.1:5640/api/1/workers/tasks/wheel-task-run/",
    "http://127.0.0.1:5640/api/1/workers/jobs/",
}
assert transport.close_count == 0
assert envelope.items[0].id.value == "abc"


class MalformedTransport(Transport):
    def send(self, request):
        if request.url.endswith("/api/1/datasets/abc/"):
            self.requests.append(request.url)
            return RuntimeResponse(status_code=200, headers={}, body=b"{")
        return Transport.send(self, request)


malformed = create_sync_client(
    UDataClientSettings(base_url="http://127.0.0.1:5640", sync_transport=MalformedTransport())
)
try:
    malformed.datasets.get("abc")
except NativeCatalogError as error:
    assert error.status_code == 200
else:
    raise AssertionError("malformed installed-wheel response was accepted")
malformed.close()


blocked_transport = Transport(version="17.7")
blocked = create_sync_client(UDataClientSettings(base_url="http://127.0.0.1:5640", sync_transport=blocked_transport))
try:
    blocked.datasets_list(
        CatalogOperationRequest(operation_id=op_id, payload={}),
        CatalogOperationGuard(operation_id=op_id),
    )
except UDataVersionError:
    assert len(blocked_transport.requests) == 1
else:
    raise AssertionError("version mismatch did not block dispatch")
blocked.close()
print("TRACER_OK")
