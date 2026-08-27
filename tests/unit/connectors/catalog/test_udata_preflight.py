"""Contract tests for the independent uData 17.6 preflight tooling."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

MODULE_PATH = Path(__file__).resolve().parents[4] / "scripts" / "extract_udata_oracle.py"
_spec = importlib.util.spec_from_file_location("extract_udata_oracle", MODULE_PATH)
assert _spec is not None and _spec.loader is not None
oracle = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(oracle)

CAPTURE_PATH = Path(__file__).resolve().parents[4] / "scripts" / "capture_udata_stack_evidence.py"
_capture_spec = importlib.util.spec_from_file_location("capture_udata_stack_evidence", CAPTURE_PATH)
assert _capture_spec is not None and _capture_spec.loader is not None
capture = importlib.util.module_from_spec(_capture_spec)
_capture_spec.loader.exec_module(capture)

SEED_PATH = Path(__file__).resolve().parents[4] / "dev" / "udata-evidence" / "seeds" / "seed.py"
_seed_spec = importlib.util.spec_from_file_location("seed_udata_evidence", SEED_PATH)
assert _seed_spec is not None and _seed_spec.loader is not None
seed = importlib.util.module_from_spec(_seed_spec)
_seed_spec.loader.exec_module(seed)

ROUTE_CAPTURE_PATH = Path(__file__).resolve().parents[4] / "scripts" / "capture_udata_route_documents.py"
_route_capture_spec = importlib.util.spec_from_file_location("capture_udata_routes", ROUTE_CAPTURE_PATH)
assert _route_capture_spec is not None and _route_capture_spec.loader is not None
route_capture = importlib.util.module_from_spec(_route_capture_spec)
_route_capture_spec.loader.exec_module(route_capture)


def _write_routes(path: Path, routes: list[dict[str, str]], *, commit: str | None = None) -> None:
    document: dict[str, object] = {"schema_version": 1, "routes": routes}
    if commit is not None:
        document["source_commit"] = commit
    path.write_text(json.dumps(document), encoding="utf-8")


def _route(method: str = "GET", path: str = "/api/1/site/") -> dict[str, str]:
    return {"method": method, "path": path, "api_version": "v1"}


def test_reconcile_requires_three_identical_independent_route_sets(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    swagger = tmp_path / "swagger.json"
    url_map = tmp_path / "url-map.json"
    routes = [_route(), _route("GET", "/api/1/datasets/")]
    _write_routes(source, routes, commit=oracle.PINNED_COMMIT)
    _write_routes(swagger, routes)
    _write_routes(url_map, routes)

    result = oracle.reconcile_route_documents(source, swagger, url_map)

    assert result["status"] == "reconciled"
    assert result["route_count"] == 2
    assert result["source_commit"] == oracle.PINNED_COMMIT
    assert set(result["input_digests"]) == {"source", "swagger", "url_map"}


def test_reconcile_fails_closed_on_missing_or_extra_route(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    swagger = tmp_path / "swagger.json"
    url_map = tmp_path / "url-map.json"
    _write_routes(source, [_route()], commit=oracle.PINNED_COMMIT)
    _write_routes(swagger, [_route(), _route("GET", "/api/1/datasets/")])
    _write_routes(url_map, [_route()])

    with pytest.raises(oracle.ReconciliationError, match="swagger"):
        oracle.reconcile_route_documents(source, swagger, url_map)


def test_reconcile_rejects_wrong_or_ambiguous_source_commit(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    swagger = tmp_path / "swagger.json"
    url_map = tmp_path / "url-map.json"
    _write_routes(source, [_route()], commit="0" * 40)
    _write_routes(swagger, [_route()])
    _write_routes(url_map, [_route()])

    with pytest.raises(oracle.PinnedSourceError, match=oracle.PINNED_COMMIT):
        oracle.reconcile_route_documents(source, swagger, url_map)


def test_route_documents_reject_duplicates_and_unknown_methods(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    _write_routes(duplicate, [_route(), _route()], commit=oracle.PINNED_COMMIT)
    with pytest.raises(oracle.ReconciliationError, match="duplicate"):
        oracle.load_route_document(duplicate, require_pinned_commit=True)

    invalid = tmp_path / "invalid.json"
    _write_routes(invalid, [_route("TRACE")], commit=oracle.PINNED_COMMIT)
    with pytest.raises(oracle.ReconciliationError, match="method"):
        oracle.load_route_document(invalid, require_pinned_commit=True)


def test_extract_source_document_uses_the_pinned_checkout_and_records_signatures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "udata-source"
    api_init = source_root / "udata" / "api" / "__init__.py"
    module = source_root / "udata" / "core" / "site" / "api.py"
    api_init.parent.mkdir(parents=True)
    module.parent.mkdir(parents=True)
    api_init.write_text(
        "def init_app(app):\n    import udata.core.site.api\n",
        encoding="utf-8",
    )
    module.write_text(
        "from udata.api import api\n\n@api.route('/site/', methods=['GET', 'PATCH'])\ndef site():\n    return {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(oracle, "_source_commit", lambda path: oracle.PINNED_COMMIT)

    document = oracle.extract_source_document(source_root)

    assert document["source_commit"] == oracle.PINNED_COMMIT
    assert document["routes"] == [
        {
            "api_version": "v1",
            "method": "GET",
            "path": "/api/1/site/",
            "signature": "udata.core.site.api:site",
        },
        {
            "api_version": "v1",
            "method": "PATCH",
            "path": "/api/1/site/",
            "signature": "udata.core.site.api:site",
        },
    ]


def test_extract_source_document_rejects_an_unpinned_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(oracle, "_source_commit", lambda path: "0" * 40)

    with pytest.raises(oracle.PinnedSourceError, match=oracle.PINNED_COMMIT):
        oracle.extract_source_document(tmp_path)


def test_extract_source_document_derives_class_methods_when_route_methods_are_implicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "udata-source"
    api_init = source_root / "udata" / "api" / "__init__.py"
    module = source_root / "udata" / "core" / "site" / "api.py"
    api_init.parent.mkdir(parents=True)
    module.parent.mkdir(parents=True)
    api_init.write_text("def init_app(app):\n    import udata.core.site.api\n", encoding="utf-8")
    module.write_text(
        "from udata.api import api\n\n"
        "@api.route('/site/')\n"
        "class SiteResource:\n"
        "    def get(self):\n"
        "        return {}\n\n"
        "    def post(self):\n"
        "        return {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(oracle, "_source_commit", lambda path: oracle.PINNED_COMMIT)

    document = oracle.extract_source_document(source_root)

    assert {route["method"] for route in document["routes"]} == {"GET", "POST"}


def test_write_preflight_is_pending_and_does_not_create_production_artifacts(tmp_path: Path) -> None:
    preflight = tmp_path / "04-PREFLIGHT.md"
    candidate = tmp_path / "candidate.json"
    result = {
        "status": "reconciled",
        "route_count": 268,
        "source_commit": oracle.PINNED_COMMIT,
        "input_digests": {"source": "a" * 64, "swagger": "b" * 64, "url_map": "c" * 64},
        "routes": [_route()],
    }

    oracle.write_candidate(candidate, result)
    oracle.write_preflight(preflight, result)

    text = preflight.read_text(encoding="utf-8")
    assert "decision: pending" in text
    assert "targets_approved: false" in text
    assert candidate.is_file()
    assert not (tmp_path / "udata-17.6.json").exists()
    with pytest.raises(oracle.PreflightError, match="pending"):
        oracle.verify_preflight(preflight)


def test_verify_preflight_accepts_count_only_but_keeps_drift_blocked(tmp_path: Path) -> None:
    preflight = tmp_path / "04-PREFLIGHT.md"
    preflight.write_text(
        "\n".join(
            [
                "---",
                "schema_version: 1",
                "status: approved",
                "decision: approve-baseline-defer-targets",
                f"source_commit: {oracle.PINNED_COMMIT}",
                "route_count: 268",
                "source_digest: " + "a" * 64,
                "swagger_digest: " + "b" * 64,
                "url_map_digest: " + "c" * 64,
                "targets_approved: false",
                "approved_by: human",
                "approved_at: 2026-08-26T00:00:00Z",
                "---",
                "",
            ]
        ),
        encoding="utf-8",
    )

    result = oracle.verify_preflight(preflight)

    assert result["production_approved"] is True
    assert result["drift_approved"] is False


def test_controlled_stack_is_loopback_digest_pinned_and_capture_is_response_free(tmp_path: Path) -> None:
    compose = Path(__file__).resolve().parents[4] / "dev" / "udata-evidence" / "compose.yaml"
    text = compose.read_text(encoding="utf-8")
    images = [line.strip() for line in text.splitlines() if line.strip().startswith("image:")]
    dockerfile = compose.with_name("Dockerfile").read_text(encoding="utf-8")

    assert len(images) == 5
    assert all("@sha256:" in image for image in images)
    assert "127.0.0.1:" in text
    assert "HOME: /tmp" in text
    assert "volumes:" not in text
    assert "mongo" in text and "redis" in text and "elasticsearch" in text and "minio" in text and "mailpit" in text
    assert oracle.PINNED_COMMIT in dockerfile
    assert "ghcr.io/astral-sh/uv:0.12.5@sha256:" in dockerfile
    assert "python:3.13-slim-bookworm@sha256:" in dockerfile
    assert "flask-caching==2.3.1" in dockerfile
    output = tmp_path / "capture.json"
    capture.write_capture("http://127.0.0.1:5640", "17.6.0", output)
    record = json.loads(output.read_text(encoding="utf-8"))
    assert set(record) == {"schema_version", "origin_digest", "version"}
    with pytest.raises(ValueError, match="loopback"):
        capture.sanitize_origin("https://www.data.gouv.fr")


def test_seed_roles_only_executes_the_fixed_loopback_compose_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    compose = tmp_path / "compose.yaml"
    compose.write_text("services: {}\n", encoding="utf-8")
    calls: list[tuple[object, ...]] = []

    def run(command: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append(tuple(command))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(seed.subprocess, "run", run)

    seed.seed_roles("http://127.0.0.1:5640", compose)

    assert calls == [
        ("docker", "compose", "-f", str(compose), "exec", "-T", "udata", "python", "-c", seed.SEED_PROGRAM)
    ]
    with pytest.raises(ValueError, match="127.0.0.1"):
        seed.seed_roles("http://localhost:5640", compose)


def test_extract_source_document_inherits_base_class_http_methods(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "udata-source"
    api_init = source_root / "udata" / "api" / "__init__.py"
    base_module = source_root / "udata" / "core" / "followers" / "api.py"
    module = source_root / "udata" / "core" / "dataset" / "api.py"
    api_init.parent.mkdir(parents=True)
    base_module.parent.mkdir(parents=True)
    module.parent.mkdir(parents=True)
    api_init.write_text("def init_app(app):\n    import udata.core.dataset.api\n", encoding="utf-8")
    base_module.write_text(
        "from udata.api import API\n\n"
        "class FollowAPI(API):\n"
        "    def get(self, id):\n        return {}\n\n"
        "    def post(self, id):\n        return {}\n\n"
        "    def delete(self, id):\n        return {}\n",
        encoding="utf-8",
    )
    module.write_text(
        "from udata.api import api\n"
        "from udata.core.followers.api import FollowAPI\n\n"
        "@api.route('/datasets/<id>/followers/')\n"
        "class DatasetFollowersAPI(FollowAPI):\n"
        "    model = None\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(oracle, "_source_commit", lambda path: oracle.PINNED_COMMIT)

    document = oracle.extract_source_document(source_root)

    followers = [route for route in document["routes"] if "followers" in route["path"]]
    assert {route["method"] for route in followers} == {"GET", "POST", "DELETE"}
    assert followers[0]["signature"] == "udata.core.dataset.api:DatasetFollowersAPI"


def test_extract_source_document_follows_transitive_stock_imports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "udata-source"
    api_init = source_root / "udata" / "api" / "__init__.py"
    importer = source_root / "udata" / "core" / "dataset" / "api.py"
    extension = source_root / "udata" / "core" / "captcha.py"
    api_init.parent.mkdir(parents=True)
    importer.parent.mkdir(parents=True)
    api_init.write_text("def init_app(app):\n    import udata.core.dataset.api\n", encoding="utf-8")
    importer.write_text("import udata.core.captcha\n", encoding="utf-8")
    extension.write_text(
        "from udata.api import apiv2\n\n"
        "ns = apiv2.namespace('captcha', 'Captcha operations')\n\n"
        "@ns.route('/')\n"
        "class CaptchaAPI:\n"
        "    def get(self):\n        return {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(oracle, "_source_commit", lambda path: oracle.PINNED_COMMIT)

    document = oracle.extract_source_document(source_root)

    assert document["routes"] == [
        {
            "api_version": "v2",
            "method": "GET",
            "path": "/api/2/captcha/",
            "signature": "udata.core.captcha:CaptchaAPI",
        }
    ]


def test_reconcile_canonicalizes_converter_and_swagger_path_spellings(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    swagger = tmp_path / "swagger.json"
    url_map = tmp_path / "url-map.json"
    _write_routes(
        source,
        [{"method": "GET", "path": "/api/1/datasets/<dataset:dataset>/", "api_version": "v1"}],
        commit=oracle.PINNED_COMMIT,
    )
    _write_routes(swagger, [{"method": "GET", "path": "/api/1/datasets/{dataset}/", "api_version": "v1"}])
    _write_routes(url_map, [{"method": "GET", "path": "/api/1/datasets/<dataset>/", "api_version": "v1"}])

    result = oracle.reconcile_route_documents(source, swagger, url_map)

    assert result["route_count"] == 1


def test_reconcile_lets_swagger_omit_only_the_oauth_blueprint(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    swagger = tmp_path / "swagger.json"
    url_map = tmp_path / "url-map.json"
    routes = [
        {"method": "GET", "path": "/api/1/site/", "api_version": "v1"},
        {"method": "POST", "path": "/oauth/token", "api_version": "oauth"},
    ]
    _write_routes(source, routes, commit=oracle.PINNED_COMMIT)
    _write_routes(swagger, [routes[0]])
    _write_routes(url_map, routes)

    assert oracle.reconcile_route_documents(source, swagger, url_map)["route_count"] == 2

    _write_routes(swagger, [])
    with pytest.raises(oracle.ReconciliationError, match="swagger"):
        oracle.reconcile_route_documents(source, swagger, url_map)


def test_url_map_capture_filters_infrastructure_and_scoped_namespaces(tmp_path: Path) -> None:
    raw = tmp_path / "raw.json"
    raw.write_text(
        json.dumps(
            [
                {"path": "/api/1/", "method": "GET", "endpoint": "api.doc"},
                {"path": "/api/1/swagger.json", "method": "GET", "endpoint": "api.specs"},
                {"path": "/api/1/proconnect/auth", "method": "GET", "endpoint": "api.proconnect_auth"},
                {"path": "/api/1/site/", "method": "GET", "endpoint": "api.site"},
                {"path": "/api/1/site/", "method": "OPTIONS", "endpoint": "api.site"},
                {"path": "/static/logo.png", "method": "GET", "endpoint": "static"},
            ]
        ),
        encoding="utf-8",
    )
    output = tmp_path / "url-map-routes.json"

    document = route_capture.capture_url_map(
        raw, output, {"/api/1/proconnect": route_capture.DEFAULT_EXCLUSION_REASONS["/api/1/proconnect"]}
    )

    assert [route["path"] for route in document["routes"]] == ["/api/1/site/"]
    assert document["excluded"] == [
        {"path": "/api/1/", "reason": "flask-restx documentation infrastructure"},
        {"path": "/api/1/swagger.json", "reason": "flask-restx documentation infrastructure"},
        {
            "path": "/api/1/proconnect/auth",
            "reason": route_capture.DEFAULT_EXCLUSION_REASONS["/api/1/proconnect"],
        },
    ]
