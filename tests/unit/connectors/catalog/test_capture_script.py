"""Loopback unit coverage for the controlled-stack evidence capture driver.

Pins the driver's credential-file handling (owner-only 0600, zero token flags),
the declared-baseline loopback posture visible at the settings seam, receipt
collection through typed methods with confirmed destructive policies, and the
bulk adapter's mapping of typed create/delete calls onto the BulkExecutor
receipt contract — all against scripted loopback fakes, before the human gate
ever runs the driver against the real stack (D-10/D-20/D-25).
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import replace
from pathlib import Path

import pytest

from datasluice.connectors.catalog.ckan.clients import create_sync_client
from datasluice.domain.catalog.auth import CKANCredential
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

MODULE_PATH = Path(__file__).resolve().parents[4] / "scripts" / "capture_stack_evidence.py"
_spec = importlib.util.spec_from_file_location("capture_stack_evidence", MODULE_PATH)
assert _spec is not None and _spec.loader is not None
capture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(capture)

LOOPBACK_ORIGIN = "http://127.0.0.1:5500"
FAKE_TOKEN = "evidence-capture-token"
STACK: dict[str, object] = {
    "ckan_version": "2.11.5",
    "images": [{"reference": "ckan/ckan-base:2.11.5", "digest": "sha256:test"}],
}


class ScriptedTransport:
    """A deterministic loopback transport answering scripted responses per action."""

    def __init__(self, responses: dict[str, tuple[int, object]] | None = None) -> None:
        self.responses = dict(responses or {})
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        action = request.url.rsplit("/", 1)[-1]
        status, result = self.responses.get(action, (200, None))
        if status == 200:
            body: dict[str, object] = {"success": True, "result": result}
        else:
            body = {"success": False, "error": {"__type": "Authorization Error", "message": "Not authorized"}}
        return RuntimeResponse(
            status_code=status,
            headers={"Content-Type": "application/json"},
            body=json.dumps(body).encode("utf-8"),
        )


def _driver_client(transport: ScriptedTransport):
    settings = replace(
        capture.build_capture_settings(LOOPBACK_ORIGIN, CKANCredential(api_token=FAKE_TOKEN)),
        sync_transport=transport,
    )
    return create_sync_client(settings)


def _action_names(transport: ScriptedTransport) -> list[str]:
    return [request.url.rsplit("/", 1)[-1] for request in transport.requests]


def test_parser_exposes_exactly_the_four_documented_flags() -> None:
    """The CLI surface is exactly origin/credentials-file/bulk-count/out-dir."""
    parser = capture.build_parser()
    option_strings = {string for action in parser._actions for string in action.option_strings}
    assert option_strings == {"-h", "--origin", "--credentials-file", "--bulk-count", "--out-dir"}
    args = parser.parse_args(
        [
            "--origin",
            LOOPBACK_ORIGIN,
            "--credentials-file",
            "/tmp/creds.json",
            "--bulk-count",
            "5",
            "--out-dir",
            "/tmp/out",
        ]
    )
    assert (args.origin, args.credentials_file, args.bulk_count, args.out_dir) == (
        LOOPBACK_ORIGIN,
        "/tmp/creds.json",
        5,
        "/tmp/out",
    )


@pytest.mark.parametrize("flag", ["--token", "--api-token", "--ckan-token"])
def test_parser_refuses_token_style_flags(flag: str) -> None:
    """No token can cross the command line: token-style flags fail to parse."""
    with pytest.raises(SystemExit):
        capture.build_parser().parse_args(
            [flag, "secret-value", "--origin", LOOPBACK_ORIGIN, "--credentials-file", "c", "--out-dir", "o"]
        )


def test_load_credentials_reads_owner_only_file_into_typed_credentials(tmp_path: Path) -> None:
    """A 0600 credentials file yields one typed credential per seeded role."""
    path = tmp_path / "creds.json"
    path.write_text(
        json.dumps(
            {
                "origin": LOOPBACK_ORIGIN,
                "tokens": {"sysadmin": "tok-sysadmin", "org_admin": "tok-org", "user": "tok-user"},
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)

    credentials = capture.load_credentials(path)

    assert set(credentials) == {"sysadmin", "org_admin", "user"}
    assert all(isinstance(credential, CKANCredential) for credential in credentials.values())
    assert "tok-sysadmin" not in repr(credentials)
    assert capture.recorded_origin(path) == LOOPBACK_ORIGIN


def test_load_credentials_rejects_group_readable_file(tmp_path: Path) -> None:
    """Group- or world-readable credentials files are refused outright."""
    path = tmp_path / "creds.json"
    path.write_text(json.dumps({"tokens": {role: "t" for role in capture.CREDENTIAL_ROLES}}), encoding="utf-8")
    path.chmod(0o640)

    with pytest.raises(SystemExit) as excinfo:
        capture.load_credentials(path)

    assert "0600" in str(excinfo.value)


def test_load_credentials_rejects_missing_role(tmp_path: Path) -> None:
    """Every seeded role must be represented before any client is constructed."""
    path = tmp_path / "creds.json"
    path.write_text(json.dumps({"tokens": {"sysadmin": "t", "org_admin": "t"}}), encoding="utf-8")
    path.chmod(0o600)

    with pytest.raises(SystemExit):
        capture.load_credentials(path)


def test_build_capture_settings_pins_declared_baseline_for_the_loopback_origin() -> None:
    """The http loopback posture is explicit at the settings seam and constructs."""
    settings = capture.build_capture_settings(LOOPBACK_ORIGIN, CKANCredential(api_token=FAKE_TOKEN))

    assert settings.probe_policy == "declared-baseline"
    assert settings.base_url == LOOPBACK_ORIGIN
    assert isinstance(settings.credential, CKANCredential)
    client = create_sync_client(replace(settings, sync_transport=ScriptedTransport()))
    assert client._probe_policy == "declared-baseline"


def test_record_provenance_embeds_version_and_image_references() -> None:
    """Provenance records the CKAN-reported version and every image reference."""
    transport = ScriptedTransport(responses={"status_show": (200, {"ckan_version": "2.11.5"})})
    client = _driver_client(transport)

    out: dict[str, object] = {}
    capture.record_provenance(client, out)

    stack = out["stack"]
    assert isinstance(stack, dict)
    assert stack["ckan_version"] == "2.11.5"
    images = stack["images"]
    assert [image["reference"] for image in images] == list(capture.EVIDENCE_IMAGES)
    assert all("@sha256:" in image["digest"] or image["digest"] == "unavailable" for image in images)


def test_sysadmin_flow_collects_one_confirmed_receipt_per_purge_family(tmp_path: Path) -> None:
    """Typed create+purge flows yield receipt artifacts for all three purge families."""
    transport = ScriptedTransport(
        responses={
            "package_create": (200, {"name": capture.SCRATCH_DATASET, "id": "pkg-1"}),
            "organization_create": (200, {"name": capture.SCRATCH_ORG, "id": "org-1"}),
            "group_create": (200, {"name": capture.SCRATCH_GROUP, "id": "grp-1"}),
        }
    )
    client = _driver_client(transport)

    paths = capture.capture_sysadmin_receipts(client, tmp_path, {"stack": STACK})

    assert len(paths) == 3
    for action in ("dataset_purge", "organization_purge", "group_purge"):
        document = json.loads((tmp_path / "receipts" / f"sysadmin-{action}.json").read_text(encoding="utf-8"))
        assert document["kind"] == "capture_receipt"
        assert document["role"] == "sysadmin"
        assert document["action"] == action
        assert document["receipt"]["outcome"] == "succeeded"
        assert document["stack"] == STACK
    assert set(_action_names(transport)) == {
        "package_create",
        "dataset_purge",
        "organization_create",
        "organization_purge",
        "group_create",
        "group_purge",
    }
    assert all(request.headers.get("Authorization") == FAKE_TOKEN for request in transport.requests)


def test_org_admin_flow_captures_success_and_forbidden_tiers(tmp_path: Path) -> None:
    """The org-admin tier yields standard-tier receipts plus a forbidden purge observation."""
    transport = ScriptedTransport(
        responses={
            "package_create": (200, {"name": capture.ORG_ADMIN_DATASET, "id": "pkg-2"}),
            "package_delete": (200, {"id": capture.ORG_ADMIN_DATASET}),
            "dataset_purge": (403, None),
        }
    )
    client = _driver_client(transport)

    paths = capture.capture_org_admin_receipts(client, tmp_path, {"stack": STACK})

    names = {path.name for path in paths}
    assert names == {
        "org_admin-package_create.json",
        "org_admin-package_delete.json",
        "org_admin-dataset_purge-forbidden.json",
    }
    for name in ("org_admin-package_create.json", "org_admin-package_delete.json"):
        document = json.loads((tmp_path / "receipts" / name).read_text(encoding="utf-8"))
        assert document["receipt"]["outcome"] == "succeeded"
    forbidden = json.loads(
        (tmp_path / "receipts" / "org_admin-dataset_purge-forbidden.json").read_text(encoding="utf-8")
    )
    assert forbidden["kind"] == "capture_forbidden"
    assert forbidden["role"] == "org_admin"
    assert forbidden["action"] == "dataset_purge"
    assert forbidden["capability_state"] == "forbidden"


def test_org_admin_flow_fails_loudly_when_the_purge_unexpectedly_succeeds(tmp_path: Path) -> None:
    """A purge that succeeds under the org-admin tier invalidates the tier evidence."""
    transport = ScriptedTransport(
        responses={
            "package_create": (200, {"name": capture.ORG_ADMIN_DATASET, "id": "pkg-2"}),
            "package_delete": (200, {"id": capture.ORG_ADMIN_DATASET}),
            "dataset_purge": (200, None),
        }
    )
    client = _driver_client(transport)

    with pytest.raises(SystemExit):
        capture.capture_org_admin_receipts(client, tmp_path, {"stack": STACK})


def test_user_flow_captures_the_regular_tier_forbidden_observation(tmp_path: Path) -> None:
    """The regular-user tier's administrative attempt is captured as forbidden."""
    transport = ScriptedTransport(responses={"organization_create": (403, None)})
    client = _driver_client(transport)

    path = capture.capture_user_forbidden(client, tmp_path, {"stack": STACK})

    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["kind"] == "capture_forbidden"
    assert document["role"] == "user"
    assert document["action"] == "organization_create"
    assert document["capability_state"] == "forbidden"


def test_identity_presence_asserts_each_identity_never_user_counts() -> None:
    """Presence is asserted per seeded identity; misses mark False without counting."""
    present = ScriptedTransport(responses={"user_show": (200, {"id": "u", "name": "u"})})
    assert capture.capture_identity_presence(_driver_client(present)) == {
        identity: True for identity in capture.IDENTITIES
    }

    missing = ScriptedTransport(responses={"user_show": (404, None)})
    assert capture.capture_identity_presence(_driver_client(missing)) == {
        identity: False for identity in capture.IDENTITIES
    }


def test_bulk_execute_item_maps_typed_create_and_delete_to_receipts() -> None:
    """The adapter returns per-item receipts from typed package_create/package_delete."""
    transport = ScriptedTransport(
        responses={
            "package_create": (200, {"name": "datasluice-capture-bulk-000"}),
            "package_delete": (200, {"id": "datasluice-capture-bulk-000"}),
        }
    )
    client = _driver_client(transport)
    item = CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "datasluice-capture-bulk-000")

    create_receipt = capture.bulk_execute_item(client, lambda: None, mode="create")(item)
    delete_receipt = capture.bulk_execute_item(client, lambda: None, mode="delete")(item)

    assert create_receipt.outcome == "succeeded"
    assert delete_receipt.outcome == "succeeded"
    assert create_receipt.target == item
    assert delete_receipt.target == item
    bodies = [json.loads(request.body or b"{}") for request in transport.requests]
    assert bodies[0] == {"name": "datasluice-capture-bulk-000", "owner_org": capture.EVIDENCE_ORG}
    assert bodies[1] == {"id": "datasluice-capture-bulk-000"}


def test_bulk_execute_item_rejects_unknown_modes() -> None:
    """Modes outside create/delete are refused before any client call."""
    with pytest.raises(ValueError):
        capture.bulk_execute_item(_driver_client(ScriptedTransport()), lambda: None, mode="purge")


def test_bulk_run_capture_streams_ordered_receipts_through_the_executor(tmp_path: Path) -> None:
    """The D-25 bulk run executes both phases through the runtime BulkExecutor."""
    transport = ScriptedTransport(
        responses={"package_create": (200, {"name": "bulk"}), "package_delete": (200, {"id": "bulk"})}
    )
    client = _driver_client(transport)

    path = capture.run_bulk_capture(client, 3, tmp_path, {"stack": STACK})

    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["kind"] == "bulk_run_evidence"
    assert document["bulk_count"] == 3
    assert document["stack"] == STACK
    assert len(document["phases"]) == 2
    for phase in document["phases"]:
        assert phase["item_receipt_indexes"] == [0, 1, 2]
        assert phase["item_outcomes"] == ["succeeded", "succeeded", "succeeded"]
        assert phase["summary"]["succeeded"] == 3
        assert phase["summary"]["failed"] == 0
        assert phase["summary"]["state"] == "completed"
        assert phase["checkpoints_recorded"] >= 1
        assert phase["last_checkpoint_settled"] == 3
        assert len(phase["plan"]["items"]) == 3
        assert phase["plan"]["items"][0]["value"] == "datasluice-capture-bulk-000"
    assert {phase["mode"] for phase in document["phases"]} == {"create", "delete"}
    create_dispatches = [name for name in _action_names(transport) if name == "package_create"]
    delete_dispatches = [name for name in _action_names(transport) if name == "package_delete"]
    assert len(create_dispatches) == 3
    assert len(delete_dispatches) == 3


def test_cleanup_scratch_purges_every_capture_dataset() -> None:
    """Post-capture cleanup purges the org-admin dataset and every bulk dataset."""
    transport = ScriptedTransport()
    client = _driver_client(transport)

    capture.cleanup_scratch(client, 2)

    assert _action_names(transport) == ["dataset_purge", "dataset_purge", "dataset_purge"]
