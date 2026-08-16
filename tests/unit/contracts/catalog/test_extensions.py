"""Contract tests for catalog connector extension metadata."""

import pytest

from datasluice.domain.catalog.extensions import (
    ActivationPolicy,
    CertificationRecord,
    ConnectorId,
    ConnectorManifest,
    OptionalInstallRequirement,
)


def _certification(connector_id: ConnectorId) -> CertificationRecord:
    return CertificationRecord(
        connector_id=connector_id,
        contract_schema_version="1.0",
        profile_version="2026.08",
        report_version="1.0",
        report_id="sha256:compliance-report",
    )


def _requirement() -> OptionalInstallRequirement:
    return OptionalInstallRequirement(
        extra="acme-portal",
        install_hint="Install DataSluice with `datasluice[acme-portal]`.",
    )


def test_connector_ids_require_namespaces_and_reserve_builtin_ids() -> None:
    assert ConnectorId.parse("acme/portal") == ConnectorId(vendor="acme", platform="portal")
    assert ConnectorId.parse("datasluice/ckan").is_builtin

    with pytest.raises(ValueError, match="vendor/platform"):
        ConnectorId.parse("portal")

    with pytest.raises(ValueError, match="reserved"):
        ConnectorId(vendor="datasluice", platform="other")


def test_third_party_manifests_need_explicit_activation_and_certification_metadata() -> None:
    connector_id = ConnectorId.parse("acme/portal")
    manifest = ConnectorManifest(
        connector_id=connector_id,
        entry_point="acme_portal.connector:create_connector",
        profile_version="2026.08",
        activation_policy=ActivationPolicy.EXPLICIT,
        optional_requirements=(_requirement(),),
        certification=_certification(connector_id),
    )

    assert manifest.activation_policy is ActivationPolicy.EXPLICIT
    assert manifest.certification is not None
    assert manifest.certification.report_id == "sha256:compliance-report"

    with pytest.raises(ValueError, match="entry point"):
        ConnectorManifest(
            connector_id=connector_id,
            entry_point="",
            profile_version="2026.08",
            activation_policy=ActivationPolicy.EXPLICIT,
            optional_requirements=(_requirement(),),
            certification=_certification(connector_id),
        )

    with pytest.raises(ValueError, match="certification"):
        ConnectorManifest(
            connector_id=connector_id,
            entry_point="acme_portal.connector:create_connector",
            profile_version="2026.08",
            activation_policy=ActivationPolicy.EXPLICIT,
            optional_requirements=(_requirement(),),
            certification=None,
        )


def test_plugins_default_to_inactive_and_builtin_overrides_require_selection() -> None:
    connector_id = ConnectorId.parse("acme/portal")
    builtin_id = ConnectorId.parse("datasluice/ckan")
    manifest = ConnectorManifest(
        connector_id=connector_id,
        entry_point="acme_portal.connector:create_connector",
        profile_version="2026.08",
        optional_requirements=(_requirement(),),
        certification=_certification(connector_id),
        overrides=builtin_id,
    )

    assert manifest.activation_policy is ActivationPolicy.INACTIVE
    assert not manifest.is_activated(None)
    assert not manifest.is_activated(builtin_id)
    assert manifest.is_activated(connector_id)

    with pytest.raises(ValueError, match="declare explicit activation"):
        manifest.require_activation(builtin_id)

    with pytest.raises(ValueError, match="declare explicit activation"):
        manifest.require_activation(connector_id)


def test_optional_requirements_are_descriptive_and_never_install_at_runtime() -> None:
    requirement = _requirement()

    assert requirement.extra == "acme-portal"
    assert requirement.install_hint == "Install DataSluice with `datasluice[acme-portal]`."

    with pytest.raises(ValueError, match="runtime installation"):
        OptionalInstallRequirement(
            extra="acme-portal",
            install_hint="Run pip install acme-portal now.",
        )
