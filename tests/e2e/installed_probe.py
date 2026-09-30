import importlib
import importlib.util
import sys

PUBLIC_SURFACE = (
    "datasluice",
    "datasluice.cli",
    "datasluice.data",
    "datasluice.data.readers",
    "datasluice.io",
    "datasluice.sync",
    "datasluice.discovery",
    "datasluice.integrations.dlt",
    "datasluice.runtime",
    "datasluice.runtime.bulk",
    "datasluice.runtime.mutation",
    "datasluice.runtime.oauth",
    "datasluice.connectors.catalog.ckan",
    "datasluice.connectors.catalog.udata",
    "datasluice.connectors.catalog.socrata",
)
OPTIONAL_DISTRIBUTIONS = (
    "boto3",
    "dlt",
    "duckdb",
    "fsspec",
    "httpx",
    "hvac",
    "keyring",
    "openpyxl",
    "opentelemetry",
    "pandas",
    "polars",
    "pyarrow",
    "zstandard",
)
FIXTURE_SETS = ("ckan", "udata", "socrata")


def _loaded_optional_distributions() -> list[str]:
    return [
        name
        for name in OPTIONAL_DISTRIBUTIONS
        if any(module == name or module.startswith(name + ".") for module in sys.modules)
    ]


def _probe_import_sweep() -> int:
    for name in PUBLIC_SURFACE:
        importlib.import_module(name)
    loaded = _loaded_optional_distributions()
    if loaded:
        print(f"public surface pulled optional distributions: {loaded}")
        return 1
    return 0


def _probe_fixture_sets() -> int:
    contracts = importlib.import_module("datasluice.contracts.catalog")
    print([contracts.load_reference_fixture_set(name).platform for name in FIXTURE_SETS])
    return 0


def _probe_no_optional_dependencies() -> int:
    absent = [importlib.util.find_spec(name) is None for name in ("pyarrow", "httpx", "fsspec")]
    print(*absent)
    return 0 if all(absent) else 1


def _probe_live_gates() -> int:
    importlib.import_module("httpx")
    from datasluice.connectors.catalog.ckan import CKANClientSettings, create_sync_client
    from datasluice.connectors.catalog.udata import UDataClientSettings
    from datasluice.connectors.catalog.udata import create_sync_client as create_udata_sync

    client = create_sync_client(CKANClientSettings(base_url="https://demo.ckan.org"))
    assert hasattr(client, "transport")
    client.close()
    udata_client = create_udata_sync(UDataClientSettings(base_url="http://127.0.0.1:5640"))
    assert hasattr(udata_client, "transport")
    udata_client.close()
    return _probe_socrata_live_gate()


def _probe_socrata_live_gate() -> int:
    module = importlib.import_module("datasluice.connectors.catalog.socrata.live")
    try:
        module.create_live_client()
    except NotImplementedError:
        return 0
    except ImportError as error:
        raise AssertionError("socrata extra gate did not unlock") from error
    raise AssertionError("socrata live seam unexpectedly returned")


def _probe_live_gate_error(platform: str) -> int:
    live = importlib.import_module(f"datasluice.connectors.catalog.{platform}.live")
    try:
        live.create_live_client()
    except ImportError as error:
        print(str(error))
        return 0
    raise AssertionError(f"{platform} live seam did not report the platform extra")


_PROBES = {
    "import-sweep": _probe_import_sweep,
    "fixture-sets": _probe_fixture_sets,
    "no-optional": _probe_no_optional_dependencies,
    "live-gates": _probe_live_gates,
}


def main(argv: list[str]) -> int:
    mode = argv[1]
    if mode == "live-gate-error":
        return _probe_live_gate_error(argv[2])
    probe = _PROBES.get(mode)
    if probe is None:
        print(f"unknown probe: {mode}")
        return 2
    return probe()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
