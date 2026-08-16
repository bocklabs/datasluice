# Apache Airflow Integration

DataSluice Phase 1 defines the explicit connector contract boundary only. It
publishes typed connector contracts, capability evidence, and reference fakes
under `datasluice.contracts.catalog`; it does not ship a live Airflow execution
surface.

The `apache-airflow-providers-datasluice` distribution registers package
metadata for the `airflow.providers.datasluice` namespace and nothing else: no
connection type, no hook, and no operators. The distribution exists so the
namespace and its discovery metadata stay reserved; it must not be installed
expecting runtime integration.

- Phase 1 owns explicit connector injection and the executable contract suite.
- Phase 2 owns connector packaging and the named install extras.
- Live provider operators arrive only with the canonical platform executors of
  the later implementation phases.
