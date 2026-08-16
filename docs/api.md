# API Reference

DataSluice ships three public API areas: the package root (catalog
models, typed catalog errors, and the retained data-plane facade), the
public catalog contract suite, and the explicit platform packages.

## Package root

Shared catalog models, typed catalog errors, and the direct-resource
data-plane facade (`DataSluice`, `DirectResourceLocator`,
`OpenedResource`, normalized records, and envelopes):

::: datasluice

## Public catalog contract suite

The executable contract API for built-in and third-party connectors —
normalized client Protocols, pinned reference fixtures, the compliance
runner and report, certification, and namespaced manifest types. This is
the only public entry point for contract execution and certification:

::: datasluice.contracts.catalog

## Platform packages

Each platform exports exactly one adapter façade class and one factory
function; imports are always explicit and package-level:

::: datasluice.connectors.catalog.ckan

::: datasluice.connectors.catalog.udata

::: datasluice.connectors.catalog.socrata

The `datasluice.connectors.catalog` namespace itself re-exports nothing.
Installable named connector extras are owned by Phase 2 packaging work
and are not part of this package surface.
