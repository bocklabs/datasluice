# DataSluice

Glossary for the DataSluice catalog SDK: typed clients for public-data catalog platforms (CKAN, uData, Socrata).

## Language

### Platforms and access

**Catalog**:
A public-data portal deployment that exposes one supported platform API.
_Avoid_: portal (in user-facing docs), site

**Connector**:
The canonical per-platform package providing typed sync and async clients for one catalog platform; the project's universal term for these bindings, including code identifiers.
_Avoid_: adapter

**Live Client**:
A connector client that communicates with a real catalog deployment over real transport, as opposed to the deterministic fakes used in contract tests.

**Normalized Catalog API**:
The small cross-platform interface shared by every connector.
_Avoid_: common API, generic layer

**Native Service Group**:
A typed projection of one platform-specific API namespace on a connector.

**Extension Mapping**:
Lossless preservation of platform-native response fields beyond the typed model fields.
_Avoid_: raw dict, extra fields

### Capabilities

**Capability Profile**:
A versioned declaration of which operations a platform API version supports and under which tier.
_Avoid_: feature matrix

**Effective Capability**:
The set of capabilities a concrete catalog deployment actually exposes, resolved at runtime.

**Core Capability**:
An operation present on every stock installation of the pinned platform version.

**Optional Capability**:
An operation available only when a server-side extension or plugin is enabled.

**Deployment-Disabled Capability**:
An operation the platform declares but the target deployment has switched off.

**OperationId**:
The atomic unit of capability evidence; evidence for one operation never implies another.

**Line-Drift Advisory**:
A local event noting that a deployment's platform version sits outside the pinned profile's release line; advisories inform but never block access.

### Evidence

**Fixture-Backed Contract Case**:
Deterministic behavior evidence drawn from the versioned corpus, run on every PR.

**Controlled Mutation Environment**:
A disposable scripted instance of a real platform used to prove write and destructive operations safely.

**Representative Deployment**:
A chosen public catalog that serves as the live-read evidence source for its platform.

**Drift Check**:
A scheduled read-only comparison of representative deployments against recorded evidence, producing advisories.

**Compliance Report**:
The machine-readable outcome of running the public contract suite against a connector.

### Safety

**Guard**:
The caller-supplied authorization assertion that must match an operation before dispatch.
