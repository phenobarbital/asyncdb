---
type: feature
base_branch: dev
---

# SDD Brainstorm: windows-compatibility

| Field | Value |
|---|---|
| **Feature** | windows-compatibility |
| **Status** | exploration |
| **Date** | 2026-09-06 |
| **Author** | Jesus Lara / Codex |

## 1. Problem Statement

AsyncDB cannot currently make a clear Windows compatibility promise for its
core package. `uvloop` is optional in project metadata, but `asyncdb.connections`
imports and invokes `install_uvloop()` during module import. Other provider
modules import optional or platform-sensitive packages at module scope, so a
consumer may be unable to import a driver factory or discover a provider merely
because an unrelated optional dependency is unavailable on Windows.

The release workflow contains a Windows job, but the wheel request is expressed
as one generic `CIBW_BUILD` selector. That makes it difficult to prove that the
Windows job requested the intended ABI/tag set, and the current validation only
checks for the compiled extension, not that a wheel can be installed and import
the core package without Linux-only extras.

### Who Is Affected

- Windows developers installing AsyncDB for supported pure-Python and
  Windows-compatible drivers.
- Package maintainers publishing release wheels and diagnosing failed builds.
- Downstream applications that only need one provider but inherit failures from
  unrelated optional imports.

### Constraints & Requirements

- Preserve the existing `AsyncDB`/`AsyncPool` factory behavior and driver names.
- Keep optional providers optional; importing the core package must not require
  every provider extra.
- Keep `uvloop` available as an opt-in performance extra, while making its
  absence or platform incompatibility non-fatal.
- Do not silently claim that every optional database driver supports Windows;
  document support per extra/provider.
- Retain the Cython extension requirement represented by
  `asyncdb/utils/types.*.so` or `.pyd` in release wheels.
- Request and publish Windows wheels for the supported CPython versions, with
  deterministic CI checks and no dependency installation solely for wheel
  inspection.
- Avoid introducing dependencies not already present in `pyproject.toml`
  without a separate decision.

## 2. Interactive Discovery Summary

### Round 0 — Flow Type

| Question | Answer |
|---|---|
| Change type | `feature` (default; this is a compatibility improvement rather than a release-blocking hotfix) |
| Base branch | `dev` (default; current branch is `dev`) |

### Round 1 — Intent and Scope

| Question | Answer |
|---|---|
| Core use case | Install/import AsyncDB on Windows and use supported drivers without Linux-only packages being imported eagerly. |
| Integration points | `pyproject.toml`, `asyncdb.connections`, `asyncdb.utils.uv`, optional driver modules, and `.github/workflows/release.yml`. |
| Expected behavior | Provider selection should load only the requested provider and report a focused missing-extra error when that provider is not installed. |
| Release behavior | The workflow should explicitly request Windows wheel builds and publish them alongside Linux and macOS artifacts. |
| Success criteria | Core import succeeds on Windows; unsupported extras are isolated; Windows wheels exist for each supported CPython ABI and pass archive/import smoke checks. |

### Round 2 — Gaps, Edge Cases, and Tradeoffs

| Question | Answer |
|---|---|
| What is the compatibility boundary? | Recommend a core-first Windows guarantee, with provider/extra support recorded separately. Native or OS-dependent providers remain opt-in and may be unsupported. |
| What happens when `uvloop` is installed on Windows? | Treat it as optional and non-fatal: do not activate it outside supported platforms, and retain the standard asyncio policy. |
| What if a requested provider dependency is missing? | Raise the existing factory-level `DriverError` with the provider and extra needed; do not mask unrelated import errors during core import. |
| What can break releases? | Broad cibuildwheel selectors, build isolation differences, Cython extension naming, wheel tests that install all extras, and artifact globbing/tag detection. |
| Main tradeoff | More explicit import boundaries and CI matrix configuration increase maintenance, but make platform failures local, diagnosable, and testable. |

## 3. Code Context

### User-provided request (preserved verbatim)

> convert `uvloop` and other linux-only dependencies on lazy-import and optional dependencies + fix the current github release.yml to request build Windows wheels

### Verified current code and contracts

- `asyncdb/connections.py:1-10` imports `install_uvloop` and calls it at module
  import time. `AsyncDB.__new__` at `asyncdb/connections.py:31-44` and
  `AsyncPool.__new__` at `asyncdb/connections.py:13-28` resolve a provider module
  dynamically through `module_exists`.
- `asyncdb/utils/uv.py:4-12` already lazy-imports `uvloop` inside
  `install_uvloop()` and catches `ImportError`, but callers currently invoke it
  unconditionally from `connections.py`. The compatibility design must address
  activation/platform policy, not only the local import statement.
- `asyncdb/utils/modules.py:6-19` uses `import_module`/`__import__` and catches
  `ImportError` while resolving a provider. This is the natural boundary for a
  focused optional-dependency error, but broad exception handling must not hide
  errors raised by provider code after its dependencies import.
- `pyproject.toml:49-67` places `asyncpg`, pandas, `aiohttp`, and other packages
  in core `dependencies`; `pyproject.toml:72-74` declares `uvloop` as an
  optional extra. The `default`, `dataframe`, and provider-specific extras are
  declared at `pyproject.toml:75-199`; several include native or platform-
  sensitive packages such as `pymssql`, `mysqlclient`, `pyodbc`, `JPype1`, and
  `acsylla`.
- `asyncdb/models/model.py:11-26` imports NumPy and datamodel at module scope and
  mutates `DB_TYPES`; this shared path must be included in a core import audit
  before moving packages out of mandatory dependencies.
- `asyncdb/drivers/dynamodb.py:30-48` imports `aiobotocore` and boto3 types in a
  guarded block and raises an actionable `DriverError` advising
  `asyncdb[boto3]`. It is an existing pattern for provider-local dependency
  checks, although the import is still performed when that driver module is
  selected.
- `asyncdb/drivers/iceberg.py:18-41` guards its optional PyIceberg, DuckDB,
  Arrow, pandas, and Polars imports and records availability. This is a useful
  pattern to evaluate for consistency, while noting that it imports all of
  those packages when the Iceberg module itself is selected.
- `asyncdb/drivers/cassandra.py:13-38` and `asyncdb/drivers/scylladb.py:11-57`
  import Cassandra/Scylla and pandas modules at module scope. These are examples
  of provider-local imports that can fail on an unsupported platform or missing
  extra without affecting other drivers.
- `asyncdb/drivers/sqlserver.py:7-10`, `mysqlclient.py:9-18`, and
  `odbc.py:9-18` import native database bindings at module scope. These should
  be classified and tested rather than assumed to be Windows-compatible.
- `.github/workflows/release.yml:7-35` runs a matrix containing
  `windows-latest`, sets `CIBW_ARCHS_WINDOWS: AMD64`, and uses the generic
  `CIBW_BUILD: cp310-* ... cp314-*`. Lines `37-55` validate only the compiled
  extension archive entry; lines `78-93` detect wheel families for deployment.
- `tests/test_dynamodb.py:1-47` demonstrates the existing test convention of
  skipping integration tests when DynamoDB Local is unavailable. A Windows
  smoke suite should remain independent of external database services.

### Research limitations and non-existent items

- The prescribed `sdd/templates/brainstorm.md` is absent from this checkout;
  this document follows the existing committed brainstorm structure and adds
  the required YAML frontmatter.
- `wikitoolkit` is not installed, so the required wiki query/page inspection
  could not be performed. No local wiki page was found in the repository tree.
- No central provider capability manifest or optional-dependency registry was
  found; the current architecture relies on dynamic module import and
  `pyproject.toml` extras.
- No Windows-specific package smoke-test workflow or provider compatibility
  matrix was found.

## 4. Options

### Option A — Provider-local lazy imports with explicit Windows wheel selectors

Move optional/native imports behind provider construction or operation paths,
keep only genuinely cross-platform runtime requirements in core metadata, and
make the release matrix express OS-specific `CIBW_BUILD` values. Add isolated
core import and wheel smoke tests plus focused missing-extra diagnostics.

**Pros**

- Smallest change aligned with the existing dynamic factory.
- Failures stay local to the requested provider.
- Does not require a new registration or packaging abstraction.
- Explicit CI selectors make the Windows build request auditable.

**Cons**

- Requires a careful audit of shared imports and transitive package usage.
- Provider support documentation can drift from metadata unless tested.
- Some large modules need internal import restructuring.

**Effort:** Medium

**Libraries / Tools:** Existing Python import machinery, setuptools optional
dependencies, cibuildwheel, and the current GitHub Actions upload-artifact flow.

**Existing code to reuse:** `asyncdb/utils/modules.py`, the guarded import
pattern in `asyncdb/drivers/iceberg.py:18-38`, the actionable error in
`asyncdb/drivers/dynamodb.py:40-48`, and `.github/workflows/release.yml`.

### Option B — Central provider manifest and lazy loader

Introduce a registry describing each provider’s module, extra name, supported
platforms, and optional dependencies. The factory consults the manifest before
importing the provider and reports structured compatibility errors. Packaging
metadata and CI selectors are updated from the same documented matrix.

**Pros**

- One discoverable source for provider capabilities and support claims.
- Better error messages and easier future platform reporting.
- Enables automated consistency checks between extras, providers, and CI.

**Cons**

- Adds a new abstraction to a simple dynamic import path.
- Requires maintaining provider metadata and avoiding circular imports.
- Higher migration and test cost than the current architecture needs.

**Effort:** High

**Libraries / Tools:** Existing `importlib`, setuptools metadata, and GitHub
Actions; no new runtime dependency is necessary.

**Existing code to reuse:** `AsyncDB`/`AsyncPool` factory contracts in
`asyncdb/connections.py:13-44` and `module_exists` in
`asyncdb/utils/modules.py:6-19`.

### Option C — Split the distribution into a minimal core and provider packages

Make the main distribution contain only the core framework and a small set of
portable drivers. Publish provider integrations as separately installable
packages or extras with independent compatibility and wheel policies.

**Pros**

- Strongest isolation of native and Linux-only dependencies.
- Smaller Windows install and faster core import.
- Provider release cadence and support can evolve independently.

**Cons**

- Breaks or complicates the current single-package driver discovery model.
- Requires package naming, versioning, documentation, and migration policy.
- Much larger release and testing surface.

**Effort:** High

**Libraries / Tools:** Existing setuptools packaging and cibuildwheel, plus
repository/package automation that does not currently exist.

**Existing code to reuse:** The driver module boundaries under
`asyncdb/drivers/` and factory convention in `asyncdb/connections.py`.

### Option D — Compatibility shim and platform-specific dependency lock sets

Keep most provider modules unchanged, but add a platform-aware dependency
selection layer and a Windows-only release environment that installs only a
curated portable set. Use import smoke tests to prevent core regressions.

**Pros**

- Lowest source churn in the short term.
- Can unblock wheel publication quickly.
- Useful as a temporary release safeguard while auditing providers.

**Cons**

- Does not truly fix eager imports when users select modules directly.
- Platform lock sets can diverge from package metadata.
- Leaves poor user experience and hidden compatibility debt.

**Effort:** Low–Medium

**Libraries / Tools:** Existing packaging metadata and cibuildwheel only.

**Existing code to reuse:** Current release workflow and `install_uvloop()`.

## 5. Recommendation

Recommend **Option A — Provider-local lazy imports with explicit Windows wheel
selectors**.

It addresses the actual failure boundary with the least architectural risk:
core import remains lightweight, provider dependencies remain opt-in, and the
release workflow directly states which ABI/tag combinations the Windows runner
must build. The existing factory already provides lazy module selection, and
the repository already contains guarded optional-import examples.

The accepted tradeoff is an audit across shared modules and provider imports.
That work is preferable to introducing a new registry or splitting the package
before the project has a verified provider compatibility matrix. Option D may
be used as a temporary CI hardening step, but it should not be the final design.

## 6. Feature Description

### User-facing behavior

Installing the core package on Windows must permit `import asyncdb` and factory
construction for drivers whose extras are installed. Installing a provider
extra should be the explicit action that brings in its implementation package.
If a requested provider is unavailable because its extra is missing or the
provider is unsupported on the platform, the error should identify the driver,
the required extra, and the compatibility boundary.

`uvloop` remains opt-in. If installed on a supported platform, the library may
activate it according to the documented policy; on Windows or when absent, the
standard asyncio event-loop policy remains active without an import failure.

### Internal behavior

The implementation should classify imports into core, provider-local optional,
and platform-specific groups. Core modules must not import provider modules or
optional output formats at module import time. Provider modules should load
their dependencies at the narrowest safe boundary and translate only genuine
missing-dependency failures into actionable driver errors.

The release workflow should use explicit OS-aware cibuildwheel selectors (or
equivalent matrix variables), build the supported CPython versions on
`windows-latest`, preserve the compiled extension check, and add a Windows
wheel smoke check that does not install the full optional dependency set.

### Edge cases and error handling

- A package may be importable while a native provider is not; provider failure
  must not poison `import asyncdb`.
- Import errors raised inside provider code must not be indiscriminately
  rewritten as “package missing” if they indicate a real provider bug.
- Wheel validation must accept `.pyd` on Windows and `.so` on Unix while
  checking the expected wheel tags.
- CI should fail if the Windows job produces no wheels, produces an unexpected
  ABI set, or publishes artifacts from the wrong platform.
- Tests must not require a running database service for the core Windows smoke
  path.

## 7. Capabilities

### New capabilities

- `windows-core-import`
- `provider-lazy-dependencies`
- `platform-aware-uvloop`
- `windows-wheel-build`
- `wheel-platform-smoke-test`

### Modified capabilities

- `dynamic-driver-loading`
- `optional-provider-installation`
- `release-artifact-publication`

## 8. Impact & Integration

| Component | Expected impact |
|---|---|
| `pyproject.toml` | Reclassify only dependencies proven optional/platform-specific; add environment markers or extras where justified; preserve existing package names. |
| `asyncdb/connections.py` | Remove or gate import-time uvloop activation; preserve factory signatures and error contract. |
| `asyncdb/utils/uv.py` | Make platform/availability policy explicit and testable. |
| `asyncdb/utils/modules.py` | Improve missing-provider diagnostics without hiding provider runtime errors. |
| `asyncdb/drivers/` | Audit and defer provider-local imports; prioritize modules importing native or Linux-sensitive packages. |
| `tests/` | Add import isolation, missing-extra, and Windows-compatible wheel smoke tests; keep integration tests service-gated. |
| `.github/workflows/release.yml` | Explicitly request Windows CPython wheels, validate tags and extension files, and retain artifact publication. |
| Documentation | Publish core Windows support and per-provider compatibility expectations. |

## 9. Open Questions

1. Which Python versions are release-supported for Windows: 3.10–3.14 as the
   current metadata/workflow suggests, or a narrower tested set? **Owner:**
   maintainers.
2. Which providers are in the initial Windows support matrix, and which should
   be marked unsupported despite installable wheels? **Owner:** maintainers.
3. Should `uvloop` be activated only through an explicit opt-in API/extra, or
   remain automatically activated when present on supported Unix platforms?
   **Owner:** maintainers.
4. Which current core dependencies (`asyncpg`, pandas, `python-magic`, NumPy
   transitive paths) are required by the public core import contract versus
   only by individual drivers? **Owner:** implementation/review.
5. Should release CI test only the core wheel, or a small portable-provider
   matrix as well? **Owner:** release maintainers.
6. Is the missing `sdd/templates/brainstorm.md` a repository defect that should
   be fixed in a separate process-maintenance task? **Owner:** SDD maintainers.

## 10. Parallelism Assessment

**Internal parallelism:** Mixed. Dependency classification and uvloop policy
can proceed alongside release workflow hardening. Provider import audits can be
split by driver families, but shared factory/import tests and `pyproject.toml`
must be coordinated.

**Cross-feature independence:** Moderate conflict risk. The work touches the
shared factory, dependency metadata, and release workflow. It should be checked
against any active release, packaging, or driver-migration work.

**Recommended isolation:** `mixed`.

**Rationale:** Separate worktrees are useful for the CI workflow and independent
provider audits, while a final integration task should own shared metadata,
factory behavior, compatibility tests, and documentation. The release workflow
must not be merged independently of the dependency/import contract it verifies.
