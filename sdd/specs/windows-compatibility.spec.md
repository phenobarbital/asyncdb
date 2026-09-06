---
# SDD flow type and base branch (FEAT-145).
type: feature
base_branch: dev
---

# Feature Specification: Windows Compatibility

**Feature ID**: FEAT-5
**Date**: 2026-09-06
**Author**: Jesus Lara / Codex
**Status**: approved
**Target version**: TBD

---

## 1. Motivation & Business Requirements

### Problem Statement

AsyncDB cannot currently make a clear Windows compatibility promise for its
core package. `uvloop` is optional in project metadata, but
`asyncdb.connections` imports and invokes `install_uvloop()` during module
import. Other provider modules import optional or platform-sensitive packages
at module scope, so a consumer may be unable to import a driver factory or
discover a provider merely because an unrelated optional dependency is
unavailable on Windows.

The release workflow contains a Windows job, but the wheel request is expressed
as one generic `CIBW_BUILD` selector. The workflow does not explicitly prove
that the Windows runner requested the intended ABI/tag set, and validation does
not test installation or core import without Linux-only extras.

### Goals

- Make the core AsyncDB package importable on Windows without installing
  unrelated provider extras.
- Keep `uvloop` optional and make its absence or incompatibility non-fatal.
- Defer optional/native provider imports until the provider is selected or the
  relevant operation is used.
- Give missing provider dependencies an actionable, provider-specific error.
- Explicitly request Windows CPython wheels in release CI for the supported
  ABI set and publish them with the other platform artifacts.
- Preserve the existing `AsyncDB`, `AsyncPool`, and dynamic driver names.
- Retain and validate the compiled Cython extension in every release wheel.

### Non-Goals

- Guaranteeing that every optional database provider supports Windows.
- Splitting AsyncDB into multiple distributions.
- Replacing the existing dynamic provider factory with a central manifest.
- Adding new third-party runtime dependencies.
- Rewriting provider implementations beyond the import and compatibility
  boundaries required for this feature.

## 2. Architectural Design

### Overview

Use provider-local lazy imports and explicit OS-aware cibuildwheel selectors.
Core modules will contain only dependencies required by the core public import
contract. Provider-specific and platform-sensitive dependencies will remain in
extras and load at the narrowest safe boundary. The existing dynamic factory
will continue selecting a provider module only when the caller requests it.

`uvloop` will remain an opt-in extra. Its activation policy will be explicit:
the library must not attempt to install it on unsupported platforms, and the
standard asyncio policy must remain usable when the package is absent.

The release workflow will express platform-specific wheel selectors, validate
wheel tags and compiled extension contents, and run a lightweight import smoke
test without installing the complete optional dependency set.

### Component Diagram

```
AsyncDB / AsyncPool factory
          │
          ▼
   module_exists(loader) ───────► requested provider module
          │                                  │
          │                                  └─ lazy optional/native imports
          ▼
  core import contract                    provider-specific error

pyproject.toml extras ───────► platform-aware release matrix
                                      │
                                      ▼
                         Linux / Windows / macOS wheels
                                      │
                                      ▼
                           archive + import smoke checks
```

### Integration Points

| Existing Component | Integration Type | Notes |
|---|---|---|
| `asyncdb.connections.AsyncDB` | modifies import boundary | Preserve factory signature and provider lookup; remove import-time side effects unrelated to requested providers. |
| `asyncdb.connections.AsyncPool` | preserves contract | Continue resolving `<driver>Pool` through the requested provider module. |
| `asyncdb.utils.uv.install_uvloop` | modifies platform policy | Keep the local optional import but make activation safe and testable per platform. |
| `asyncdb.utils.modules.module_exists` | improves diagnostics | Preserve dynamic import behavior while distinguishing missing provider dependencies from provider runtime errors. |
| `pyproject.toml` | dependency metadata | Move only audited provider/platform-sensitive packages out of core dependencies; preserve named extras. |
| `.github/workflows/release.yml` | release integration | Request OS-specific CPython wheels and validate Windows artifacts before upload. |
| Cython `asyncdb/utils/types` extension | build contract | Accept `.pyd` on Windows and `.so` on Unix; every wheel must contain its compiled extension. |

### Data Models

No new runtime data model is required. The implementation may introduce test
fixtures or a small internal provider-dependency description if needed, but a
new central provider registry is outside the recommended design.

### New Public Interfaces

No new public interface is required. Existing factory and utility interfaces
remain stable. If maintainers choose explicit uvloop opt-in rather than
automatic activation on supported Unix platforms, that API decision must be
resolved before implementation and documented as a compatibility change.

## 3. Module Breakdown

### Module 1: Core Import and uvloop Policy

- **Path**: `asyncdb/connections.py`, `asyncdb/utils/uv.py`
- **Responsibility**: Prevent import-time platform failures and define safe
  optional uvloop activation while preserving factory behavior.
- **Depends on**: Existing `module_exists` loader and asyncio standard library.

### Module 2: Optional Provider Import Boundaries

- **Path**: `asyncdb/drivers/*.py`, prioritized by native/platform-sensitive
  imports.
- **Responsibility**: Defer or guard provider dependencies, classify supported
  versus unsupported extras, and emit actionable missing-extra errors.
- **Depends on**: Module 1 factory behavior; existing guarded-import pattern in
  `asyncdb/drivers/iceberg.py` and DynamoDB error handling.

### Module 3: Dependency Metadata and Compatibility Matrix

- **Path**: `pyproject.toml` and provider-support documentation.
- **Responsibility**: Keep core dependencies minimal and portable, retain
  provider extras, and document the initial Windows support boundary.
- **Depends on**: Results of the provider import audit and test matrix.

### Module 4: Windows Wheel Release Pipeline

- **Path**: `.github/workflows/release.yml`
- **Responsibility**: Use explicit OS-specific cibuildwheel selectors, build
  supported Windows CPython wheels, validate tags and extension contents, and
  publish artifacts.
- **Depends on**: Module 3 dependency metadata and Module 5 smoke tests.

### Module 5: Portability and Wheel Tests

- **Path**: `tests/` (new focused import/packaging tests; existing integration
  tests remain service-gated)
- **Responsibility**: Verify core imports without optional providers, provider
  failures are localized, uvloop behavior is safe, and wheel archives are
  valid for each platform.
- **Depends on**: Modules 1–4.

## 4. Test Specification

### Unit Tests

| Test | Module | Description |
|---|---|---|
| `test_core_import_without_uvloop` | Module 1 | Imports the core package with `uvloop` unavailable and confirms no import failure. |
| `test_uvloop_not_activated_on_windows` | Module 1 | Mocks a Windows platform and verifies the standard asyncio policy remains usable. |
| `test_requested_provider_missing_extra` | Module 2 | Selects a provider whose dependency is unavailable and checks for a focused driver/extra error. |
| `test_unrelated_provider_dependency_does_not_poison_factory` | Module 2 | Confirms a missing optional provider package does not prevent core import or another portable provider from loading. |
| `test_import_error_inside_provider_is_not_masked` | Module 2 | Ensures genuine provider initialization/import failures are not all rewritten as missing-dependency errors. |
| `test_dependency_groups_match_support_matrix` | Module 3 | Checks audited optional/platform-sensitive packages are not accidentally required by core metadata. |

### Integration Tests

| Test | Description |
|---|---|
| `test_windows_core_wheel_import` | Installs a built Windows wheel in an isolated environment and imports the core package without all extras. |
| `test_windows_wheel_contains_pyd_extension` | Verifies each Windows wheel contains `asyncdb/utils/types.*.pyd`. |
| `test_release_matrix_produces_expected_windows_tags` | Validates the Windows artifact set contains the supported CPython ABI/platform tags and no unsupported architecture. |
| `test_portable_provider_factory_on_windows` | Selects the initial supported portable provider set on Windows without external database services. |

### Test Data / Fixtures

- A subprocess-based import fixture with optional packages hidden or excluded
  from `sys.path`.
- A mocked `sys.platform`/platform detector for uvloop policy tests.
- Synthetic wheel archives or CI-produced wheel paths for tag and extension
  validation; no database service is required.
- A provider compatibility table defining the initial core and optional
  Windows support boundary.

## 5. Acceptance Criteria

- [ ] `import asyncdb` succeeds on Windows with no `uvloop` installation.
- [ ] Importing the core package does not import unrelated provider modules or
      provider-only optional/native dependencies.
- [ ] `uvloop` remains available through an optional extra and is never required
      for core import or Windows operation.
- [ ] On Windows, uvloop activation is skipped safely and standard asyncio
      remains functional.
- [ ] Each provider with an optional dependency has a focused failure message
      naming the provider and installation extra when its dependency is absent.
- [ ] Genuine provider import/runtime errors are not indiscriminately masked as
      missing optional dependencies.
- [ ] The audited core dependency list contains only dependencies required by
      the core public import contract and supported baseline behavior.
- [ ] Provider-specific and platform-sensitive packages remain installable via
      explicit extras, with the initial Windows support matrix documented.
- [ ] The release workflow explicitly requests the supported Windows CPython
      wheel selectors on `windows-latest`.
- [ ] Release CI produces at least one Windows wheel for every supported CPython
      ABI and the expected `win_amd64` platform tag.
- [ ] Every produced wheel contains the compiled `asyncdb/utils/types` module
      (`.pyd` on Windows, `.so` on Unix where applicable).
- [ ] Wheel/import smoke tests do not install the full optional dependency set
      and do not require external database services.
- [ ] Existing `AsyncDB`, `AsyncPool`, and driver names remain backwards
      compatible.
- [ ] Focused tests pass, and the release workflow’s YAML/configuration checks
      pass before publication.

## 6. Codebase Contract

> This section records verified current contracts. Implementation agents must
> re-check these paths if the base branch changes before task execution.

### Verified Imports

```python
from asyncdb import AsyncDB, AsyncPool  # asyncdb/__init__.py:8
from asyncdb.utils import install_uvloop  # asyncdb/utils/__init__.py:2-7
from asyncdb.utils.modules import module_exists  # asyncdb/utils/modules.py:2-19
from asyncdb.exceptions import DriverError  # asyncdb/connections.py:3
from asyncdb.drivers.base import InitDriver, BasePool  # asyncdb/drivers/base.py
```

Provider-local imports verified in their source files include:

```python
from aiobotocore.session import AioSession  # asyncdb/drivers/dynamodb.py:42
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer  # ...:43
import duckdb  # asyncdb/drivers/iceberg.py:19
from pyiceberg.catalog import load_catalog  # asyncdb/drivers/iceberg.py:23
from cassandra.cluster import Cluster  # asyncdb/drivers/cassandra.py:15
import acsylla as c  # asyncdb/drivers/scylladb.py:15
import pymssql  # asyncdb/drivers/sqlserver.py:7
import MySQLdb  # asyncdb/drivers/mysqlclient.py:9
import pyodbc  # asyncdb/drivers/odbc.py:11
```

### Existing Class and Function Signatures

```python
# asyncdb/connections.py:13-28
class AsyncPool:
    def __new__(cls, driver: str = "dummy", **kwargs) -> AbstractDriver:
        ...

# asyncdb/connections.py:31-44
class AsyncDB:
    def __new__(cls, driver: str = "dummy", **kwargs) -> AbstractDriver:
        ...

# asyncdb/connections.py:47-60
def asyncdb(driver: str = "pg", *args, **kwargs) -> T_aobj:
    ...

# asyncdb/utils/uv.py:4-12
def install_uvloop():
    ...

# asyncdb/utils/modules.py:6-19
def module_exists(module_name, classpath):
    ...
```

`AsyncDB` resolves `asyncdb.drivers.<driver>` and looks up the requested class
at `asyncdb/connections.py:37-41`. `AsyncPool` uses the same module path and
looks up `<driver>Pool` at `asyncdb/connections.py:20-25`.

### Integration Points

| New/Modified Component | Connects To | Via | Verified At |
|---|---|---|---|
| Core import policy | `install_uvloop` | import/call boundary | `asyncdb/connections.py:6-10`, `asyncdb/utils/uv.py:4-12` |
| Provider factory | selected driver class | `module_exists` | `asyncdb/connections.py:20-25`, `37-41`, `asyncdb/utils/modules.py:6-19` |
| Dependency diagnostics | provider module import | `DriverError` | `asyncdb/drivers/dynamodb.py:40-48` |
| Optional provider guard | PyIceberg family | `_PYICEBERG_AVAILABLE` and `_require_pyiceberg` pattern | `asyncdb/drivers/iceberg.py:18-38`, `62-63` |
| Release validation | compiled extension | wheel archive path match | `.github/workflows/release.yml:37-55` |

### Does NOT Exist

- `scripts.sdd` was added as SDD support during this workflow, but no runtime
  provider capability manifest exists.
- No central optional-dependency registry exists in `asyncdb/`.
- No Windows-specific provider compatibility matrix or wheel import smoke test
  exists in `tests/` or `.github/workflows/`.
- No guarantee exists that every provider extra is Windows-installable.

## 7. Implementation Notes & Constraints

### Patterns to Follow

- Preserve the dynamic module-selection pattern in
  `asyncdb/utils/modules.py:6-19`.
- Follow the guarded optional-import pattern in
  `asyncdb/drivers/iceberg.py:18-38`.
- Follow the actionable missing-extra error in
  `asyncdb/drivers/dynamodb.py:40-48`.
- Keep asynchronous behavior and avoid adding blocking work to async methods.
- Use existing setuptools extras and cibuildwheel; do not add a dependency
  without verifying its necessity and compatibility.
- Keep wheel inspection independent of dependency installation, as the current
  release workflow deliberately avoids installing `uvloop` on Windows
  (`.github/workflows/release.yml:37-42`).

### Known Risks / Gotchas

- Moving a package out of core dependencies can break shared modules such as
  `asyncdb/models/model.py:11-26`; import tests must cover the actual core path.
- Broad `except ImportError` blocks can hide real provider bugs. Catch missing
  dependency failures only at a deliberate boundary.
- `asyncdb/drivers/outputs/` contains module-scope pandas, Arrow, and Polars
  imports; output format loading must be included in the audit.
- Native providers such as Cassandra/Scylla, ODBC, MySQLdb, and pymssql may
  remain unsupported on Windows even after lazy loading.
- The current workflow uses `CIBW_BUILD` at `.github/workflows/release.yml:28`
  for all runners. OS-specific selectors must not accidentally omit Linux or
  macOS artifacts while fixing Windows.
- The existing extension check accepts `.so` and `.pyd` but does not verify
  wheel tags; tag validation must account for Windows `win_amd64` naming.

### External Dependencies

| Package | Version / Source | Reason |
|---|---|---|
| `uvloop` | `==0.21.0`, existing `uvloop` extra (`pyproject.toml:72-74`) | Optional Unix event-loop acceleration; never a Windows core requirement. |
| `cibuildwheel` | Existing CI tool (`.github/workflows/release.yml:21-35`) | Cross-platform wheel builds, including Windows. |
| `setuptools` | Existing build-system dependency (`pyproject.toml:1-7`) | Optional dependency metadata and wheel build. |
| Provider packages | Existing extras (`pyproject.toml:75-199`) | Must remain provider-local and be audited for platform support. |

## 8. Open Questions

- [x] Which Python versions are release-supported for Windows: 3.10–3.14 as
  the current metadata/workflow suggests, or a narrower tested set? — *Owner:
  maintainers*: from 3.10 to 3.14
- [x] Which providers are in the initial Windows support matrix, and which
  should be marked unsupported despite installable wheels? — *Owner:
  maintainers*: all providers under "default" extra package
- [x] Should `uvloop` be activated only through an explicit opt-in API/extra,
  or remain automatically activated when present on supported Unix platforms?
  — *Owner: maintainers*: automatically activated when present
- [x] Which current core dependencies (`asyncpg`, pandas, `python-magic`, NumPy
  transitive paths) are required by the public core import contract versus
  only by individual drivers? — *Owner: implementation/review*: core dependencies + default drivers
- [x] Should release CI test only the core wheel, or a small portable-provider
  matrix as well? — *Owner: release maintainers*: test only core wheel
- [x] Is the SDD tooling/template synchronization from `../ai-parrot` a
  repository maintenance dependency that should be kept in sync separately?
  — *Owner: SDD maintainers*: no

## Worktree Strategy

**Isolation:** mixed

Dependency classification, uvloop policy, release workflow hardening, and
provider-family audits can proceed independently. Shared changes to
`pyproject.toml`, the factory, compatibility tests, and documentation require
one integration task before merge. The release workflow must be validated
against the dependency/import contract it publishes.

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | 2026-09-06 | Jesus Lara / Codex | Initial draft from `windows-compatibility` brainstorm. |
