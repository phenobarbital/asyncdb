# TASK-27: Add Core Windows Portability Tests

**Feature**: FEAT-5 — Windows Compatibility
**Spec**: `sdd/specs/windows-compatibility.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: TASK-23, TASK-24, TASK-25, TASK-26
**Assigned-to**: unassigned

---

## Context

The feature needs a durable regression suite proving that the core wheel can be
imported on Windows without all optional dependencies and that provider errors
remain localized. This task assembles the cross-module tests after the import,
metadata, and release contracts are implemented.

## Scope

- Add subprocess/import-isolation tests for the core package.
- Add tests for the approved core-wheel-only CI smoke path.
- Verify extension naming (`.pyd` on Windows, `.so` on Unix) and expected wheel
  tags using synthetic or CI-provided wheel archives.
- Keep database integration tests independent and service-gated.

**NOT in scope**: Provider implementation changes, broad end-to-end database
coverage, or publishing artifacts.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `tests/test_windows_compatibility.py` | CREATE | Core import, uvloop, and provider isolation regression tests. |
| `tests/test_release_wheel.py` | CREATE or MODIFY | Archive/tag smoke checks shared with release CI. |
| `tests/conftest.py` | MODIFY only if required | Reusable subprocess/wheel fixtures without affecting integration tests. |

## Codebase Contract (Anti-Hallucination)

### Verified Imports

```python
from asyncdb import AsyncDB, AsyncPool  # asyncdb/__init__.py:8
from asyncdb.utils import install_uvloop  # asyncdb/utils/__init__.py:2-7
from asyncdb.drivers.dynamodb import dynamodb, dynamodbPool  # tests/test_dynamodb.py:18
```

### Existing Signatures to Use

```python
# asyncdb/connections.py:20-25
class AsyncPool:
    def __new__(cls, driver: str = "dummy", **kwargs) -> AbstractDriver:
        ...

# asyncdb/connections.py:37-44
class AsyncDB:
    def __new__(cls, driver: str = "dummy", **kwargs) -> AbstractDriver:
        ...

# .github/workflows/release.yml:47-55
wheels = glob.glob("dist/*.whl")
```

### Does NOT Exist

- No Windows-specific import smoke test exists.
- No wheel tag/extension test module exists.
- No test fixture starts a database service for this feature’s core smoke path.

## Implementation Notes

### Pattern to Follow

Use subprocess isolation for package-import tests so the test process’s
installed optional packages cannot hide eager imports. Follow
`tests/test_dynamodb.py:33-47` for service availability gating only where an
integration test genuinely needs an external service.

### Key Constraints

- CI smoke tests cover the core wheel only, per the approved spec.
- Tests must run on Python 3.10–3.14 and Windows where applicable.
- Do not require DynamoDB Local, Cassandra, Redis, or other services.

### References in Codebase

- `tests/test_dynamodb.py:1-47` — skip/gating convention.
- `asyncdb/__init__.py:8` — public import surface.
- `.github/workflows/release.yml:37-55` — current archive check.

## Acceptance Criteria

- [ ] Core wheel import test passes with optional provider dependencies absent.
- [ ] uvloop absence and Windows behavior are covered.
- [ ] Wheel tests validate supported tags and compiled extension names.
- [ ] Tests do not require external database services.
- [ ] Focused portability tests pass on the supported matrix.

## Test Specification

```python
def test_core_wheel_imports_without_optional_dependencies():
    ...

def test_wheel_extension_matches_platform(wheel_path):
    ...

def test_supported_windows_tag(wheel_path):
    ...
```

## Completion Note

**Completed by**: sdd-worker (Claude Sonnet 5)
**Date**: 2026-09-06
**Notes**: Created `tests/test_windows_compatibility.py` assembling the
cross-module regression suite: (1) a subprocess-based core-import test that
blocks every optional/native provider module audited in TASK-24 (pymssql,
MySQLdb, aioodbc, pyodbc, cassandra, acsylla, uvloop) and confirms
`import asyncdb` plus loading the portable `dummy` driver still succeed in
a fresh interpreter; (2) two uvloop-policy regression tests reusing the
TASK-23 contract (safe no-op when absent, and Windows never attempts the
import while the standard asyncio policy stays usable); (3) a
factory-level isolation test proving a missing-dependency `DriverError` for
one provider (`sqlserver`) does not poison subsequent loading of the
portable `dummy` provider; (4) two wheel tag/extension tests
(`test_wheel_extension_matches_platform`, `test_supported_windows_tag`)
that import and reuse the pure-Python helpers and fixtures added in
`tests/test_release_wheel.py` (TASK-26) via `from .test_release_wheel import
...`, rather than re-implementing them. No changes were needed in
`tests/conftest.py` — none of the new tests require its Postgres-oriented
fixtures. All 6 new tests pass
(`pytest tests/test_windows_compatibility.py -q`); the full FEAT-5 test
set (uvloop, optional-drivers, package-metadata, release-wheel,
windows-compatibility — 25 tests) passes together, and `pytest tests/
--collect-only` still collects all 375 repository tests with no collection
errors. No external database service is required by any test added across
FEAT-5.
**Deviations from spec**: none.
