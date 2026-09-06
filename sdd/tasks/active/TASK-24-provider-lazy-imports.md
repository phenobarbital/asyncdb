# TASK-24: Defer Optional Provider Imports

**Feature**: FEAT-5 — Windows Compatibility
**Spec**: `sdd/specs/windows-compatibility.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: L (4-8h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

Several driver modules import native or optional packages at module scope. The
factory should fail only when a selected provider cannot load, not because an
unrelated provider is unavailable. This task audits the default-provider scope
and defers imports at provider boundaries.

## Scope

- Audit and refactor module-scope optional/native imports for providers in the
  approved `default` support scope.
- Prioritize Cassandra/Scylla, SQL Server, MySQLdb, ODBC, and output/provider
  modules identified in the spec contract.
- Preserve provider APIs and translate genuine missing dependency failures into
  actionable provider/extra errors.
- Add tests proving unrelated missing dependencies do not poison core import.

**NOT in scope**: Removing dependencies from `pyproject.toml`, changing uvloop
activation, or modifying release CI.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `asyncdb/drivers/*.py` | MODIFY | Defer or guard audited optional/native imports. |
| `asyncdb/drivers/outputs/*.py` | MODIFY | Prevent unrelated optional output imports from breaking core paths. |
| `asyncdb/utils/modules.py` | MODIFY if required | Preserve dynamic loading while improving missing-provider diagnostics. |
| `tests/test_optional_drivers.py` | CREATE | Isolated import and missing-extra tests. |

## Codebase Contract (Anti-Hallucination)

### Verified Imports

```python
from asyncdb.utils.modules import module_exists  # asyncdb/utils/modules.py:2-19
from asyncdb.exceptions import DriverError  # asyncdb/connections.py:3
import pymssql  # asyncdb/drivers/sqlserver.py:7
import MySQLdb  # asyncdb/drivers/mysqlclient.py:9
import aioodbc  # asyncdb/drivers/odbc.py:9
import pyodbc  # asyncdb/drivers/odbc.py:11
import acsylla as c  # asyncdb/drivers/scylladb.py:15
from cassandra.cluster import Cluster  # asyncdb/drivers/cassandra.py:15
```

### Existing Signatures to Use

```python
# asyncdb/utils/modules.py:6-19
def module_exists(module_name, classpath):
    ...

# asyncdb/connections.py:31-44
class AsyncDB:
    def __new__(cls, driver: str = "dummy", **kwargs) -> AbstractDriver:
        ...
```

### Does NOT Exist

- No central provider capability or optional-dependency manifest exists.
- No shared helper currently maps a provider name to its extra name.
- No Windows provider compatibility matrix exists.

## Implementation Notes

### Pattern to Follow

Use the guarded import and availability-check pattern in
`asyncdb/drivers/iceberg.py:18-38`, and the actionable dependency error in
`asyncdb/drivers/dynamodb.py:40-48`. Do not catch every `ImportError` raised by
provider code if it represents a real implementation failure.

### Key Constraints

- Preserve direct imports of explicitly selected providers where possible.
- Keep provider names and factory behavior unchanged.
- Do not introduce a central registry in this task.

### References in Codebase

- `asyncdb/drivers/iceberg.py:18-63` — guarded optional imports.
- `asyncdb/drivers/dynamodb.py:40-48` — missing-extra error.
- `asyncdb/connections.py:13-60` — lazy factory selection.

## Acceptance Criteria

- [ ] Missing optional provider packages do not prevent `import asyncdb`.
- [ ] Selecting a missing provider reports its provider and installation extra.
- [ ] Existing provider behavior is unchanged when dependencies are installed.
- [ ] Genuine provider errors are not indiscriminately masked.
- [ ] Focused tests pass: `pytest tests/test_optional_drivers.py -q`.

## Test Specification

```python
def test_core_import_without_unrelated_provider_dependency():
    ...

def test_selected_provider_missing_extra_has_actionable_error():
    ...

def test_provider_runtime_import_error_is_not_rewritten():
    ...
```

## Completion Note

**Completed by**: unassigned
**Date**: YYYY-MM-DD
**Notes**: Pending implementation.
**Deviations from spec**: none
