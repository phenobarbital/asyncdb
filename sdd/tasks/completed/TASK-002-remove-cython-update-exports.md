# TASK-002: Remove Cython Files and Update Build Config & Exports

**Feature**: exception-migration
**Spec**: sdd/specs/exception-migration.spec.md
**Status**: pending
**Priority**: high
**Depends-on**: TASK-001
**Assigned-to**: unassigned

## Context

With the pure Python exceptions module created in TASK-001, the Cython source files and build configuration are now redundant. This task removes them and updates the package exports.

## Scope

1. **Delete Cython files:**
   - `asyncdb/exceptions/exceptions.pyx`
   - `asyncdb/exceptions/exceptions.pxd`

2. **Remove from `setup.py`:**
   - Delete the `Extension(name='asyncdb.exceptions.exceptions', ...)` entry from the `extensions` list.

3. **Update `asyncdb/exceptions/__init__.py`:**
   - Add `AsyncDBException` to the imports and `__all__`.

4. **Verify no `cimport` references:**
   - Grep the entire codebase for `cimport.*exceptions` or `from asyncdb.exceptions cimport`.
   - If found, convert them to standard Python imports.

5. **Clean up compiled artifacts:**
   - Remove any `asyncdb/exceptions/exceptions.c` (generated C file).
   - Remove any `asyncdb/exceptions/exceptions.cpython-*.so` (compiled extension).

## Files to Create/Modify

- `asyncdb/exceptions/exceptions.pyx` — **delete**
- `asyncdb/exceptions/exceptions.pxd` — **delete**
- `asyncdb/exceptions/__init__.py` — **modify** (add AsyncDBException to imports/exports)
- `setup.py` — **modify** (remove exceptions Extension entry)

## Implementation Notes

- When editing `setup.py`, only remove the specific Extension entry for `asyncdb.exceptions.exceptions`. Do not touch other extensions.
- The `__init__.py` change is a single line addition to the import and one to `__all__`.
- After deletion, verify the pure Python module still imports correctly — the `.py` file takes precedence now that there's no `.so`.

## Reference Code

- Current `setup.py` extension entry (around line 11-16):
  ```python
  Extension(
      name='asyncdb.exceptions.exceptions',
      sources=['asyncdb/exceptions/exceptions.pyx'],
      extra_compile_args=COMPILE_ARGS,
      language="c"
  ),
  ```
- Current `__init__.py`: `asyncdb/exceptions/__init__.py`

## Acceptance Criteria

- [ ] `asyncdb/exceptions/exceptions.pyx` deleted.
- [ ] `asyncdb/exceptions/exceptions.pxd` deleted.
- [ ] `setup.py` no longer references `asyncdb.exceptions.exceptions`.
- [ ] `asyncdb/exceptions/__init__.py` exports `AsyncDBException`.
- [ ] No `cimport` references to exceptions remain in the codebase.
- [ ] No compiled `.c` or `.so` artifacts for exceptions remain.
- [ ] `from asyncdb.exceptions import AsyncDBException` works.
- [ ] `from asyncdb.exceptions import ProviderError, DriverError` still works.

## Test Specification

```python
import importlib

def test_import_asyncdb_exception():
    from asyncdb.exceptions import AsyncDBException
    assert AsyncDBException is not None

def test_all_exports_available():
    from asyncdb.exceptions import (
        AsyncDBException, ProviderError, DriverError, DataError,
        NotSupported, UninitializedError, ConnectionTimeout,
        ConnectionMissing, NoDataFound, TooManyConnections,
        EmptyStatement, UnknownPropertyError, StatementError,
        ConditionsError, ModelError,
    )

def test_no_cython_module():
    import asyncdb.exceptions.exceptions as mod
    # Should be a .py module, not a .so
    assert mod.__file__.endswith('.py')
```

## Output

When complete, the agent must:
1. Move this file to `sdd/tasks/completed/`
2. Update `sdd/tasks/.index.json` status to "done"
3. Add a brief completion note below

### Completion Note
Completed 2026-03-20. Deleted `exceptions.pyx` and `exceptions.pxd` via `git rm`.
Removed the `asyncdb.exceptions.exceptions` Extension entry from `setup.py`.
Added `AsyncDBException` to imports and `__all__` in `asyncdb/exceptions/__init__.py`.
No `cimport` references found in source code. Compiled `.c`/`.so` artifacts were
not present in the worktree (not git-tracked). All acceptance criteria met.
