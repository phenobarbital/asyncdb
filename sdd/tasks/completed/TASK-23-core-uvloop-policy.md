# TASK-23: Make Core Import and uvloop Policy Windows-Safe

**Feature**: FEAT-5 — Windows Compatibility
**Spec**: `sdd/specs/windows-compatibility.spec.md`
**Status**: pending
**Priority**: high
**Estimated effort**: M (2-4h)
**Depends-on**: none
**Assigned-to**: unassigned

---

## Context

The core factory currently imports and invokes `install_uvloop()` as a module
side effect. This task isolates that policy so `import asyncdb` remains safe on
Windows while preserving automatic uvloop activation when available, as
resolved in the approved spec.

## Scope

- Refactor the core import/activation boundary in `asyncdb/connections.py` and
  `asyncdb/utils/uv.py`.
- Preserve `AsyncDB`, `AsyncPool`, and `asyncdb` factory signatures and dynamic
  provider lookup.
- Ensure absent uvloop and Windows execution are non-fatal.
- Add focused unit tests for absent uvloop and Windows policy behavior.

**NOT in scope**: Provider-specific imports, dependency metadata changes, or
release workflow changes.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `asyncdb/connections.py` | MODIFY | Remove unsafe import-time behavior while preserving factory contracts. |
| `asyncdb/utils/uv.py` | MODIFY | Implement safe platform-aware uvloop activation. |
| `tests/test_uvloop.py` | CREATE | Test missing uvloop, supported-platform activation, and Windows behavior. |

## Codebase Contract (Anti-Hallucination)

### Verified Imports

```python
from .utils import install_uvloop  # asyncdb/connections.py:6
from asyncdb.utils import install_uvloop  # asyncdb/utils/__init__.py:2-7
```

### Existing Signatures to Use

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
```

### Does NOT Exist

- No platform-specific uvloop policy function exists beyond
  `install_uvloop()`.
- No public configuration object for event-loop policy exists.

## Implementation Notes

### Pattern to Follow

Keep optional imports inside the narrowest function boundary and preserve the
existing no-op behavior when uvloop cannot be imported. Use standard-library
platform detection and avoid adding a dependency.

### Key Constraints

- Automatic activation when uvloop is present on supported platforms is part of
  the approved spec.
- Windows must retain a usable standard asyncio event-loop policy.
- Do not change factory exception types or provider names.

### References in Codebase

- `asyncdb/connections.py:13-60` — factory contracts.
- `asyncdb/utils/uv.py:4-12` — current optional import behavior.

## Acceptance Criteria

- [ ] Core import does not fail when uvloop is absent.
- [ ] Windows policy does not attempt unsupported uvloop activation.
- [ ] uvloop activates automatically when present on supported platforms.
- [ ] Existing factory signatures and behavior remain compatible.
- [ ] Focused tests pass: `pytest tests/test_uvloop.py -q`.

## Test Specification

```python
def test_install_uvloop_is_noop_when_missing():
    ...

def test_install_uvloop_skips_windows():
    ...

def test_asyncdb_factories_keep_dynamic_lookup():
    ...
```

## Completion Note

**Completed by**: sdd-worker (Claude Sonnet 5)
**Date**: 2026-09-06
**Notes**: `install_uvloop()` now checks `sys.platform` and returns early
(no-op) on Windows before ever attempting to import `uvloop`, and wraps the
activation call in a defensive `except Exception` in addition to
`ImportError` so it can never raise during `import asyncdb`. The module-scope
call in `asyncdb/connections.py` is preserved (factory signatures unchanged)
with a clarifying comment. Added `tests/test_uvloop.py` with 4 tests covering:
missing uvloop, Windows skip behavior, automatic activation on a supported
platform when installed, and preserved dynamic driver lookup for
`AsyncDB`/`AsyncPool`/`asyncdb`. All 4 tests pass (`pytest tests/test_uvloop.py -q`).
**Deviations from spec**: none
