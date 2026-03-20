# TASK-001: Create Pure Python Exception Classes

**Feature**: exception-migration
**Spec**: sdd/specs/exception-migration.spec.md
**Status**: pending
**Priority**: high
**Depends-on**: none
**Assigned-to**: unassigned

## Context

The AsyncDB exception hierarchy is currently defined in Cython (`exceptions.pyx` / `exceptions.pxd`). This task creates the pure Python replacement module that preserves the exact same API and behavior.

## Scope

Create `asyncdb/exceptions/exceptions.py` containing all 15 exception classes with:

1. Identical class names and inheritance hierarchy.
2. `AsyncDBException` base class with:
   - `__init__(self, message, *args, code=0, **kwargs)` — same signature as Cython version.
   - Attributes: `code` (int), `message` (str), `stacktrace` (Optional[str]).
   - `__repr__` and `__str__` returning `"{message}, code: {code}"`.
   - `get()` method returning `self.message`.
   - **Critical**: The Cython version stores `self.args = kwargs`, which shadows `Exception.args`. In pure Python, store kwargs in `self.kwargs` instead to avoid breaking `Exception.args`. For backward compatibility, if any downstream code accesses `.args` expecting kwargs, add a note — but `Exception.args` semantics should be preserved.
3. `NoDataFound` with custom `__init__` defaulting to `code=404` and message `"Data Not Found"`.
4. All other subclasses as empty classes inheriting from the correct parent.
5. Proper type annotations on all `__init__` parameters and class attributes.

## Files to Create/Modify

- `asyncdb/exceptions/exceptions.py` — **create** (new pure Python module)

## Implementation Notes

- Use standard Python `class` definitions (no metaclasses needed).
- Add `from typing import Optional, Any` for type hints.
- Keep the copyright header: `# Copyright (C) 2018-present Jesus Lara`.
- Do NOT delete the `.pyx`/`.pxd` files yet — that's TASK-002.
- The module must be importable standalone: `from asyncdb.exceptions.exceptions import AsyncDBException`.

## Reference Code

- Current Cython implementation: `asyncdb/exceptions/exceptions.pyx` (lines 1-83)
- Current declarations: `asyncdb/exceptions/exceptions.pxd` (lines 1-49)

## Acceptance Criteria

- [ ] `asyncdb/exceptions/exceptions.py` exists with all 15 classes.
- [ ] Class hierarchy matches the Cython version exactly.
- [ ] `AsyncDBException.__init__` signature: `(message, *args, code=0, **kwargs)`.
- [ ] `NoDataFound()` defaults to code=404 and message "Data Not Found".
- [ ] `__repr__`, `__str__`, and `get()` behave identically to the Cython version.
- [ ] Type annotations on `AsyncDBException` attributes and `__init__`.
- [ ] `Exception.args` is NOT shadowed (kwargs stored separately).
- [ ] Module is importable: `python -c "from asyncdb.exceptions.exceptions import AsyncDBException"`.

## Test Specification

```python
from asyncdb.exceptions.exceptions import (
    AsyncDBException, ProviderError, DriverError, ModelError,
    ConnectionMissing, DataError, NotSupported, EmptyStatement,
    UninitializedError, ConnectionTimeout, NoDataFound,
    TooManyConnections, UnknownPropertyError, StatementError,
    ConditionsError,
)

def test_base_exception_init():
    exc = AsyncDBException("test error", code=42)
    assert exc.message == "test error"
    assert exc.code == 42
    assert str(exc) == "test error, code: 42"
    assert repr(exc) == "test error, code: 42"
    assert exc.get() == "test error"

def test_base_exception_defaults():
    exc = AsyncDBException("simple")
    assert exc.code == 0
    assert exc.stacktrace is None

def test_no_data_found_defaults():
    exc = NoDataFound()
    assert exc.code == 404
    assert exc.message == "Data Not Found"

def test_hierarchy():
    assert issubclass(ProviderError, AsyncDBException)
    assert issubclass(DriverError, AsyncDBException)
    assert issubclass(NoDataFound, ProviderError)
    assert issubclass(UninitializedError, ProviderError)
    assert issubclass(ConnectionTimeout, ProviderError)

def test_isinstance():
    exc = NoDataFound("missing")
    assert isinstance(exc, NoDataFound)
    assert isinstance(exc, ProviderError)
    assert isinstance(exc, AsyncDBException)
    assert isinstance(exc, Exception)

def test_exception_args_not_shadowed():
    exc = AsyncDBException("msg", "extra1", "extra2")
    assert isinstance(exc.args, tuple)
    assert exc.args[0] == "msg"
```

## Output

When complete, the agent must:
1. Move this file to `sdd/tasks/completed/`
2. Update `sdd/tasks/.index.json` status to "done"
3. Add a brief completion note below

### Completion Note
(Agent fills this in when done)
