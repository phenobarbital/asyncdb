# TASK-003: Write Unit Tests for Exception Hierarchy

**Feature**: exception-migration
**Spec**: sdd/specs/exception-migration.spec.md
**Status**: pending
**Priority**: medium
**Depends-on**: TASK-001, TASK-002
**Assigned-to**: unassigned

## Context

With the pure Python exceptions in place and Cython artifacts removed, this task adds comprehensive unit tests to validate the migration is correct and prevent regressions.

## Scope

Create a test file covering:

1. **Construction**: All 15 exception classes can be instantiated.
2. **Hierarchy**: `isinstance` and `issubclass` checks across the full tree.
3. **Base class behavior**: `__init__`, `__repr__`, `__str__`, `get()`, default values.
4. **NoDataFound special case**: default message and code=404.
5. **Kwargs handling**: kwargs are stored without shadowing `Exception.args`.
6. **Stacktrace**: Stored when passed as kwarg.
7. **Catching**: `try/except` with parent classes catches child exceptions.
8. **Raising**: All exceptions can be raised and caught normally.

## Files to Create/Modify

- `tests/test_exceptions.py` — **create** (new test file)

## Implementation Notes

- Use `pytest` (no `pytest-asyncio` needed — exceptions are synchronous).
- Use parametrize for repetitive checks across all 15 classes.
- Keep tests simple and focused — one assertion concept per test function.

## Reference Code

- Pure Python module: `asyncdb/exceptions/exceptions.py` (from TASK-001)
- Existing test patterns: check `tests/` directory for style conventions.

## Acceptance Criteria

- [ ] `tests/test_exceptions.py` exists.
- [ ] Tests cover all 15 exception classes.
- [ ] Tests verify the complete inheritance hierarchy.
- [ ] Tests verify `__repr__`, `__str__`, and `get()` output.
- [ ] Tests verify `NoDataFound` defaults (code=404, message="Data Not Found").
- [ ] Tests verify kwargs handling and `Exception.args` integrity.
- [ ] Tests verify raise/catch behavior with parent classes.
- [ ] All tests pass: `pytest tests/test_exceptions.py -v`.

## Test Specification

```python
import pytest
from asyncdb.exceptions import (
    AsyncDBException, ProviderError, DriverError, ModelError,
    ConnectionMissing, DataError, NotSupported, EmptyStatement,
    UninitializedError, ConnectionTimeout, NoDataFound,
    TooManyConnections, UnknownPropertyError, StatementError,
    ConditionsError,
)

ALL_EXCEPTIONS = [
    AsyncDBException, ProviderError, DriverError, ModelError,
    ConnectionMissing, DataError, NotSupported, EmptyStatement,
    UninitializedError, ConnectionTimeout, NoDataFound,
    TooManyConnections, UnknownPropertyError, StatementError,
    ConditionsError,
]

@pytest.mark.parametrize("exc_class", ALL_EXCEPTIONS)
def test_can_instantiate(exc_class):
    exc = exc_class("test")
    assert isinstance(exc, Exception)

@pytest.mark.parametrize("exc_class", ALL_EXCEPTIONS)
def test_can_raise_and_catch(exc_class):
    with pytest.raises(exc_class):
        raise exc_class("test")

def test_catch_child_with_parent():
    with pytest.raises(ProviderError):
        raise NoDataFound("missing")

    with pytest.raises(AsyncDBException):
        raise DriverError("fail")

def test_base_repr_str():
    exc = AsyncDBException("boom", code=500)
    assert str(exc) == "boom, code: 500"
    assert repr(exc) == "boom, code: 500"

def test_base_get():
    exc = AsyncDBException("hello")
    assert exc.get() == "hello"

def test_no_data_found_defaults():
    exc = NoDataFound()
    assert exc.code == 404
    assert "Data Not Found" in exc.message

def test_stacktrace_kwarg():
    exc = AsyncDBException("err", stacktrace="traceback here")
    assert exc.stacktrace == "traceback here"

def test_exception_args_tuple():
    exc = AsyncDBException("msg", "a", "b")
    assert isinstance(exc.args, tuple)
```

## Output

When complete, the agent must:
1. Move this file to `sdd/tasks/completed/`
2. Update `sdd/tasks/.index.json` status to "done"
3. Add a brief completion note below

### Completion Note
(Agent fills this in when done)
