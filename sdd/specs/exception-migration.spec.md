# FEAT-001: exception-migration

**Title**: Migrate Exception Classes from Cython to Pure Python
**Status**: draft
**Created**: 2026-03-19
**Author**: Jesus Lara

---

## Problem Statement

The AsyncDB exception classes are currently implemented in Cython (`asyncdb/exceptions/exceptions.pyx` + `.pxd`), compiled via `setup.py` as a C extension. This introduces:

1. **Build complexity** — requires a C compiler toolchain and Cython at install time.
2. **Portability friction** — wheels must be built per platform; sdist installs can fail without build tools.
3. **Maintenance overhead** — `.pyx`/`.pxd` dual-file maintenance for classes that gain no measurable performance benefit from Cython (exception construction is never on a hot path).
4. **Developer experience** — IDE tooling, type checkers, and debuggers work poorly with Cython-defined exceptions.

Exception classes are simple Python objects with no compute-intensive logic — Cython adds no value here.

---

## Proposed Solution

Replace the Cython-based exception module (`exceptions.pyx` + `exceptions.pxd`) with a pure Python module (`exceptions.py`) that preserves the exact same class hierarchy, API, and public interface.

### Key Design Decisions

1. **Drop-in replacement**: The new `exceptions.py` must export the same 15 classes with identical names, inheritance, `__init__` signatures, and behavior.
2. **Remove from Cython build**: Delete the `Extension(name='asyncdb.exceptions.exceptions', ...)` entry from `setup.py`.
3. **Preserve `__init__.py`**: The public API in `asyncdb/exceptions/__init__.py` stays unchanged — downstream code importing from `asyncdb.exceptions` sees zero difference.
4. **Add `AsyncDBException` to exports**: Currently missing from `__all__` in `__init__.py`; fix this as part of the migration.
5. **Type annotations**: Add proper type hints to the base class attributes and `__init__` signatures.

---

## Architectural Design

### Current Architecture

```
asyncdb/exceptions/
├── __init__.py          # Re-exports from .exceptions (Cython module)
├── exceptions.pyx       # 15 exception classes (cdef class)
├── exceptions.pxd       # Cython declarations for cimport
└── handlers.py          # Async exception handlers (pure Python, unchanged)
```

`setup.py` compiles `exceptions.pyx` → `exceptions.cpython-*.so`.

### Target Architecture

```
asyncdb/exceptions/
├── __init__.py          # Re-exports from .exceptions (now pure Python)
├── exceptions.py        # 15 exception classes (plain Python)
└── handlers.py          # Async exception handlers (unchanged)
```

`setup.py` no longer lists the exceptions extension. The `.pyx` and `.pxd` files are deleted.

### Class Hierarchy (preserved exactly)

```
Exception
└── AsyncDBException (code, message, stacktrace, args)
    ├── ProviderError
    │   ├── UninitializedError
    │   ├── ConnectionTimeout
    │   ├── NoDataFound (code=404)
    │   ├── TooManyConnections
    │   ├── UnknownPropertyError
    │   ├── StatementError
    │   └── ConditionsError
    ├── DriverError
    ├── ModelError
    ├── ConnectionMissing
    ├── DataError
    ├── NotSupported
    └── EmptyStatement
```

---

## Scope

### In Scope

- Create `asyncdb/exceptions/exceptions.py` with all 15 classes.
- Delete `asyncdb/exceptions/exceptions.pyx` and `asyncdb/exceptions/exceptions.pxd`.
- Remove the `asyncdb.exceptions.exceptions` entry from the `extensions` list in `setup.py`.
- Add `AsyncDBException` to `__all__` in `asyncdb/exceptions/__init__.py`.
- Add type annotations to `AsyncDBException.__init__` and attributes.
- Verify no Cython `cimport` references to these exceptions exist elsewhere.
- Write unit tests for the exception hierarchy.

### Out of Scope

- Migrating other Cython modules (e.g., `asyncdb.utils.types`).
- Changing exception semantics or adding new exception classes.
- Modifying `handlers.py` (already pure Python).
- Changing how downstream drivers import/use these exceptions.

---

## Acceptance Criteria

- [ ] All 15 exception classes exist in pure Python with identical names and hierarchy.
- [ ] `AsyncDBException.__init__` accepts the same signature: `(message, *args, code=0, **kwargs)`.
- [ ] `NoDataFound` defaults to `code=404` with message `"Data Not Found"`.
- [ ] `__repr__` and `__str__` return `"{message}, code: {code}"`.
- [ ] `.get()` returns the message string.
- [ ] `asyncdb/exceptions/__init__.py` exports all 15 classes + `AsyncDBException`.
- [ ] `setup.py` no longer lists the exceptions Cython extension.
- [ ] `.pyx` and `.pxd` files are deleted.
- [ ] `isinstance()` checks work correctly across the hierarchy.
- [ ] All existing tests pass without modification.
- [ ] No remaining `cimport` references to the exceptions module.
- [ ] New unit tests cover construction, inheritance, `repr`/`str`, and `get()`.

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Other Cython modules `cimport` exceptions | Low | High | Grep for `cimport.*exceptions`; fix any found |
| Downstream code uses Cython-specific features | Low | Medium | All consumer code uses standard Python `from asyncdb.exceptions import X` |
| Behavioral difference in `self.args` | Medium | Low | Cython `cdef class` stores `self.args = kwargs`; Python `Exception` uses `self.args` as a tuple — need to use a different attribute name or handle carefully |
| Pickle/serialization compatibility | Low | Low | Exceptions are rarely serialized; test if needed |

---

## Worktree Strategy

- **Isolation unit**: `per-spec` (sequential tasks).
- **Rationale**: All tasks modify the same small module (`asyncdb/exceptions/`). No parallelism benefit.
- **Cross-feature dependencies**: None. This is a standalone refactor.

---

## Dependencies

- None. This feature has no external dependencies.

---

## Notes

- The `self.args` attribute in the current Cython code shadows `Exception.args` (which is a tuple of positional args). The pure Python version should rename this to `self.kwargs` or `self.extra` to avoid the conflict, while keeping backward compatibility via a property if needed.
- `handlers.py` is already pure Python and requires no changes.
