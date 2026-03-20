# TASK-013: Unit Tests for Refactored Delta Write Interface

**Feature**: FEAT-003 — migrate-delta-driver
**Spec**: sdd/specs/migrate-delta-driver.spec.md
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: TASK-011, TASK-012

---

## Objective

Create `tests/test_delta_write.py` with comprehensive tests for the refactored
`write()` and `create()` methods, covering all modes, schema evolution, configuration
passing, data type handling, and backward compatibility.

## Acceptance Criteria

1. Test file `tests/test_delta_write.py` exists and passes with `pytest`.
2. Tests cover all four write modes: `append`, `overwrite`, `ignore`, `error`.
3. Test verifies `schema_mode="merge"` allows adding new columns.
4. Test verifies `configuration` dict is forwarded (e.g., read back table properties).
5. Test verifies `if_exists` emits `DeprecationWarning` and still works.
6. Test verifies Pandas DataFrame input works.
7. Test verifies Polars DataFrame input works (converted to Arrow internally).
8. Test verifies PyArrow Table input works.
9. Test verifies `create()` with `schema_mode` and `configuration` works.
10. Test verifies `predicate` parameter for targeted overwrite.
11. All tests use `tmp_path` fixture for isolated Delta table directories.
12. Tests are async (`@pytest.mark.asyncio`).

## Implementation Notes

- Use `tmp_path` (pytest built-in) for all Delta table destinations.
- Create small DataFrames (3-5 rows) for fast test execution.
- For the deprecation warning test, use `pytest.warns(DeprecationWarning)`.
- For `schema_mode="merge"`, write a DataFrame with columns [a, b], then write
  another with [a, b, c] using merge — verify the resulting table has [a, b, c].
- For `predicate` test: write initial data, then overwrite with
  `mode="overwrite", predicate="id > 2"` — verify partial overwrite.
- For `configuration` test: write with `configuration={"delta.appendOnly": "true"}`,
  then read back `DeltaTable(path).metadata().configuration`.

## Files to Create/Modify

- **Create**: `tests/test_delta_write.py`

## Test Strategy

```bash
pytest tests/test_delta_write.py -v
```
