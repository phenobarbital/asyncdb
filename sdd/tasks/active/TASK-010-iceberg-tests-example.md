# TASK-010: Tests & Example Script

**Feature**: FEAT-002 — apache-iceberg-support
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: [TASK-007, TASK-008, TASK-009]

---

## Objective

Create comprehensive tests and an example script demonstrating the Iceberg driver.

## Acceptance Criteria

1. `tests/test_iceberg.py` exists with pytest + pytest-asyncio tests covering:
   - Driver instantiation and factory auto-discovery.
   - Connection to a local SQL-backed catalog (SQLite, no external services).
   - Namespace CRUD (create, list, properties, drop).
   - Table lifecycle (create from PyArrow schema, load, exists, rename, drop).
   - Write data (append PyArrow Table, append Pandas DataFrame).
   - Overwrite data.
   - Upsert data with identifier fields.
   - Read data as PyArrow, Pandas, Polars.
   - Query via DuckDB SQL.
   - Metadata and snapshot history.
   - Error handling (non-existent table, invalid namespace).
   - Context manager usage.
2. `examples/test_iceberg.py` exists with a self-contained usage example showing:
   - Connecting to a local catalog.
   - Creating namespace and table.
   - Writing and reading data.
   - Querying via DuckDB.
   - Cleanup.
3. All tests pass with `pytest tests/test_iceberg.py -v`.
4. Tests use `tmp_path` fixture for isolated file-system catalogs.

## Implementation Notes

- Use SQLite-backed catalog for tests (no external infrastructure needed):
  ```python
  params = {
      "catalog_name": "test",
      "catalog_type": "sql",
      "catalog_properties": {
          "uri": f"sqlite:///{tmp_path}/catalog.db",
          "warehouse": str(tmp_path / "warehouse"),
      },
  }
  ```
- Follow existing test patterns in `tests/` directory.

## Files to Create

- `tests/test_iceberg.py`
- `examples/test_iceberg.py`

## Test Strategy

- Run `pytest tests/test_iceberg.py -v` — all tests must pass.
