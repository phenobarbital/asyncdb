# TASK-004: Iceberg Driver Scaffold & Connection Lifecycle

**Feature**: FEAT-002 — apache-iceberg-support
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: []

---

## Objective

Create the `asyncdb/drivers/iceberg.py` file with the `iceberg` class extending `InitDriver`. Implement the constructor, `connection()`, `close()`, context manager (`__aenter__`/`__aexit__`), and the `pyproject.toml` optional dependency group.

## Acceptance Criteria

1. `asyncdb/drivers/iceberg.py` exists with class `iceberg(InitDriver)`.
2. `_provider = "iceberg"`, `_syntax = "nosql"`.
3. Constructor accepts `params` dict with `catalog_name`, `catalog_type`, `catalog_properties`, `namespace`, `storage_options`.
4. `connection()` calls `pyiceberg.catalog.load_catalog()` via `asyncio.to_thread()` and stores the catalog in `self._connection`.
5. `close()` releases the catalog reference and sets `_connected = False`.
6. `async with` context manager works (connect on enter, close on exit).
7. `AsyncDB("iceberg", params={...})` factory auto-discovers the driver.
8. `pyproject.toml` has `iceberg` optional dependency group with `pyiceberg[pyarrow,pandas,duckdb,polars,s3fs,gcsfs,sql-postgres,hive,ray]>=0.11.0`.
9. All PyIceberg exceptions are caught and wrapped in `DriverError`.

## Implementation Notes

- Follow the pattern in `asyncdb/drivers/delta.py` for `InitDriver` subclassing.
- Use `asyncio.to_thread()` for all blocking PyIceberg calls.
- Import pyiceberg lazily or guard with try/except for optional dependency.

## Files to Create/Modify

- **Create**: `asyncdb/drivers/iceberg.py`
- **Modify**: `pyproject.toml` (add `iceberg` extras group)

## Test Strategy

- Unit test: instantiate driver with mock params, verify attributes.
- Integration test: connect to a local SQLite-backed catalog (no external services needed).
