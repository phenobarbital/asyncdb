# TASK-006: Table Lifecycle Operations

**Feature**: FEAT-002 — apache-iceberg-support
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: [TASK-005]

---

## Objective

Implement full table lifecycle management: create, load, register, check existence, rename, drop, list tables, and get table schema/metadata.

## Acceptance Criteria

1. `create_table(table_id, schema, partition_spec=None, **kwargs)` creates a table from a PyArrow schema or Iceberg Schema object.
2. `register_table(table_id, metadata_location)` registers an existing Iceberg table by metadata location.
3. `load_table(table_id)` loads and returns a table reference, stores it internally for subsequent operations.
4. `table_exists(table_id)` returns `True`/`False`.
5. `rename_table(from_id, to_id)` renames a table in the catalog.
6. `drop_table(table_id, purge=False)` drops the table (optionally purging data files).
7. `tables(namespace="")` lists table identifiers in the given or default namespace.
8. `table(tablename="")` returns table schema and metadata as a dict.
9. `schema(table_id)` returns the PyArrow schema of the table.
10. `metadata(table_id)` returns table metadata (properties, location, snapshots count).
11. All methods async, wrapped in `asyncio.to_thread()`.
12. `table_id` supports both `"namespace.table"` dot notation and tuple `("namespace", "table")`.

## Implementation Notes

- PyIceberg API: `catalog.create_table()`, `catalog.load_table()`, `catalog.register_table()`, `catalog.table_exists()`, `catalog.rename_table()`, `catalog.drop_table()`, `catalog.list_tables()`.
- For `create_table` with PyArrow schema, PyIceberg handles the conversion natively.
- Store the last loaded table in `self._current_table` for use by read/write methods.

## Files to Modify

- `asyncdb/drivers/iceberg.py`

## Test Strategy

- Create table with PyArrow schema, verify existence, load it, check schema, rename, drop.
