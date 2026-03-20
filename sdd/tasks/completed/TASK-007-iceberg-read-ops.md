# TASK-007: Read Operations & DuckDB Query Engine

**Feature**: FEAT-002 — apache-iceberg-support
**Status**: pending
**Priority**: high
**Effort**: L
**Depends on**: [TASK-006]

---

## Objective

Implement all read operations: scan, get, query (via DuckDB), queryrow, to_df, with multi-format output support (arrow, pandas, polars, duckdb).

## Acceptance Criteria

1. `scan(table_id, row_filter=None, selected_fields=None, snapshot_id=None)` returns a PyArrow Table via `table.scan().to_arrow()`.
2. `get(table_id, columns=None, row_filter=None, factory="arrow")` scans with column/row pruning and returns in requested format.
3. `query(sentence, table_id=None, factory="arrow", **kwargs)` executes SQL via DuckDB over the Iceberg table's Arrow dataset.
4. `queryrow(sentence, table_id=None, factory="arrow")` returns a single row.
5. `to_df(table_id, factory="pandas", **kwargs)` converts full table to DataFrame.
6. `fetch_all` is alias for `query`.
7. `fetch_one` is alias for `queryrow`.
8. Factory parameter supports: `"arrow"` (PyArrow Table), `"pandas"` (DataFrame), `"polars"` (Polars DataFrame), `"duckdb"` (DuckDB relation).
9. DuckDB query pattern: load table as Arrow dataset, register in in-memory DuckDB, execute SQL, return in factory format (same pattern as `delta.py`).
10. `snapshot_id` parameter on `scan()` enables reading from historical snapshots.
11. All methods async; blocking I/O wrapped in `asyncio.to_thread()`.

## Implementation Notes

- For DuckDB queries, follow the exact pattern in `delta.py:query()`: `duckdb.connect()` → `con.register(tablename, dataset)` → `con.execute(sentence)` → convert to factory format.
- PyIceberg scan API: `table.scan(row_filter=..., selected_fields=(...,)).to_arrow()`.
- For Polars output: `pl.from_arrow(arrow_table)`.
- For Pandas output: `arrow_table.to_pandas()`.

## Files to Modify

- `asyncdb/drivers/iceberg.py`

## Test Strategy

- Write test data, then scan with/without filters, query via DuckDB SQL, verify output formats.
