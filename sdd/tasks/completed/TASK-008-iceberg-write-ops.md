# TASK-008: Write Operations (Append, Overwrite, Upsert, Add Files)

**Feature**: FEAT-002 — apache-iceberg-support
**Status**: pending
**Priority**: high
**Effort**: L
**Depends on**: [TASK-006]

---

## Objective

Implement all write operations: append, overwrite, partial overwrite, upsert, delete rows, and add existing Parquet files to a table.

## Acceptance Criteria

1. `write(data, table_id, mode="append", **kwargs)` appends or overwrites data.
   - Accepts PyArrow Table, Pandas DataFrame, or Polars DataFrame.
   - Converts Pandas/Polars to Arrow before writing.
   - `mode="append"` → `table.append(arrow_table)`.
   - `mode="overwrite"` → `table.overwrite(arrow_table)`.
2. `overwrite(data, table_id, overwrite_filter=None, **kwargs)` performs partial overwrite with a filter expression.
   - Uses `table.overwrite(arrow_table, overwrite_filter=filter_expr)`.
3. `upsert(data, table_id, join_cols=None, **kwargs)` merges data using identifier fields.
   - Uses PyIceberg's native `table.upsert(arrow_table)` (requires identifier_field_ids on schema).
   - Returns upsert result with `rows_updated` and `rows_inserted`.
   - If `join_cols` is provided, documents that identifier fields must be set on the Iceberg schema.
4. `add_files(table_id, file_paths, **kwargs)` registers existing Parquet files into the Iceberg table.
   - Uses `table.add_files(file_paths)`.
5. `delete(table_id, delete_filter, **kwargs)` deletes rows matching a filter expression.
   - Uses `table.delete(delete_filter)`.
6. All methods async; blocking I/O wrapped in `asyncio.to_thread()`.
7. Proper error handling with `DriverError` wrapping.

## Implementation Notes

- PyIceberg upsert (v0.11+): `table.upsert(df)` returns `UpsertResult(rows_updated, rows_inserted)`. Schema must have `identifier_field_ids`.
- For Polars → Arrow: `polars_df.to_arrow()`.
- For Pandas → Arrow: `pa.Table.from_pandas(pandas_df)`.
- `add_files` requires the Parquet files to be compatible with the table schema.

## Files to Modify

- `asyncdb/drivers/iceberg.py`

## Test Strategy

- Append data, verify row count. Overwrite, verify replacement. Upsert with identifier fields, verify updates and inserts. Add external Parquet file, verify data accessible. Delete with filter, verify removal.
