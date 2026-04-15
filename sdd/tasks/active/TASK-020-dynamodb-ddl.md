# TASK-020: DynamoDB DDL & Index Management

**Feature**: FEAT-004 — dynamodb-driver
**Status**: pending
**Priority**: medium
**Effort**: M
**Depends on**: [TASK-015]

---

## Objective

Implement table DDL operations (`create_table`, `delete_table`, `describe_table`,
`tables`, `table`, `wait_for_table`) and GSI/LSI index management
(`create_index`, `delete_index`, `list_indexes`).

## Acceptance Criteria

1. `create_table(table, key_schema, attribute_definitions, billing_mode="PAY_PER_REQUEST", **kwargs)`
   calls `client.create_table()`. Supports `GlobalSecondaryIndexes` and
   `LocalSecondaryIndexes` via kwargs. Returns table description dict.
2. `delete_table(table)` calls `client.delete_table()`. Returns `True`.
3. `describe_table(table)` calls `client.describe_table()`. Returns table metadata dict.
4. `tables(**kwargs)` calls `client.list_tables()` with auto-pagination via
   `ExclusiveStartTableName`. Returns `list[str]`.
5. `table(tablename)` is an alias for `describe_table()`.
6. `wait_for_table(table, status="ACTIVE", timeout=300)` polls `describe_table()`
   until `TableStatus == status` or timeout. Returns `True` or raises `DriverError`.
7. `create_index(table, index_name, key_schema, projection, **kwargs)` calls
   `client.update_table()` with `GlobalSecondaryIndexUpdates` Create action.
   Returns update response dict.
8. `delete_index(table, index_name)` calls `client.update_table()` with
   `GlobalSecondaryIndexUpdates` Delete action. Returns update response dict.
9. `list_indexes(table)` extracts GSI/LSI info from `describe_table()` response.
   Returns `list[dict]`.
10. Error handling: `ResourceNotFoundException` → `DriverError`, etc.
11. All type hints and Google-style docstrings present.

## Implementation Notes

- `create_table` minimal payload:
  ```python
  {
      "TableName": table,
      "KeySchema": key_schema,
      "AttributeDefinitions": attribute_definitions,
      "BillingMode": billing_mode,
  }
  ```
- `wait_for_table` polling loop:
  ```python
  import time
  start = time.monotonic()
  while time.monotonic() - start < timeout:
      desc = await self.describe_table(table)
      if desc.get("TableStatus") == status:
          return True
      await asyncio.sleep(2)
  raise DriverError(f"Table {table} did not reach {status} within {timeout}s")
  ```
- GSI create via `update_table`:
  ```python
  {"GlobalSecondaryIndexUpdates": [{"Create": {
      "IndexName": index_name,
      "KeySchema": key_schema,
      "Projection": projection,
  }}]}
  ```

## Codebase Contract

### From TASK-015

- `self._connection` — aiobotocore DynamoDB client
- `self._default_table` — fallback table name

### Verified Exceptions

- `DriverError` — `asyncdb/exceptions/exceptions.py:78`

### Does NOT Exist

- No DDL methods exist yet — this task creates them all
- No `column_info()` needed — DynamoDB is schemaless
