# TASK-016: DynamoDB CRUD Operations

**Feature**: FEAT-004 — dynamodb-driver
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: [TASK-015]

---

## Objective

Implement the Redis-style single-item CRUD methods: `get`, `set`, `delete`,
`write`, and `update` on the `dynamodb` driver class.

## Acceptance Criteria

1. `get(table, key, **kwargs)` calls `client.get_item()`, deserializes, returns `dict` or `None`.
2. `set(table, item, **kwargs)` serializes and calls `client.put_item()`, returns `True`.
3. `delete(table, key, **kwargs)` calls `client.delete_item()`, returns `True`.
4. `write(table, item, **kwargs)` is an alias for `set()`.
5. `update(table, key, update_expression, expression_values, **kwargs)` calls
   `client.update_item()`, returns updated attributes dict.
6. All methods use `self._default_table` when `table` is not provided (fallback
   from `use()` / constructor `params["table"]`).
7. All methods use `self._serialize()` / `self._deserialize()` for type conversion.
8. All methods pass through extra `**kwargs` to the DynamoDB API (e.g.,
   `ConditionExpression`, `ReturnValues`).
9. Error handling: `ClientError` → `DriverError` with descriptive message.
10. `fetch_one(table, key, **kwargs)` implemented as alias for `get()`.
11. All type hints and Google-style docstrings present.

## Implementation Notes

- `get_item` returns `{"Item": {...}}` — check for key existence, return `None` if missing.
- `put_item` requires `{"TableName": ..., "Item": serialized_item}`.
- `update_item` requires serializing `ExpressionAttributeValues` individually.
- Pass `kwargs` through to allow `ConditionExpression`, `ReturnValues`, etc.

## Codebase Contract

### Must Import (from TASK-015 scaffold)

```python
# Already in dynamodb.py from TASK-015:
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
```

### Verified Signatures (from TASK-015)

Methods added in TASK-015 that this task uses:
- `self._serialize(item: dict) -> dict`
- `self._deserialize(item: dict) -> dict`
- `self._connection` — the aiobotocore DynamoDB client
- `self._default_table` — default table name from `use()` or constructor

### Does NOT Exist

- No `get()`, `set()`, `delete()`, `write()`, `update()` methods yet — this task creates them
- No `fetch_one()` implementation yet — this task creates it
