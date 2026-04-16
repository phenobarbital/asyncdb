# TASK-019: DynamoDB Batch Operations

**Feature**: FEAT-004 — dynamodb-driver
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: [TASK-016]

---

## Objective

Implement `write_batch()` and `get_batch()` methods with auto-chunking and
unprocessed-item retry handling.

## Acceptance Criteria

1. `write_batch(table, items, **kwargs)` calls `client.batch_write_item()`.
2. Auto-chunks items into groups of 25 (DynamoDB `BatchWriteItem` limit).
3. Handles `UnprocessedItems` in response — retries with exponential backoff
   (or simple retry loop, max 3 attempts).
4. Returns `True` when all items are written.
5. `get_batch(table, keys, **kwargs)` calls `client.batch_get_item()`.
6. Auto-chunks keys into groups of 100 (DynamoDB `BatchGetItem` limit).
7. Handles `UnprocessedKeys` with retry.
8. Returns `list[dict]` of deserialized items.
9. Both methods serialize/deserialize via `TypeSerializer`/`TypeDeserializer`.
10. Error handling: wrap `ClientError` → `DriverError`.
11. All type hints and Google-style docstrings present.

## Implementation Notes

- `batch_write_item` payload:
  ```python
  {"RequestItems": {table: [{"PutRequest": {"Item": serialized}} for ...]}}
  ```
- `batch_get_item` payload:
  ```python
  {"RequestItems": {table: {"Keys": [serialized_key for ...]}}}
  ```
- Use `self._chunk_items()` from TASK-015 scaffold.
- Simple retry for unprocessed items:
  ```python
  for attempt in range(max_retries):
      response = await self._connection.batch_write_item(...)
      unprocessed = response.get("UnprocessedItems", {})
      if not unprocessed:
          break
      await asyncio.sleep(2 ** attempt * 0.1)  # backoff
      request_items = unprocessed
  ```

## Codebase Contract

### From TASK-015 / TASK-016

- `self._connection` — aiobotocore DynamoDB client
- `self._serialize(item)` / `self._deserialize(item)` — type marshalling
- `self._deserialize_items(items)` — batch deserialization
- `self._chunk_items(items, chunk_size)` — chunking helper
- `self._default_table` — fallback table name

### Does NOT Exist

- No `write_batch()` or `get_batch()` methods yet — this task creates them
