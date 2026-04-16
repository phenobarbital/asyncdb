# TASK-018: DynamoDB PartiQL Support

**Feature**: FEAT-004 — dynamodb-driver
**Status**: pending
**Priority**: medium
**Effort**: S
**Depends on**: [TASK-017]

---

## Objective

Implement dedicated PartiQL methods: `partiql()` and `partiql_batch()`.
Ensure `execute()` correctly routes PartiQL statements.

## Acceptance Criteria

1. `partiql(statement, parameters=None, **kwargs)` calls
   `client.execute_statement(Statement=..., Parameters=...)`.
   Returns `list[dict]` of deserialized items.
2. `partiql()` serializes `parameters` list items via `TypeSerializer`.
3. `partiql()` handles pagination via `NextToken` if present.
4. `partiql_batch(statements, **kwargs)` calls `client.batch_execute_statement()`.
   Each statement is `{"Statement": str, "Parameters": [...]}`.
   Returns `list[dict]` of results.
5. `execute()` (from TASK-017) correctly detects and routes PartiQL — verify
   integration works end-to-end.
6. `execute_many()` routes list of string statements to `batch_execute_statement`.
7. All type hints and Google-style docstrings present.

## Implementation Notes

- PartiQL `execute_statement` returns `{"Items": [...], "NextToken": "..."}`.
- Parameters must be serialized individually:
  ```python
  serialized_params = [{"S": v} if isinstance(v, str) else self._serializer.serialize(v)
                       for v in parameters]
  ```
  Actually, use `TypeSerializer().serialize(v)` for each parameter value.
- `batch_execute_statement` accepts up to 25 statements per call — chunk if needed.

## Codebase Contract

### From TASK-015 / TASK-017

- `self._connection` — aiobotocore DynamoDB client
- `self._serialize(item)` / `self._deserialize(item)` — type marshalling
- `self._deserialize_items(items)` — batch deserialization
- `self._is_partiql(sentence)` — detection helper
- `self._chunk_items(items, chunk_size)` — chunking helper
- `execute()` already has PartiQL routing from TASK-017

### Does NOT Exist

- No `partiql()` or `partiql_batch()` methods yet — this task creates them
