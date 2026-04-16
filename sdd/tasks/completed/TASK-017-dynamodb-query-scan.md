# TASK-017: DynamoDB Query & Scan Operations

**Feature**: FEAT-004 — dynamodb-driver
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: [TASK-015]

---

## Objective

Implement `query()`, `queryrow()`, and `fetch_all()` (scan) methods with
auto-pagination support. Also implement the `execute()` and `execute_many()`
abstract methods.

## Acceptance Criteria

1. `query(table, **kwargs)` calls `client.query()` with auto-pagination via
   `LastEvaluatedKey`. Returns `list[dict]` of deserialized items.
2. `query()` passes through `KeyConditionExpression`, `FilterExpression`,
   `ExpressionAttributeValues`, `ExpressionAttributeNames`, `IndexName`, etc.
3. `queryrow(table, **kwargs)` calls `query()` with `Limit=1`, returns single `dict` or `None`.
4. `fetch_all(table, **kwargs)` calls `client.scan()` with auto-pagination.
   Returns `list[dict]` of all deserialized items.
5. `execute(sentence, *args, **kwargs)`:
   - If `sentence` is a string and `_is_partiql(sentence)` is True → route to PartiQL
     (call `client.execute_statement()`).
   - Otherwise, treat as `put_item` write operation.
6. `execute_many(sentences, *args)` calls `client.batch_execute_statement()` for
   PartiQL or iterates `put_item` for dicts.
7. `prepare(sentence)` raises `NotImplementedError` (DynamoDB has no prepared statements).
8. All results pass through `self._serializer` if configured (non-native output format).
9. All type hints and Google-style docstrings present.

## Implementation Notes

- Auto-pagination pattern:
  ```python
  items = []
  kwargs["TableName"] = table
  while True:
      response = await self._connection.query(**kwargs)  # or .scan()
      items.extend(self._deserialize_items(response.get("Items", [])))
      last_key = response.get("LastEvaluatedKey")
      if not last_key:
          break
      kwargs["ExclusiveStartKey"] = last_key
  ```
- Use `self._paginate_query()` helper from TASK-015 scaffold.
- `execute()` PartiQL detection: use `self._is_partiql(sentence)` from TASK-015.

## Codebase Contract

### Verified Abstract Methods (must satisfy)

**From `DatabaseBackend`** — `asyncdb/interfaces/database.py`:
- `async def query(self, sentence: Union[str, list], **kwargs) -> Optional[Sequence]` (line 98)
- `async def queryrow(self, sentence: Union[str, list]) -> Optional[Iterable]` (line 111)
- `async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]` (line 85)
- `async def execute_many(self, sentence: list, *args) -> Optional[Any]` (line 92)
- `async def prepare(self, sentence: Union[str, list]) -> Any` (line 123)
- `async def fetch_all(self, sentence: str, **kwargs) -> list[Sequence]` (line 135)

### From TASK-015 Scaffold

- `self._paginate_query(method, table, **kwargs)` — auto-pagination helper
- `self._is_partiql(sentence)` — PartiQL detection
- `self._deserialize_items(items)` — batch deserialization
- `self._connection` — aiobotocore DynamoDB client
- `self._serializer` — OutputFactory instance (from InitDriver)

### Does NOT Exist

- No `query()`, `queryrow()`, `fetch_all()`, `execute()`, `execute_many()` implementations yet
- DynamoDB has no `prepare()` concept — raise `NotImplementedError`
