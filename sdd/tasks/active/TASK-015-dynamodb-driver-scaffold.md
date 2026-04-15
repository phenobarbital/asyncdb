# TASK-015: DynamoDB Driver Scaffold & Connection Lifecycle

**Feature**: FEAT-004 — dynamodb-driver
**Status**: pending
**Priority**: high
**Effort**: M
**Depends on**: []

---

## Objective

Create the `asyncdb/drivers/dynamodb.py` file with the `dynamodb` driver class
scaffold: imports, class definition, constructor, connection/close lifecycle,
async context manager, type marshalling utilities, and the `_KwargsTerminator` mixin.

## Acceptance Criteria

1. `asyncdb/drivers/dynamodb.py` exists with class `dynamodb(InitDriver, _KwargsTerminator)`.
2. `_provider = "dynamodb"`, `_syntax = "nosql"`.
3. Constructor accepts `params` dict with: `aws_access_key_id`, `aws_secret_access_key`,
   `region_name`, `endpoint_url`, `aws_session_token`, `profile_name`, `table`.
4. `connection()` creates `aiobotocore.AioSession` and `session.create_client("dynamodb", ...)`.
5. `close()` closes the client via `client.close()`.
6. `__aenter__`/`__aexit__` work (inherited + override if needed).
7. `_serialize(item)` converts Python dict → DynamoDB format via `TypeSerializer`.
8. `_deserialize(item)` converts DynamoDB item → Python dict via `TypeDeserializer`.
9. `_deserialize_items(items)` deserializes a list.
10. `_is_partiql(sentence)` detects SQL-like strings.
11. `_chunk_items(items, chunk_size)` splits lists into chunks.
12. `_paginate_query(method, table, **kwargs)` auto-paginates via `LastEvaluatedKey`.
13. `use(database)` sets default table name.
14. `is_closed()` and `is_connected()` work correctly.
15. `test_connection()` verifies connectivity (e.g., `list_tables` with limit=1).
16. Stub all abstract methods (`execute`, `query`, `queryrow`, `execute_many`,
    `prepare`, `fetch_all`, `fetch_one`) with `raise NotImplementedError`.
17. `AsyncDB("dynamodb", params={...})` factory auto-discovery works.
18. Graceful import handling: if `aiobotocore` not installed, raise `DriverError`.
19. All type hints and Google-style docstrings present.

## Implementation Notes

- Follow the iceberg driver pattern for `_KwargsTerminator` usage. Duplicate it
  in `dynamodb.py` (don't modify iceberg.py in this task).
- The `aiobotocore` client is an async context manager itself; store the client
  AND the context manager so `close()` can call `__aexit__` properly:
  ```python
  self._session = aiobotocore.AioSession()
  self._client_ctx = self._session.create_client("dynamodb", **client_kwargs)
  self._connection = await self._client_ctx.__aenter__()
  ```
- For `close()`:
  ```python
  await self._client_ctx.__aexit__(None, None, None)
  ```
- `TypeSerializer` and `TypeDeserializer` are imported from `boto3.dynamodb.types`.

## Codebase Contract

### Must Import

```python
from ..exceptions import ConnectionTimeout, DriverError
# Source: asyncdb/exceptions/exceptions.py:78 (DriverError), :106 (ConnectionTimeout)

from .base import InitDriver
# Source: asyncdb/drivers/base.py:33
```

### Verified Signatures

**`InitDriver.__init__`** — `asyncdb/drivers/base.py:43`:
```python
def __init__(self, loop: Union[asyncio.AbstractEventLoop, None] = None,
             params: Union[dict, None] = None, **kwargs)
```

**`OutputFactory`** — `asyncdb/drivers/outputs/output.py:8-29`:
```python
class OutputFactory:
    def __new__(cls, driver, frmt: str, *args, **kwargs)
```
- Called by `InitDriver.__init__` via `self.output_format("native")` (base.py:54)

### Does NOT Exist

- `asyncdb/drivers/dynamodb.py` — this task creates it
- No shared `_KwargsTerminator` in `base.py` — must be duplicated from `iceberg.py:44-59`
- No boto/aiobotocore import anywhere in existing drivers
- No AWS session helper or credential manager in asyncdb
