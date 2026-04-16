# TASK-021: DynamoDB Pool Driver

**Feature**: FEAT-004 — dynamodb-driver
**Status**: pending
**Priority**: medium
**Effort**: M
**Depends on**: [TASK-015]

---

## Objective

Implement `dynamodbPool` class extending `BasePool` with shared `aiobotocore`
session and HTTP connection pooling via `aiohttp.TCPConnector`.

## Acceptance Criteria

1. `dynamodbPool(BasePool)` class exists in `asyncdb/drivers/dynamodb.py`.
2. Constructor accepts same `params` dict as `dynamodb` driver plus
   `max_pool_connections` (default: 10).
3. `connect(**kwargs)` creates `aiobotocore.AioSession` with a shared
   `aiohttp.TCPConnector(limit=max_pool_connections)`.
4. `disconnect(timeout=5)` closes all clients and the connector.
5. `acquire()` creates a new `dynamodb` driver instance that shares the pool's
   session. Returns the driver instance.
6. `release(connection, timeout=10)` closes the individual client from `acquire()`.
7. `AsyncPool("dynamodb", params={...})` factory auto-discovery works
   (class named `dynamodbPool`).
8. Pool works with async context manager (`async with pool as p:`).
9. Multiple `acquire()`/`release()` cycles work correctly.
10. Error handling: connection failures → `ConnectionTimeout` or `DriverError`.
11. All type hints and Google-style docstrings present.

## Implementation Notes

- `BasePool.__init__` expects `dsn` param but DynamoDB doesn't use DSN.
  Pass `dsn=""` and ignore it. The `params` dict carries all config.
- Shared connector pattern:
  ```python
  import aiohttp
  from aiobotocore.session import AioSession
  from aiobotocore.config import AioConfig

  connector = aiohttp.TCPConnector(limit=max_pool_connections)
  config = AioConfig(connector_args={"connector": connector})
  self._session = AioSession()
  # Each acquire() creates a client from the shared session
  ```
- `acquire()` creates a `dynamodb` instance and manually sets its `_connection`:
  ```python
  driver = dynamodb(params=self._params)
  ctx = self._session.create_client("dynamodb", **self._client_kwargs)
  driver._client_ctx = ctx
  driver._connection = await ctx.__aenter__()
  driver._connected = True
  return driver
  ```
- `release()` closes the individual client:
  ```python
  await connection._client_ctx.__aexit__(None, None, None)
  connection._connected = False
  ```

## Codebase Contract

### Must Import

```python
from .base import BasePool
# Source: asyncdb/drivers/base.py:17-30
```

### Verified Signatures

**`BasePool.__init__`** — `asyncdb/drivers/base.py:23-30`:
```python
def __init__(self, dsn: Union[str, None] = None,
             loop: asyncio.AbstractEventLoop = None,
             params: Optional[Union[dict, None]] = None, **kwargs)
```

**`PoolBackend` abstract methods** — `asyncdb/interfaces/pool.py:11-65`:
- `async def connect(self) -> "PoolBackend"` (line 37)
- `async def disconnect(self, timeout: int = 5) -> None` (line 45)
- `async def acquire(self)` (line 54)
- `async def release(self, connection, timeout: int = 10) -> None` (line 61)

### Reference: `redisPool`

**`asyncdb/drivers/redis.py:27-59`** — pattern to follow:
- Inherits `BasePool`
- `connect()` creates pool, stores in `self._pool` / `self._connection`
- `acquire()` returns driver instance with shared connection
- `release()` releases individual connection

### Does NOT Exist

- No `dynamodbPool` class yet — this task creates it
- `BasePool` has `ConnectionDSNBackend` in MRO — `_dsn_template` must be set
  (can be empty string `""` since DynamoDB doesn't use DSN)
